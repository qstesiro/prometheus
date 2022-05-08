// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// ExponentialBlockRanges returns the time ranges based on the stepSize.
// [2h, 6h, 18h, 48h] 以三倍增长 ???
func ExponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ {
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}

	return ranges
}

// Compactor provides compaction against an underlying storage
// of time series data.
type Compactor interface {
	// Plan returns a set of directories that can be compacted concurrently.
	// The directories can be overlapping.
	// Results returned when compactions are in progress are undefined.
	// 不能在压缩进行时同时执行 ???
	Plan(dir string) ([]string, error)

	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	Compact(dest string, dirs []string, open []*Block) (ulid.ULID, error)
}

// LeveledCompactor implements the Compactor interface.
// 实现了Compactor接口
type LeveledCompactor struct {
	metrics   *compactorMetrics // 指标信息
	logger    log.Logger
	ranges    []int64
	chunkPool chunkenc.Pool
	ctx       context.Context // 非正常用法 ???
}

type compactorMetrics struct {
	ran               prometheus.Counter
	populatingBlocks  prometheus.Gauge
	overlappingBlocks prometheus.Counter
	duration          prometheus.Histogram
	chunkSize         prometheus.Histogram
	chunkSamples      prometheus.Histogram
	chunkRange        prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.populatingBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_compaction_populating_block",
		Help: "Set to 1 when a block is currently being written to the disk.",
	})
	m.overlappingBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_vertical_compactions_total",
		Help: "Total number of compactions done on overlapping blocks.",
	})
	m.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	})
	m.chunkSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_size_bytes",
		Help:    "Final size of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(32, 1.5, 12),
	})
	m.chunkSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_samples",
		Help:    "Final number of samples on their first compaction",
		Buckets: prometheus.ExponentialBuckets(4, 1.5, 12),
	})
	m.chunkRange = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_range_seconds",
		Help:    "Final time range of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10),
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.populatingBlocks,
			m.overlappingBlocks,
			m.duration,
			m.chunkRange,
			m.chunkSamples,
			m.chunkSize,
		)
	}
	return m
}

// NewLeveledCompactor returns a LeveledCompactor.
func NewLeveledCompactor(ctx context.Context, r prometheus.Registerer, l log.Logger, ranges []int64, pool chunkenc.Pool) (*LeveledCompactor, error) {
	if len(ranges) == 0 {
		return nil, errors.Errorf("at least one range must be provided")
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	return &LeveledCompactor{
		ranges:    ranges,
		chunkPool: pool,
		logger:    l,
		metrics:   newCompactorMetrics(r),
		ctx:       ctx, // 非正常用法 ???
	}, nil
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

// Plan returns a list of compactable blocks in the provided directory.
func (c *LeveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}
	if len(dirs) < 1 {
		return nil, nil
	}

	var dms []dirMeta
	for _, dir := range dirs {
		meta, _, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

// 执行以步骤:
// - 按时间排序目录
// - 选择重叠目录(如果有则返回)
// - 选择可合并目录(如果有则返回)
// - 数据范围小于中间窗口数据大小且墓碑数大于序列数(有则返回)
// - 墓碑数据占总序列数比例大于5%(有则返回)
func (c *LeveledCompactor) plan(dms []dirMeta) ([]string, error) {
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})
	// 选择重叠数据
	res := c.selectOverlappingDirs(dms)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	// 剔除最近生成的数据
	dms = dms[:len(dms)-1]
	// 选择符合时间窗口大小的数据
	for _, dm := range c.selectDirs(dms) {
		res = append(res, dm.dir)
	}
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	for i := len(dms) - 1; i >= 0; i-- {
		meta := dms[i].meta
		if meta.MaxTime-meta.MinTime < c.ranges[len(c.ranges)/2] {
			// If the block is entirely deleted, then we don't care about the block being big enough.
			// TODO: This is assuming single tombstone is for distinct series, which might be no true.
			if meta.Stats.NumTombstones > 0 && meta.Stats.NumTombstones >= meta.Stats.NumSeries {
				return []string{dms[i].dir}, nil
			}
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []string{dms[i].dir}, nil
		}
	}

	return nil, nil
}

// selectDirs returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
// 每次选择从小到大的时间窗口进行筛选(找到则返回)
// 所以每次执行都是优先处理小时间窗口内的数据
func (c *LeveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}
	// 在调用前已经剔除了最近生成的block
	// 不明白此处为什么还要再剔除 ???
	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			continue
		}

	Outer:
		for _, p := range parts {
			// Do not select the range if it has a block whose compaction failed.
			for _, dm := range p {
				if dm.meta.Compaction.Failed {
					continue Outer
				}
			}

			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// selectOverlappingDirs returns all dirs with overlapping time ranges.
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func (c *LeveledCompactor) selectOverlappingDirs(ds []dirMeta) []string {
	if len(ds) < 2 {
		return nil
	}
	var overlappingDirs []string
	globalMaxt := ds[0].meta.MaxTime
	for i, d := range ds[1:] {
		if d.meta.MinTime < globalMaxt {
			if len(overlappingDirs) == 0 { // When it is the first overlap, need to add the last one as well.
				overlappingDirs = append(overlappingDirs, ds[i].dir)
			}
			overlappingDirs = append(overlappingDirs, d.dir)
		} else if len(overlappingDirs) > 0 {
			break
		}
		if d.meta.MaxTime > globalMaxt {
			globalMaxt = d.meta.MaxTime
		}
	}
	return overlappingDirs
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 {
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		for ; i < len(ds); i++ {
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if ds[i].meta.MaxTime > t0+tr {
				break
			}
			group = append(group, ds[i])
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group)
		}
	}

	return splitDirs
}

// CompactBlockMetas merges many block metas into one, combining it's source blocks together
// and adjusting compaction level.
func CompactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	res := &BlockMeta{
		ULID:    uid,
		MinTime: blocks[0].MinTime,
	}

	sources := map[ulid.ULID]struct{}{}
	// For overlapping blocks, the Maxt can be
	// in any block so we track it globally.
	maxt := int64(math.MinInt64)

	for _, b := range blocks {
		if b.MaxTime > maxt { // 记录最大时间
			maxt = b.MaxTime
		}
		if b.Compaction.Level > res.Compaction.Level { // 记录最大级别
			res.Compaction.Level = b.Compaction.Level
		}
		for _, s := range b.Compaction.Sources { // 记录源block
			sources[s] = struct{}{}
		}
		res.Compaction.Parents = append(res.Compaction.Parents, BlockDesc{ // 记录父block
			ULID:    b.ULID,
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
		})
	}
	res.Compaction.Level++ // 增大级别

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	res.MaxTime = maxt
	return res
}

// Compact creates a new block in the compactor's directory from the blocks in the
// provided directories.
func (c *LeveledCompactor) Compact(dest string, dirs []string, open []*Block) (uid ulid.ULID, err error) {
	var (
		blocks []BlockReader
		bs     []*Block
		metas  []*BlockMeta
		uids   []string
	)
	start := time.Now()

	for _, d := range dirs {
		meta, _, err := readMetaFile(d)
		if err != nil {
			return uid, err
		}

		var b *Block

		// Use already open blocks if we can, to avoid
		// having the index data in memory twice.
		for _, o := range open {
			if meta.ULID == o.Meta().ULID {
				b = o
				break
			}
		}

		if b == nil {
			var err error
			b, err = OpenBlock(c.logger, d, c.chunkPool)
			if err != nil {
				return uid, err
			}
			defer b.Close()
		}

		metas = append(metas, meta)
		blocks = append(blocks, b)
		bs = append(bs, b)
		uids = append(uids, meta.ULID.String())
	}

	uid = ulid.MustNew(ulid.Now(), rand.Reader)

	meta := CompactBlockMetas(uid, metas...)
	err = c.write(dest, meta, blocks...)
	if err == nil {
		if meta.Stats.NumSamples == 0 {
			for _, b := range bs {
				// 此处只设置可删除标记在reloadBlocks函数中会判定此标记并删除相应的block
				b.meta.Compaction.Deletable = true
				n, err := writeMetaFile(c.logger, b.dir, &b.meta)
				if err != nil {
					level.Error(c.logger).Log(
						"msg", "Failed to write 'Deletable' to meta file after compaction",
						"ulid", b.meta.ULID,
					)
				}
				b.numBytesMeta = n
			}
			uid = ulid.ULID{}
			level.Info(c.logger).Log(
				"msg", "compact blocks resulted in empty block",
				"count", len(blocks),
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		} else {
			level.Info(c.logger).Log(
				"msg", "compact blocks",
				"count", len(blocks),
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"ulid", meta.ULID,
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		}
		return uid, nil
	}

	errs := tsdb_errors.NewMulti(err)
	if err != context.Canceled {
		for _, b := range bs {
			if err := b.setCompactionFailed(); err != nil {
				errs.Add(errors.Wrapf(err, "setting compaction failed for block: %s", b.Dir()))
			}
		}
	}

	return uid, errs.Err()
}

func (c *LeveledCompactor) Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error) {
	start := time.Now()

	uid := ulid.MustNew(ulid.Now(), rand.Reader)

	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	if parent != nil {
		meta.Compaction.Parents = []BlockDesc{
			{
				ULID:    parent.ULID,
				MinTime: parent.MinTime,
				MaxTime: parent.MaxTime,
			},
		}
	}

	err := c.write(dest, meta, b)
	if err != nil {
		return uid, err
	}

	if meta.Stats.NumSamples == 0 {
		level.Info(c.logger).Log(
			"msg", "write block resulted in empty block",
			"mint", meta.MinTime,
			"maxt", meta.MaxTime,
			"duration", time.Since(start),
		)
		return ulid.ULID{}, nil
	}

	level.Info(c.logger).Log(
		"msg", "write block",
		"mint", meta.MinTime,
		"maxt", meta.MaxTime,
		"ulid", meta.ULID,
		"duration", time.Since(start),
	)
	return uid, nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

// write creates a new block that is the union of the provided blocks into dir.
func (c *LeveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + tmpForCreationBlockDirSuffix
	var closers []io.Closer
	defer func(t time.Time) {
		err = tsdb_errors.NewMulti(err, tsdb_errors.CloseAll(closers)).Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(c.logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw ChunkWriter
	// 对应chunks目录
	chunkw, err = chunks.NewWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        c.metrics.chunkSize,
			samples:     c.metrics.chunkSamples,
			trange:      c.metrics.chunkRange,
		}
	}
	// 对应index文件
	indexw, err := index.NewWriter(c.ctx, filepath.Join(tmp, indexFilename))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	closers = append(closers, indexw)
	// 填充数据
	if err := c.populateBlock(blocks, meta, indexw, chunkw); err != nil {
		return errors.Wrap(err, "populate block")
	}
	// 判定是否需要退出
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	errs := tsdb_errors.NewMulti()
	for _, w := range closers {
		errs.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if errs.Err() != nil {
		return errs.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}
	// 写入元数据
	if _, err = writeMetaFile(c.logger, tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	// Create an empty tombstones file.
	// 创建空的墓碑文件在populateBlock函数中写Block时已经进行了过滤
	// 墓碑文件开始时只是创建空文件占位
	// 在写入chunks目录中的chunk文件时会使用HeadRange读获取时已经过滤了需要删除的内容
	// 在进行多目录压缩时根据源目录中墓碑文件的内容进行过滤保证新创建的目标文件中只包含有效数据
	// 所以新创建目录中的墓碑文件也是空的
	// 墓碑文件只有在手工调用了删除api才会创建实际内容
	if _, err := tombstones.WriteFile(c.logger, tmp, tombstones.NewMemTombstones()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make it visible in destination dir by moving it from tmp one.
	if err := fileutil.Replace(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
// 认知复杂度过高了(不敢拆分一下大哥) ???
func (c *LeveledCompactor) populateBlock(blocks []BlockReader, meta *BlockMeta, indexw IndexWriter, chunkw ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		sets        []storage.ChunkSeriesSet
		symbols     index.StringIter
		closers     []io.Closer
		overlapping bool
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
		c.metrics.populatingBlocks.Set(0)
	}()
	c.metrics.populatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	for i, b := range blocks {
		// 是否需要退出
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}
		// 以下分支似乎没有什么作用其中的overlapping与globalMaxt没有实际的使用 ???
		if !overlapping {
			if i > 0 && b.Meta().MinTime < globalMaxt {
				c.metrics.overlappingBlocks.Inc()
				overlapping = true
				level.Info(c.logger).Log("msg", "Found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}
		// 索引读
		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)
		// chunk读
		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)
		// 墓碑读
		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %+v", b.Meta())
		}
		closers = append(closers, tombsr)
		// 序列处理
		k, v := index.AllPostingsKey()
		// 因为使用AllPostings所以实际返回所有序列id
		all, err := indexr.Postings(k, v)
		if err != nil {
			return err
		}
		// 按label.name排序后按label.value排序再按series.id排序
		// 返回index.ListPostings类型内部包含排序后series.id
		all = indexr.SortedPostings(all)
		// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
		sets = append(sets, newBlockChunkSeriesSet(indexr, chunkr, tombsr, all, meta.MinTime, meta.MaxTime-1))
		// 符号合并
		syms := indexr.Symbols()
		if i == 0 {
			symbols = syms
			continue
		}
		symbols = NewMergedStringIter(symbols, syms)
	}
	// 写符号
	// idx := 0 // for debug ???
	for symbols.Next() {
		// fmt.Printf("%d: %+v\n", idx, symbols.At()) // for debug ???
		// idx += 1
		if err := indexw.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if symbols.Err() != nil {
		return errors.Wrap(symbols.Err(), "next symbol")
	}

	var (
		ref  = uint64(0)
		chks []chunks.Meta
	)

	set := sets[0]
	if len(sets) > 1 {
		// Merge series using compacting chunk series merger.
		set = storage.NewMergeChunkSeriesSet(sets, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	}

	// Iterate over all sorted chunk series.
	// Next中使用墓碑进行了判定
	for set.Next() {
		// 是否需要退出
		// 多次出现可以封装成函数 ???
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}
		// 一次循环获取的元素与一个series对应
		s := set.At()
		chksIter := s.Iterator()
		chks = chks[:0]
		// Next中使用墓碑进行了判定
		for chksIter.Next() {
			// We are not iterating in streaming way over chunk as it's more efficient to do bulk write for index and
			// chunk file purposes.
			chks = append(chks, chksIter.At())
		}
		if chksIter.Err() != nil {
			return errors.Wrap(chksIter.Err(), "chunk iter")
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			continue
		}
		// 写入Chunk数据
		if err := chunkw.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		// 添加序列
		if err := indexw.AddSeries(ref, s.Labels(), chks...); err != nil {
			return errors.Wrap(err, "add series")
		}
		// 更新元数据
		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++
		for _, chk := range chks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}
		// 释放chunk
		for _, chk := range chks {
			if err := c.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}
		ref++
	}
	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	return nil
}
