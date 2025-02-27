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

// Package tsdb implements a time series storage for float64 sample data.
package tsdb

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	_ "github.com/prometheus/prometheus/tsdb/goversion" // Load the package into main to make sure minium Go version is met.
	"github.com/prometheus/prometheus/tsdb/wal"
)

const (
	// Default duration of a block in milliseconds.
	// ??? 2h 毫秒为单位
	DefaultBlockDuration = int64(2 * time.Hour / time.Millisecond)
	// ??? 测试
	// DefaultBlockDuration = int64(10 * time.Second / time.Millisecond)

	// Block dir suffixes to make deletion and creation operations atomic.
	// We decided to do suffixes instead of creating meta.json as last (or delete as first) one,
	// because in error case you still can recover meta.json from the block content within local TSDB dir.
	// TODO(bwplotka): TSDB can end up with various .tmp files (e.g meta.json.tmp, WAL or segment tmp file. Think
	// about removing those too on start to save space. Currently only blocks tmp dirs are removed.
	tmpForDeletionBlockDirSuffix = ".tmp-for-deletion"
	tmpForCreationBlockDirSuffix = ".tmp-for-creation"
	// Pre-2.21 tmp dir suffix, used in clean-up functions.
	tmpLegacy = ".tmp"
)

var (
	// ErrNotReady is returned if the underlying storage is not ready yet.
	ErrNotReady = errors.New("TSDB not ready")
)

// DefaultOptions used for the DB. They are sane for setups using
// millisecond precision timestamps.
func DefaultOptions() *Options {
	return &Options{
		WALSegmentSize:            wal.DefaultSegmentSize,                        // ??? 128M
		RetentionDuration:         int64(15 * 24 * time.Hour / time.Millisecond), // ??? 15d 毫秒为单位
		MinBlockDuration:          DefaultBlockDuration,                          // ??? 2h 毫秒为单位
		MaxBlockDuration:          DefaultBlockDuration,                          // ??? 2h 毫秒为单位
		NoLockfile:                false,
		AllowOverlappingBlocks:    false,
		WALCompression:            false,
		StripeSize:                DefaultStripeSize,             // ??? 16K
		HeadChunksWriteBufferSize: chunks.DefaultWriteBufferSize, // ??? 4M
	}
}

// Options of the DB storage.
type Options struct {
	// Segments (wal files) max size.
	// WALSegmentSize = 0, segment size is default size.
	// WALSegmentSize > 0, segment size is WALSegmentSize.
	// WALSegmentSize < 0, wal is disabled.
	WALSegmentSize int

	// Duration of persisted data to keep.
	// Unit agnostic as long as unit is consistent with MinBlockDuration and MaxBlockDuration.
	// Typically it is in milliseconds.
	RetentionDuration int64

	// Maximum number of bytes in blocks to be retained.
	// 0 or less means disabled.
	// NOTE: For proper storage calculations need to consider
	// the size of the WAL folder which is not added when calculating
	// the current size of the database.
	MaxBytes int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool

	// Overlapping blocks are allowed if AllowOverlappingBlocks is true.
	// This in-turn enables vertical compaction and vertical query merge.
	AllowOverlappingBlocks bool

	// WALCompression will turn on Snappy compression for records on the WAL.
	WALCompression bool

	// StripeSize is the size in entries of the series hash map. Reducing the size will save memory but impact performance.
	StripeSize int

	// The timestamp range of head blocks after which they get persisted.
	// It's the minimum duration of any persisted block.
	// Unit agnostic as long as unit is consistent with RetentionDuration and MaxBlockDuration.
	// Typically it is in milliseconds.
	MinBlockDuration int64

	// The maximum timestamp range of compacted blocks.
	// Unit agnostic as long as unit is consistent with MinBlockDuration and RetentionDuration.
	// Typically it is in milliseconds.
	MaxBlockDuration int64 // ??? 实际传入 36h

	// HeadChunksWriteBufferSize configures the write buffer size used by the head chunks mapper.
	HeadChunksWriteBufferSize int

	// SeriesLifecycleCallback specifies a list of callbacks that will be called during a lifecycle of a series.
	// It is always a no-op in Prometheus and mainly meant for external users who import TSDB.
	SeriesLifecycleCallback SeriesLifecycleCallback

	// BlocksToDelete is a function which returns the blocks which can be deleted.
	// It is always the default time and size based retention in Prometheus and
	// mainly meant for external users who import TSDB.
	BlocksToDelete BlocksToDeleteFunc
}

type BlocksToDeleteFunc func(blocks []*Block) map[ulid.ULID]struct{}

// 实现了storage.Storage接口
// DB handles reads and writes of time series falling into
// a hashed partition of a seriedb.
type DB struct {
	dir   string
	lockf fileutil.Releaser

	logger         log.Logger
	metrics        *dbMetrics
	opts           *Options
	chunkPool      chunkenc.Pool
	compactor      Compactor // NewLeveledCompactor实例
	blocksToDelete BlocksToDeleteFunc

	// Mutex for that must be held when modifying the general block layout.
	// 查询与写操作互斥
	// 查询操作(使用读锁)
	// - Querier
	// - ChunkQuerier
	// - StartTime
	// - Blocks
	// - Snapshot
	// - newDBMetrics
	// 写操作(使用写锁)
	// - reloadBlocks
	// - Delete
	// - CleanTombstones
	// - Close
	mtx    sync.RWMutex // 读写锁
	blocks []*Block

	head *Head

	compactc chan struct{}
	donec    chan struct{}
	stopc    chan struct{}

	// cmtx ensures that compactions and deletions don't run simultaneously.
	// 写操作之间互斥
	// - Delete(删除序列)
	// - CleanTombstones
	// - CompactHead
	// - Compact
	// - Snapshot
	// - run函数中执行reloadBlocks前
	cmtx sync.Mutex

	// autoCompactMtx ensures that no compaction gets triggered while
	// changing the autoCompact var.
	// 保护autoCompact并发(但是为什么不使用go.uber.org/atomic库) ???
	autoCompactMtx sync.Mutex
	autoCompact    bool

	// Cancel a running compaction when a shutdown is initiated.
	compactCancel context.CancelFunc
}

// 学习指标好样例 ???
type dbMetrics struct {
	loadedBlocks         prometheus.GaugeFunc
	symbolTableSize      prometheus.GaugeFunc
	reloads              prometheus.Counter
	reloadsFailed        prometheus.Counter
	compactionsFailed    prometheus.Counter
	compactionsTriggered prometheus.Counter
	compactionsSkipped   prometheus.Counter
	sizeRetentionCount   prometheus.Counter
	timeRetentionCount   prometheus.Counter
	startTime            prometheus.GaugeFunc
	tombCleanTimer       prometheus.Histogram
	blocksBytes          prometheus.Gauge
	maxBytes             prometheus.Gauge
}

func newDBMetrics(db *DB, r prometheus.Registerer) *dbMetrics {
	m := &dbMetrics{}

	m.loadedBlocks = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_blocks_loaded",
		Help: "Number of currently loaded data blocks",
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		return float64(len(db.blocks))
	})
	m.symbolTableSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_symbol_table_size_bytes",
		Help: "Size of symbol table in memory for loaded blocks",
	}, func() float64 {
		db.mtx.RLock()
		blocks := db.blocks[:]
		db.mtx.RUnlock()
		symTblSize := uint64(0)
		for _, b := range blocks {
			symTblSize += b.GetSymbolTableSize()
		}
		return float64(symTblSize)
	})
	m.reloads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_reloads_total",
		Help: "Number of times the database reloaded block data from disk.",
	})
	m.reloadsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_reloads_failures_total",
		Help: "Number of times the database failed to reloadBlocks block data from disk.",
	})
	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_triggered_total",
		Help: "Total number of triggered compactions for the partition.",
	})
	m.compactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_failed_total",
		Help: "Total number of compactions that failed for the partition.",
	})
	m.timeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_time_retentions_total",
		Help: "The number of times that blocks were deleted because the maximum time limit was exceeded.",
	})
	m.compactionsSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_skipped_total",
		Help: "Total number of skipped compactions due to disabled auto compaction.",
	})
	m.startTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_lowest_timestamp",
		Help: "Lowest timestamp value stored in the database. The unit is decided by the library consumer.",
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		if len(db.blocks) == 0 {
			return float64(db.head.MinTime())
		}
		return float64(db.blocks[0].meta.MinTime)
	})
	m.tombCleanTimer = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "prometheus_tsdb_tombstone_cleanup_seconds",
		Help: "The time taken to recompact blocks to remove tombstones.",
	})
	m.blocksBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_storage_blocks_bytes",
		Help: "The number of bytes that are currently used for local storage by all blocks.",
	})
	m.maxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_retention_limit_bytes",
		Help: "Max number of bytes to be retained in the tsdb blocks, configured 0 means disabled",
	})
	m.sizeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_size_retentions_total",
		Help: "The number of times that blocks were deleted because the maximum number of bytes was exceeded.",
	})

	if r != nil {
		r.MustRegister(
			m.loadedBlocks,
			m.symbolTableSize,
			m.reloads,
			m.reloadsFailed,
			m.compactionsFailed,
			m.compactionsTriggered,
			m.compactionsSkipped,
			m.sizeRetentionCount,
			m.timeRetentionCount,
			m.startTime,
			m.tombCleanTimer,
			m.blocksBytes,
			m.maxBytes,
		)
	}
	return m
}

// ErrClosed is returned when the db is closed.
var ErrClosed = errors.New("db already closed")

// DBReadOnly provides APIs for read only operations on a database.
// Current implementation doesn't support concurrency so
// all API calls should happen in the same go routine.
// 提供给promtool使用
// 与可读写数据同时运行时存在资源竞争所以最好是在promethus停止后再进行promtool
type DBReadOnly struct {
	logger  log.Logger
	dir     string
	closers []io.Closer
	closed  chan struct{}
}

// OpenDBReadOnly opens DB in the given directory for read only operations.
func OpenDBReadOnly(dir string, l log.Logger) (*DBReadOnly, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, errors.Wrap(err, "opening the db dir")
	}

	if l == nil {
		l = log.NewNopLogger()
	}

	return &DBReadOnly{
		logger: l,
		dir:    dir,
		closed: make(chan struct{}),
	}, nil
}

// FlushWAL creates a new block containing all data that's currently in the memory buffer/WAL.
// Samples that are in existing blocks will not be written to the new block.
// Note that if the read only database is running concurrently with a
// writable database then writing the WAL to the database directory can race.
func (db *DBReadOnly) FlushWAL(dir string) (returnErr error) {
	blockReaders, err := db.Blocks()
	if err != nil {
		return errors.Wrap(err, "read blocks")
	}
	maxBlockTime := int64(math.MinInt64)
	if len(blockReaders) > 0 {
		maxBlockTime = blockReaders[len(blockReaders)-1].Meta().MaxTime
	}
	w, err := wal.Open(db.logger, filepath.Join(db.dir, "wal"))
	if err != nil {
		return err
	}
	opts := DefaultHeadOptions()
	opts.ChunkDirRoot = db.dir
	head, err := NewHead(nil, db.logger, w, opts)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = tsdb_errors.NewMulti(
			returnErr,
			errors.Wrap(head.Close(), "closing Head"),
		).Err()
	}()
	// Set the min valid time for the ingested wal samples
	// to be no lower than the maxt of the last block.
	if err := head.Init(maxBlockTime); err != nil {
		return errors.Wrap(err, "read WAL")
	}
	mint := head.MinTime()
	maxt := head.MaxTime()
	rh := NewRangeHead(head, mint, maxt)
	compactor, err := NewLeveledCompactor(
		context.Background(),
		nil,
		db.logger,
		ExponentialBlockRanges(DefaultOptions().MinBlockDuration, 3, 5),
		chunkenc.NewPool(),
	)
	if err != nil {
		return errors.Wrap(err, "create leveled compactor")
	}
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	_, err = compactor.Write(dir, rh, mint, maxt+1, nil)
	return errors.Wrap(err, "writing WAL")
}

func (db *DBReadOnly) loadDataAsQueryable(maxt int64) (storage.SampleAndChunkQueryable, error) {
	select {
	case <-db.closed:
		return nil, ErrClosed
	default:
	}
	blockReaders, err := db.Blocks()
	if err != nil {
		return nil, err
	}
	blocks := make([]*Block, len(blockReaders))
	for i, b := range blockReaders {
		b, ok := b.(*Block)
		if !ok {
			return nil, errors.New("unable to convert a read only block to a normal block")
		}
		blocks[i] = b
	}

	opts := DefaultHeadOptions()
	opts.ChunkDirRoot = db.dir
	head, err := NewHead(nil, db.logger, nil, opts)
	if err != nil {
		return nil, err
	}
	maxBlockTime := int64(math.MinInt64)
	if len(blocks) > 0 {
		maxBlockTime = blocks[len(blocks)-1].Meta().MaxTime
	}

	// Also add the WAL if the current blocks don't cover the requests time range.
	if maxBlockTime <= maxt {
		if err := head.Close(); err != nil {
			return nil, err
		}
		w, err := wal.Open(db.logger, filepath.Join(db.dir, "wal"))
		if err != nil {
			return nil, err
		}
		opts := DefaultHeadOptions()
		opts.ChunkDirRoot = db.dir
		head, err = NewHead(nil, db.logger, w, opts)
		if err != nil {
			return nil, err
		}
		// Set the min valid time for the ingested wal samples
		// to be no lower than the maxt of the last block.
		if err := head.Init(maxBlockTime); err != nil {
			return nil, errors.Wrap(err, "read WAL")
		}
		// Set the wal to nil to disable all wal operations.
		// This is mainly to avoid blocking when closing the head.
		head.wal = nil
	}

	db.closers = append(db.closers, head)
	return &DB{
		dir:    db.dir,
		logger: db.logger,
		blocks: blocks,
		head:   head,
	}, nil
}

// Querier loads the blocks and wal and returns a new querier over the data partition for the given time range.
// Current implementation doesn't support multiple Queriers.
func (db *DBReadOnly) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := db.loadDataAsQueryable(maxt)
	if err != nil {
		return nil, err
	}
	return q.Querier(ctx, mint, maxt)
}

// ChunkQuerier loads blocks and the wal and returns a new chunk querier over the data partition for the given time range.
// Current implementation doesn't support multiple ChunkQueriers.
func (db *DBReadOnly) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := db.loadDataAsQueryable(maxt)
	if err != nil {
		return nil, err
	}
	return q.ChunkQuerier(ctx, mint, maxt)
}

// Blocks returns a slice of block readers for persisted blocks.
func (db *DBReadOnly) Blocks() ([]BlockReader, error) {
	select {
	case <-db.closed:
		return nil, ErrClosed
	default:
	}
	loadable, corrupted, err := openBlocks(db.logger, db.dir, nil, nil)
	if err != nil {
		return nil, err
	}

	// Corrupted blocks that have been superseded by a loadable block can be safely ignored.
	for _, block := range loadable {
		for _, b := range block.Meta().Compaction.Parents {
			delete(corrupted, b.ULID)
		}
	}
	if len(corrupted) > 0 {
		for _, b := range loadable {
			if err := b.Close(); err != nil {
				level.Warn(db.logger).Log("msg", "Closing block failed", "err", err, "block", b)
			}
		}
		errs := tsdb_errors.NewMulti()
		for ulid, err := range corrupted {
			errs.Add(errors.Wrapf(err, "corrupted block %s", ulid.String()))
		}
		return nil, errs.Err()
	}

	if len(loadable) == 0 {
		return nil, nil
	}

	sort.Slice(loadable, func(i, j int) bool {
		return loadable[i].Meta().MinTime < loadable[j].Meta().MinTime
	})

	blockMetas := make([]BlockMeta, 0, len(loadable))
	for _, b := range loadable {
		blockMetas = append(blockMetas, b.Meta())
	}
	if overlaps := OverlappingBlocks(blockMetas); len(overlaps) > 0 {
		level.Warn(db.logger).Log("msg", "Overlapping blocks found during opening", "detail", overlaps.String())
	}

	// Close all previously open readers and add the new ones to the cache.
	for _, closer := range db.closers {
		closer.Close()
	}

	blockClosers := make([]io.Closer, len(loadable))
	blockReaders := make([]BlockReader, len(loadable))
	for i, b := range loadable {
		blockClosers[i] = b
		blockReaders[i] = b
	}
	db.closers = blockClosers

	return blockReaders, nil
}

// Close all block readers.
func (db *DBReadOnly) Close() error {
	select {
	case <-db.closed:
		return ErrClosed
	default:
	}
	close(db.closed)

	return tsdb_errors.CloseAll(db.closers)
}

// Open returns a new DB in the given directory. If options are empty, DefaultOptions will be used.
func Open(dir string, l log.Logger, r prometheus.Registerer, opts *Options) (db *DB, err error) {
	var rngs []int64
	opts, rngs = validateOpts(opts, nil)
	return open(dir, l, r, opts, rngs)
}

func validateOpts(opts *Options, rngs []int64) (*Options, []int64) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if opts.StripeSize <= 0 {
		opts.StripeSize = DefaultStripeSize
	}
	if opts.HeadChunksWriteBufferSize <= 0 {
		opts.HeadChunksWriteBufferSize = chunks.DefaultWriteBufferSize
	}
	if opts.MinBlockDuration <= 0 {
		opts.MinBlockDuration = DefaultBlockDuration
	}
	if opts.MinBlockDuration > opts.MaxBlockDuration {
		opts.MaxBlockDuration = opts.MinBlockDuration
	}

	if len(rngs) == 0 {
		// Start with smallest block duration and create exponential buckets until the exceed the
		// configured maximum block duration.
		rngs = ExponentialBlockRanges(opts.MinBlockDuration, 10, 3)
	}
	return opts, rngs
}

// 返回值必须同时命名或不命名,可以使用_占位 !!!
func open(dir string, l log.Logger, r prometheus.Registerer, opts *Options, rngs []int64) (_ *DB, returnedErr error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if l == nil {
		l = log.NewNopLogger()
	}

	// 最长10个时间段或不超过最大驻留
	for i, v := range rngs {
		if v > opts.MaxBlockDuration {
			rngs = rngs[:i]
			break
		}
	}

	// Fixup bad format written by Prometheus 2.1.
	if err := repairBadIndexVersion(l, dir); err != nil {
		return nil, errors.Wrap(err, "repair bad index version")
	}

	walDir := filepath.Join(dir, "wal")

	// Migrate old WAL if one exists.
	if err := MigrateWAL(l, walDir); err != nil {
		return nil, errors.Wrap(err, "migrate WAL")
	}
	// Remove garbage, tmp blocks.
	if err := removeBestEffortTmpDirs(l, dir); err != nil {
		return nil, errors.Wrap(err, "remove tmp dirs")
	}

	db := &DB{
		dir:            dir,
		logger:         l,
		opts:           opts,
		compactc:       make(chan struct{}, 1), // 注意这个1,保证了最多只有一次请求未被处理
		donec:          make(chan struct{}),
		stopc:          make(chan struct{}),
		autoCompact:    true,
		chunkPool:      chunkenc.NewPool(), // 基于sync.Pool实现
		blocksToDelete: opts.BlocksToDelete,
	}
	// 失败情况进行清理
	defer func() {
		// Close files if startup fails somewhere.
		if returnedErr == nil {
			return
		}

		close(db.donec) // DB is never run if it was an error, so close this channel here.

		returnedErr = tsdb_errors.NewMulti(
			returnedErr,
			errors.Wrap(db.Close(), "close DB after failed startup"),
		).Err()
	}()

	if db.blocksToDelete == nil {
		db.blocksToDelete = DefaultBlocksToDelete(db)
	}

	if !opts.NoLockfile {
		absdir, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		lockf, _, err := fileutil.Flock(filepath.Join(absdir, "lock"))
		if err != nil {
			return nil, errors.Wrap(err, "lock DB directory")
		}
		db.lockf = lockf
	}

	var err error
	ctx, cancel := context.WithCancel(context.Background())
	db.compactor, err = NewLeveledCompactor(ctx, r, l, rngs, db.chunkPool)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "create leveled compactor")
	}
	db.compactCancel = cancel

	var wlog *wal.WAL
	segmentSize := wal.DefaultSegmentSize // 默认128M
	// Wal is enabled.
	// WAL设计可配制但实际配制参数被隐藏[storage.tsdb.wal-segment-size]
	if opts.WALSegmentSize >= 0 {
		// Wal is set to a custom size.
		if opts.WALSegmentSize > 0 {
			segmentSize = opts.WALSegmentSize
		}
		// 创建wal指定大小
		wlog, err = wal.NewSize(l, r, walDir, segmentSize, opts.WALCompression)
		if err != nil {
			return nil, err
		}
	}

	headOpts := DefaultHeadOptions()
	headOpts.ChunkRange = rngs[0] // 修改了默认的块压缩时间(只是为什么使用rngs[0]不明白) ???
	headOpts.ChunkDirRoot = dir
	headOpts.ChunkPool = db.chunkPool
	headOpts.ChunkWriteBufferSize = opts.HeadChunksWriteBufferSize // 4M
	headOpts.StripeSize = opts.StripeSize                          // 16K
	headOpts.SeriesCallback = opts.SeriesLifecycleCallback         // 当前版本总是空
	db.head, err = NewHead(r, l, wlog, headOpts)
	if err != nil {
		return nil, err
	}

	// Register metrics after assigning the head block.
	db.metrics = newDBMetrics(db, r)
	maxBytes := opts.MaxBytes
	if maxBytes < 0 {
		maxBytes = 0
	}
	db.metrics.maxBytes.Set(float64(maxBytes))

	if err := db.reload(); err != nil {
		return nil, err
	}
	// Set the min valid time for the ingested samples
	// to be no lower than the maxt of the last block.
	blocks := db.Blocks()
	minValidTime := int64(math.MinInt64)
	if len(blocks) > 0 {
		minValidTime = blocks[len(blocks)-1].Meta().MaxTime
	}

	if initErr := db.head.Init(minValidTime); initErr != nil {
		db.head.metrics.walCorruptionsTotal.Inc()
		level.Warn(db.logger).Log("msg", "Encountered WAL read error, attempting repair", "err", initErr)
		// head.Init如果失败只能是由wal处理引起
		// 所以此处直接进行wal修复就行了
		if err := wlog.Repair(initErr); err != nil {
			return nil, errors.Wrap(err, "repair corrupted WAL")
		}
	}

	go db.run()

	return db, nil
}

func removeBestEffortTmpDirs(l log.Logger, dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if isTmpBlockDir(fi) {
			if err := os.RemoveAll(filepath.Join(dir, fi.Name())); err != nil {
				level.Error(l).Log("msg", "failed to delete tmp block dir", "dir", filepath.Join(dir, fi.Name()), "err", err)
				continue
			}
			level.Info(l).Log("msg", "Found and deleted tmp block dir", "dir", filepath.Join(dir, fi.Name()))
		}
	}
	return nil
}

// StartTime implements the Storage interface.
func (db *DB) StartTime() (int64, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	if len(db.blocks) > 0 {
		return db.blocks[0].Meta().MinTime, nil
	}
	return db.head.MinTime(), nil
}

// Dir returns the directory of the database.
func (db *DB) Dir() string {
	return db.dir
}

func (db *DB) run() {
	defer close(db.donec)

	backoff := time.Duration(0)

	for {
		select {
		case <-db.stopc:
			return
		case <-time.After(backoff):
			// 如果Compact失败的情况下会调整backoff的大小
			// 指数级增长但保证数值范围区间(1s, 1m)
		}

		select {
		case <-time.After(1 * time.Minute):
			// 触发压缩逻辑尝试压缩线逻辑处理
			db.cmtx.Lock()
			if err := db.reloadBlocks(); err != nil {
				level.Error(db.logger).Log("msg", "reloadBlocks", "err", err)
			}
			db.cmtx.Unlock()
			// 这用法 ???
			select {
			case db.compactc <- struct{}{}: // 触发压缩线逻辑处理(不一定执行压缩只是尝试)
			default:
			}
		case <-db.compactc:
			// 两种情况会触发压缩线处理逻辑(尝试处理)
			// - 每次采样被提交(dbAppender.Commit中判断head是否到达可压缩时间)
			// - 分钟周期(实际还要增加backoff等待时间)触发
			db.metrics.compactionsTriggered.Inc()

			db.autoCompactMtx.Lock()
			if db.autoCompact { // 当前恒定为true
				level.Info(db.logger).Log("msg", "---------------------------- start compact ???") // for debug ???
				// 注意Compact函数中会锁定cmtx
				if err := db.Compact(); err != nil {
					level.Error(db.logger).Log("msg", "compaction failed", "err", err)
					backoff = exponential(backoff, 1*time.Second, 1*time.Minute)
				} else {
					backoff = 0
				}
			} else {
				db.metrics.compactionsSkipped.Inc()
			}
			db.autoCompactMtx.Unlock()
		case <-db.stopc:
			return
		}
	}
}

// Appender opens a new appender against the database.
func (db *DB) Appender(ctx context.Context) storage.Appender {
	return dbAppender{db: db, Appender: db.head.Appender(ctx)}
}

// dbAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
// 实现了storage.Appender接口
type dbAppender struct {
	storage.Appender
	db *DB
}

// Hook了headAppdender.Commit
func (a dbAppender) Commit() error {
	err := a.Appender.Commit()
	level.Info(a.db.logger).Log("msg", "---------------------------- commit sample ???")
	// We could just run this check every few minutes practically. But for benchmarks
	// and high frequency use cases this is the safer way.
	// 判定是否需要处理压缩逻辑
	if a.db.head.compactable() {
		select {
		case a.db.compactc <- struct{}{}: // 触发压缩线逻辑处理(一定会执行因为已经判定过了)
		default:
		}
	}
	return err
}

// Compact data if possible. After successful compaction blocks are reloaded
// which will also delete the blocks that fall out of the retention window.
// Old blocks are only deleted on reloadBlocks based on the new block's parent information.
// See DB.reloadBlocks documentation for further information.
func (db *DB) Compact() (returnErr error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()
	defer func() {
		if returnErr != nil {
			db.metrics.compactionsFailed.Inc()
		}
	}()

	lastBlockMaxt := int64(math.MinInt64)
	defer func() {
		returnErr = tsdb_errors.NewMulti(
			returnErr,
			errors.Wrap(db.head.truncateWAL(lastBlockMaxt), "WAL truncation in Compact defer"),
		).Err()
	}()

	start := time.Now()
	// Check whether we have pending head blocks that are ready to be persisted.
	// They have the highest priority.
	// 压缩头数据
	for {
		select {
		case <-db.stopc:
			return nil
		default:
		}
		if !db.head.compactable() {
			break
		}
		mint := db.head.MinTime()
		maxt := rangeForTimestamp(mint, db.head.chunkRange.Load())

		// Wrap head into a range that bounds all reads to it.
		// We remove 1 millisecond from maxt because block
		// intervals are half-open: [b.MinTime, b.MaxTime). But
		// chunk intervals are closed: [c.MinTime, c.MaxTime];
		// so in order to make sure that overlaps are evaluated
		// consistently, we explicitly remove the last value
		// from the block interval here.
		if err := db.compactHead(NewRangeHead(db.head, mint, maxt-1)); err != nil {
			return errors.Wrap(err, "compact head")
		}
		// Consider only successful compactions for WAL truncation.
		lastBlockMaxt = maxt
	}

	// Clear some disk space before compacting blocks, especially important
	// when Head compaction happened over a long time range.
	// 截断WAL数据
	if err := db.head.truncateWAL(lastBlockMaxt); err != nil {
		return errors.Wrap(err, "WAL truncation in Compact")
	}

	compactionDuration := time.Since(start)
	if compactionDuration.Milliseconds() > db.head.chunkRange.Load() {
		level.Warn(db.logger).Log(
			"msg", "Head compaction took longer than the block time range, compactions are falling behind and won't be able to catch up",
			"duration", compactionDuration.String(),
			"block_range", db.head.chunkRange.Load(),
		)
	}
	// 压缩与合并块
	return db.compactBlocks()
}

// CompactHead compacts the given RangeHead.
func (db *DB) CompactHead(head *RangeHead) error {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	if err := db.compactHead(head); err != nil {
		return errors.Wrap(err, "compact head")
	}

	if err := db.head.truncateWAL(head.BlockMaxTime()); err != nil {
		return errors.Wrap(err, "WAL truncation")
	}
	return nil
}

// compactHead compacts the given RangeHead.
// The compaction mutex should be held before calling this method.
func (db *DB) compactHead(head *RangeHead) error {
	// 将当前head中在区间(包含有重叠)mmap-chunk与head-chunk压缩成block
	uid, err := db.compactor.Write(db.dir, head, head.MinTime(), head.BlockMaxTime(), nil)
	if err != nil {
		return errors.Wrap(err, "persist head block")
	}
	// 重新装载块保证新落地的块在内存中有元信息被记录
	if err := db.reloadBlocks(); err != nil {
		if errRemoveAll := os.RemoveAll(filepath.Join(db.dir, uid.String())); errRemoveAll != nil {
			return tsdb_errors.NewMulti(
				errors.Wrap(err, "reloadBlocks blocks"),
				errors.Wrapf(errRemoveAll, "delete persisted head block after failed db reloadBlocks:%s", uid),
			).Err()
		}
		return errors.Wrap(err, "reloadBlocks blocks")
	}
	// 截断内存中所有小于块最大时间内容
	// truncateMemory中更新head.minTime
	if err = db.head.truncateMemory(head.BlockMaxTime()); err != nil {
		return errors.Wrap(err, "head memory truncate")
	}
	return nil
}

// compactBlocks compacts all the eligible on-disk blocks.
// The compaction mutex should be held before calling this method.
func (db *DB) compactBlocks() (err error) {
	// Check for compactions of multiple blocks.
	for {
		plan, err := db.compactor.Plan(db.dir)
		if err != nil {
			return errors.Wrap(err, "plan compaction")
		}
		if len(plan) == 0 {
			break
		}

		select {
		case <-db.stopc:
			return nil
		default:
		}

		uid, err := db.compactor.Compact(db.dir, plan, db.blocks)
		if err != nil {
			return errors.Wrapf(err, "compact %s", plan)
		}

		if err := db.reloadBlocks(); err != nil { // 重新装载
			if err := os.RemoveAll(filepath.Join(db.dir, uid.String())); err != nil {
				return errors.Wrapf(err, "delete compacted block after failed db reloadBlocks:%s", uid)
			}
			return errors.Wrap(err, "reloadBlocks blocks")
		}
	}

	return nil
}

// getBlock iterates a given block range to find a block by a given id.
// If found it returns the block itself and a boolean to indicate that it was found.
// 就是判定一下
func getBlock(allBlocks []*Block, id ulid.ULID) (*Block, bool) {
	for _, b := range allBlocks {
		if b.Meta().ULID == id {
			return b, true
		}
	}
	return nil, false
}

// reload reloads blocks and truncates the head and its WAL.
func (db *DB) reload() error {
	if err := db.reloadBlocks(); err != nil {
		return errors.Wrap(err, "reloadBlocks")
	}
	if len(db.blocks) == 0 {
		return nil
	}
	if err := db.head.Truncate(db.blocks[len(db.blocks)-1].MaxTime()); err != nil {
		return errors.Wrap(err, "head truncate")
	}
	return nil
}

// reloadBlocks reloads blocks without touching head.
// Blocks that are obsolete due to replacement or retention will be deleted.
// reloadBlocks执行以下几步操作:
// - 删除已损坏的block文件及其父block(如果存在)
// - 装载所有正常的block
func (db *DB) reloadBlocks() (err error) {
	defer func() {
		if err != nil {
			db.metrics.reloadsFailed.Inc()
		}
		db.metrics.reloads.Inc()
	}()

	// Now that we reload TSDB every minute, there is high chance for race condition with a reload
	// triggered by CleanTombstones(). We need to lock the reload to avoid the situation where
	// a normal reload and CleanTombstones try to delete the same block.
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// corrupted 代表打开失败
	loadable, corrupted, err := openBlocks(db.logger, db.dir, db.blocks, db.chunkPool)
	if err != nil {
		return err
	}

	// 获取可删除块ULID(只是ULIDs不包含值)
	deletableULIDs := db.blocksToDelete(loadable)
	deletable := make(map[ulid.ULID]*Block, len(deletableULIDs))

	// Mark all parents of loaded blocks as deletable (no matter if they exists). This makes it resilient against the process
	// crashing towards the end of a compaction but before deletions. By doing that, we can pick up the deletion where it left off during a crash.
	for _, block := range loadable {
		// 设置可删除块
		if _, ok := deletableULIDs[block.meta.ULID]; ok {
			deletable[block.meta.ULID] = block
		}
		for _, b := range block.Meta().Compaction.Parents {
			if _, ok := corrupted[b.ULID]; ok {
				delete(corrupted, b.ULID)
				level.Warn(db.logger).Log("msg", "Found corrupted block, but replaced by compacted one so it's safe to delete. This should not happen with atomic deletes.", "block", b.ULID)
			}
			// 记录下父块后续再设置值
			// 可能会在当前的外层循环再命中deletableULIDs(L1017)
			// 或是在后续的循环中处理(L1057)
			deletable[b.ULID] = nil
		}
	}

	// 经过上面的循环corrupted中保存了可装载的但是却已损坏的块
	// 所以如果不为空则需要退出
	if len(corrupted) > 0 {
		// Corrupted but no child loaded for it.
		// Close all new blocks to release the lock for windows.
		for _, block := range loadable {
			if _, open := getBlock(db.blocks, block.Meta().ULID); !open {
				block.Close()
			}
		}
		errs := tsdb_errors.NewMulti()
		for ulid, err := range corrupted {
			errs.Add(errors.Wrapf(err, "corrupted block %s", ulid.String()))
		}
		return errs.Err()
	}

	var (
		toLoad     []*Block
		blocksSize int64
	)
	// All deletable blocks should be unloaded.
	// NOTE: We need to loop through loadable one more time as there might be loadable ready to be removed (replaced by compacted block).
	for _, block := range loadable {
		// 保证为之前记录的父设置值
		if _, ok := deletable[block.Meta().ULID]; ok {
			deletable[block.Meta().ULID] = block
			continue
		}

		// 记录真正可装载的块
		toLoad = append(toLoad, block)
		blocksSize += block.Size()
	}
	db.metrics.blocksBytes.Set(float64(blocksSize))

	sort.Slice(toLoad, func(i, j int) bool {
		return toLoad[i].Meta().MinTime < toLoad[j].Meta().MinTime
	})
	if !db.opts.AllowOverlappingBlocks {
		if err := validateBlockSequence(toLoad); err != nil {
			return errors.Wrap(err, "invalid block sequence")
		}
	}

	// Swap new blocks first for subsequently created readers to be seen.
	oldBlocks := db.blocks
	db.blocks = toLoad

	blockMetas := make([]BlockMeta, 0, len(toLoad))
	for _, b := range toLoad {
		blockMetas = append(blockMetas, b.Meta())
	}
	if overlaps := OverlappingBlocks(blockMetas); len(overlaps) > 0 {
		level.Warn(db.logger).Log("msg", "Overlapping blocks found during reloadBlocks", "detail", overlaps.String())
	}

	// Append blocks to old, deletable blocks, so we can close them.
	// 感觉没有必要处理已装载的块了,loadable中记录data目录中所有块已经包含了装载的块
	for _, b := range oldBlocks {
		if _, ok := deletable[b.Meta().ULID]; ok {
			deletable[b.Meta().ULID] = b
		}
	}
	if err := db.deleteBlocks(deletable); err != nil {
		return errors.Wrapf(err, "delete %v blocks", len(deletable))
	}
	return nil
}

func openBlocks(l log.Logger, dir string, loaded []*Block, chunkPool chunkenc.Pool) (blocks []*Block, corrupted map[ulid.ULID]error, err error) {
	bDirs, err := blockDirs(dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "find blocks")
	}

	corrupted = make(map[ulid.ULID]error)
	for _, bDir := range bDirs {
		meta, _, err := readMetaFile(bDir)
		if err != nil {
			level.Error(l).Log("msg", "Failed to read meta.json for a block during reloadBlocks. Skipping", "dir", bDir, "err", err)
			continue
		}

		// See if we already have the block in memory or open it otherwise.
		block, open := getBlock(loaded, meta.ULID) // getBlock只是判定block是否已经被打开
		if !open {
			block, err = OpenBlock(l, bDir, chunkPool)
			if err != nil {
				corrupted[meta.ULID] = err
				continue
			}
		}
		blocks = append(blocks, block)
	}
	return blocks, corrupted, nil
}

// DefaultBlocksToDelete returns a filter which decides time based and size based
// retention from the options of the db.
func DefaultBlocksToDelete(db *DB) BlocksToDeleteFunc {
	return func(blocks []*Block) map[ulid.ULID]struct{} {
		return deletableBlocks(db, blocks)
	}
}

// deletableBlocks returns all currently loaded blocks past retention policy or already compacted into a new block.
// 筛选出所有可删除block
// 筛选出所有超过最大驻留时间的block
// 筛选出所有超过最大驻留大小的block
// 返回block的ulid
func deletableBlocks(db *DB, blocks []*Block) map[ulid.ULID]struct{} {
	deletable := make(map[ulid.ULID]struct{})

	// Sort the blocks by time - newest to oldest (largest to smallest timestamp).
	// This ensures that the retentions will remove the oldest  blocks.
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Meta().MaxTime > blocks[j].Meta().MaxTime
	})

	for _, block := range blocks {
		if block.Meta().Compaction.Deletable {
			deletable[block.Meta().ULID] = struct{}{}
		}
	}

	for ulid := range BeyondTimeRetention(db, blocks) {
		deletable[ulid] = struct{}{}
	}

	for ulid := range BeyondSizeRetention(db, blocks) {
		deletable[ulid] = struct{}{}
	}

	return deletable
}

// BeyondTimeRetention returns those blocks which are beyond the time retention
// set in the db options.
func BeyondTimeRetention(db *DB, blocks []*Block) (deletable map[ulid.ULID]struct{}) {
	// Time retention is disabled or no blocks to work with.
	if len(blocks) == 0 || db.opts.RetentionDuration == 0 {
		return
	}

	deletable = make(map[ulid.ULID]struct{})
	for i, block := range blocks {
		// The difference between the first block and this block is larger than
		// the retention period so any blocks after that are added as deletable.
		if i > 0 && blocks[0].Meta().MaxTime-block.Meta().MaxTime > db.opts.RetentionDuration {
			for _, b := range blocks[i:] {
				deletable[b.meta.ULID] = struct{}{}
			}
			db.metrics.timeRetentionCount.Inc()
			break
		}
	}
	return deletable
}

// BeyondSizeRetention returns those blocks which are beyond the size retention
// set in the db options.
func BeyondSizeRetention(db *DB, blocks []*Block) (deletable map[ulid.ULID]struct{}) {
	// Size retention is disabled or no blocks to work with.
	if len(blocks) == 0 || db.opts.MaxBytes <= 0 {
		return
	}

	deletable = make(map[ulid.ULID]struct{})

	// Initializing size counter with WAL size and Head chunks
	// written to disk, as that is part of the retention strategy.
	blocksSize := db.Head().Size()
	for i, block := range blocks {
		blocksSize += block.Size()
		if blocksSize > int64(db.opts.MaxBytes) {
			// Add this and all following blocks for deletion.
			for _, b := range blocks[i:] {
				deletable[b.meta.ULID] = struct{}{}
			}
			db.metrics.sizeRetentionCount.Inc()
			break
		}
	}
	return deletable
}

// deleteBlocks closes the block if loaded and deletes blocks from the disk if exists.
// When the map contains a non nil block object it means it is loaded in memory
// so needs to be closed first as it might need to wait for pending readers to complete.
func (db *DB) deleteBlocks(blocks map[ulid.ULID]*Block) error {
	for ulid, block := range blocks {
		if block != nil {
			if err := block.Close(); err != nil {
				level.Warn(db.logger).Log("msg", "Closing block failed", "err", err, "block", ulid)
			}
		}

		toDelete := filepath.Join(db.dir, ulid.String())
		if _, err := os.Stat(toDelete); os.IsNotExist(err) {
			// Noop.
			continue
		} else if err != nil {
			return errors.Wrapf(err, "stat dir %v", toDelete)
		}

		// Replace atomically to avoid partial block when process would crash during deletion.
		tmpToDelete := filepath.Join(db.dir, fmt.Sprintf("%s%s", ulid, tmpForDeletionBlockDirSuffix))
		if err := fileutil.Replace(toDelete, tmpToDelete); err != nil {
			return errors.Wrapf(err, "replace of obsolete block for deletion %s", ulid)
		}
		if err := os.RemoveAll(tmpToDelete); err != nil {
			return errors.Wrapf(err, "delete obsolete block %s", ulid)
		}
		level.Info(db.logger).Log("msg", "Deleting obsolete block", "block", ulid)
	}

	return nil
}

// validateBlockSequence returns error if given block meta files indicate that some blocks overlaps within sequence.
func validateBlockSequence(bs []*Block) error {
	if len(bs) <= 1 {
		return nil
	}

	var metas []BlockMeta
	for _, b := range bs {
		metas = append(metas, b.meta)
	}

	overlaps := OverlappingBlocks(metas)
	if len(overlaps) > 0 {
		return errors.Errorf("block time ranges overlap: %s", overlaps)
	}

	return nil
}

// TimeRange specifies minTime and maxTime range.
type TimeRange struct {
	Min, Max int64
}

// Overlaps contains overlapping blocks aggregated by overlapping range.
type Overlaps map[TimeRange][]BlockMeta

// String returns human readable string form of overlapped blocks.
func (o Overlaps) String() string {
	var res []string
	for r, overlaps := range o {
		var groups []string
		for _, m := range overlaps {
			groups = append(groups, fmt.Sprintf(
				"<ulid: %s, mint: %d, maxt: %d, range: %s>",
				m.ULID.String(),
				m.MinTime,
				m.MaxTime,
				(time.Duration((m.MaxTime-m.MinTime)/1000)*time.Second).String(),
			))
		}
		res = append(res, fmt.Sprintf(
			"[mint: %d, maxt: %d, range: %s, blocks: %d]: %s",
			r.Min, r.Max,
			(time.Duration((r.Max-r.Min)/1000)*time.Second).String(),
			len(overlaps),
			strings.Join(groups, ", ")),
		)
	}
	return strings.Join(res, "\n")
}

// OverlappingBlocks returns all overlapping blocks from given meta files.
func OverlappingBlocks(bm []BlockMeta) Overlaps {
	if len(bm) <= 1 {
		return nil
	}
	var (
		overlaps [][]BlockMeta

		// pending contains not ended blocks in regards to "current" timestamp.
		pending = []BlockMeta{bm[0]}
		// continuousPending helps to aggregate same overlaps to single group.
		continuousPending = true
	)

	// We have here blocks sorted by minTime. We iterate over each block and treat its minTime as our "current" timestamp.
	// We check if any of the pending block finished (blocks that we have seen before, but their maxTime was still ahead current
	// timestamp). If not, it means they overlap with our current block. In the same time current block is assumed pending.
	for _, b := range bm[1:] {
		var newPending []BlockMeta

		for _, p := range pending {
			// "b.MinTime" is our current time.
			if b.MinTime >= p.MaxTime {
				continuousPending = false
				continue
			}

			// "p" overlaps with "b" and "p" is still pending.
			newPending = append(newPending, p)
		}

		// Our block "b" is now pending.
		pending = append(newPending, b)
		if len(newPending) == 0 {
			// No overlaps.
			continue
		}

		if continuousPending && len(overlaps) > 0 {
			overlaps[len(overlaps)-1] = append(overlaps[len(overlaps)-1], b)
			continue
		}
		overlaps = append(overlaps, append(newPending, b))
		// Start new pendings.
		continuousPending = true
	}

	// Fetch the critical overlapped time range foreach overlap groups.
	overlapGroups := Overlaps{}
	for _, overlap := range overlaps {

		minRange := TimeRange{Min: 0, Max: math.MaxInt64}
		for _, b := range overlap {
			if minRange.Max > b.MaxTime {
				minRange.Max = b.MaxTime
			}

			if minRange.Min < b.MinTime {
				minRange.Min = b.MinTime
			}
		}
		overlapGroups[minRange] = overlap
	}

	return overlapGroups
}

func (db *DB) String() string {
	return "HEAD"
}

// Blocks returns the databases persisted blocks.
func (db *DB) Blocks() []*Block {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.blocks
}

// Head returns the databases's head.
func (db *DB) Head() *Head {
	return db.head
}

// Close the partition.
func (db *DB) Close() error {
	close(db.stopc)
	if db.compactCancel != nil {
		db.compactCancel()
	}
	<-db.donec

	db.mtx.Lock()
	defer db.mtx.Unlock()

	var g errgroup.Group

	// blocks also contains all head blocks.
	for _, pb := range db.blocks {
		g.Go(pb.Close)
	}

	errs := tsdb_errors.NewMulti(g.Wait())
	if db.lockf != nil {
		errs.Add(db.lockf.Release())
	}
	if db.head != nil {
		errs.Add(db.head.Close())
	}
	return errs.Err()
}

// DisableCompactions disables auto compactions.
// promtool/tsdb.go
func (db *DB) DisableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = false
	level.Info(db.logger).Log("msg", "Compactions disabled")
}

// EnableCompactions enables auto compactions.
func (db *DB) EnableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = true
	level.Info(db.logger).Log("msg", "Compactions enabled")
}

// Snapshot writes the current data to the directory. If withHead is set to true it
// will create a new block containing all data that's currently in the memory buffer/WAL.
func (db *DB) Snapshot(dir string, withHead bool) error {
	if dir == db.dir {
		return errors.Errorf("cannot snapshot into base directory")
	}
	if _, err := ulid.ParseStrict(dir); err == nil {
		return errors.Errorf("dir must not be a valid ULID")
	}

	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		level.Info(db.logger).Log("msg", "Snapshotting block", "block", b)

		if err := b.Snapshot(dir); err != nil {
			return errors.Wrapf(err, "error snapshotting block: %s", b.Dir())
		}
	}
	if !withHead {
		return nil
	}

	mint := db.head.MinTime()
	maxt := db.head.MaxTime()
	head := NewRangeHead(db.head, mint, maxt)
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	if _, err := db.compactor.Write(dir, head, mint, maxt+1, nil); err != nil {
		return errors.Wrap(err, "snapshot head block")
	}
	return nil
}

// Querier returns a new querier over the data partition for the given time range.
func (db *DB) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	var blocks []BlockReader

	db.mtx.RLock()
	defer db.mtx.RUnlock()
	// 获取所有与[mint,maxt]区间的重叠的block
	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			blocks = append(blocks, b)
		}
	}
	// 判定head是否在时间窗口中
	if maxt >= db.head.MinTime() {
		// 增加head中的内容
		blocks = append(blocks, NewRangeHead(db.head, mint, maxt))
	}
	// 通过BlockReader构建Querier
	blockQueriers := make([]storage.Querier, 0, len(blocks))
	for _, b := range blocks {
		q, err := NewBlockQuerier(b, mint, maxt)
		if err == nil {
			blockQueriers = append(blockQueriers, q)
			continue
		}
		// If we fail, all previously opened queriers must be closed.
		for _, q := range blockQueriers {
			// TODO(bwplotka): Handle error.
			_ = q.Close()
		}
		return nil, errors.Wrapf(err, "open querier for block %s", b)
	}
	return storage.NewMergeQuerier(
		blockQueriers,
		nil,
		storage.ChainedSeriesMerge,
	), nil
}

// ChunkQuerier returns a new chunk querier over the data partition for the given time range.
func (db *DB) ChunkQuerier(_ context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	var blocks []BlockReader

	db.mtx.RLock()
	defer db.mtx.RUnlock()
	// 获取所有与[mint,maxt]区间的重叠的block
	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			blocks = append(blocks, b)
		}
	}
	// 判定head是否在时间窗口中
	if maxt >= db.head.MinTime() {
		// 增加head中的内容
		blocks = append(blocks, NewRangeHead(db.head, mint, maxt))
	}
	// 通过BlockReader构建Querier
	blockQueriers := make([]storage.ChunkQuerier, 0, len(blocks))
	for _, b := range blocks {
		q, err := NewBlockChunkQuerier(b, mint, maxt)
		if err == nil {
			blockQueriers = append(blockQueriers, q)
			continue
		}
		// If we fail, all previously opened queriers must be closed.
		for _, q := range blockQueriers {
			// TODO(bwplotka): Handle error.
			_ = q.Close()
		}
		return nil, errors.Wrapf(err, "open querier for block %s", b)
	}

	return storage.NewMergeChunkQuerier(
		blockQueriers,
		nil,
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge),
	), nil
}

func rangeForTimestamp(t int64, width int64) (maxt int64) {
	// 重新计算t,保证t是width的整数倍,从而保证range的连续性
	// t0  t1  t2    t3 无倍数对齐[w=3]
	// |___|___|_____|_
	// t0  t1  t2  t3`  有倍数对齐[t3`=(t3/w)*w+w, w=3]
	// |___|___|___|___
	//  123 678 ABC DEF
	return (t/width)*width + width
}

// Delete implements deletion of metrics. It only has atomicity guarantees on a per-block basis.
func (db *DB) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	var g errgroup.Group

	db.mtx.RLock()
	defer db.mtx.RUnlock()
	// 处理所有block
	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			g.Go(func(b *Block) func() error {
				return func() error { return b.Delete(mint, maxt, ms...) }
			}(b))
		}
	}
	// 处理head
	g.Go(func() error {
		return db.head.Delete(mint, maxt, ms...)
	})
	return g.Wait()
}

// CleanTombstones re-writes any blocks with tombstones.
func (db *DB) CleanTombstones() (err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	start := time.Now()
	defer db.metrics.tombCleanTimer.Observe(time.Since(start).Seconds())

	cleanUpCompleted := false
	// Repeat cleanup until there is no tombstones left.
	for !cleanUpCompleted {
		cleanUpCompleted = true

		for _, pb := range db.Blocks() {
			uid, safeToDelete, cleanErr := pb.CleanTombstones(db.Dir(), db.compactor)
			if cleanErr != nil {
				return errors.Wrapf(cleanErr, "clean tombstones: %s", pb.Dir())
			}
			// 没有任何数据需要被删除
			if !safeToDelete {
				// There was nothing to clean.
				continue
			}

			// In case tombstones of the old block covers the whole block,
			// then there would be no resultant block to tell the parent.
			// The lock protects against race conditions when deleting blocks
			// during an already running reload.
			// 当前块数据有部分时间重叠产生了新的块
			// 标记为可删除后续会在reloadBlocks中删除
			db.mtx.Lock()
			pb.meta.Compaction.Deletable = safeToDelete
			db.mtx.Unlock()
			cleanUpCompleted = false
			if err = db.reloadBlocks(); err == nil { // Will try to delete old block.
				// Successful reload will change the existing blocks.
				// We need to loop over the new set of blocks.
				// 成功重新装载block重新开始循环
				// reloadBlocks中会删除被标记的parent块
				break
			}

			// Delete new block if it was created.
			// 如果reloadBlocks失败则删除新创建的块
			if uid != nil && *uid != (ulid.ULID{}) {
				dir := filepath.Join(db.Dir(), uid.String())
				if err := os.RemoveAll(dir); err != nil {
					level.Error(db.logger).Log("msg", "failed to delete block after failed `CleanTombstones`", "dir", dir, "err", err)
				}
			}
			return errors.Wrap(err, "reload blocks")
		}
	}
	return nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}

// isTmpBlockDir returns dir that consists of block dir ULID and tmp extension.
// 以下类型文件何种情况被创建 ???
// 01F0QVNJAXF92BTEBSZVVFSA25.tmp-for-deletion
// 01F0QVNJAXF92BTEBSZVVFSA25.tmp-for-creation
// 01F0QVNJAXF92BTEBSZVVFSA25.tmp
func isTmpBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}

	fn := fi.Name()
	ext := filepath.Ext(fn)
	if ext == tmpForDeletionBlockDirSuffix || ext == tmpForCreationBlockDirSuffix || ext == tmpLegacy {
		if _, err := ulid.ParseStrict(fn[:len(fn)-len(ext)]); err == nil {
			return true
		}
	}
	return false
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func sequenceFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil {
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name()))
	}
	return res, nil
}

func nextSequenceFile(dir string) (string, int, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}

	i := uint64(0)
	for _, f := range files {
		j, err := strconv.ParseUint(f.Name(), 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func exponential(d, min, max time.Duration) time.Duration {
	d *= 2
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}
