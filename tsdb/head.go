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
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic" // 值得一看 !!!

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wal"
)

var (
	// ErrInvalidSample is returned if an appended sample is not valid and can't
	// be ingested.
	ErrInvalidSample = errors.New("invalid sample")
	// ErrAppenderClosed is returned if an appender has already be successfully
	// rolled back or committed.
	ErrAppenderClosed = errors.New("appender closed")
)

// Head handles reads and writes of time series data within a time window.
type Head struct {
	chunkRange atomic.Int64
	numSeries  atomic.Uint64
	// 初始设置在initAppender.Append中调用initTime设置[min,max]为[sample.T,sample.T]
	// 初始化loadWAL中调用updateMinMaxTime更新区间
	// headAppender.Commit中调用updateMinMaxTime更新区间
	minTime, maxTime      atomic.Int64 // Current min and max of the samples included in the head.
	minValidTime          atomic.Int64 // Mint allowed to be added to the head. It shouldn't be lower than the maxt of the last persisted block.
	lastWALTruncationTime atomic.Int64
	lastSeriesID          atomic.Uint64

	metrics      *headMetrics
	opts         *HeadOptions
	wal          *wal.WAL
	logger       log.Logger
	appendPool   sync.Pool
	seriesPool   sync.Pool
	bytesPool    sync.Pool
	memChunkPool sync.Pool

	// All series addressable by their ID or hash.
	series *stripeSeries

	// 并发读写symbols互斥
	// 写操作
	// - head.getOrCreateWithID
	// - head.gc
	// 读操作
	// - headIndexReader.Symbols
	// - headIndexReader.LabelNames
	// - headIndexReader.LabelValues
	symMtx sync.RWMutex
	// 记录Symbols为headIndexReader所用
	// 记录有sample还驻留在内存中(包括普通chunk与headChunk)的series所有标签名与标签值(通过map去重)
	// 数据map[label.name/label.value]struct{}
	symbols map[string]struct{}

	// 并发读写deleted互斥
	// 写操作
	// - head.truncateWAL
	// 读操作
	// - head.truncateWAL中的keep
	// - head.gc
	deletedMtx sync.Mutex
	// 记录compactHead过程中从headChunk中删除的series(因为对应的samples数据都已经落地磁盘)
	// WAL创建Checkpoint用于判定是否需要保留series
	deleted map[uint64]int // Deleted series, and what WAL segment they must be kept until.

	// 记录Postings为headIndexReader所用
	// 记录有sample还驻留在内存中(包括普通chunk与headChunk)的series标签对应的值对应的refid
	// 数据map[label.name]map[label.value][]id
	postings *index.MemPostings // Postings lists for terms.

	tombstones *tombstones.MemTombstones

	iso *isolation

	// 并发读写cardinalityCache互斥
	cardinalityMutex      sync.Mutex
	cardinalityCache      *index.PostingsStats // Posting stats cache which will expire after 30sec.
	lastPostingsStatsCall time.Duration        // Last posting stats call (PostingsCardinalityStats()) time for caching.

	// chunkDiskMapper is used to write and read Head chunks to/from disk.
	// 是多个memSeries共用(单实例)
	chunkDiskMapper *chunks.ChunkDiskMapper

	// 保护closed并保持关闭与读操作之间互斥
	// - chunkRange(Chunks中被调用)
	// - Close
	closedMtx sync.Mutex
	closed    bool
}

// HeadOptions are parameters for the Head block.
type HeadOptions struct {
	ChunkRange int64
	// ChunkDirRoot is the parent directory of the chunks directory.
	ChunkDirRoot         string
	ChunkPool            chunkenc.Pool
	ChunkWriteBufferSize int
	// StripeSize sets the number of entries in the hash map, it must be a power of 2.
	// A larger StripeSize will allocate more memory up-front, but will increase performance when handling a large number of series.
	// A smaller StripeSize reduces the memory allocated, but can decrease performance with large number of series.
	StripeSize     int
	SeriesCallback SeriesLifecycleCallback
}

func DefaultHeadOptions() *HeadOptions {
	return &HeadOptions{
		ChunkRange:           DefaultBlockDuration, // 2h
		ChunkDirRoot:         "",
		ChunkPool:            chunkenc.NewPool(),
		ChunkWriteBufferSize: chunks.DefaultWriteBufferSize, // 4M
		StripeSize:           DefaultStripeSize,             // 16K
		SeriesCallback:       &noopSeriesLifecycleCallback{},
	}
}

// 记录Head运行时各种指标
type headMetrics struct {
	activeAppenders          prometheus.Gauge
	series                   prometheus.GaugeFunc
	seriesCreated            prometheus.Counter
	seriesRemoved            prometheus.Counter
	seriesNotFound           prometheus.Counter
	chunks                   prometheus.Gauge
	chunksCreated            prometheus.Counter
	chunksRemoved            prometheus.Counter
	gcDuration               prometheus.Summary
	samplesAppended          prometheus.Counter
	outOfBoundSamples        prometheus.Counter
	outOfOrderSamples        prometheus.Counter
	walTruncateDuration      prometheus.Summary
	walCorruptionsTotal      prometheus.Counter
	walTotalReplayDuration   prometheus.Gauge
	headTruncateFail         prometheus.Counter
	headTruncateTotal        prometheus.Counter
	checkpointDeleteFail     prometheus.Counter
	checkpointDeleteTotal    prometheus.Counter
	checkpointCreationFail   prometheus.Counter
	checkpointCreationTotal  prometheus.Counter
	mmapChunkCorruptionTotal prometheus.Counter
}

// 从学习prometheus指标相关库的角度来看源码是比较好的学习资料 !!!
func newHeadMetrics(h *Head, r prometheus.Registerer) *headMetrics {
	m := &headMetrics{
		activeAppenders: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_active_appenders",
			Help: "Number of currently active appender transactions",
		}),
		series: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_series",
			Help: "Total number of series in the head block.",
		}, func() float64 {
			return float64(h.NumSeries())
		}),
		seriesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_created_total",
			Help: "Total number of series created in the head",
		}),
		seriesRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_removed_total",
			Help: "Total number of series removed in the head",
		}),
		seriesNotFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_not_found_total",
			Help: "Total number of requests for series that were not found.",
		}),
		chunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_chunks",
			Help: "Total number of chunks in the head block.",
		}),
		chunksCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_created_total",
			Help: "Total number of chunks created in the head",
		}),
		chunksRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_removed_total",
			Help: "Total number of chunks removed in the head",
		}),
		gcDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_head_gc_duration_seconds",
			Help: "Runtime of garbage collection in the head block.",
		}),
		walTruncateDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_wal_truncate_duration_seconds",
			Help: "Duration of WAL truncation.",
		}),
		walCorruptionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_wal_corruptions_total",
			Help: "Total number of WAL corruptions.",
		}),
		walTotalReplayDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_data_replay_duration_seconds",
			Help: "Time taken to replay the data on disk.",
		}),
		samplesAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_samples_appended_total",
			Help: "Total number of appended samples.",
		}),
		outOfBoundSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_bound_samples_total",
			Help: "Total number of out of bound samples ingestion failed attempts.",
		}),
		outOfOrderSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_order_samples_total",
			Help: "Total number of out of order samples ingestion failed attempts.",
		}),
		headTruncateFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_failed_total",
			Help: "Total number of head truncations that failed.",
		}),
		headTruncateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_total",
			Help: "Total number of head truncations attempted.",
		}),
		checkpointDeleteFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
			Help: "Total number of checkpoint deletions that failed.",
		}),
		checkpointDeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_total",
			Help: "Total number of checkpoint deletions attempted.",
		}),
		checkpointCreationFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_failed_total",
			Help: "Total number of checkpoint creations that failed.",
		}),
		checkpointCreationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_total",
			Help: "Total number of checkpoint creations attempted.",
		}),
		mmapChunkCorruptionTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_mmap_chunk_corruptions_total",
			Help: "Total number of memory-mapped chunk corruptions.",
		}),
	}

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			m.series,
			m.chunks,
			m.chunksCreated,
			m.chunksRemoved,
			m.seriesCreated,
			m.seriesRemoved,
			m.seriesNotFound,
			m.gcDuration,
			m.walTruncateDuration,
			m.walCorruptionsTotal,
			m.walTotalReplayDuration,
			m.samplesAppended,
			m.outOfBoundSamples,
			m.outOfOrderSamples,
			m.headTruncateFail,
			m.headTruncateTotal,
			m.checkpointDeleteFail,
			m.checkpointDeleteTotal,
			m.checkpointCreationFail,
			m.checkpointCreationTotal,
			m.mmapChunkCorruptionTotal,
			// Metrics bound to functions and not needed in tests
			// can be created and registered on the spot.
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_max_time",
				Help: "Maximum timestamp of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MaxTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_min_time",
				Help: "Minimum time bound of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MinTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_low_watermark",
				Help: "The lowest TSDB append ID that is still referenced.",
			}, func() float64 {
				return float64(h.iso.lowWatermark())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_high_watermark",
				Help: "The highest TSDB append ID that has been given out.",
			}, func() float64 {
				return float64(h.iso.lastAppendID())
			}),
		)
	}
	return m
}

const cardinalityCacheExpirationTime = time.Duration(30) * time.Second

// PostingsCardinalityStats returns top 10 highest cardinality stats By label and value names.
func (h *Head) PostingsCardinalityStats(statsByLabelName string) *index.PostingsStats {
	h.cardinalityMutex.Lock()
	defer h.cardinalityMutex.Unlock()
	currentTime := time.Duration(time.Now().Unix()) * time.Second
	seconds := currentTime - h.lastPostingsStatsCall
	if seconds > cardinalityCacheExpirationTime {
		h.cardinalityCache = nil
	}
	if h.cardinalityCache != nil {
		return h.cardinalityCache
	}
	h.cardinalityCache = h.postings.Stats(statsByLabelName)
	h.lastPostingsStatsCall = time.Duration(time.Now().Unix()) * time.Second

	return h.cardinalityCache
}

// NewHead opens the head block in dir.
func NewHead(r prometheus.Registerer, l log.Logger, wal *wal.WAL, opts *HeadOptions) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	if opts.ChunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", opts.ChunkRange)
	}
	if opts.SeriesCallback == nil {
		opts.SeriesCallback = &noopSeriesLifecycleCallback{} // 总是设置为空
	}
	h := &Head{
		wal:        wal,
		logger:     l,
		opts:       opts,
		series:     newStripeSeries(opts.StripeSize, opts.SeriesCallback),
		symbols:    map[string]struct{}{},
		postings:   index.NewUnorderedMemPostings(), // 无序
		tombstones: tombstones.NewMemTombstones(),
		iso:        newIsolation(),
		deleted:    map[uint64]int{},
		memChunkPool: sync.Pool{
			New: func() interface{} {
				return &memChunk{}
			},
		},
	}
	h.chunkRange.Store(opts.ChunkRange)
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	h.lastWALTruncationTime.Store(math.MinInt64)
	h.metrics = newHeadMetrics(h, r)

	if opts.ChunkPool == nil {
		opts.ChunkPool = chunkenc.NewPool()
	}

	var err error
	h.chunkDiskMapper, err = chunks.NewChunkDiskMapper(
		mmappedChunksDir(opts.ChunkDirRoot),
		opts.ChunkPool,
		opts.ChunkWriteBufferSize,
	)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func mmappedChunksDir(dir string) string { return filepath.Join(dir, "chunks_head") }

// processWALSamples adds a partition of samples it receives to the head and passes
// them on to other workers.
// Samples before the mint timestamp are discarded.
func (h *Head) processWALSamples(
	minValidTime int64,
	input <-chan []record.RefSample, output chan<- []record.RefSample,
) (unknownRefs uint64) {
	defer close(output) // 竟然在此关闭outputs ???

	// Mitigate lock contention in getByID.
	refSeries := map[uint64]*memSeries{}

	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

	for samples := range input {
		for _, s := range samples {
			if s.T < minValidTime { // 丢弃时间过小的采样
				continue
			}
			ms := refSeries[s.Ref]
			if ms == nil {
				ms = h.series.getByID(s.Ref)
				if ms == nil { // 丢弃未找到对应refid的采样
					unknownRefs++
					continue
				}
				refSeries[s.Ref] = ms
			}
			if _, chunkCreated := ms.append(s.T, s.V, 0, h.chunkDiskMapper); chunkCreated {
				h.metrics.chunksCreated.Inc()
				h.metrics.chunks.Inc()
			}
			if s.T > maxt {
				maxt = s.T
			}
			if s.T < mint {
				mint = s.T
			}
		}
		output <- samples
	}
	h.updateMinMaxTime(mint, maxt)

	return unknownRefs
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	for {
		// 正常情况下只执行一次
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if h.minTime.CAS(lt, mint) {
			break
		}
	}
	for {
		// 每次都执行
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if h.maxTime.CAS(ht, maxt) {
			break
		}
	}
}

// 装载单个wal文件并不是装载整个wal相关文件(检查点或普通段)
// 函数这么多代码应该进一步拆分 ???
// 内部调用updateMinMaxTime更新区间
func (h *Head) loadWAL(r *wal.Reader, multiRef map[uint64]uint64, mmappedChunks map[uint64][]*mmappedChunk) (err error) {
	// Track number of samples that referenced a series we don't know about
	// for error reporting.
	var unknownRefs atomic.Uint64

	// Start workers that each process samples for a partition of the series ID space.
	// They are connected through a ring of channels which ensures that all sample batches
	// read from the WAL are processed in order.
	var (
		wg      sync.WaitGroup
		n       = runtime.GOMAXPROCS(0)
		inputs  = make([]chan []record.RefSample, n)
		outputs = make([]chan []record.RefSample, n)

		dec    record.Decoder
		shards = make([][]record.RefSample, n)

		decoded                      = make(chan interface{}, 10)
		decodeErr, seriesCreationErr error
		seriesPool                   = sync.Pool{
			New: func() interface{} {
				return []record.RefSeries{}
			},
		}
		samplesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSample{}
			},
		}
		tstonesPool = sync.Pool{
			New: func() interface{} {
				return []tombstones.Stone{}
			},
		}
	)

	defer func() {
		// For CorruptionErr ensure to terminate all workers before exiting.
		_, ok := err.(*wal.CorruptionErr)
		if ok || seriesCreationErr != nil {
			for i := 0; i < n; i++ {
				close(inputs[i])
				for range outputs[i] { // 竟然在processWALSamples中关闭outputs ???
				}
			}
			wg.Wait()
		}
	}()

	wg.Add(n)
	for i := 0; i < n; i++ {
		// 使用300为什么不使用2^x ???
		outputs[i] = make(chan []record.RefSample, 300)
		inputs[i] = make(chan []record.RefSample, 300)

		go func(input <-chan []record.RefSample, output chan<- []record.RefSample) {
			unknown := h.processWALSamples(h.minValidTime.Load(), input, output)
			unknownRefs.Add(unknown)
			wg.Done()
		}(inputs[i], outputs[i])
	}

	go func() {
		defer close(decoded)
		for r.Next() {
			rec := r.Record() // 每次读取一个record
			switch dec.Type(rec) {
			case record.Series:
				// 代码中多次使用此种处理方式节省空间且高效 !!!
				series := seriesPool.Get().([]record.RefSeries)[:0]
				series, err = dec.Series(rec, series)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode series"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- series
			case record.Samples:
				samples := samplesPool.Get().([]record.RefSample)[:0]
				samples, err = dec.Samples(rec, samples)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode samples"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- samples
			case record.Tombstones:
				tstones := tstonesPool.Get().([]tombstones.Stone)[:0]
				tstones, err = dec.Tombstones(rec, tstones)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode tombstones"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- tstones
			default:
				// Noop.
			}
		}
	}()

Outer:
	for d := range decoded {
		switch v := d.(type) {
		case []record.RefSeries:
			for _, s := range v {
				series, created, err := h.getOrCreateWithID(s.Ref, s.Labels.Hash(), s.Labels)
				if err != nil {
					seriesCreationErr = err
					break Outer
				}

				if created {
					// If this series gets a duplicate record, we don't restore its mmapped chunks,
					// and instead restore everything from WAL records.
					// 对于checkpoint中遗留的series不会找到对应的map-check
					series.mmappedChunks = mmappedChunks[series.ref]

					h.metrics.chunks.Add(float64(len(series.mmappedChunks)))
					h.metrics.chunksCreated.Add(float64(len(series.mmappedChunks)))
					// 对于checkpoint中遗留的series不会找到对应的map-check
					// 所以遗留的series只会保留一个空的memSeries
					// 后续切割逻辑触发时会把series清理出内存
					if len(series.mmappedChunks) > 0 {
						h.updateMinMaxTime(series.minTime(), series.maxTime())
					}
				} else {
					// TODO(codesome) Discard old samples and mmapped chunks and use mmap chunks for the new series ID.

					// There's already a different ref for this series.
					multiRef[s.Ref] = series.ref // 记录refid映射
				}

				if h.lastSeriesID.Load() < s.Ref {
					h.lastSeriesID.Store(s.Ref)
				}
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			seriesPool.Put(v)
		case []record.RefSample:
			samples := v
			// We split up the samples into chunks of 5000 samples or less.
			// With O(300 * #cores) in-flight sample batches, large scrapes could otherwise
			// cause thousands of very large in flight buffers occupying large amounts
			// of unused memory.
			for len(samples) > 0 {
				m := 5000 // 感觉5000与300应该没有什么特殊的含义吧:) ???
				if len(samples) < m {
					m = len(samples)
				}
				for i := 0; i < n; i++ {
					var buf []record.RefSample
					select {
					case buf = <-outputs[i]:
					default:
					}
					shards[i] = buf[:0]
				}
				for _, sam := range samples[:m] {
					if r, ok := multiRef[sam.Ref]; ok { // 转换refid
						sam.Ref = r
					}
					mod := sam.Ref % uint64(n)
					shards[mod] = append(shards[mod], sam)
				}
				for i := 0; i < n; i++ {
					inputs[i] <- shards[i]
				}
				samples = samples[m:]
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			// tag作用 ???
			samplesPool.Put(v)
		case []tombstones.Stone:
			for _, s := range v {
				for _, itv := range s.Intervals {
					if itv.Maxt < h.minValidTime.Load() {
						continue
					}
					if m := h.series.getByID(s.Ref); m == nil {
						unknownRefs.Inc()
						continue
					}
					h.tombstones.AddInterval(s.Ref, itv)
				}
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			// 不明白 ???
			tstonesPool.Put(v)
		default:
			panic(fmt.Errorf("unexpected decoded type: %T", d))
		}
	}

	if decodeErr != nil {
		return decodeErr
	}
	if seriesCreationErr != nil {
		// Drain the channel to unblock the goroutine.
		for range decoded {
		}
		return seriesCreationErr
	}

	// Signal termination to each worker and wait for it to close its output channel.
	for i := 0; i < n; i++ {
		close(inputs[i])
		for range outputs[i] { // 竟然在processWALSamples中关闭outputs ???
		}
	}
	wg.Wait()

	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read records")
	}

	if unknownRefs.Load() > 0 {
		level.Warn(h.logger).Log("msg", "Unknown series references", "count", unknownRefs.Load())
	}
	return nil
}

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that it
// limits the ingested samples to the head min valid time.
// - 执行gc()清理超出时间窗口的数据
// - 装载mmappedChunk文件
// - 装载wal文件(先检查点数据,后普通段数据)
func (h *Head) Init(minValidTime int64) error {
	h.minValidTime.Store(minValidTime)
	defer h.postings.EnsureOrder()
	// loadWAL保证区间已更新
	defer h.gc() // After loading the wal remove the obsolete data from the head.

	level.Info(h.logger).Log("msg", "Replaying on-disk memory mappable chunks if any")
	start := time.Now()
	// 装载head_chunks文件
	mmappedChunks, err := h.loadMmappedChunks()
	if err != nil {
		level.Error(h.logger).Log("msg", "Loading on-disk chunks failed", "err", err)
		if _, ok := errors.Cause(err).(*chunks.CorruptionErr); ok {
			h.metrics.mmapChunkCorruptionTotal.Inc()
		}
		// If this fails, data will be recovered from WAL.
		// Hence we wont lose any data (given WAL is not corrupt).
		mmappedChunks = h.removeCorruptedMmappedChunks(err)
	}

	level.Info(h.logger).Log("msg", "On-disk memory mappable chunks replay completed", "duration", time.Since(start).String())
	if h.wal == nil {
		level.Info(h.logger).Log("msg", "WAL not found")
		return nil
	}

	level.Info(h.logger).Log("msg", "Replaying WAL, this may take a while") // 回放wal数据
	// 先回填检查点数据,再回填普通段文件(检查点数据时间早于普通段数据)
	checkpointReplayStart := time.Now()
	// Backfill the checkpoint first if it exists.(回填检查点数据)
	dir, startFrom, err := wal.LastCheckpoint(h.wal.Dir()) // 检查点只有一个
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}
	multiRef := map[uint64]uint64{}
	if err == nil {
		sr, err := wal.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(h.logger).Log("msg", "Error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There's likely little data that can be recovered anyway.
		if err := h.loadWAL(wal.NewReader(sr), multiRef, mmappedChunks); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++ // 索引加一
		level.Info(h.logger).Log("msg", "WAL checkpoint loaded")
	}
	checkpointReplayDuration := time.Since(checkpointReplayStart)

	walReplayStart := time.Now()
	// Find the last segment.
	_, last, err := wal.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.(回填普通段文件数据)
	for i := startFrom; i <= last; i++ {
		s, err := wal.OpenReadSegment(wal.SegmentName(h.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		sr := wal.NewSegmentBufReader(s)
		err = h.loadWAL(wal.NewReader(sr), multiRef, mmappedChunks)
		if err := sr.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "Error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
		level.Info(h.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	walReplayDuration := time.Since(start)
	h.metrics.walTotalReplayDuration.Set(walReplayDuration.Seconds())
	level.Info(h.logger).Log(
		"msg", "WAL replay completed",
		"checkpoint_replay_duration", checkpointReplayDuration.String(),
		"wal_replay_duration", time.Since(walReplayStart).String(),
		"total_replay_duration", walReplayDuration.String(),
	)

	return nil
}

// SetMinValidTime sets the minimum timestamp the head can ingest.
// 没有被调用过 ???
func (h *Head) SetMinValidTime(minValidTime int64) {
	h.minValidTime.Store(minValidTime)
}

// 装载chunks_head目录文件
func (h *Head) loadMmappedChunks() (map[uint64][]*mmappedChunk, error) {
	mmappedChunks := map[uint64][]*mmappedChunk{} // map[seriesRef][]*mmappedChunk{}
	if err := h.chunkDiskMapper.IterateAllChunks(
		func(seriesRef, chunkRef uint64, mint, maxt int64, numSamples uint16) error {
			if maxt < h.minValidTime.Load() {
				return nil
			}
			slice := mmappedChunks[seriesRef] // 一个序列可对应多个mmappedChunk
			if len(slice) > 0 {
				if slice[len(slice)-1].maxTime >= mint {
					return &chunks.CorruptionErr{
						Err: errors.Errorf("out of sequence m-mapped chunk for series ref %d", seriesRef),
					}
				}
			}
			slice = append(
				slice,
				&mmappedChunk{
					ref:        chunkRef,
					minTime:    mint,
					maxTime:    maxt,
					numSamples: numSamples,
				},
			)
			mmappedChunks[seriesRef] = slice
			return nil
		},
	); err != nil {
		return nil, errors.Wrap(err, "iterate on on-disk chunks")
	}
	return mmappedChunks, nil
}

// removeCorruptedMmappedChunks attempts to delete the corrupted mmapped chunks and if it fails, it clears all the previously
// loaded mmapped chunks.
func (h *Head) removeCorruptedMmappedChunks(err error) map[uint64][]*mmappedChunk {
	level.Info(h.logger).Log("msg", "Deleting mmapped chunk files")

	if err := h.chunkDiskMapper.DeleteCorrupted(err); err != nil {
		level.Info(h.logger).Log("msg", "Deletion of mmap chunk files failed, discarding chunk files completely", "err", err)
		return map[uint64][]*mmappedChunk{}
	}

	level.Info(h.logger).Log("msg", "Deletion of mmap chunk files successful, reattempting m-mapping the on-disk chunks")
	mmappedChunks, err := h.loadMmappedChunks()
	if err != nil {
		level.Error(h.logger).Log("msg", "Loading on-disk chunks failed, discarding chunk files completely", "err", err)
		mmappedChunks = map[uint64][]*mmappedChunk{}
	}

	return mmappedChunks
}

// Truncate removes old data before mint from the head and WAL.
func (h *Head) Truncate(mint int64) (err error) {
	initialize := h.MinTime() == math.MaxInt64
	if err := h.truncateMemory(mint); err != nil {
		return err
	}
	if initialize {
		return nil
	}
	return h.truncateWAL(mint)
}

// truncateMemory removes old data before mint from the head.
func (h *Head) truncateMemory(mint int64) (err error) {
	defer func() {
		if err != nil {
			h.metrics.headTruncateFail.Inc()
		}
	}()
	initialize := h.MinTime() == math.MaxInt64

	if h.MinTime() >= mint && !initialize {
		return nil
	}
	h.minTime.Store(mint) // 区间更新
	h.minValidTime.Store(mint)

	// Ensure that max time is at least as high as min time.
	for h.MaxTime() < mint {
		h.maxTime.CAS(h.MaxTime(), mint)
	}

	// This was an initial call to Truncate after loading blocks on startup.
	// We haven't read back the WAL yet, so do not attempt to truncate it.
	if initialize {
		return nil
	}

	h.metrics.headTruncateTotal.Inc()
	start := time.Now()
	// 实际清理内存序列与采样(内存部分)
	actualMint := h.gc()
	level.Info(h.logger).Log("msg", "Head GC completed", "duration", time.Since(start))
	h.metrics.gcDuration.Observe(time.Since(start).Seconds())
	// 注意: actualMint <= h.minTime 情况下h.minTime不会修改(压缩线时刻不变)
	if actualMint > h.minTime.Load() {
		// The actual mint of the Head is higher than the one asked to truncate. ???
		// 在此种情况下最终的h.minValidTime必定落入(mint, h.MaxTime()-h.chunkRange.Load()/2]
		// +-----|---------------------------------------------------------+
		// mint  h.MaxTime()-h.chunkRange.Load()/2                         h.maxTime
		// 此段代码的功能两种解释:
		// - 对于下次启始时间点的优化(跳过部分空白)
		//   不直接使用actualMint可能是因为actualMint紧靠h.maxTime
		// - 历史遗留没有相应处理因为此部分代码没有任何的副作用
		appendableMinValidTime := h.appendableMinValidTime()
		if actualMint < appendableMinValidTime {
			h.minTime.Store(actualMint)
			h.minValidTime.Store(actualMint)
		} else {
			// The actual min time is in the appendable window.
			// So we set the mint to the appendableMinValidTime.
			h.minTime.Store(appendableMinValidTime)
			h.minValidTime.Store(appendableMinValidTime)
		}
	}

	// Truncate the chunk m-mapper.
	// 清除mmap-chunk数据(磁盘部分)
	if err := h.chunkDiskMapper.Truncate(mint); err != nil {
		return errors.Wrap(err, "truncate chunks.HeadReadWriter")
	}
	return nil
}

// truncateWAL removes old data before mint from the WAL.
func (h *Head) truncateWAL(mint int64) error {
	if h.wal == nil || mint <= h.lastWALTruncationTime.Load() {
		return nil
	}
	start := time.Now()
	h.lastWALTruncationTime.Store(mint)

	first, last, err := wal.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}
	// Start a new segment, so low ingestion volume TSDB don't have more WAL than
	// needed.
	// 创建一个新的段文件
	if err := h.wal.NextSegment(); err != nil {
		return errors.Wrap(err, "next segment")
	}
	// 不处理最后一个段文件(也就是新创建的)为了避免读写冲突
	last--        // Never consider last segment for checkpoint.
	if last < 0 { // 当前没有段文件
		return nil // no segments yet.
	}
	// The lower two thirds of segments should contain mostly obsolete samples.
	// If we have less than two segments, it's not worth checkpointing yet.
	// With the default 2h blocks, this will keeping up to around 3h worth
	// of WAL segments.
	// 只处理当前段的前2/3个(至少三个段)
	// 如果取当前wal中所有的段文件能保证处理所有数据但是实际上范围过大因为会多处理最近一小时的数据
	// 取前2/3是与压缩逻辑保持一致(正常情况下wal的前2/3与被压缩的head中的2/3数据量比较接近但并不一致)
	// 所以取前2/3会导致会导致实际处理的wal数据与压缩head数据范围不一致所以引入deleted记录被删除的序列
	// 关于deleted处理的详情参数gc()函数
	last = first + (last-first)*2/3
	if last <= first {
		return nil
	}
	// deleted详情参见Head.gc()函数
	keep := func(id uint64) bool {
		if h.series.getByID(id) != nil {
			return true
		}
		// 可以使用RWMutex
		h.deletedMtx.Lock()
		// 判定deleted
		_, ok := h.deleted[id]
		h.deletedMtx.Unlock()
		return ok
	}
	h.metrics.checkpointCreationTotal.Inc()
	if _, err = wal.Checkpoint(h.logger, h.wal, first, last, keep, mint); err != nil {
		h.metrics.checkpointCreationFail.Inc()
		if _, ok := errors.Cause(err).(*wal.CorruptionErr); ok {
			h.metrics.walCorruptionsTotal.Inc()
		}
		return errors.Wrap(err, "create checkpoint")
	}
	if err := h.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there's a checkpoint
		// that supersedes them.
		level.Error(h.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	// deleted详情参见Head.gc()函数
	h.deletedMtx.Lock()
	for ref, segment := range h.deleted {
		if segment < first { // 此处算法是否可以 segment <= last 就行了 ???
			delete(h.deleted, ref)
		}
	}
	h.deletedMtx.Unlock()

	h.metrics.checkpointDeleteTotal.Inc()
	if err := wal.DeleteCheckpoints(h.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// They will just be ignored since a higher checkpoint exists.
		level.Error(h.logger).Log("msg", "delete old checkpoints", "err", err)
		h.metrics.checkpointDeleteFail.Inc()
	}
	h.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())

	level.Info(h.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))

	return nil
}

// initTime initializes a head with the first timestamp. This only needs to be called
// for a completely fresh head with an empty WAL.
// 设置设置[mint,maxt] mint == maxt
func (h *Head) initTime(t int64) {
	// NewHead中设置minTime=math.MaxInt64,maxTime=math.MinIn64
	// 如果minTime==math.MaxInt64才设置minTime
	if !h.minTime.CAS(math.MaxInt64, t) {
		return
	}
	// Ensure that max time is initialized to at least the min time we just set.
	// Concurrent appenders may already have set it to a higher value.
	// 如果maxTime==math.MinInt64才设置maxTime
	h.maxTime.CAS(math.MinInt64, t)
}

type Stats struct {
	NumSeries         uint64
	MinTime, MaxTime  int64
	IndexPostingStats *index.PostingsStats
}

// Stats returns important current HEAD statistics. Note that it is expensive to
// calculate these.
func (h *Head) Stats(statsByLabelName string) *Stats {
	return &Stats{
		NumSeries:         h.NumSeries(),
		MaxTime:           h.MaxTime(),
		MinTime:           h.MinTime(),
		IndexPostingStats: h.PostingsCardinalityStats(statsByLabelName),
	}
}

// 实现了tsdb.BlockReader接口
type RangeHead struct {
	head       *Head
	mint, maxt int64
}

// tsdb.BlockReader.Size函数定义在head.go最后
// NewRangeHead returns a *RangeHead.
func NewRangeHead(head *Head, mint, maxt int64) *RangeHead {
	return &RangeHead{
		head: head,
		mint: mint,
		maxt: maxt,
	}
}

func (h *RangeHead) Index() (IndexReader, error) {
	// 返回tsdb.headIndexReader实例
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *RangeHead) Chunks() (ChunkReader, error) {
	// 返回tsdb.headChunkReader实例
	return h.head.chunksRange(h.mint, h.maxt, h.head.iso.State())
}

func (h *RangeHead) Tombstones() (tombstones.Reader, error) {
	// 返回tombstones.MemTombstones实例
	return h.head.tombstones, nil
}

// 以为会有什么文章 :(
func (h *RangeHead) MinTime() int64 {
	return h.mint
}

// MaxTime returns the max time of actual data fetch-able from the head.
// This controls the chunks time range which is closed [b.MinTime, b.MaxTime].
// 以为会有什么文章 :(
func (h *RangeHead) MaxTime() int64 {
	return h.maxt
}

// BlockMaxTime returns the max time of the potential block created from this head.
// It's different to MaxTime as we need to add +1 millisecond to block maxt because block
// intervals are half-open: [b.MinTime, b.MaxTime). Block intervals are always +1 than the total samples it includes.
func (h *RangeHead) BlockMaxTime() int64 {
	return h.MaxTime() + 1
}

func (h *RangeHead) NumSeries() uint64 {
	return h.head.NumSeries()
}

func (h *RangeHead) Meta() BlockMeta {
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    h.head.Meta().ULID,
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// String returns an human readable representation of the range head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *RangeHead) String() string {
	return fmt.Sprintf("range head (mint: %d, maxt: %d)", h.MinTime(), h.MaxTime())
}

/*
   # minValidTime = MinInt64
   [ minTime = MaxInt64
   ] maxTime = MinInt64
   + 原点(零)

   初始没有任何数据
   <#]-~~~---+-----------------------------------------------------[> head时间窗口
   <#--~~~---+--------[-----------]---------------------------------> appender.Append一批数据后appender时间容器
   <#--~~~---+--------[-----------]---------------------------------> appender.Comment后head空间窗口

   初始有数据
   <---~~~---+---#----[------------------------]---------------------> head时间窗口
   <---~~~---+---#---------------------------------[-------]---------> appender.Append一批数据后appender时间容器
   <---~~~---+---#----[------------------------------------]---------> appender.Comment后head空间窗口

   执行一次压缩后Head中有mappedchunk或headchunk被保留
   <---~~~---+-------#[------------------------]---------------------> head时间窗口
   <---~~~---+-------#-----------------------------[-------]---------> appender.Append一批数据后appender时间容器
   <---~~~---+-------#[------------------------------------]---------> appender.Comment后head空间窗口

   执行一次压缩后且所有Head数据都被写入Block中
   <---~~~---+---]---#[----------------------------------------------> head时间窗口
   <---~~~---+-------#-----------------------------[-------]---------> appender.Append一批数据后appender时间容器
   <---~~~---+-------#[------------------------------------]---------> appender.Comment后head空间窗口
*/

// 实现了storage.Appender接口
// 只有当data下没有任何历史数据时才会创建initAppender
// initAppender is a helper to initialize the time bounds of the head
// upon the first sample it receives.
type initAppender struct {
	app  storage.Appender // 引用headAppender实例
	head *Head
}

func (a *initAppender) Append(ref uint64, lset labels.Labels, t int64, v float64) (uint64, error) {
	if a.app != nil {
		return a.app.Append(ref, lset, t, v)
	}

	a.head.initTime(t)        // 初始化head时间窗口[minTime, maxTime]
	a.app = a.head.appender() // 设置app
	return a.app.Append(ref, lset, t, v)
}

func (a *initAppender) Commit() error {
	if a.app == nil {
		return nil
	}
	return a.app.Commit()
}

func (a *initAppender) Rollback() error {
	if a.app == nil {
		return nil
	}
	return a.app.Rollback()
}

// Appender returns a new Appender on the database.
func (h *Head) Appender(_ context.Context) storage.Appender {
	h.metrics.activeAppenders.Inc()

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	// 只有当data下没有任何历史数据时才会创建initAppender
	// initAppender主要用于初始化minTime与maxTime
	// NewHead中设置minTime=math.MaxInt64,maxTime=math.MinIn64
	// data目录下有数据情况下会在truncateMemory中设置minTime=maxTime=Blocks.MaxTime
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{
			head: h,
		}
	}
	return h.appender()
}

func (h *Head) appender() *headAppender {
	appendID := h.iso.newAppendID()
	cleanupAppendIDsBelow := h.iso.lowWatermark()

	return &headAppender{
		head:                  h,
		minValidTime:          h.appendableMinValidTime(),
		mint:                  math.MaxInt64,
		maxt:                  math.MinInt64,
		samples:               h.getAppendBuffer(),
		sampleSeries:          h.getSeriesBuffer(),
		appendID:              appendID,
		cleanupAppendIDsBelow: cleanupAppendIDsBelow,
	}
}

func (h *Head) appendableMinValidTime() int64 {
	// Setting the minimum valid time to whichever is greater, the head min valid time or the compaction window,
	// ensures that no samples will be added within the compaction window to avoid races.
	return max(h.minValidTime.Load(), h.MaxTime()-h.chunkRange.Load()/2)
}

// 哈哈
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (h *Head) getAppendBuffer() []record.RefSample {
	b := h.appendPool.Get()
	if b == nil {
		return make([]record.RefSample, 0, 512)
	}
	return b.([]record.RefSample)
}

func (h *Head) putAppendBuffer(b []record.RefSample) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.appendPool.Put(b[:0])
}

func (h *Head) getSeriesBuffer() []*memSeries {
	b := h.seriesPool.Get()
	if b == nil {
		return make([]*memSeries, 0, 512)
	}
	return b.([]*memSeries)
}

func (h *Head) putSeriesBuffer(b []*memSeries) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.seriesPool.Put(b[:0])
}

func (h *Head) getBytesBuffer() []byte {
	b := h.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (h *Head) putBytesBuffer(b []byte) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.bytesPool.Put(b[:0])
}

// 实现了storage.Appender接口
type headAppender struct {
	head         *Head
	minValidTime int64 // No samples below this timestamp are allowed.
	mint, maxt   int64

	series       []record.RefSeries // 序列只有第一次出现才会记录
	samples      []record.RefSample // 采样持续记录
	sampleSeries []*memSeries

	appendID, cleanupAppendIDsBelow uint64 // iso相关后续再分析 ???
	closed                          bool
}

// 记录单条采样数据只写入内存列表
// 两个列表一个记录采样另一个记录序列
// storage.Appender
func (a *headAppender) Append(ref uint64, lset labels.Labels, t int64, v float64) (uint64, error) {
	// 判定是否非法
	if t < a.minValidTime {
		a.head.metrics.outOfBoundSamples.Inc()
		return 0, storage.ErrOutOfBounds
	}

	// 此处理进行series的过滤操作
	s := a.head.series.getByID(ref)
	// series首次出现
	if s == nil {
		// 判定是否有错误标签
		// Ensure no empty labels have gotten through.
		lset = lset.WithoutEmpty()
		if len(lset) == 0 {
			return 0, errors.Wrap(ErrInvalidSample, "empty labelset")
		}
		// 判定是否有错误标签
		if l, dup := lset.HasDuplicateLabelNames(); dup {
			return 0, errors.Wrap(ErrInvalidSample, fmt.Sprintf(`label name "%s" is not unique`, l))
		}

		var created bool
		var err error
		// 存在以下几个问题: ???
		// - 数据还未提交但是lable数据已经在getOrCreate函数中被记录到symbols与postings中
		// - hash与scrapeLoop.append中重复计算
		s, created, err = a.head.getOrCreate(lset.Hash(), lset)
		if err != nil {
			return 0, err
		}
		if created {
			// 只在新写入的序列才会记录
			a.series = append(a.series, record.RefSeries{
				Ref:    s.ref,
				Labels: lset,
			})
		}
	}

	s.Lock()
	// 仅仅判断数据是否合法
	if err := s.appendable(t, v); err != nil {
		s.Unlock()
		if err == storage.ErrOutOfOrderSample {
			a.head.metrics.outOfOrderSamples.Inc()
		}
		return 0, err
	}
	s.pendingCommit = true
	s.Unlock()
	// 调整当前appender的时间区间
	// 正常情况下只执行一次
	if t < a.mint {
		a.mint = t
	}
	// 每次都执行
	if t > a.maxt {
		a.maxt = t
	}
	// samples与sampleSeries是1vs1关系
	// series只有新创建的才会记录
	// 记录采样值
	a.samples = append(a.samples, record.RefSample{
		Ref: s.ref,
		T:   t,
		V:   v,
	})
	// 记录采样对应的序列
	a.sampleSeries = append(a.sampleSeries, s)
	return s.ref, nil
}

// 提交函数中在实际写入前先写入WAL
func (a *headAppender) log() error {
	if a.head.wal == nil {
		return nil
	}
	// 从池中获取一个缓冲区
	buf := a.head.getBytesBuffer()
	defer func() { a.head.putBytesBuffer(buf) }()

	var rec []byte
	var enc record.Encoder
	// 写序列
	// 注: 只有新采集的序列才会写入wal
	//     已经采集过的在head与wal中有记录不会再次写入
	//     除非是序列在压缩时被删除(head与wal都清理了)
	//     后续再被采集到才会被记录
	if len(a.series) > 0 {
		rec = enc.Series(a.series, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log series")
		}
	}
	// 写采样
	if len(a.samples) > 0 {
		rec = enc.Samples(a.samples, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log samples")
		}
	}
	return nil
}

// Commit调用时机是当一个被抓取目标的一次抓取结果执行完Append后立即执行提交
// 也就是代表一个目标的一次抓取对应一次提交
// 提交之前写入数据[按批次]
// 先记录WAL再写入采样对应序列的chunk中
// 因为记录了WAL所以只要成功提交后就保证数据不会丢失
// storage.Appender
func (a *headAppender) Commit() (err error) {
	if a.closed {
		return ErrAppenderClosed
	}
	// 标记为关闭一次提交或回滚后不再使用
	defer func() { a.closed = true }()
	// 记录WAL
	if err := a.log(); err != nil {
		//nolint: errcheck
		// 这个处理看不明白, log已经失败了,再执行Rollback(其内部再次调用log) ???
		a.Rollback() // Most likely the same error will happen again.
		return errors.Wrap(err, "write to WAL")
	}

	defer a.head.metrics.activeAppenders.Dec()
	defer a.head.putAppendBuffer(a.samples)
	defer a.head.putSeriesBuffer(a.sampleSeries)
	defer a.head.iso.closeAppend(a.appendID)

	total := len(a.samples)
	var series *memSeries
	for i, s := range a.samples {
		series = a.sampleSeries[i]
		// 写入采样对应序列的chunk中
		// 序列加锁处理(读数据操作也会加锁保证获取迭代器操作没有并发读写)
		series.Lock()
		ok, chunkCreated := series.append(s.T, s.V, a.appendID, a.head.chunkDiskMapper)
		series.cleanupAppendIDsBelow(a.cleanupAppendIDsBelow)
		series.pendingCommit = false
		series.Unlock()
		// 指标处理
		if !ok {
			total--
			a.head.metrics.outOfOrderSamples.Inc()
		}
		if chunkCreated {
			a.head.metrics.chunks.Inc()
			a.head.metrics.chunksCreated.Inc()
		}
	}
	// 指标处理
	a.head.metrics.samplesAppended.Add(float64(total))
	a.head.updateMinMaxTime(a.mint, a.maxt) // 更新时间区间

	return nil
}

// storage.Appender
func (a *headAppender) Rollback() (err error) {
	if a.closed {
		return ErrAppenderClosed
	}
	// 标记为关闭一次提交或回滚后不再使用
	defer func() { a.closed = true }()
	defer a.head.metrics.activeAppenders.Dec()
	defer a.head.iso.closeAppend(a.appendID)
	defer a.head.putSeriesBuffer(a.sampleSeries)

	var series *memSeries
	for i := range a.samples {
		series = a.sampleSeries[i]
		series.Lock()
		series.cleanupAppendIDsBelow(a.cleanupAppendIDsBelow)
		series.pendingCommit = false
		series.Unlock()
	}
	a.head.putAppendBuffer(a.samples)
	a.samples = nil

	// Series are created in the head memory regardless of rollback. Thus we have
	// to log them to the WAL in any case.
	return a.log()
}

// Delete all samples in the range of [mint, maxt] for series that satisfy the given
// label matchers.
func (h *Head) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	// Do not delete anything beyond the currently valid range.
	mint, maxt = clampInterval(mint, maxt, h.MinTime(), h.MaxTime())

	ir := h.indexRange(mint, maxt)

	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return errors.Wrap(err, "select series")
	}

	var stones []tombstones.Stone
	for p.Next() {
		series := h.series.getByID(p.At())

		series.RLock()
		t0, t1 := series.minTime(), series.maxTime()
		series.RUnlock()
		if t0 == math.MinInt64 || t1 == math.MinInt64 {
			continue
		}
		// Delete only until the current values and not beyond.
		t0, t1 = clampInterval(mint, maxt, t0, t1)
		stones = append(stones, tombstones.Stone{Ref: p.At(), Intervals: tombstones.Intervals{{Mint: t0, Maxt: t1}}})
	}
	if p.Err() != nil {
		return p.Err()
	}
	// 处理wal
	if h.wal != nil {
		var enc record.Encoder
		if err := h.wal.Log(enc.Tombstones(stones, nil)); err != nil {
			return err
		}
	}
	// 处理head内存
	for _, s := range stones {
		h.tombstones.AddInterval(s.Ref, s.Intervals[0])
	}

	return nil
}

// 清理超出驻留时间的数据,其中处理压缩线逻辑
// gc removes data before the minimum timestamp from the head.
// It returns the actual min times of the chunks present in the Head.
func (h *Head) gc() int64 {
	// Only data strictly lower than this timestamp must be deleted.
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	// 截断所有chunks包括headChunk(压缩也会处理headChunk)都小于minTime的serie应该被删除
	deleted, chunksRemoved, actualMint := h.series.gc(mint)
	seriesRemoved := len(deleted)
	// 指标记录
	h.metrics.seriesRemoved.Add(float64(seriesRemoved))
	h.metrics.chunksRemoved.Add(float64(chunksRemoved))
	h.metrics.chunks.Sub(float64(chunksRemoved))
	h.numSeries.Sub(uint64(seriesRemoved))

	// Remove deleted series IDs from the postings lists.
	// 删除所有采样已经落地磁盘的序列
	h.postings.Delete(deleted)

	if h.wal != nil {
		_, last, _ := wal.Segments(h.wal.Dir())
		h.deletedMtx.Lock()
		// Keep series records until we're past segment 'last'
		// because the WAL will still have samples records with
		// this ref ID. If we didn't keep these series records then
		// on start up when we replay the WAL, or any other code
		// that reads the WAL, wouldn't be able to use those
		// samples since we would have no labels for that ref ID.
		//
		// 每次执行压缩操作时如果某个序列的所有数据(map-chunk与head-chunk)都被压缩入block文件
		// 那么此序列会被清理出内存,在同一次压缩操作中的后续checkpoint生成过程中可以删除wal中的序列
		// 但是每次生成checkpoint只会取前2/3的段文件所以导致驻留的段文件中可能会被删除序列对应的采样数据
		// 如果直接删除序列且prometheus重启(异常崩溃或人工重启)在回放wal的过程导致采样无法找到对应的序列
		// 所以使用deleted记录被删除的序列保证不会出现采样无法对应序列的情况,但是序列什么时候删除呢?
		// 就是在当前最后一个驻留的段文件被处理完后再删除deleted中的序列,因为被保存的采样不会直接最后一个段文件
		// 样例:
		// [s1, s2, s3], s4, s5     first=s1, last=s3, deleted[refid] = s5
		// [c, s4, s5], s6, s7      first=s4, last=s5, deleted[refid] = s7  (s5被压缩,可以删除refid<=s5的序列)
		// [c, s6, s7, s8], s9, s10 first=s4, last=s5, deleted[refid] = s10 (s7被压缩,可以删除refid<=s7的序列)
		// 疑问: ???
		// 标签与采样保持逻辑同步没有问题但是没有实际意义,因为当压缩数据已经写入到block中后,wal中遗留的采样
		// 在重放数据时不应该再重新被写入了,如果再写入head中是错误的,而在数据重放的逻辑也确实时过滤了采样
		// 但是会将序列写入(但是对应的采样为空),而这个无效的序列会在下一次压缩时被清理掉
		// 但是如果不做deleted记录而是在压缩数据时对于被清理也内存的序列在创建checkpoint时直接将其同步删除
		// (也就是不保持标签与采样的逻辑同步)根据当前重放wal的代码逻辑也不会有问题,采样会根据minValidTime
		// 被过滤掉(参见head.processWALSamples)
		for ref := range deleted {
			h.deleted[ref] = last // 创建checkpoint使用
		}
		h.deletedMtx.Unlock()
	}

	// Rebuild symbols and label value indices from what is left in the postings terms.
	// symMtx ensures that append of symbols and postings is disabled for rebuild time.
	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	symbols := make(map[string]struct{}, len(h.symbols))
	if err := h.postings.Iter(func(l labels.Label, _ index.Postings) error {
		symbols[l.Name] = struct{}{}
		symbols[l.Value] = struct{}{}
		return nil
	}); err != nil {
		// This should never happen, as the iteration function only returns nil.
		panic(err)
	}
	h.symbols = symbols

	return actualMint
}

// Tombstones returns a new reader over the head's tombstones
func (h *Head) Tombstones() (tombstones.Reader, error) {
	return h.tombstones, nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64, h.iso.State())
}

func (h *Head) chunksRange(mint, maxt int64, is *isolationState) (*headChunkReader, error) {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	if h.closed {
		return nil, errors.New("can't read from a closed head")
	}
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headChunkReader{
		head:     h,
		mint:     mint,
		maxt:     maxt,
		isoState: is,
	}, nil
}

// NumSeries returns the number of active series in the head.
func (h *Head) NumSeries() uint64 {
	return h.numSeries.Load()
}

// Meta returns meta information about the head.
// The head is dynamic so will return dynamic results.
func (h *Head) Meta() BlockMeta {
	var id [16]byte
	copy(id[:], "______head______")
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    ulid.ULID(id),
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return h.minTime.Load()
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return h.maxTime.Load()
}

// compactable returns whether the head has a compactable range.
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
func (h *Head) compactable() bool {
	return h.MaxTime()-h.MinTime() > h.chunkRange.Load()/2*3
}

// Close flushes the WAL and closes the head.
func (h *Head) Close() error {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	h.closed = true
	errs := tsdb_errors.NewMulti(h.chunkDiskMapper.Close())
	if h.wal != nil {
		errs.Add(h.wal.Close())
	}
	return errs.Err()
}

// String returns an human readable representation of the TSDB head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *Head) String() string {
	return "head"
}

// 实现了tsdb.ChunkReader接口
type headChunkReader struct {
	head       *Head
	mint, maxt int64
	isoState   *isolationState
}

func (h *headChunkReader) Close() error {
	h.isoState.Close()
	return nil
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panicks if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
func packChunkID(seriesID, chunkID uint64) uint64 {
	if seriesID > (1<<40)-1 {
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 {
		panic("chunk ID exceeds 3 bytes")
	}
	return (seriesID << 24) | chunkID
}

func unpackChunkID(id uint64) (seriesID, chunkID uint64) {
	return id >> 24, (id << 40) >> 40
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	sid, cid := unpackChunkID(ref)

	s := h.head.series.getByID(sid)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, storage.ErrNotFound
	}

	s.Lock()
	c, garbageCollect, err := s.chunk(int(cid), h.head.chunkDiskMapper)
	if err != nil {
		s.Unlock() // defer s.Unlock() ???
		return nil, err
	}
	defer func() {
		if garbageCollect {
			// Set this to nil so that Go GC can collect it after it has been used.
			c.chunk = nil
			s.memChunkPool.Put(c)
		}
	}()

	// This means that the chunk is outside the specified range.
	if !c.OverlapsClosedInterval(h.mint, h.maxt) {
		s.Unlock() // defer s.Unlock() ???
		return nil, storage.ErrNotFound
	}
	s.Unlock()

	return &safeChunk{
		Chunk:           c.chunk,
		s:               s,
		cid:             int(cid),
		isoState:        h.isoState,
		chunkDiskMapper: h.head.chunkDiskMapper,
	}, nil
}

// headChunk压缩时不是并发安全(具体细节后续再研究) ???
type safeChunk struct {
	chunkenc.Chunk  // tsdb.memChunk
	s               *memSeries
	cid             int
	isoState        *isolationState
	chunkDiskMapper *chunks.ChunkDiskMapper
}

func (c *safeChunk) Iterator(reuseIter chunkenc.Iterator) chunkenc.Iterator {
	// +----------+--------------------------------+-----------+
	// | 总数(2b) |          已写入数据            |  新数据   |
	// +----------+--------------------------------+-----------+
	// 数据锁定后不会再有数据写入
	// Iter通过记录总数限定迭代数据不会超过"已写入数据"部分
	// 后续解锁后新数据写入只会操作"新数据"部分
	// 所以并发读写的缓冲区中不同的部分也就没有并发问题
	// 有效解决了并发读写值得借鉴 !!!
	c.s.Lock()
	it := c.s.iterator(c.cid, c.isoState, c.chunkDiskMapper, reuseIter)
	c.s.Unlock()
	return it
}

// 实现了tsdb.IndexReader接口
type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() index.StringIter {
	h.head.symMtx.RLock()
	res := make([]string, 0, len(h.head.symbols))
	// 实际使用中这个表估计会很大,标签名不会太多,但是标签值太多了 ???
	for s := range h.head.symbols {
		res = append(res, s)
	}
	h.head.symMtx.RUnlock()

	sort.Strings(res)
	return index.NewStringListIter(res) // 实际index.stringListIter
}

// SortedLabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := h.LabelValues(name, matchers...)
	if err == nil {
		sort.Strings(values)
	}
	return values, err
}

// LabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	// !(h.maxt >= h.head.MinTime() && h.mint <= head.MaxTime())
	// 已经出现至少三次可以封装成函数 ???
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		return []string{}, nil
	}
	// 不需要匹配直接返回所有值
	if len(matchers) == 0 {
		h.head.symMtx.RLock()
		defer h.head.symMtx.RUnlock()
		return h.head.postings.LabelValues(name), nil
	}

	return labelValuesWithMatchers(h, name, matchers...)
}

// LabelNames returns all the unique label names present in the head
// that are within the time range mint to maxt.
func (h *headIndexReader) LabelNames() ([]string, error) {
	h.head.symMtx.RLock()
	// !(h.maxt >= h.head.MinTime() && h.mint <= head.MaxTime())
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		h.head.symMtx.RUnlock()
		return []string{}, nil
	}

	labelNames := h.head.postings.LabelNames()
	h.head.symMtx.RUnlock()

	sort.Strings(labelNames)
	return labelNames, nil
}

// Postings returns the postings list iterator for the label pairs.
func (h *headIndexReader) Postings(name string, values ...string) (index.Postings, error) {
	res := make([]index.Postings, 0, len(values))
	for _, value := range values {
		res = append(res, h.head.postings.Get(name, value))
	}
	return index.Merge(res...), nil // 实际index.mergedPostings
}

// 排序规则参见lables.Compare函数
func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	for p.Next() {
		s := h.head.series.getByID(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "Looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	ep := make([]uint64, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

// lbls,chks使用指针还不如直接使用返回值
// chks = [
//   seriesId(5)|firstChunkId(3),
//   ...,
//   seriesId(5)|firstChunkId+len(mmap-chunk)(3),
//   seriesId(5)|headChunkId
// ]
// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint64, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := h.head.series.getByID(ref)

	if s == nil {
		h.head.metrics.seriesNotFound.Inc()
		return storage.ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]
	// 获取mmap-chunk
	for i, c := range s.mmappedChunks {
		// Do not expose chunks that are outside of the specified range.
		// 有重叠的mmapChunk也会被处理
		if !c.OverlapsClosedInterval(h.mint, h.maxt) {
			continue
		}
		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     packChunkID(s.ref, uint64(s.chunkID(i))), // 只使用了Ref
		})
	}
	// 如果headChunk与时间区间重叠需要在压缩时一并处理headChunk ???
	if s.headChunk != nil && s.headChunk.OverlapsClosedInterval(h.mint, h.maxt) {
		*chks = append(*chks, chunks.Meta{
			MinTime: s.headChunk.minTime,
			MaxTime: math.MaxInt64,                                               // Set the head chunks as open (being appended to).
			Ref:     packChunkID(s.ref, uint64(s.chunkID(len(s.mmappedChunks)))), // 只使用了Ref
		})
	}

	return nil
}

// LabelValueFor returns label value for the given label name in the series referred to by ID.
func (h *headIndexReader) LabelValueFor(id uint64, label string) (string, error) {
	memSeries := h.head.series.getByID(id)
	if memSeries == nil {
		return "", storage.ErrNotFound
	}

	value := memSeries.lset.Get(label)
	if value == "" {
		return "", storage.ErrNotFound
	}

	return value, nil
}

// hash = labels.Labels.Hash()
func (h *Head) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	// Just using `getOrSet` below would be semantically sufficient, but we'd create
	// a new series on every sample inserted via Add(), which causes allocations
	// and makes our series IDs rather random and harder to compress in postings.
	s := h.series.getByHash(hash, lset)
	if s != nil {
		return s, false, nil
	}

	// Optimistically assume that we are the first one to create the series.
	// 其实就是单调递增
	id := h.lastSeriesID.Inc()

	return h.getOrCreateWithID(id, hash, lset)
}

func (h *Head) getOrCreateWithID(id, hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	// 不明白为什么不先判断是否已经存在而是先创建一个新的 ???
	// 代码应该是一开始这样写的,但是后续发现了问题并在getOrCreate中增加了提前的判断
	s := newMemSeries(lset, id, h.chunkRange.Load(), &h.memChunkPool)

	s, created, err := h.series.getOrSet(hash, s)
	if err != nil {
		return nil, false, err
	}
	if !created {
		return s, false, nil
	}

	h.metrics.seriesCreated.Inc()
	h.numSeries.Inc()

	h.symMtx.Lock()
	defer h.symMtx.Unlock()
	// 写入符号表
	for _, l := range lset {
		h.symbols[l.Name] = struct{}{}
		h.symbols[l.Value] = struct{}{}
	}
	// 写入posting(包括allPostingsKey)
	h.postings.Add(id, lset)
	return s, true, nil
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries // k值为hash

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if labels.Equal(prev.lset, s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	// 删除时效率不高(不过在实际使用中应该很少有删除的情况)
	for _, s := range m[hash] {
		if !labels.Equal(s.lset, lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

const (
	// DefaultStripeSize is the default number of entries to allocate in the stripeSeries hash map.
	DefaultStripeSize = 1 << 14 // 16K
)

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
type stripeSeries struct {
	size int
	// series/hashes/locks的i值为id计算的hash(id&uint64(s.size-1))
	// [id&uint64(s.size-1)]map[id]*memSeries
	series []map[uint64]*memSeries // k值为id,主要用于存储(猜测)
	// [id&uint64(s.size-1)]map[labels.Lables.Hash()]*memSeries
	hashes                  []seriesHashmap // k值为labels的hash(labels.Lables.Hash()),主要用于查询(猜测)
	locks                   []stripeLock
	seriesLifecycleCallback SeriesLifecycleCallback
}

type stripeLock struct {
	// 以下是标准库中锁的定义
	// type Mutex struct {
	//     key int32;
	//     sema int32;
	// }
	// type RWMutex struct {
	//     w           Mutex  // held if there are pending writers
	//     writerSem   uint32 // semaphore for writers to wait for completing readers
	//     readerSem   uint32 // semaphore for readers to wait for completing writers
	//     readerCount int32  // number of pending readers
	//     readerWait  int32  // number of departing readers
	// }
	sync.RWMutex // 24byte
	// Padding to avoid multiple locks being on the same cache line.
	// 提升效率避免乒乓效应 !!!
	// https://juejin.cn/post/6844903779276439560
	_ [40]byte
}

// stripeSize = 16384(16K)
func newStripeSeries(stripeSize int, seriesCallback SeriesLifecycleCallback) *stripeSeries {
	s := &stripeSeries{
		size:                    stripeSize,
		series:                  make([]map[uint64]*memSeries, stripeSize),
		hashes:                  make([]seriesHashmap, stripeSize),
		locks:                   make([]stripeLock, stripeSize),
		seriesLifecycleCallback: seriesCallback,
	}

	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
func (s *stripeSeries) gc(mint int64) (map[uint64]struct{}, int, int64) {
	var (
		deleted                  = map[uint64]struct{}{}
		deletedForCallback       = []labels.Labels{}
		rmChunks                 = 0
		actualMint         int64 = math.MaxInt64
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	for i := 0; i < s.size; i++ {
		s.locks[i].Lock()

		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()
				rmChunks += series.truncateChunksBefore(mint)

				// 还有chunk数据大(等)于mint的series保留
				if len(series.mmappedChunks) > 0 || series.headChunk != nil || series.pendingCommit {
					seriesMint := series.minTime()
					if seriesMint < actualMint { // 查找所有序列中的最小左边界
						actualMint = seriesMint
					}
					series.Unlock()
					continue
				}

				// 所有chunks(包括headChunk)都小于minTime的serie应该被删除
				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				// 怎么搞成int,不是uint64吗 ???
				j := int(series.ref) & (s.size - 1)
				// 此处需要设计index因为两个列表使用的算法不同
				// - hash & uint64(s.size-1)
				// - series.ref & uint64(s.size-1)
				if i != j {
					s.locks[j].Lock()
				}
				// 记录所有sample.t < mint的series删除
				// 从series/hashes中删除series
				deleted[series.ref] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)
				deletedForCallback = append(deletedForCallback, series.lset)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()

		s.seriesLifecycleCallback.PostDeletion(deletedForCallback...)
		deletedForCallback = deletedForCallback[:0]
	}

	if actualMint == math.MaxInt64 {
		actualMint = mint
	}

	return deleted, rmChunks, actualMint
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, series *memSeries) (*memSeries, bool, error) {
	// PreCreation is called here to avoid calling it inside the lock.
	// It is not necessary to call it just before creating a series,
	// rather it gives a 'hint' whether to create a series or not.
	createSeriesErr := s.seriesLifecycleCallback.PreCreation(series.lset)

	i := hash & uint64(s.size-1)
	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, series.lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false, nil
	}
	if createSeriesErr == nil {
		s.hashes[i].set(hash, series)
	}
	s.locks[i].Unlock()

	if createSeriesErr != nil {
		// The callback prevented creation of series.
		return nil, false, createSeriesErr
	}
	// Setting the series in the s.hashes marks the creation of series
	// as any further calls to this methods would return that series.
	s.seriesLifecycleCallback.PostCreation(series.lset)

	i = series.ref & uint64(s.size-1)

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true, nil
}

type sample struct {
	t int64
	v float64
}

func newSample(t int64, v float64) tsdbutil.Sample { return sample{t, v} }
func (s sample) T() int64                          { return s.t }
func (s sample) V() float64                        { return s.v }

// memSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
type memSeries struct {
	sync.RWMutex

	ref           uint64
	lset          labels.Labels
	mmappedChunks []*mmappedChunk
	/*
	                 +--------+---------+---------+---------+
	   mmappedChunk  + chunk1 | chunk2  | chunk3  |   head  |
	                 +---+----+----+----+----+----+---------+
	                     |         |         |
	              +------+         | +-------+
	           +--+----------------+ |
	           |  |                  |
	           |  |  +------------+  |  +------------+  -+
	           |  |  |    head    |  |  |    head    |   |
	           |  |  +------------+  |  +------------+   |
	           |  +--+   chunk1   |  +--+   chunk3   |   +-- 128M
	           |     +------------+     +------------+   |
	           +-----+   chunk2   |                      |
	                 +------------+                     -+
	*/
	headChunk    *memChunk
	chunkRange   int64
	firstChunkID int // 每次截断会增加被删除的chunk个数,所以是单调递增

	nextAt        int64     // Timestamp at which to cut the next chunk.
	sampleBuf     [4]sample // 记录最新写入headChunk的4个采样[具体作用还没有看明白] ???
	pendingCommit bool      // Whether there are samples waiting to be committed to this series.
	// 每次切割新的headChunk会赋值一个新的chunkenc.xorAppender
	app chunkenc.Appender // Current appender for the chunk.

	memChunkPool *sync.Pool

	txs *txRing
}

func newMemSeries(lset labels.Labels, id uint64, chunkRange int64, memChunkPool *sync.Pool) *memSeries {
	s := &memSeries{
		lset:         lset,
		ref:          id,
		chunkRange:   chunkRange,
		nextAt:       math.MinInt64,
		txs:          newTxRing(4),
		memChunkPool: memChunkPool,
	}
	return s
}

func (s *memSeries) minTime() int64 {
	if len(s.mmappedChunks) > 0 {
		return s.mmappedChunks[0].minTime
	}
	if s.headChunk != nil {
		return s.headChunk.minTime
	}
	return math.MinInt64
}

func (s *memSeries) maxTime() int64 {
	c := s.head()
	if c == nil {
		return math.MinInt64
	}
	return c.maxTime
}

// 创建新的Chunk
func (s *memSeries) cutNewHeadChunk(mint int64, chunkDiskMapper *chunks.ChunkDiskMapper) *memChunk {
	s.mmapCurrentHeadChunk(chunkDiskMapper)

	s.headChunk = &memChunk{
		chunk:   chunkenc.NewXORChunk(),
		minTime: mint,
		maxTime: math.MinInt64, // append中会调整
	}

	// Set upper bound on when the next chunk must be started. An earlier timestamp
	// may be chosen dynamically at a later point.
	// 设置下一个切割点(chunkRange默认2h)
	s.nextAt = rangeForTimestamp(mint, s.chunkRange)
	// 初始化appender(chunkenc.xorAppender类型)
	app, err := s.headChunk.chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return s.headChunk
}

func (s *memSeries) mmapCurrentHeadChunk(chunkDiskMapper *chunks.ChunkDiskMapper) {
	if s.headChunk == nil {
		// There is no head chunk, so nothing to m-map here.
		return
	}
	// s.ref单调递增
	chunkRef, err := chunkDiskMapper.WriteChunk(s.ref, s.headChunk.minTime, s.headChunk.maxTime, s.headChunk.chunk)
	if err != nil {
		if err != chunks.ErrChunkDiskMapperClosed {
			panic(err)
		}
	}
	s.mmappedChunks = append(s.mmappedChunks, &mmappedChunk{
		ref:        chunkRef, // 高4位为文件编号,低4位为文件内偏移
		numSamples: uint16(s.headChunk.chunk.NumSamples()),
		minTime:    s.headChunk.minTime,
		maxTime:    s.headChunk.maxTime,
	})
}

// appendable checks whether the given sample is valid for appending to the series.
func (s *memSeries) appendable(t int64, v float64) error {
	c := s.head()
	if c == nil {
		return nil
	}

	if t > c.maxTime {
		return nil
	}
	if t < c.maxTime {
		return storage.ErrOutOfOrderSample
	}
	// We are allowing exact duplicates as we can encounter them in valid cases
	// like federation and erroring out at that time would be extremely noisy.
	if math.Float64bits(s.sampleBuf[3].v) != math.Float64bits(v) {
		return storage.ErrDuplicateSampleForTimestamp
	}
	return nil
}

// chunk returns the chunk for the chunk id from memory or by m-mapping it from the disk.
// If garbageCollect is true, it means that the returned *memChunk
// (and not the chunkenc.Chunk inside it) can be garbage collected after it's usage.
func (s *memSeries) chunk(id int, chunkDiskMapper *chunks.ChunkDiskMapper) (chunk *memChunk, garbageCollect bool, err error) {
	// ix represents the index of chunk in the s.mmappedChunks slice. The chunk id's are
	// incremented by 1 when new chunk is created, hence (id - firstChunkID) gives the slice index.
	// The max index for the s.mmappedChunks slice can be len(s.mmappedChunks)-1, hence if the ix
	// is len(s.mmappedChunks), it represents the next chunk, which is the head chunk.
	ix := id - s.firstChunkID
	if ix < 0 || ix > len(s.mmappedChunks) {
		return nil, false, storage.ErrNotFound
	}
	if ix == len(s.mmappedChunks) {
		if s.headChunk == nil {
			return nil, false, errors.New("invalid head chunk")
		}
		return s.headChunk, false, nil
	}
	chk, err := chunkDiskMapper.Chunk(s.mmappedChunks[ix].ref)
	if err != nil {
		if _, ok := err.(*chunks.CorruptionErr); ok {
			panic(err)
		}
		return nil, false, err
	}
	mc := s.memChunkPool.Get().(*memChunk)
	mc.chunk = chk
	mc.minTime = s.mmappedChunks[ix].minTime
	mc.maxTime = s.mmappedChunks[ix].maxTime
	return mc, true, nil
}

// pos实际是当前index
func (s *memSeries) chunkID(pos int) int {
	return pos + s.firstChunkID
}

// truncateChunksBefore removes all chunks from the series that
// have no timestamp at or after mint.
// Chunk IDs remain unchanged.
func (s *memSeries) truncateChunksBefore(mint int64) (removed int) {
	// 只有整个head-chunk小于压缩时间才会将其清理出内存
	// 但在压缩处理时如果head-chunk有重叠会对head-chunk进行压缩(通过tombstones处理)
	// 并调整head的mint与minValidTime为压缩时间(保证已被压缩的数据从head查询不到)
	// 有可能出现headChunk与压缩区间完全重叠且被压缩的情况
	// 如: 某series数据抓取到某时刻后没有数据了(网络不通/抓取目标程序崩溃)
	if s.headChunk != nil && s.headChunk.maxTime < mint {
		// If head chunk is truncated, we can truncate all mmapped chunks.
		removed = 1 + len(s.mmappedChunks)
		s.firstChunkID += removed
		s.headChunk = nil
		s.mmappedChunks = nil
		return removed
	}
	if len(s.mmappedChunks) > 0 {
		for i, c := range s.mmappedChunks {
			// 只有整个map-chunk小于压缩时间才会将其清理出内存
			// 但在压缩处理时如果map-chunk有重叠会对map-chunk进行压缩(通过tombstones处理)
			// 并调整head的mint与minValidTime为压缩时间(保证已被压缩的数据从head查询不到)
			// 未重叠部分会在下次压缩被处理
			if c.maxTime >= mint {
				break
			}
			removed = i + 1 // c.maxTime < mint
		}
		s.mmappedChunks = append(s.mmappedChunks[:0], s.mmappedChunks[removed:]...)
		s.firstChunkID += removed
	}
	return removed
}

// 写入采样数据,其中处理切割线逻辑
// append adds the sample (t, v) to the series. The caller also has to provide
// the appendID for isolation. (The appendID can be zero, which results in no
// isolation for this append.)
// It is unsafe to call this concurrently with s.iterator(...) without holding the series lock.
func (s *memSeries) append(t int64, v float64, appendID uint64, chunkDiskMapper *chunks.ChunkDiskMapper) (sampleInOrder, chunkCreated bool) {
	// Based on Gorilla white papers this offers near-optimal compression ratio
	// so anything bigger that this has diminishing returns and increases
	// the time range within which we have to decompress all samples.
	// 假定每分钟采样一次,2小时采样120次
	const samplesPerChunk = 120

	c := s.head() // 获取head-chunk

	// head-chunk为nil
	if c == nil {
		// 如果有map-chunk代表是应用重启后序列的首批采样
		if len(s.mmappedChunks) > 0 && s.mmappedChunks[len(s.mmappedChunks)-1].maxTime >= t {
			// Out of order sample. Sample timestamp is already in the mmaped chunks, so ignore it.
			return false, false
		}
		// There is no chunk in this series yet, create the first chunk for the sample.
		// 如果没有map-chunk代表是应用首次启动的首批采样
		c = s.cutNewHeadChunk(t, chunkDiskMapper)
		chunkCreated = true
	}
	numSamples := c.chunk.NumSamples()
	// fmt.Printf("%s = %d\n", s.lset.String(), numSamples) // for debug ???

	// Out of order sample.
	if c.maxTime >= t {
		return false, chunkCreated
	}
	// If we reach 25% of a chunk's desired sample count, set a definitive time
	// at which to start the next chunk.
	// At latest it must happen at the timestamp set when the chunk was cut.
	//
	// 根据采样频率动态调整切割线位置(1/4)
	// 采样频率越高切割越频繁反之切割频率越低
	// 但是切割最大时间窗口不会大于chunkRange(当前硬编码2h)
	// 按此逻辑来看我们实际生产中所使用的采样频率来计算实际的切割触发都会少于2h
	// 时间窗口区间 [mint, mint+2h] 相当于只能等比缩小不能等比放大
	//
	// 例如:
	// 采样频率5s来计算切割频率大约为10m
	// t1 = 5s * 30 = 150 = 2.5m
	// t2 = t1 * 4 = 2.5m * 4 = 10m
	// 采样频率30s来计算切割频率大约为1h
	// t1 = 30s * 30 = 900 = 15m
	// t2 = t1 * 4 = 15m * 4 = 1h
	// 采样频率90s来计算切割频率大约为3h
	// t1 = 90s * 30 = 2700 = 45m
	// t2 = t1 * 4 = 45m * 4 = 2h (不能大于2h)
	//
	// 采用此算法可以保证每次切割有相同的采样个数(120个采样),无论采样周期是多少
	// 而每个map-chunk中保存120个采样在后续(解)压缩时有更好高的效率(具体细节看论文)
	//
	if numSamples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.nextAt)
	}
	if t >= s.nextAt {
		// 每次cut会重新设置s.nextAt=rangeForTimestamp(mint, s.chunkRange)
		c = s.cutNewHeadChunk(t, chunkDiskMapper)
		chunkCreated = true
	}
	// 添加数据
	// app在cutNewHeadChunk中被赋值类型是chunkenc.xorAppender
	s.app.Append(t, v)

	c.maxTime = t // 代表当前头采样的最新时间
	// 此处与事务隔离有关还没有具体分析 ???
	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	if appendID > 0 {
		s.txs.add(appendID)
	}

	return true, chunkCreated
}

// cleanupAppendIDsBelow cleans up older appendIDs. Has to be called after
// acquiring lock.
func (s *memSeries) cleanupAppendIDsBelow(bound uint64) {
	s.txs.cleanupAppendIDsBelow(bound)
}

// computeChunkEndTime estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

// iterator returns a chunk iterator.
// It is unsafe to call this concurrently with s.append(...) without holding the series lock.
func (s *memSeries) iterator(id int, isoState *isolationState, chunkDiskMapper *chunks.ChunkDiskMapper, it chunkenc.Iterator) chunkenc.Iterator {
	c, garbageCollect, err := s.chunk(id, chunkDiskMapper)
	// TODO(fabxc): Work around! An error will be returns when a querier have retrieved a pointer to a
	// series's chunk, which got then garbage collected before it got
	// accessed.  We must ensure to not garbage collect as long as any
	// readers still hold a reference.
	if err != nil {
		return chunkenc.NewNopIterator()
	}
	defer func() {
		if garbageCollect {
			// Set this to nil so that Go GC can collect it after it has been used.
			// This should be done always at the end.
			c.chunk = nil
			s.memChunkPool.Put(c)
		}
	}()

	ix := id - s.firstChunkID

	numSamples := c.chunk.NumSamples()
	stopAfter := numSamples

	if isoState != nil {
		totalSamples := 0    // Total samples in this series.
		previousSamples := 0 // Samples before this chunk.

		for j, d := range s.mmappedChunks {
			totalSamples += int(d.numSamples)
			if j < ix {
				previousSamples += int(d.numSamples)
			}
		}

		if s.headChunk != nil {
			totalSamples += s.headChunk.chunk.NumSamples()
		}

		// Removing the extra transactionIDs that are relevant for samples that
		// come after this chunk, from the total transactionIDs.
		appendIDsToConsider := s.txs.txIDCount - (totalSamples - (previousSamples + numSamples))

		// Iterate over the appendIDs, find the first one that the isolation state says not
		// to return.
		it := s.txs.iterator()
		for index := 0; index < appendIDsToConsider; index++ {
			appendID := it.At()
			if appendID <= isoState.maxAppendID { // Easy check first.
				if _, ok := isoState.incompleteAppends[appendID]; !ok {
					it.Next()
					continue
				}
			}
			stopAfter = numSamples - (appendIDsToConsider - index)
			if stopAfter < 0 {
				stopAfter = 0 // Stopped in a previous chunk.
			}
			break
		}
	}

	if stopAfter == 0 {
		return chunkenc.NewNopIterator()
	}

	if id-s.firstChunkID < len(s.mmappedChunks) {
		if stopAfter == numSamples {
			return c.chunk.Iterator(it)
		}
		if msIter, ok := it.(*stopIterator); ok {
			msIter.Iterator = c.chunk.Iterator(msIter.Iterator)
			msIter.i = -1
			msIter.stopAfter = stopAfter
			return msIter
		}
		return &stopIterator{
			Iterator:  c.chunk.Iterator(it),
			i:         -1,
			stopAfter: stopAfter,
		}
	}
	// Serve the last 4 samples for the last chunk from the sample buffer
	// as their compressed bytes may be mutated by added samples.
	if msIter, ok := it.(*memSafeIterator); ok {
		msIter.Iterator = c.chunk.Iterator(msIter.Iterator)
		msIter.i = -1
		msIter.total = numSamples
		msIter.stopAfter = stopAfter
		msIter.buf = s.sampleBuf
		return msIter
	}
	return &memSafeIterator{
		stopIterator: stopIterator{
			Iterator:  c.chunk.Iterator(it),
			i:         -1,
			stopAfter: stopAfter,
		},
		total: numSamples,
		buf:   s.sampleBuf,
	}
}

func (s *memSeries) head() *memChunk {
	return s.headChunk
}

type memChunk struct {
	chunk            chunkenc.Chunk // chunkenc.XORChunk类型
	minTime, maxTime int64
}

// OverlapsClosedInterval returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	// !(mc.mint > maxt || mint > mc.maxt) 无交集[不重叠]
	return mc.minTime <= maxt && mint <= mc.maxTime
}

// 实现了chunkenc.Iterator
type stopIterator struct {
	chunkenc.Iterator

	i, stopAfter int
}

func (it *stopIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	return it.Iterator.Next()
}

// 实现了chunkenc.Iterator
type memSafeIterator struct {
	stopIterator

	total int
	buf   [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

type mmappedChunk struct {
	ref              uint64 // 高4位文件编号低4位文件内偏移
	numSamples       uint16
	minTime, maxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *mmappedChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	// !(mc.minTime > maxt || mint > mc.maxTime) 无交集[不重叠]
	return mc.minTime <= maxt && mint <= mc.maxTime
}

// SeriesLifecycleCallback specifies a list of callbacks that will be called during a lifecycle of a series.
// It is always a no-op in Prometheus and mainly meant for external users who import TSDB.
// All the callbacks should be safe to be called concurrently.
// It is up to the user to implement soft or hard consistency by making the callbacks
// atomic or non-atomic. Atomic callbacks can cause degradation performance.
type SeriesLifecycleCallback interface {
	// PreCreation is called before creating a series to indicate if the series can be created.
	// A non nil error means the series should not be created.
	PreCreation(labels.Labels) error
	// PostCreation is called after creating a series to indicate a creation of series.
	PostCreation(labels.Labels)
	// PostDeletion is called after deletion of series.
	PostDeletion(...labels.Labels)
}

type noopSeriesLifecycleCallback struct{}

func (noopSeriesLifecycleCallback) PreCreation(labels.Labels) error { return nil }
func (noopSeriesLifecycleCallback) PostCreation(labels.Labels)      {}
func (noopSeriesLifecycleCallback) PostDeletion(...labels.Labels)   {}

func (h *Head) Size() int64 {
	var walSize int64
	if h.wal != nil {
		walSize, _ = h.wal.Size()
	}
	cdmSize, _ := h.chunkDiskMapper.Size()
	return walSize + cdmSize
}

// 定义在这儿,我也是醉了
func (h *RangeHead) Size() int64 {
	return h.head.Size()
}
