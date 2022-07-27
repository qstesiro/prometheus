// Copyright 2020 The Prometheus Authors
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

// This file holds boilerplate adapters for generic MergeSeriesSet and MergeQuerier functions, so we can have one optimized
// solution that works for both ChunkSeriesSet as well as SeriesSet.

package storage

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

// 实现了storage.Querier接口
type querierAdapter struct {
	// 结构继承接口(新用法) ???
	// 细想一下这种用法会产生复杂的问题,值得深入研究
	// - 继承多个接口,接口函数有重叠(如: genericQuerierAdapter)
	// - 创建对象实例时(如果只需要部分功能)是否可以只实现部分(个别)接口的方法
	genericQuerier // mergeGenericQuerier
}

func (q *querierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) SeriesSet {
	return &seriesSetAdapter{q.genericQuerier.Select(sortSeries, hints, matchers...)}
}

// 实现了storage.ChunkQuerier接口
type chunkQuerierAdapter struct {
	// 结构继承接口(新用法) ???
	// 细想一下这种用法会产生复杂的问题,值得深入研究
	// - 继承多个接口,接口函数有重叠(如: genericQuerierAdapter)
	// - 创建对象实例时(如果只需要部分功能)是否可以只实现部分(个别)接口的方法
	genericQuerier // mergeGenericQuerier
}

func (q *chunkQuerierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) ChunkSeriesSet {
	return &chunkSeriesSetAdapter{q.genericQuerier.Select(sortSeries, hints, matchers...)}
}

// ------------------------------------------------------------------

// 注意与storage.Querier接口的不相同
// Select函数返回的是genericSeriesSet ???
type genericQuerier interface {
	LabelQuerier
	Select(bool, *SelectHints, ...*labels.Matcher) genericSeriesSet
}

// 与SeriesSet只有At() Series不同
type genericSeriesSet interface {
	Next() bool
	// storage.Series继承自storage.Labels
	At() Labels // SeriesSet.At() Series
	Err() error
	Warnings() Warnings
}

type genericSeriesMergeFunc func(...Labels) Labels

// ------------------------------------------------------------------

// 实现了genericQuerier接口
type genericQuerierAdapter struct {
	LabelQuerier

	// One-of. If both are set, Querier will be used.
	// 二选一,同时设置只使用Querier
	q  Querier      // blockQuerier
	cq ChunkQuerier // blockChunkQuerier
}

func newGenericQuerierFrom(q Querier) genericQuerier {
	return &genericQuerierAdapter{LabelQuerier: q, q: q}
}

func newGenericQuerierFromChunk(cq ChunkQuerier) genericQuerier {
	return &genericQuerierAdapter{LabelQuerier: cq, cq: cq}
}

func (q *genericQuerierAdapter) Select(
	sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher,
) genericSeriesSet {
	if q.q != nil {
		return &genericSeriesSetAdapter{q.q.Select(sortSeries, hints, matchers...)}
	}
	return &genericChunkSeriesSetAdapter{q.cq.Select(sortSeries, hints, matchers...)}
}

// ------------------------------------------------------------------

// 实现了storage.SeriesSet接口
type seriesSetAdapter struct {
	genericSeriesSet // lazyGenericSeriesSet
}

func (a *seriesSetAdapter) At() Series {
	return a.genericSeriesSet.At().(Series)
}

// 实现了genericSeriesSet接口
type genericSeriesSetAdapter struct {
	SeriesSet // blockSeriesSet
}

func (a *genericSeriesSetAdapter) At() Labels {
	return a.SeriesSet.At()
}

// -------------------------------------------------------------

// 实现了storage.ChunkSeriesSet接口
type chunkSeriesSetAdapter struct {
	genericSeriesSet
}

func (a *chunkSeriesSetAdapter) At() ChunkSeries {
	return a.genericSeriesSet.At().(ChunkSeries)
}

// 实现了genericSeriesSet接口
type genericChunkSeriesSetAdapter struct {
	ChunkSeriesSet // blockChunkSeriesSet
}

func (a *genericChunkSeriesSetAdapter) At() Labels {
	return a.ChunkSeriesSet.At()
}

// ------------------------------------------------------------

type seriesMergerAdapter struct {
	VerticalSeriesMergeFunc
}

// 只是为了做类型转换 ???
func (a *seriesMergerAdapter) Merge(s ...Labels) Labels {
	buf := make([]Series, 0, len(s))
	for _, ser := range s {
		buf = append(buf, ser.(Series))
	}
	return a.VerticalSeriesMergeFunc(buf...)
}

type chunkSeriesMergerAdapter struct {
	VerticalChunkSeriesMergeFunc
}

// 只是为了做类型转换 ???
func (a *chunkSeriesMergerAdapter) Merge(s ...Labels) Labels {
	buf := make([]ChunkSeries, 0, len(s))
	for _, ser := range s {
		buf = append(buf, ser.(ChunkSeries))
	}
	return a.VerticalChunkSeriesMergeFunc(buf...)
}

type noopGenericSeriesSet struct{}

func (noopGenericSeriesSet) Next() bool { return false }

func (noopGenericSeriesSet) At() Labels { return nil }

func (noopGenericSeriesSet) Err() error { return nil }

func (noopGenericSeriesSet) Warnings() Warnings { return nil }
