// Copyright 2018 The Prometheus Authors

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

package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"

	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// CheckpointStats returns stats about a created checkpoint.
type CheckpointStats struct {
	DroppedSeries     int
	DroppedSamples    int
	DroppedTombstones int
	TotalSeries       int // Processed series including dropped ones.
	TotalSamples      int // Processed samples including dropped ones.
	TotalTombstones   int // Processed tombstones including dropped ones.
}

// LastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, ErrNotFound is returned.
// 21/03/15 22:38:32 Mark
// 只返回最近子目录的全路径
func LastCheckpoint(dir string) (string, int, error) {
	checkpoints, err := listCheckpoints(dir)
	if err != nil {
		return "", 0, err
	}

	if len(checkpoints) == 0 {
		return "", 0, record.ErrNotFound
	}

	checkpoint := checkpoints[len(checkpoints)-1]
	return filepath.Join(dir, checkpoint.name), checkpoint.index, nil
}

// DeleteCheckpoints deletes all checkpoints in a directory below a given index.
// 21/03/15 22:47:13 Mark
// 删除所有小于等于maxIndex的检查点子目录
func DeleteCheckpoints(dir string, maxIndex int) error {
	checkpoints, err := listCheckpoints(dir)
	if err != nil {
		return err
	}

	errs := tsdb_errors.NewMulti()
	for _, checkpoint := range checkpoints {
		if checkpoint.index >= maxIndex {
			break
		}
		errs.Add(os.RemoveAll(filepath.Join(dir, checkpoint.name)))
	}
	return errs.Err()
}

const checkpointPrefix = "checkpoint."

// Checkpoint creates a compacted checkpoint of segments in range [first, last] in the given WAL.
// It includes the most recent checkpoint if it exists.
// All series not satisfying keep and samples below mint are dropped.
//
// The checkpoint is stored in a directory named checkpoint.N in the same
// segmented format as the original WAL itself.
// This makes it easy to read it through the WAL package and concatenate
// it with the original WAL.
func Checkpoint(logger log.Logger, w *WAL, from, to int, keep func(id uint64) bool, mint int64) (*CheckpointStats, error) {
	stats := &CheckpointStats{}
	var sgmReader io.ReadCloser

	level.Info(logger).Log("msg", "Creating checkpoint", "from_segment", from, "to_segment", to, "mint", mint)

	{

		var sgmRange []SegmentRange
		dir, idx, err := LastCheckpoint(w.Dir())
		if err != nil && err != record.ErrNotFound {
			return nil, errors.Wrap(err, "find last checkpoint")
		}
		last := idx + 1
		if err == nil {
			if from > last {
				return nil, fmt.Errorf("unexpected gap to last checkpoint. expected:%v, requested:%v", last, from)
			}
			// Ignore WAL files below the checkpoint. They shouldn't exist to begin with.
			from = last

			sgmRange = append(sgmRange, SegmentRange{Dir: dir, Last: math.MaxInt32})
		}

		sgmRange = append(sgmRange, SegmentRange{Dir: w.Dir(), First: from, Last: to})
		sgmReader, err = NewSegmentsRangeReader(sgmRange...)
		if err != nil {
			return nil, errors.Wrap(err, "create segment reader")
		}
		// 21/03/15 22:47:39 Quiz
		// 超出块作用域并不会被调用只会在函数返回前调用
		defer sgmReader.Close()
	}

	cpdir := checkpointDir(w.Dir(), to)
	// 21/03/15 22:49:12 Mark
	// checkpoint.%08d.tmp
	cpdirtmp := cpdir + ".tmp"

	if err := os.RemoveAll(cpdirtmp); err != nil {
		return nil, errors.Wrap(err, "remove previous temporary checkpoint dir")
	}

	if err := os.MkdirAll(cpdirtmp, 0777); err != nil {
		return nil, errors.Wrap(err, "create checkpoint dir")
	}
	// 21/03/15 22:49:30 Mark
	// 创建新WAL对象用于写检查点
	cp, err := New(nil, nil, cpdirtmp, w.CompressionEnabled())
	if err != nil {
		return nil, errors.Wrap(err, "open checkpoint")
	}

	// Ensures that an early return caused by an error doesn't leave any tmp files.
	defer func() {
		cp.Close()
		os.RemoveAll(cpdirtmp)
	}()

	r := NewReader(sgmReader)

	var (
		series  []record.RefSeries
		samples []record.RefSample
		tstones []tombstones.Stone
		dec     record.Decoder
		enc     record.Encoder
		buf     []byte
		recs    [][]byte
	)
	for r.Next() {
		series, samples, tstones = series[:0], samples[:0], tstones[:0]

		// We don't reset the buffer since we batch up multiple records
		// before writing them to the checkpoint.
		// Remember where the record for this iteration starts.
		start := len(buf)
		rec := r.Record()

		switch dec.Type(rec) {
		case record.Series:
			series, err = dec.Series(rec, series)
			if err != nil {
				return nil, errors.Wrap(err, "decode series")
			}
			// Drop irrelevant series in place.
			// 21/03/16 14:42:28 Mark
			// 这个用法妙使用相同内存空间
			repl := series[:0]
			for _, s := range series {
				// 21/03/16 15:45:51 Quiz
				// 这个keep函数用于判定是否要保留series
				// 但是其具体内容没有看明白需要后续进一步分析
				if keep(s.Ref) {
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				buf = enc.Series(repl, buf)
			}
			stats.TotalSeries += len(series)
			stats.DroppedSeries += len(series) - len(repl)

		case record.Samples:
			samples, err = dec.Samples(rec, samples)
			if err != nil {
				return nil, errors.Wrap(err, "decode samples")
			}
			// Drop irrelevant samples in place.
			// 21/03/16 15:03:28 Mark
			// 这个用法妙使用相同内存空间
			repl := samples[:0]
			for _, s := range samples {
				if s.T >= mint {
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				buf = enc.Samples(repl, buf)
			}
			stats.TotalSamples += len(samples)
			stats.DroppedSamples += len(samples) - len(repl)

		case record.Tombstones:
			tstones, err = dec.Tombstones(rec, tstones)
			if err != nil {
				return nil, errors.Wrap(err, "decode deletes")
			}
			// Drop irrelevant tombstones in place.
			// 21/03/16 15:03:48 Mark
			// 这个用法妙使用相同内存空间
			repl := tstones[:0]
			for _, s := range tstones {
				for _, iv := range s.Intervals {
					if iv.Maxt >= mint {
						repl = append(repl, s)
						break
					}
				}
			}
			if len(repl) > 0 {
				buf = enc.Tombstones(repl, buf)
			}
			stats.TotalTombstones += len(tstones)
			stats.DroppedTombstones += len(tstones) - len(repl)

		default:
			// Unknown record type, probably from a future Prometheus version.
			continue
		}
		if len(buf[start:]) == 0 {
			continue // All contents discarded.
		}
		recs = append(recs, buf[start:])

		// Flush records in 1 MB increments.
		if len(buf) > 1*1024*1024 {
			if err := cp.Log(recs...); err != nil {
				return nil, errors.Wrap(err, "flush records")
			}
			buf, recs = buf[:0], recs[:0]
		}
	}
	// If we hit any corruption during checkpointing, repairing is not an option.
	// The head won't know which series records are lost.
	if r.Err() != nil {
		return nil, errors.Wrap(r.Err(), "read segments")
	}

	// Flush remaining records.
	if err := cp.Log(recs...); err != nil {
		return nil, errors.Wrap(err, "flush records")
	}
	if err := cp.Close(); err != nil {
		return nil, errors.Wrap(err, "close checkpoint")
	}

	// Sync temporary directory before rename.
	df, err := fileutil.OpenDir(cpdirtmp)
	if err != nil {
		return nil, errors.Wrap(err, "open temporary checkpoint directory")
	}
	if err := df.Sync(); err != nil {
		df.Close()
		return nil, errors.Wrap(err, "sync temporary checkpoint directory")
	}
	if err = df.Close(); err != nil {
		return nil, errors.Wrap(err, "close temporary checkpoint directory")
	}

	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return nil, errors.Wrap(err, "rename checkpoint directory")
	}

	return stats, nil
}

// 21/03/15 22:49:44 Mark
// 创建检查点子目录全路径[index8位十进制]
func checkpointDir(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf(checkpointPrefix+"%08d", i))
}

type checkpointRef struct {
	// 21/03/15 22:49:54 Mark
	// 检查点目录名称
	name  string
	index int
}

// 21/03/15 22:50:04 Mark
// 遍历指定目录下所有文件包括子目录筛选出符合检查点目录名称的子目录
func listCheckpoints(dir string) (refs []checkpointRef, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(files); i++ {
		fi := files[i]
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		if !fi.IsDir() {
			return nil, errors.Errorf("checkpoint %s is not a directory", fi.Name())
		}
		idx, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}

		refs = append(refs, checkpointRef{name: fi.Name(), index: idx})
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].index < refs[j].index
	})

	return refs, nil
}
