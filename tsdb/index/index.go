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

package index

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

const (
	// MagicIndex 4 bytes at the head of an index file.
	MagicIndex = 0xBAAAD700
	// HeaderLen represents number of bytes reserved of index for header.
	HeaderLen = 5

	// FormatV1 represents 1 version of index.
	FormatV1 = 1
	// FormatV2 represents 2 version of index.
	FormatV2 = 2

	indexFilename = "index"
)

type indexWriterSeries struct {
	labels labels.Labels
	chunks []chunks.Meta // series file offset of chunks
}

type indexWriterSeriesSlice []*indexWriterSeries

func (s indexWriterSeriesSlice) Len() int      { return len(s) }
func (s indexWriterSeriesSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s indexWriterSeriesSlice) Less(i, j int) bool {
	return labels.Compare(s[i].labels, s[j].labels) < 0
}

type indexWriterStage uint8

const (
	idxStageNone indexWriterStage = iota
	idxStageSymbols
	idxStageSeries
	idxStageDone
)

func (s indexWriterStage) String() string {
	switch s {
	case idxStageNone:
		return "none"
	case idxStageSymbols:
		return "symbols"
	case idxStageSeries:
		return "series"
	case idxStageDone:
		return "done"
	}
	return "<unknown>"
}

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

type symbolCacheEntry struct {
	index          uint32
	lastValue      string
	lastValueIndex uint32
}

// Writer implements the IndexWriter interface for the standard
// serialization format.
// 实现了tsdb.IndexWriter接口
type Writer struct {
	ctx context.Context

	// For the main index file.
	f *FileWriter

	// Temporary file for postings.
	fP *FileWriter
	// Temporary file for posting offsets table.
	fPO   *FileWriter
	cntPO uint64

	toc           TOC
	stage         indexWriterStage
	postingsStart uint64 // Due to padding, can differ from TOC entry.

	// Reusable memory.
	buf1 encoding.Encbuf // 处理数据外层(数据长度,CRC等)
	buf2 encoding.Encbuf // 处理数据内层(具体数据内容)

	numSymbols  int
	symbols     *Symbols
	symbolFile  *fileutil.MmapFile
	lastSymbol  string
	symbolCache map[string]symbolCacheEntry // 只在AddSeries函数中使用可以按局部变量处理 ???

	labelIndexes []labelIndexHashEntry // Label index offsets.
	labelNames   map[string]uint64     // Label names, and their usage.

	// Hold last series to validate that clients insert new series in order.
	lastSeries labels.Labels
	lastRef    uint64

	crc32 hash.Hash

	Version int
}

// TOC represents index Table Of Content that states where each section of index starts.
type TOC struct {
	Symbols           uint64
	Series            uint64
	LabelIndices      uint64
	LabelIndicesTable uint64
	Postings          uint64
	PostingsTable     uint64
}

// NewTOCFromByteSlice return parsed TOC from given index byte slice.
func NewTOCFromByteSlice(bs ByteSlice) (*TOC, error) {
	if bs.Len() < indexTOCLen {
		return nil, encoding.ErrInvalidSize
	}
	b := bs.Range(bs.Len()-indexTOCLen, bs.Len())

	expCRC := binary.BigEndian.Uint32(b[len(b)-4:])
	d := encoding.Decbuf{B: b[:len(b)-4]}

	if d.Crc32(castagnoliTable) != expCRC {
		return nil, errors.Wrap(encoding.ErrInvalidChecksum, "read TOC")
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &TOC{
		Symbols:           d.Be64(),
		Series:            d.Be64(),
		LabelIndices:      d.Be64(),
		LabelIndicesTable: d.Be64(),
		Postings:          d.Be64(),
		PostingsTable:     d.Be64(),
	}, nil
}

// NewWriter returns a new Writer to the given filename. It serializes data in format version 2.
func NewWriter(ctx context.Context, fn string) (*Writer, error) {
	dir := filepath.Dir(fn)

	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	defer df.Close() // Close for platform windows.

	if err := os.RemoveAll(fn); err != nil {
		return nil, errors.Wrap(err, "remove any existing index at path")
	}

	// Main index file we are building.
	f, err := NewFileWriter(fn)
	if err != nil {
		return nil, err
	}
	// Temporary file for postings.
	fP, err := NewFileWriter(fn + "_tmp_p")
	if err != nil {
		return nil, err
	}
	// Temporary file for posting offset table.
	fPO, err := NewFileWriter(fn + "_tmp_po")
	if err != nil {
		return nil, err
	}
	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	iw := &Writer{
		ctx:   ctx,
		f:     f,
		fP:    fP,
		fPO:   fPO,
		stage: idxStageNone,

		// Reusable memory.
		buf1: encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		buf2: encoding.Encbuf{B: make([]byte, 0, 1<<22)},

		symbolCache: make(map[string]symbolCacheEntry, 1<<8),
		labelNames:  make(map[string]uint64, 1<<8),
		crc32:       newCRC32(),
	}
	if err := iw.writeMeta(); err != nil {
		return nil, err
	}
	return iw, nil
}

func (w *Writer) write(bufs ...[]byte) error {
	return w.f.Write(bufs...)
}

func (w *Writer) writeAt(buf []byte, pos uint64) error {
	return w.f.WriteAt(buf, pos)
}

func (w *Writer) addPadding(size int) error {
	return w.f.AddPadding(size)
}

type FileWriter struct {
	f    *os.File
	fbuf *bufio.Writer
	pos  uint64
	name string
}

func NewFileWriter(name string) (*FileWriter, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &FileWriter{
		f:    f,
		fbuf: bufio.NewWriterSize(f, 1<<22),
		pos:  0,
		name: name,
	}, nil
}

func (fw *FileWriter) Pos() uint64 {
	return fw.pos
}

func (fw *FileWriter) Write(bufs ...[]byte) error {
	for _, b := range bufs {
		n, err := fw.fbuf.Write(b)
		fw.pos += uint64(n)
		if err != nil {
			return err
		}
		// For now the index file must not grow beyond 64GiB. Some of the fixed-sized
		// offset references in v1 are only 4 bytes large.
		// Once we move to compressed/varint representations in those areas, this limitation
		// can be lifted.
		if fw.pos > 16*math.MaxUint32 {
			return errors.Errorf("%q exceeding max size of 64GiB", fw.name)
		}
	}
	return nil
}

func (fw *FileWriter) Flush() error {
	return fw.fbuf.Flush()
}

func (fw *FileWriter) WriteAt(buf []byte, pos uint64) error {
	if err := fw.Flush(); err != nil {
		return err
	}
	_, err := fw.f.WriteAt(buf, int64(pos))
	return err
}

// AddPadding adds zero byte padding until the file size is a multiple size.
// 实际是根据需要(是否是size的整数倍)来增加padding
func (fw *FileWriter) AddPadding(size int) error {
	p := fw.pos % uint64(size)
	if p == 0 {
		return nil
	}
	p = uint64(size) - p

	if err := fw.Write(make([]byte, p)); err != nil {
		return errors.Wrap(err, "add padding")
	}
	return nil
}

func (fw *FileWriter) Close() error {
	if err := fw.Flush(); err != nil {
		return err
	}
	if err := fw.f.Sync(); err != nil {
		return err
	}
	return fw.f.Close()
}

func (fw *FileWriter) Remove() error {
	return os.Remove(fw.name)
}

// ensureStage handles transitions between write stages and ensures that IndexWriter
// methods are called in an order valid for the implementation.
func (w *Writer) ensureStage(s indexWriterStage) error {
	// 判定是否退出
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	if w.stage == s {
		return nil
	}
	// 补缺少的阶段(还有这种情况发生) ???
	if w.stage < s-1 {
		// A stage has been skipped.
		if err := w.ensureStage(s - 1); err != nil {
			return err
		}
	}
	if w.stage > s {
		return errors.Errorf("invalid stage %q, currently at %q", s, w.stage)
	}

	// Mark start of sections in table of contents.
	// 偏移设置穿插于其中
	switch s {
	case idxStageSymbols:
		w.toc.Symbols = w.f.pos
		if err := w.startSymbols(); err != nil {
			return err
		}
	case idxStageSeries:
		if err := w.finishSymbols(); err != nil {
			return err
		}
		w.toc.Series = w.f.pos

	case idxStageDone: // Writer.Close函数中执行
		w.toc.LabelIndices = w.f.pos
		// LabelIndices generation depends on the posting offset
		// table produced at this stage.
		// 填充postings与postings-offset两个临时文件
		if err := w.writePostingsToTmpFiles(); err != nil {
			return err
		}
		if err := w.writeLabelIndices(); err != nil {
			return err
		}

		w.toc.Postings = w.f.pos
		if err := w.writePostings(); err != nil {
			return err
		}

		w.toc.LabelIndicesTable = w.f.pos
		if err := w.writeLabelIndexesOffsetTable(); err != nil {
			return err
		}

		w.toc.PostingsTable = w.f.pos
		if err := w.writePostingsOffsetTable(); err != nil {
			return err
		}
		if err := w.writeTOC(); err != nil {
			return err
		}
	}

	w.stage = s
	return nil
}

func (w *Writer) writeMeta() error {
	w.buf1.Reset()
	w.buf1.PutBE32(MagicIndex)
	w.buf1.PutByte(FormatV2)

	return w.write(w.buf1.Get())
}

// AddSeries adds the series one at a time along with its chunks.
// 写入Series部分(labels与chunks)
// ┌───────────────────────────────────────┐
// │ ┌───────────────────────────────────┐ │
// │ │   series_1                        │ │
// │ ├───────────────────────────────────┤ │
// │ │                 . . .             │ │
// │ ├───────────────────────────────────┤ │
// │ │   series_n                        │ │
// │ └───────────────────────────────────┘ │
// └───────────────────────────────────────┘
// ┌──────────────────────────────────────────────────────────────────────────┐
// │ len <uvarint>                                                            │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ ┌──────────────────────────────────────────────────────────────────────┐ │
// │ │                     labels count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ ref(l_i.name) <uvarint32>                  │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(l_i.value) <uvarint32>                 │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │                     chunks count <uvarint64>                         │ │
// │ ├──────────────────────────────────────────────────────────────────────┤ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_0.mint <varint64>                        │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_0.maxt - c_0.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_0.data) <uvarint64>                  │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │              ┌────────────────────────────────────────────┐          │ │
// │ │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ c_i.maxt - c_i.mint <uvarint64>            │          │ │
// │ │              ├────────────────────────────────────────────┤          │ │
// │ │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │ │
// │ │              └────────────────────────────────────────────┘          │ │
// │ │                             ...                                      │ │
// │ └──────────────────────────────────────────────────────────────────────┘ │
// ├──────────────────────────────────────────────────────────────────────────┤
// │ CRC32 <4b>                                                               │
// └──────────────────────────────────────────────────────────────────────────┘
func (w *Writer) AddSeries(ref uint64, lset labels.Labels, chunks ...chunks.Meta) error {
	if err := w.ensureStage(idxStageSeries); err != nil {
		return err
	}
	// 按labels排序
	if labels.Compare(lset, w.lastSeries) <= 0 {
		return errors.Errorf("out-of-order series added with label set %q", lset)
	}
	// 每个block中series.ref都是从0开始
	if ref < w.lastRef && len(w.lastSeries) != 0 {
		return errors.Errorf("series with reference greater than %d already added", ref)
	}
	// We add padding to 16 bytes to increase the addressable space we get through 4 byte
	// series references.
	// 实际是根据需要(是否是16的整数倍)来增加padding
	if err := w.addPadding(16); err != nil {
		return errors.Errorf("failed to write padding bytes: %v", err)
	}
	if w.f.pos%16 != 0 {
		return errors.Errorf("series write not 16-byte aligned at %d", w.f.pos)
	}
	// 处理label部分
	// ┌──────────────────────────────────────────────────────────────────────┐
	// │                     labels count <uvarint64>                         │
	// ├──────────────────────────────────────────────────────────────────────┤
	// │              ┌────────────────────────────────────────────┐          │
	// │              │ ref(l_i.name) <uvarint32>                  │          │
	// │              ├────────────────────────────────────────────┤          │
	// │              │ ref(l_i.value) <uvarint32>                 │          │
	// │              └────────────────────────────────────────────┘          │
	// │                             ...                                      │
	// └──────────────────────────────────────────────────────────────────────┘
	w.buf2.Reset()
	w.buf2.PutUvarint(len(lset))
	for _, l := range lset {
		var err error
		cacheEntry, ok := w.symbolCache[l.Name]
		nameIndex := cacheEntry.index
		if !ok {
			nameIndex, err = w.symbols.ReverseLookup(l.Name)
			if err != nil {
				return errors.Errorf("symbol entry for %q does not exist, %v", l.Name, err)
			}
		}
		w.labelNames[l.Name]++         // 标签名出现的次数
		w.buf2.PutUvarint32(nameIndex) // 符号表中的索引
		// 算法设计精巧(根据有序的特性只保存最后出现的值)保证缓存项个数与序列个数相一致节省内存
		valueIndex := cacheEntry.lastValueIndex
		if !ok || cacheEntry.lastValue != l.Value {
			valueIndex, err = w.symbols.ReverseLookup(l.Value)
			if err != nil {
				return errors.Errorf("symbol entry for %q does not exist, %v", l.Value, err)
			}
			w.symbolCache[l.Name] = symbolCacheEntry{
				index:          nameIndex,
				lastValue:      l.Value,
				lastValueIndex: valueIndex,
			}
		}
		w.buf2.PutUvarint32(valueIndex) // 符号表中的索引
	}
	// 处理chunks部分
	// ┌──────────────────────────────────────────────────────────────────────┐
	// │                     chunks count <uvarint64>                         │
	// ├──────────────────────────────────────────────────────────────────────┤
	// │              ┌────────────────────────────────────────────┐          │
	// │              │ c_0.mint <varint64>                        │          │
	// │              ├────────────────────────────────────────────┤          │
	// │              │ c_0.maxt - c_0.mint <uvarint64>            │          │
	// │              ├────────────────────────────────────────────┤          │
	// │              │ ref(c_0.data) <uvarint64>                  │          │
	// │              └────────────────────────────────────────────┘          │
	// │              ┌────────────────────────────────────────────┐          │
	// │              │ c_i.mint - c_i-1.maxt <uvarint64>          │          │
	// │              ├────────────────────────────────────────────┤          │
	// │              │ c_i.maxt - c_i.mint <uvarint64>            │          │
	// │              ├────────────────────────────────────────────┤          │
	// │              │ ref(c_i.data) - ref(c_i-1.data) <varint64> │          │
	// │              └────────────────────────────────────────────┘          │
	// │                             ...                                      │
	// └──────────────────────────────────────────────────────────────────────┘
	w.buf2.PutUvarint(len(chunks))
	if len(chunks) > 0 {
		c := chunks[0]
		w.buf2.PutVarint64(c.MinTime)
		w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
		w.buf2.PutUvarint64(c.Ref)
		t0 := c.MaxTime
		ref0 := int64(c.Ref) // 此处使用ref具体细节参见chunks.Meta与chunks.writeChunks
		// 注意: 除第一项外后续项的数据都是按delta方式处理
		for _, c := range chunks[1:] {
			w.buf2.PutUvarint64(uint64(c.MinTime - t0))
			w.buf2.PutUvarint64(uint64(c.MaxTime - c.MinTime))
			t0 = c.MaxTime

			w.buf2.PutVarint64(int64(c.Ref) - ref0)
			ref0 = int64(c.Ref)
		}
	}

	w.buf1.Reset()
	// 写入len
	w.buf1.PutUvarint(w.buf2.Len())
	// 写入crc32
	w.buf2.PutHash(w.crc32)

	if err := w.write(w.buf1.Get(), w.buf2.Get()); err != nil {
		return errors.Wrap(err, "write series data")
	}

	w.lastSeries = append(w.lastSeries[:0], lset...)
	w.lastRef = ref

	return nil
}

func (w *Writer) startSymbols() error {
	// We are at w.toc.Symbols.
	// Leave 4 bytes of space for the length, and another 4 for the number of symbols
	// which will both be calculated later.
	// 占位4字节用于写入长度,另外4字节写入符号个数
	return w.write([]byte("alenblen"))
}

// 写符号表
func (w *Writer) AddSymbol(sym string) error {
	if err := w.ensureStage(idxStageSymbols); err != nil {
		return err
	}
	if w.numSymbols != 0 && sym <= w.lastSymbol {
		return errors.Errorf("symbol %q out-of-order", sym)
	}
	w.lastSymbol = sym
	w.numSymbols++ // 符号计数
	w.buf1.Reset()
	w.buf1.PutUvarintStr(sym)
	return w.write(w.buf1.Get())
}

// ┌────────────────────┬─────────────────────┐
// │ len <4b> (排除len) │ #symbols <4b>       │
// ├────────────────────┴─────────────────────┤
// │ ┌──────────────────────┬───────────────┐ │
// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
// │ ├──────────────────────┴───────────────┤ │
// │ │                . . .                 │ │
// │ ├──────────────────────┬───────────────┤ │
// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
// │ └──────────────────────┴───────────────┘ │
// ├──────────────────────────────────────────┤
// │ CRC32 <4b> (不包含len与symbols)          |
// └──────────────────────────────────────────┘
func (w *Writer) finishSymbols() error {
	// Write out the length and symbol count.
	w.buf1.Reset()
	w.buf1.PutBE32int(int(w.f.pos - w.toc.Symbols - 4)) // 符号数据长度(不包含len的空间)
	w.buf1.PutBE32int(int(w.numSymbols))                // 符号个数
	if err := w.writeAt(w.buf1.Get(), w.toc.Symbols); err != nil {
		return err
	}

	hashPos := w.f.pos // 记录crc文件偏移位置
	// Leave space for the hash. We can only calculate it
	// now that the number of symbols is known, so mmap and do it from there.
	// 占位4字节写入crc
	if err := w.write([]byte("hash")); err != nil {
		return err
	}
	if err := w.f.Flush(); err != nil {
		return err
	}

	sf, err := fileutil.OpenMmapFile(w.f.name)
	if err != nil {
		return err
	}
	w.symbolFile = sf
	// CRC32只计算符号数据不包含len与symbols
	hash := crc32.Checksum(w.symbolFile.Bytes()[w.toc.Symbols+4:hashPos], castagnoliTable)
	w.buf1.Reset()
	w.buf1.PutBE32(hash)
	if err := w.writeAt(w.buf1.Get(), hashPos); err != nil {
		return err
	}

	// Load in the symbol table efficiently for the rest of the index writing.
	w.symbols, err = NewSymbols(realByteSlice(w.symbolFile.Bytes()), FormatV2, int(w.toc.Symbols))
	if err != nil {
		return errors.Wrap(err, "read symbols")
	}
	return nil
}

// 文档中说明 This is no longer used. ???
func (w *Writer) writeLabelIndices() error {
	if err := w.fPO.Flush(); err != nil {
		return err
	}

	// Find all the label values in the tmp posting offset table.
	f, err := fileutil.OpenMmapFile(w.fPO.name)
	if err != nil {
		return err
	}
	defer f.Close()

	d := encoding.NewDecbufRaw(realByteSlice(f.Bytes()), int(w.fPO.pos))
	cnt := w.cntPO
	current := []byte{}
	values := []uint32{}
	for d.Err() == nil && cnt > 0 {
		cnt--
		d.Uvarint()                           // Keycount. 当前恒定为2
		name := d.UvarintBytes()              // Label name.
		value := yoloString(d.UvarintBytes()) // Label value.
		d.Uvarint64()                         // Offset.
		// name = ""
		if len(name) == 0 {
			continue // All index is ignored.
		}

		if !bytes.Equal(name, current) && len(values) > 0 {
			// We've reached a new label name.
			if err := w.writeLabelIndex(string(current), values); err != nil {
				return err
			}
			values = values[:0]
		}
		current = name
		sid, err := w.symbols.ReverseLookup(value)
		if err != nil {
			return err
		}
		values = append(values, sid)
	}
	if d.Err() != nil {
		return d.Err()
	}

	// Handle the last label.
	// 处理可能的剩余
	if len(values) > 0 {
		if err := w.writeLabelIndex(string(current), values); err != nil {
			return err
		}
	}
	return nil
}

// ┌───────────────┬────────────────┬────────────────┐
// │ len <4b>      │ #names <4b>    │ #entries <4b>  │
// ├───────────────┴────────────────┴────────────────┤
// │ ┌─────────────────────────────────────────────┐ │
// │ │ ref(value_0) <4b>                           │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ...                                         │ │
// │ ├─────────────────────────────────────────────┤ │
// │ │ ref(value_n) <4b>                           │ │
// │ └─────────────────────────────────────────────┘ │
// │                      . . .                      │
// ├─────────────────────────────────────────────────┤
// │ CRC32 <4b>                                      │
// └─────────────────────────────────────────────────┘
func (w *Writer) writeLabelIndex(name string, values []uint32) error {
	// Align beginning to 4 bytes for more efficient index list scans.
	if err := w.addPadding(4); err != nil {
		return err
	}
	// 写入标签索引表时使用
	w.labelIndexes = append(w.labelIndexes, labelIndexHashEntry{
		keys:   []string{name},
		offset: w.f.pos,
	})

	startPos := w.f.pos
	// Leave 4 bytes of space for the length, which will be calculated later.
	if err := w.write([]byte("alen")); err != nil {
		return err
	}
	w.crc32.Reset()

	w.buf1.Reset()
	// 代表标签名称的个数,当前恒定为1,代表只记录一个标签名称
	w.buf1.PutBE32int(1) // Number of names.
	// 值的符号表索引
	w.buf1.PutBE32int(len(values))
	w.buf1.WriteToHash(w.crc32)
	if err := w.write(w.buf1.Get()); err != nil {
		return err
	}

	for _, v := range values {
		w.buf1.Reset()
		w.buf1.PutBE32(v)
		w.buf1.WriteToHash(w.crc32)
		if err := w.write(w.buf1.Get()); err != nil {
			return err
		}
	}

	// Write out the length.
	w.buf1.Reset()
	w.buf1.PutBE32int(int(w.f.pos - startPos - 4))
	if err := w.writeAt(w.buf1.Get(), startPos); err != nil {
		return err
	}

	w.buf1.Reset()
	w.buf1.PutHashSum(w.crc32)
	return w.write(w.buf1.Get())
}

// writeLabelIndexesOffsetTable writes the label indices offset table.
// 文档中说明 This is no longer used. ???
// ┌─────────────────────┬──────────────────────┐
// │ len <4b>            │ #entries <4b>        │
// ├─────────────────────┴──────────────────────┤
// │ ┌────────────────────────────────────────┐ │
// │ │  n = 1 <1b>                            │ │
// │ ├──────────────────────┬─────────────────┤ │
// │ │ len(name) <uvarint>  │ name <bytes>    │ │
// │ ├──────────────────────┴─────────────────┤ │
// │ │  offset <uvarint64>                    │ │
// │ └────────────────────────────────────────┘ │
// │                    . . .                   │
// ├────────────────────────────────────────────┤
// │  CRC32 <4b>                                │
// └────────────────────────────────────────────┘
func (w *Writer) writeLabelIndexesOffsetTable() error {
	startPos := w.f.pos
	// Leave 4 bytes of space for the length, which will be calculated later.
	// 长度占位
	if err := w.write([]byte("alen")); err != nil {
		return err
	}
	w.crc32.Reset()

	w.buf1.Reset()
	// entries个数
	w.buf1.PutBE32int(len(w.labelIndexes))
	w.buf1.WriteToHash(w.crc32)
	if err := w.write(w.buf1.Get()); err != nil {
		return err
	}

	for _, e := range w.labelIndexes {
		w.buf1.Reset()
		w.buf1.PutUvarint(len(e.keys)) // 恒定为1
		for _, k := range e.keys {
			w.buf1.PutUvarintStr(k)
		}
		w.buf1.PutUvarint64(e.offset)
		w.buf1.WriteToHash(w.crc32)
		if err := w.write(w.buf1.Get()); err != nil {
			return err
		}
	}
	// Write out the length.
	w.buf1.Reset()
	w.buf1.PutBE32int(int(w.f.pos - startPos - 4))
	if err := w.writeAt(w.buf1.Get(), startPos); err != nil {
		return err
	}

	w.buf1.Reset()
	w.buf1.PutHashSum(w.crc32)
	return w.write(w.buf1.Get())
}

// writePostingsOffsetTable writes the postings offset table.
func (w *Writer) writePostingsOffsetTable() error {
	// Ensure everything is in the temporary file.
	if err := w.fPO.Flush(); err != nil {
		return err
	}

	startPos := w.f.pos
	// Leave 4 bytes of space for the length, which will be calculated later.
	// 保留4字节长度
	if err := w.write([]byte("alen")); err != nil {
		return err
	}

	// Copy over the tmp posting offset table, however we need to
	// adjust the offsets.
	adjustment := w.postingsStart // writePostings函数中记录(不直接使用toc.Postings是因为可能有padding)

	w.buf1.Reset()
	w.crc32.Reset()
	// entries个数
	w.buf1.PutBE32int(int(w.cntPO)) // Count.
	w.buf1.WriteToHash(w.crc32)
	if err := w.write(w.buf1.Get()); err != nil {
		return err
	}

	f, err := fileutil.OpenMmapFile(w.fPO.name)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	d := encoding.NewDecbufRaw(realByteSlice(f.Bytes()), int(w.fPO.pos))
	cnt := w.cntPO
	// 第一个项name="",label=""
	for d.Err() == nil && cnt > 0 {
		w.buf1.Reset()
		w.buf1.PutUvarint(d.Uvarint())                     // Keycount.(读出n=2)
		w.buf1.PutUvarintStr(yoloString(d.UvarintBytes())) // Label name.
		w.buf1.PutUvarintStr(yoloString(d.UvarintBytes())) // Label value.
		w.buf1.PutUvarint64(d.Uvarint64() + adjustment)    // Offset.(指向Postings段中对应Posting项的offset)
		w.buf1.WriteToHash(w.crc32)
		if err := w.write(w.buf1.Get()); err != nil {
			return err
		}
		cnt--
	}
	if d.Err() != nil {
		return d.Err()
	}

	// Cleanup temporary file.
	if err := f.Close(); err != nil {
		return err
	}
	f = nil
	if err := w.fPO.Close(); err != nil {
		return err
	}
	if err := w.fPO.Remove(); err != nil {
		return err
	}
	w.fPO = nil

	// Write out the length.
	w.buf1.Reset()
	w.buf1.PutBE32int(int(w.f.pos - startPos - 4)) // 计算出长度
	if err := w.writeAt(w.buf1.Get(), startPos); err != nil {
		return err
	}

	// Finally write the hash.
	// crc32的计算范围包含entries与data部分(不包含len部分)
	w.buf1.Reset()
	w.buf1.PutHashSum(w.crc32)
	return w.write(w.buf1.Get())
}

const indexTOCLen = 6*8 + crc32.Size

func (w *Writer) writeTOC() error {
	w.buf1.Reset()

	w.buf1.PutBE64(w.toc.Symbols)
	w.buf1.PutBE64(w.toc.Series)
	w.buf1.PutBE64(w.toc.LabelIndices)
	w.buf1.PutBE64(w.toc.LabelIndicesTable)
	w.buf1.PutBE64(w.toc.Postings)
	w.buf1.PutBE64(w.toc.PostingsTable)

	w.buf1.PutHash(w.crc32)

	return w.write(w.buf1.Get())
}

// 函数认知复杂度太高了 ???
// 主要是在 Temporary file for posting offsets table 中记录一些临时数据
func (w *Writer) writePostingsToTmpFiles() error {
	names := make([]string, 0, len(w.labelNames))
	for n := range w.labelNames {
		names = append(names, n)
	}
	sort.Strings(names)

	if err := w.f.Flush(); err != nil {
		return err
	}
	// 当前只包含symbol与series两部分
	f, err := fileutil.OpenMmapFile(w.f.name)
	if err != nil {
		return err
	}
	defer f.Close()
	// 处理label.name="",lable.value=""的postings数据
	// Write out the special all posting.
	// 获取序列区中的每个序列偏移(为label.name="",lable.value=""准备postings数据)
	offsets := []uint32{}
	d := encoding.NewDecbufRaw(realByteSlice(f.Bytes()), int(w.toc.LabelIndices)) // 解码符号与序列
	// 跳过符号偏移到序列(从缓冲区中删除了symbol的数据,整个缓冲区中只包含了series部分所以len也就是series部分的长度)
	d.Skip(int(w.toc.Series))
	for d.Len() > 0 {
		d.ConsumePadding()                               // 跳过padding
		startPos := w.toc.LabelIndices - uint64(d.Len()) // 计算每个series的起始
		if startPos%16 != 0 {
			return errors.Errorf("series not 16-byte aligned at %d", startPos)
		}
		offsets = append(offsets, uint32(startPos/16)) // 为什么除16,用时再乘 ???
		// Skip to next series.
		// 跳过当前series
		x := d.Uvarint()       // 单个序列长度
		d.Skip(x + crc32.Size) // 加单个序列CRC32的4字节
		if err := d.Err(); err != nil {
			return err
		}
	}
	// Temporary file for posting offsets table. [fPO]
	// ┌─────────────────────────────────────────┐
	// │  n = 2 <1b>                             │
	// ├──────────────────────┬──────────────────┤
	// │ len("") <uvarint>    │ "" <bytes>       │
	// ├──────────────────────┼──────────────────┤
	// │ len("") <uvarint>    │ "" <bytes>       │
	// ├──────────────────────┴──────────────────┤
	// │ fP <uvarint64>                          |--------------------+
	// └─────────────────────────────────────────┘                    |
	// Temporary file for postings. [fP] 包含当前块中的所有序列引用   |
	// ┌────────────────────┬────────────────────┐                    |
	// │ len <4b>           │ #entries <4b>      |<-------------------+
	// ├────────────────────┴────────────────────┤
	// │ ┌─────────────────────────────────────┐ │
	// │ │ ref(series_1) <4b>                  │ │
	// │ ├─────────────────────────────────────┤ │
	// │ │ ...                                 │ │
	// │ ├─────────────────────────────────────┤ │
	// │ │ ref(series_n) <4b>                  │ │
	// │ └─────────────────────────────────────┘ │
	// ├─────────────────────────────────────────┤
	// │ CRC32 <4b>                              │
	// └─────────────────────────────────────────┘
	// 写label.name="",lable.value=""对应的postings数据
	if err := w.writePosting("", "", offsets); err != nil {
		return err
	}
	maxPostings := uint64(len(offsets)) // No label name can have more postings than this.

	// 开始处理每个label.name=label.value的postings数据
	// - 一种算法就是逐个遍历name处理每对name/value(时间长内存少)
	// - 另一种算法就完全遍历所有name得到完整的postings数据(时间短内存大)
	// 此处进行了算法优化就是一次获取部分names数据具体多少的判定标准为一次处理的数据offset个数不超过series总个数
	for len(names) > 0 {
		batchNames := []string{}
		var c uint64
		// Try to bunch up label names into one loop, but avoid
		// using more memory than a single label name can.
		// 通过name出现的次数(也就是value的个数不去重)这代表postings中offset的个数不会超过series总个数
		// 单个name出现的次数最少出现一次,最多出现series总个数(每个series中都包含)
		// 以此保证在不超过单个name遍历的最大可能内存使用的情况下尽可能的在一次遍历中多处理name
		for len(names) > 0 {
			if w.labelNames[names[0]]+c > maxPostings {
				break
			}
			batchNames = append(batchNames, names[0])
			c += w.labelNames[names[0]]
			names = names[1:]
		}

		nameSymbols := map[uint32]string{}
		for _, name := range batchNames {
			sid, err := w.symbols.ReverseLookup(name)
			if err != nil {
				return err
			}
			nameSymbols[sid] = name // 注意是索引不是偏移
		}
		// map[label.name]map[lable.value]offset/16
		// name/value为索引
		postings := map[uint32]map[uint32][]uint32{}

		d := encoding.NewDecbufRaw(realByteSlice(f.Bytes()), int(w.toc.LabelIndices))
		d.Skip(int(w.toc.Series))
		// 外层循环迭代series
		for d.Len() > 0 {
			d.ConsumePadding()                               // 如果有padding则跳过
			startPos := w.toc.LabelIndices - uint64(d.Len()) // 单个series部分的起始值
			l := d.Uvarint()                                 // Length of this series in bytes.(单个series数据长度)
			startLen := d.Len()                              // 单个series部分总数据长度包括len与crc
			// 内层循环迭代series中的每个lable
			// See if label names we want are in the series.
			numLabels := d.Uvarint() // series的个数(labels count部分)
			for i := 0; i < numLabels; i++ {
				lno := uint32(d.Uvarint()) // 索引
				lvo := uint32(d.Uvarint()) // 索引

				if _, ok := nameSymbols[lno]; ok {
					if _, ok := postings[lno]; !ok {
						postings[lno] = map[uint32][]uint32{}
					}
					postings[lno][lvo] = append(postings[lno][lvo], uint32(startPos/16))
				}
			}
			// Skip to next series.(跳到下一个series的偏移)
			d.Skip(l - (startLen - d.Len()) + crc32.Size)
			if err := d.Err(); err != nil {
				return err
			}
		}

		for _, name := range batchNames {
			// Write out postings for this label name.
			sid, err := w.symbols.ReverseLookup(name)
			if err != nil {
				return err
			}
			values := make([]uint32, 0, len(postings[sid]))
			for v := range postings[sid] { // v标签值引用ID
				values = append(values, v)

			}
			// Symbol numbers are in order, so the strings will also be in order.
			sort.Sort(uint32slice(values))
			for _, v := range values {
				value, err := w.symbols.Lookup(v)
				if err != nil {
					return err
				}
				// Temporary file for posting offsets table. [fPO]
				// ┌─────────────────────────────────────────┐
				// │  n = 2 <1b>                             │
				// ├──────────────────────┬──────────────────┤
				// │ len(name) <uvarint>  │ name <bytes>     │
				// ├──────────────────────┼──────────────────┤
				// │ len(value) <uvarint> │ value <bytes>    │
				// ├──────────────────────┴──────────────────┤
				// │ fP <uvarint64>                          │------+
				// └─────────────────────────────────────────┘      |
				// Temporary file for postings. [fP]                |
				// ┌────────────────────┬────────────────────┐      |
				// │ len <4b>           │ #entries <4b>      │<-----+
				// ├────────────────────┴────────────────────┤
				// │ ┌─────────────────────────────────────┐ │
				// │ │ ref(series_1) <4b>                  │ │
				// │ ├─────────────────────────────────────┤ │
				// │ │ ...                                 │ │
				// │ ├─────────────────────────────────────┤ │
				// │ │ ref(series_n) <4b>                  │ │
				// │ └─────────────────────────────────────┘ │
				// ├─────────────────────────────────────────┤
				// │ CRC32 <4b>                              │
				// └─────────────────────────────────────────┘
				if err := w.writePosting(name, value, postings[sid][v]); err != nil {
					return err
				}
			}
		}
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		default:
		}

	}
	return nil
}

// Temporary file for posting offsets table. [fPO]
// ┌─────────────────────────────────────────┐
// │  n = 2 <1b>                             │
// ├──────────────────────┬──────────────────┤
// │ len(name) <uvarint>  │ name <bytes>     │
// ├──────────────────────┼──────────────────┤
// │ len(value) <uvarint> │ value <bytes>    │
// ├──────────────────────┴──────────────────┤
// │ fP <uvarint64>                          │
// └─────────────────────────────────────────┘
// Temporary file for postings. [fP]
// ┌────────────────────┬────────────────────┐
// │ len <4b>           │ #entries <4b>      │
// ├────────────────────┴────────────────────┤
// │ ┌─────────────────────────────────────┐ │
// │ │ ref(series_1) <4b>                  │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ...                                 │ │
// │ ├─────────────────────────────────────┤ │
// │ │ ref(series_n) <4b>                  │ │
// │ └─────────────────────────────────────┘ │
// ├─────────────────────────────────────────┤
// │ CRC32 <4b>                              │
// └─────────────────────────────────────────┘
func (w *Writer) writePosting(name, value string, offs []uint32) error {
	// Align beginning to 4 bytes for more efficient postings list scans.
	// 根据当前文件偏移位置尝试对齐
	// 只对齐fP不处理fPO ???
	if err := w.fP.AddPadding(4); err != nil {
		return err
	}
	// 处理Postings Offset
	// Write out postings offset table to temporary file as we go.
	w.buf1.Reset()
	w.buf1.PutUvarint(2)
	w.buf1.PutUvarintStr(name)
	w.buf1.PutUvarintStr(value)
	// 偏移位置暂时写入Posting临时文件的起始偏移
	w.buf1.PutUvarint64(w.fP.pos) // This is relative to the postings tmp file, not the final index file.
	if err := w.fPO.Write(w.buf1.Get()); err != nil {
		return err
	}
	w.cntPO++ // 对应PO(postings offsets)的entries字段
	// 处理Postings
	w.buf1.Reset()
	// offs[i] = series-startpos(每个series启始位置)/16
	w.buf1.PutBE32int(len(offs)) // entries
	// ref(series_x)
	for _, off := range offs {
		if off > (1<<32)-1 {
			return errors.Errorf("series offset %d exceeds 4 bytes", off)
		}
		w.buf1.PutBE32(off)
	}

	w.buf2.Reset()
	w.buf2.PutBE32int(w.buf1.Len()) // len
	w.buf1.PutHash(w.crc32)         // CRC32
	return w.fP.Write(w.buf2.Get(), w.buf1.Get())
}

func (w *Writer) writePostings() error {
	// There's padding in the tmp file, make sure it actually works.
	// 注: 第一项写入是不会填充padding
	if err := w.f.AddPadding(4); err != nil {
		return err
	}
	w.postingsStart = w.f.pos

	// Copy temporary file into main index.
	if err := w.fP.Flush(); err != nil {
		return err
	}
	// 第一个Posting包含当前块中所有序列引用
	if _, err := w.fP.f.Seek(0, 0); err != nil {
		return err
	}
	// Don't need to calculate a checksum, so can copy directly.
	n, err := io.CopyBuffer(w.f.fbuf, w.fP.f, make([]byte, 1<<20))
	if err != nil {
		return err
	}
	if uint64(n) != w.fP.pos {
		return errors.Errorf("wrote %d bytes to posting temporary file, but only read back %d", w.fP.pos, n)
	}
	w.f.pos += uint64(n)

	if err := w.fP.Close(); err != nil {
		return err
	}
	if err := w.fP.Remove(); err != nil {
		return err
	}
	w.fP = nil
	return nil
}

type uint32slice []uint32

func (s uint32slice) Len() int           { return len(s) }
func (s uint32slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s uint32slice) Less(i, j int) bool { return s[i] < s[j] }

type labelIndexHashEntry struct {
	keys   []string
	offset uint64
}

func (w *Writer) Close() error {
	// Even if this fails, we need to close all the files.
	// 关闭前写入除符号表与序列之外的其它部分
	// 感觉真是思维方式奇特[与MemPostings中保存ID有异曲同工之妙,呵呵]
	ensureErr := w.ensureStage(idxStageDone)

	if w.symbolFile != nil {
		if err := w.symbolFile.Close(); err != nil {
			return err
		}
	}
	if w.fP != nil {
		if err := w.fP.Close(); err != nil {
			return err
		}
	}
	if w.fPO != nil {
		if err := w.fPO.Close(); err != nil {
			return err
		}
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	return ensureErr
}

// StringIter iterates over a sorted list of strings.
type StringIter interface {
	// Next advances the iterator and returns true if another value was found.
	Next() bool

	// At returns the value at the current iterator position.
	At() string

	// Err returns the last error of the iterator.
	Err() error
}

// 实现了tsdb.IndexReader接口
type Reader struct {
	b   ByteSlice
	toc *TOC

	// Close that releases the underlying resources of the byte slice.
	c io.Closer

	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present.
	postings map[string][]postingOffset
	// For the v1 format, labelname -> labelvalue -> offset.
	postingsV1 map[string]map[string]uint64

	symbols     *Symbols
	nameSymbols map[uint32]string // Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.

	dec *Decoder

	version int
}

type postingOffset struct {
	value string
	off   int
}

// ByteSlice abstracts a byte slice.
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}

// NewReader returns a new index reader on the given byte slice. It automatically
// handles different format versions.
func NewReader(b ByteSlice) (*Reader, error) {
	return newReader(b, ioutil.NopCloser(nil))
}

// NewFileReader returns a new index reader against the given index file.
func NewFileReader(path string) (*Reader, error) {
	f, err := fileutil.OpenMmapFile(path) // 内存映射方式打开文件
	if err != nil {
		return nil, err
	}
	r, err := newReader(realByteSlice(f.Bytes()), f)
	if err != nil {
		return nil, tsdb_errors.NewMulti(
			err,
			f.Close(),
		).Err()
	}

	return r, nil
}

func newReader(b ByteSlice, c io.Closer) (*Reader, error) {
	r := &Reader{
		b:        b,
		c:        c,
		postings: map[string][]postingOffset{},
	}

	// Verify header.
	if r.b.Len() < HeaderLen {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "index header")
	}
	if m := binary.BigEndian.Uint32(r.b.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(r.b.Range(4, 5)[0])

	if r.version != FormatV1 && r.version != FormatV2 {
		return nil, errors.Errorf("unknown index file version %d", r.version)
	}

	var err error
	r.toc, err = NewTOCFromByteSlice(b)
	if err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	r.symbols, err = NewSymbols(r.b, r.version, int(r.toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	if r.version == FormatV1 {
		// Earlier V1 formats don't have a sorted postings offset table, so
		// load the whole offset table into memory.
		r.postingsV1 = map[string]map[string]uint64{}
		if err := ReadOffsetTable(r.b, r.toc.PostingsTable, func(key []string, off uint64, _ int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}
			if _, ok := r.postingsV1[key[0]]; !ok {
				r.postingsV1[key[0]] = map[string]uint64{}
				r.postings[key[0]] = nil // Used to get a list of labelnames in places.
			}
			r.postingsV1[key[0]][key[1]] = off
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
	} else {
		var lastKey []string
		lastOff := 0
		valueCount := 0
		// For the postings offset table we keep every label name but only every nth
		// label value (plus the first and last one), to save memory.
		if err := ReadOffsetTable(r.b, r.toc.PostingsTable, func(key []string, _ uint64, off int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}
			if _, ok := r.postings[key[0]]; !ok {
				// Next label name.
				r.postings[key[0]] = []postingOffset{}
				if lastKey != nil {
					// Always include last value for each label name.
					r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
				}
				lastKey = nil
				valueCount = 0
			}
			if valueCount%32 == 0 {
				r.postings[key[0]] = append(r.postings[key[0]], postingOffset{value: key[1], off: off})
				lastKey = nil
			} else {
				lastKey = key
				lastOff = off
			}
			valueCount++
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
		}
		// Trim any extra space in the slices.
		for k, v := range r.postings {
			l := make([]postingOffset, len(v))
			copy(l, v)
			r.postings[k] = l
		}
	}

	r.nameSymbols = make(map[uint32]string, len(r.postings))
	for k := range r.postings {
		if k == "" {
			continue
		}
		off, err := r.symbols.ReverseLookup(k)
		if err != nil {
			return nil, errors.Wrap(err, "reverse symbol lookup")
		}
		r.nameSymbols[off] = k
	}

	r.dec = &Decoder{LookupSymbol: r.lookupSymbol}

	return r, nil
}

// Version returns the file format version of the underlying index.
func (r *Reader) Version() int {
	return r.version
}

// Range marks a byte range.
type Range struct {
	Start, End int64
}

// PostingsRanges returns a new map of byte range in the underlying index file
// for all postings lists.
func (r *Reader) PostingsRanges() (map[labels.Label]Range, error) {
	m := map[labels.Label]Range{}
	if err := ReadOffsetTable(r.b, r.toc.PostingsTable, func(key []string, off uint64, _ int) error {
		if len(key) != 2 {
			return errors.Errorf("unexpected key length for posting table %d", len(key))
		}
		d := encoding.NewDecbufAt(r.b, int(off), castagnoliTable)
		if d.Err() != nil {
			return d.Err()
		}
		m[labels.Label{Name: key[0], Value: key[1]}] = Range{
			Start: int64(off) + 4,
			End:   int64(off) + 4 + int64(d.Len()),
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}
	return m, nil
}

type Symbols struct {
	bs      ByteSlice
	version int
	off     int

	offsets []int // 存储1/symbolFactor的偏移数(每隔symbolFactor个偏移存储)
	seen    int   // 符号个数
}

const symbolFactor = 32

// NewSymbols returns a Symbols object for symbol lookups.
func NewSymbols(bs ByteSlice, version int, off int) (*Symbols, error) {
	s := &Symbols{
		bs:      bs,
		version: version,
		off:     off,
	}
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)
	var (
		origLen = d.Len()
		cnt     = d.Be32int()
		basePos = off + 4
	)
	// 填充offsets(此处算法设计精巧后续Lookup与ReverseLookup处理方式漂亮)
	s.offsets = make([]int, 0, 1+cnt/symbolFactor)
	for d.Err() == nil && s.seen < cnt {
		if s.seen%symbolFactor == 0 {
			s.offsets = append(s.offsets, basePos+origLen-d.Len()) // 每次长度缩短
		}
		// 跳过符号具体值
		d.UvarintBytes() // The symbol.
		s.seen++
	}
	if d.Err() != nil {
		return nil, d.Err()
	}
	return s, nil
}

// 参数是索引(不是偏移)
func (s Symbols) Lookup(o uint32) (string, error) {
	d := encoding.Decbuf{
		B: s.bs.Range(0, s.bs.Len()),
	}

	if s.version == FormatV2 {
		if int(o) >= s.seen {
			return "", errors.Errorf("unknown symbol offset %d", o) // o参数应该是编号不是偏移 ???
		}
		/*
		                      0     o            1                  2
		   +------------------+-----+------------+------------------+------>
		                      32    34           64                 96
		*/
		// 直接跳到偏移位置(o=43的情况直接定位到索引0的位置)
		d.Skip(s.offsets[int(o/symbolFactor)])
		// Walk until we find the one we want.(向前查找)
		for i := o - (o / symbolFactor * symbolFactor); i > 0; i-- {
			d.UvarintBytes()
		}
	} else {
		d.Skip(int(o))
	}
	sym := d.UvarintStr()
	if d.Err() != nil {
		return "", d.Err()
	}
	return sym, nil
}

// 返回值是索引(不是偏移)
func (s Symbols) ReverseLookup(sym string) (uint32, error) {
	if len(s.offsets) == 0 {
		return 0, errors.Errorf("unknown symbol %q - no symbols", sym)
	}
	/*
	   执行流程
	   - 先定位到索引1
	   - 回退到索引0
	   - 向前查找
	                      0 <--------------- 1                  2
	   +------------------+-----+------------+------------------+------>
	                      32    sym          64                96
	*/
	// 找到第一个大于sym的偏移索引
	i := sort.Search(len(s.offsets), func(i int) bool {
		// Any decoding errors here will be lost, however
		// we already read through all of this at startup.
		d := encoding.Decbuf{
			B: s.bs.Range(0, s.bs.Len()),
		}
		d.Skip(s.offsets[i])
		return yoloString(d.UvarintBytes()) > sym // symbols是有序的
	})
	d := encoding.Decbuf{
		B: s.bs.Range(0, s.bs.Len()),
	}
	// 回退一个索引位置
	if i > 0 {
		i--
	}
	// 直接跳到偏移位置
	d.Skip(s.offsets[i])
	res := i * 32
	var lastLen int
	var lastSymbol string
	// 向前查找
	for d.Err() == nil && res <= s.seen {
		lastLen = d.Len()
		lastSymbol = yoloString(d.UvarintBytes())
		if lastSymbol >= sym {
			break
		}
		res++
	}
	if d.Err() != nil {
		return 0, d.Err()
	}
	if lastSymbol != sym {
		return 0, errors.Errorf("unknown symbol %q", sym)
	}
	if s.version == FormatV2 {
		return uint32(res), nil
	}
	return uint32(s.bs.Len() - lastLen), nil
}

func (s Symbols) Size() int {
	return len(s.offsets) * 8
}

func (s Symbols) Iter() StringIter {
	d := encoding.NewDecbufAt(s.bs, s.off, castagnoliTable)
	cnt := d.Be32int()
	return &symbolsIter{
		d:   d,
		cnt: cnt,
	}
}

// symbolsIter implements StringIter.
type symbolsIter struct {
	d   encoding.Decbuf
	cnt int
	cur string
	err error
}

func (s *symbolsIter) Next() bool {
	if s.cnt == 0 || s.err != nil {
		return false
	}
	s.cur = yoloString(s.d.UvarintBytes())
	s.cnt--
	if s.d.Err() != nil {
		s.err = s.d.Err()
		return false
	}
	return true
}

func (s symbolsIter) At() string { return s.cur }
func (s symbolsIter) Err() error { return s.err }

// ReadOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func ReadOffsetTable(bs ByteSlice, off uint64, f func([]string, uint64, int) error) error {
	d := encoding.NewDecbufAt(bs, int(off), castagnoliTable)
	startLen := d.Len()
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		offsetPos := startLen - d.Len()
		keyCount := d.Uvarint()
		// The Postings offset table takes only 2 keys per entry (name and value of label),
		// and the LabelIndices offset table takes only 1 key per entry (a label name).
		// Hence setting the size to max of both, i.e. 2.
		keys := make([]string, 0, 2)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d.UvarintStr())
		}
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(keys, o, offsetPos); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

// Close the reader and its underlying resources.
func (r *Reader) Close() error {
	return r.c.Close()
}

func (r *Reader) lookupSymbol(o uint32) (string, error) {
	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}
	return r.symbols.Lookup(o)
}

// Symbols returns an iterator over the symbols that exist within the index.
func (r *Reader) Symbols() StringIter {
	return r.symbols.Iter()
}

// SymbolTableSize returns the symbol table size in bytes.
func (r *Reader) SymbolTableSize() uint64 {
	return uint64(r.symbols.Size())
}

// SortedLabelValues returns value tuples that exist for the given label name.
// It is not safe to use the return value beyond the lifetime of the byte slice
// passed into the Reader.
func (r *Reader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := r.LabelValues(name, matchers...)
	if err == nil && r.version == FormatV1 {
		sort.Strings(values)
	}
	return values, err
}

// LabelValues returns value tuples that exist for the given label name.
// It is not safe to use the return value beyond the lifetime of the byte slice
// passed into the Reader.
// TODO(replay): Support filtering by matchers
func (r *Reader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		return nil, errors.Errorf("matchers parameter is not implemented: %+v", matchers)
	}

	if r.version == FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		values := make([]string, 0, len(e))
		for k := range e {
			values = append(values, k)
		}
		return values, nil

	}
	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e) == 0 {
		return nil, nil
	}
	values := make([]string, 0, len(e)*symbolFactor)

	d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsTable), nil)
	d.Skip(e[0].off)
	lastVal := e[len(e)-1].value

	skip := 0
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()      // Keycount.
			d.UvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}
		s := yoloString(d.UvarintBytes()) //Label value.
		values = append(values, s)
		if s == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings offset entry")
	}
	return values, nil
}

// LabelValueFor returns label value for the given label name in the series referred to by ID.
func (r *Reader) LabelValueFor(id uint64, label string) (string, error) {
	offset := id
	// In version 2 series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.
	if r.version == FormatV2 {
		offset = id * 16
	}
	d := encoding.NewDecbufUvarintAt(r.b, int(offset), castagnoliTable)
	buf := d.Get()
	if d.Err() != nil {
		return "", errors.Wrap(d.Err(), "label values for")
	}

	value, err := r.dec.LabelValueFor(buf, label)
	if err != nil {
		return "", storage.ErrNotFound
	}

	if value == "" {
		return "", storage.ErrNotFound
	}

	return value, nil
}

// Series reads the series with the given ID and writes its labels and chunks into lbls and chks.
func (r *Reader) Series(id uint64, lbls *labels.Labels, chks *[]chunks.Meta) error {
	offset := id
	// In version 2 series IDs are no longer exact references but series are 16-byte padded
	// and the ID is the multiple of 16 of the actual position.
	if r.version == FormatV2 {
		offset = id * 16
	}
	d := encoding.NewDecbufUvarintAt(r.b, int(offset), castagnoliTable)
	if d.Err() != nil {
		return d.Err()
	}
	return errors.Wrap(r.dec.Series(d.Get(), lbls, chks), "read series")
}

func (r *Reader) Postings(name string, values ...string) (Postings, error) {
	if r.version == FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return EmptyPostings(), nil
		}
		res := make([]Postings, 0, len(values))
		for _, v := range values {
			postingsOff, ok := e[v]
			if !ok {
				continue
			}
			// Read from the postings table.
			d := encoding.NewDecbufAt(r.b, int(postingsOff), castagnoliTable)
			_, p, err := r.dec.Postings(d.Get())
			if err != nil {
				return nil, errors.Wrap(err, "decode postings")
			}
			res = append(res, p)
		}
		return Merge(res...), nil
	}

	e, ok := r.postings[name]
	if !ok {
		return EmptyPostings(), nil
	}

	if len(values) == 0 {
		return EmptyPostings(), nil
	}

	res := make([]Postings, 0, len(values))
	skip := 0
	valueIndex := 0
	for valueIndex < len(values) && values[valueIndex] < e[0].value {
		// Discard values before the start.
		valueIndex++
	}
	for valueIndex < len(values) {
		value := values[valueIndex]

		i := sort.Search(len(e), func(i int) bool { return e[i].value >= value })
		if i == len(e) {
			// We're past the end.
			break
		}
		if i > 0 && e[i].value != value {
			// Need to look from previous entry.
			i--
		}
		// Don't Crc32 the entire postings offset table, this is very slow
		// so hope any issues were caught at startup.
		d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsTable), nil)
		d.Skip(e[i].off)

		// Iterate on the offset table.
		var postingsOff uint64 // The offset into the postings table.
		for d.Err() == nil {
			if skip == 0 {
				// These are always the same number of bytes,
				// and it's faster to skip than parse.
				skip = d.Len()
				d.Uvarint()      // Keycount.
				d.UvarintBytes() // Label name.
				skip -= d.Len()
			} else {
				d.Skip(skip)
			}
			v := d.UvarintBytes()       // Label value.
			postingsOff = d.Uvarint64() // Offset.
			for string(v) >= value {
				if string(v) == value {
					// Read from the postings table.
					d2 := encoding.NewDecbufAt(r.b, int(postingsOff), castagnoliTable)
					_, p, err := r.dec.Postings(d2.Get())
					if err != nil {
						return nil, errors.Wrap(err, "decode postings")
					}
					res = append(res, p)
				}
				valueIndex++
				if valueIndex == len(values) {
					break
				}
				value = values[valueIndex]
			}
			if i+1 == len(e) || value >= e[i+1].value || valueIndex == len(values) {
				// Need to go to a later postings offset entry, if there is one.
				break
			}
		}
		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "get postings offset entry")
		}
	}

	return Merge(res...), nil
}

// SortedPostings returns the given postings list reordered so that the backing series
// are sorted.
func (r *Reader) SortedPostings(p Postings) Postings {
	return p
}

// Size returns the size of an index file.
func (r *Reader) Size() int64 {
	return int64(r.b.Len())
}

// LabelNames returns all the unique label names present in the index.
func (r *Reader) LabelNames() ([]string, error) {
	labelNames := make([]string, 0, len(r.postings))
	for name := range r.postings {
		if name == allPostingsKey.Name {
			// This is not from any metric.
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

// NewStringListIter returns a StringIter for the given sorted list of strings.
func NewStringListIter(s []string) StringIter {
	return &stringListIter{l: s}
}

// 实现了StringIter接口
// symbolsIter implements StringIter.
type stringListIter struct {
	l   []string
	cur string
}

func (s *stringListIter) Next() bool {
	if len(s.l) == 0 {
		return false
	}
	s.cur = s.l[0]
	s.l = s.l[1:]
	return true
}
func (s stringListIter) At() string { return s.cur }
func (s stringListIter) Err() error { return nil }

// Decoder provides decoding methods for the v1 and v2 index file format.
//
// It currently does not contain decoding methods for all entry types but can be extended
// by them if there's demand.
type Decoder struct {
	LookupSymbol func(uint32) (string, error)
}

// Postings returns a postings list for b and its number of elements.
func (dec *Decoder) Postings(b []byte) (int, Postings, error) {
	d := encoding.Decbuf{B: b}
	n := d.Be32int()
	l := d.Get()
	return n, newBigEndianPostings(l), d.Err()
}

// LabelValueFor decodes a label for a given series.
func (dec *Decoder) LabelValueFor(b []byte, label string) (string, error) {
	d := encoding.Decbuf{B: b}
	k := d.Uvarint()

	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())

		if d.Err() != nil {
			return "", errors.Wrap(d.Err(), "read series label offsets")
		}

		ln, err := dec.LookupSymbol(lno)
		if err != nil {
			return "", errors.Wrap(err, "lookup label name")
		}

		if ln == label {
			lv, err := dec.LookupSymbol(lvo)
			if err != nil {
				return "", errors.Wrap(err, "lookup label value")
			}

			return lv, nil
		}
	}

	return "", d.Err()
}

// Series decodes a series entry from the given byte slice into lset and chks.
func (dec *Decoder) Series(b []byte, lbls *labels.Labels, chks *[]chunks.Meta) error {
	*lbls = (*lbls)[:0]
	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	k := d.Uvarint()

	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())

		if d.Err() != nil {
			return errors.Wrap(d.Err(), "read series label offsets")
		}

		ln, err := dec.LookupSymbol(lno)
		if err != nil {
			return errors.Wrap(err, "lookup label name")
		}
		lv, err := dec.LookupSymbol(lvo)
		if err != nil {
			return errors.Wrap(err, "lookup label value")
		}

		*lbls = append(*lbls, labels.Label{Name: ln, Value: lv})
	}

	// Read the chunks meta data.
	k = d.Uvarint()

	if k == 0 {
		return nil
	}

	t0 := d.Varint64()
	maxt := int64(d.Uvarint64()) + t0
	ref0 := int64(d.Uvarint64())

	*chks = append(*chks, chunks.Meta{
		Ref:     uint64(ref0),
		MinTime: t0,
		MaxTime: maxt,
	})
	t0 = maxt

	for i := 1; i < k; i++ {
		mint := int64(d.Uvarint64()) + t0
		maxt := int64(d.Uvarint64()) + mint

		ref0 += d.Varint64()
		t0 = maxt

		if d.Err() != nil {
			return errors.Wrapf(d.Err(), "read meta for chunk %d", i)
		}

		*chks = append(*chks, chunks.Meta{
			Ref:     uint64(ref0),
			MinTime: mint,
			MaxTime: maxt,
		})
	}
	return d.Err()
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
