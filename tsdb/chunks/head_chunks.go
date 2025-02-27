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

package chunks

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

// Head chunk file header fields constants.
const (
	// MagicHeadChunks is 4 bytes at the beginning of a head chunk file.
	MagicHeadChunks = 0x0130BC91

	headChunksFormatV1 = 1
)

var (
	// ErrChunkDiskMapperClosed returned by any method indicates
	// that the ChunkDiskMapper was closed.
	ErrChunkDiskMapperClosed = errors.New("ChunkDiskMapper closed")
)

const (
	// MintMaxtSize is the size of the mint/maxt for head chunk file and chunks.
	MintMaxtSize = 8
	// SeriesRefSize is the size of series reference on disk.
	SeriesRefSize = 8
	// HeadChunkFileHeaderSize is the total size of the header for the head chunk file.
	// MagicChunksSize + ChunksFormatVersionSize + segmentHeaderPaddingSize = 8B
	// ┌──────────────────────────────┐
	// │  magic(0x85BD40DD) <4 byte>  │
	// ├──────────────────────────────┤
	// │    version(1) <1 byte>       │
	// ├──────────────────────────────┤
	// │    padding(0) <3 byte>       │
	// ├──────────────────────────────┤
	HeadChunkFileHeaderSize = SegmentHeaderSize
	// MaxHeadChunkFileSize is the max size of a head chunk file.
	MaxHeadChunkFileSize = 128 * 1024 * 1024 // 128 MiB.
	// CRCSize is the size of crc32 sum on disk.
	CRCSize = 4
	// MaxHeadChunkMetaSize is the max size of an mmapped chunks minus the chunks data.
	// Max because the uvarint size can be smaller.
	// ChunksFormatVersionSize = encoding(实际存储) = 1
	// MaxChunkLengthFieldSize = binary.MaxVarintLen32(5字节)
	// 整个chunk文件存储格式(MaxHeadChunkMetaSize中不包含data部分) ┌─────────────────────┬───────────────────────┬───────────────────────┬───────────────────┬───────────────┬──────────────┬────────────────┐
	// │ series ref <8 byte> │ mint <8 byte, uint64> │ maxt <8 byte, uint64> │ encoding <1 byte> │ len <uvarint> │ data <bytes> │ CRC32 <4 byte> │
	// └─────────────────────┴───────────────────────┴───────────────────────┴───────────────────┴───────────────┴──────────────┴────────────────┘
	MaxHeadChunkMetaSize = SeriesRefSize + 2*MintMaxtSize + ChunksFormatVersionSize + MaxChunkLengthFieldSize + CRCSize // 34byte
	// MinWriteBufferSize is the minimum write buffer size allowed.
	MinWriteBufferSize = 64 * 1024 // 64KB.
	// MaxWriteBufferSize is the maximum write buffer size allowed.
	MaxWriteBufferSize = 8 * 1024 * 1024 // 8 MiB.
	// DefaultWriteBufferSize is the default write buffer size.
	DefaultWriteBufferSize = 4 * 1024 * 1024 // 4 MiB.
)

// CorruptionErr is an error that's returned when corruption is encountered.
type CorruptionErr struct {
	Dir       string
	FileIndex int
	Err       error
}

func (e *CorruptionErr) Error() string {
	return errors.Wrapf(e.Err, "corruption in head chunk file %s", segmentFile(e.Dir, e.FileIndex)).Error()
}

// ChunkDiskMapper is for writing the Head block chunks to the disk
// and access chunks via mmapped file.
type ChunkDiskMapper struct {
	curFileNumBytes atomic.Int64 // Bytes written in current open file.

	/// Writer.
	/// 写相关
	/*
	                     +----------+
	                     |  chunk   |
	                     +-----+----+
	                           |
	                 +---------+---------+
	   chkWriter     |  writeBufferSize  |
	                 +---------+---------+
	                           |
	             +-------------+---------------+
	             |            head             |
	             +-----------------------------+
	   curFile   |           chunk1            |
	             +-----------------------------+
	             |           chunk2            |
	             +-----------------------------+
	*/

	dir             *os.File
	writeBufferSize int

	curFile         *os.File // File being written to.
	curFileSequence int      // Index of current open file being appended to.
	curFileMaxt     int64    // Used for the size retention.

	byteBuf      [MaxHeadChunkMetaSize]byte // Buffer used to write the header of the chunk.
	chkWriter    *bufio.Writer              // Writer for the current open file.
	crc32        hash.Hash
	writePathMtx sync.Mutex

	/// Reader.
	/// 读相关
	// The int key in the map is the file number on the disk.
	mmappedChunkFiles map[int]*mmappedChunkFile // Contains the m-mapped files for each chunk file mapped with its index.
	closers           map[int]io.Closer         // Closers for resources behind the byte slices.
	readPathMtx       sync.RWMutex              // Mutex used to protect the above 2 maps.
	pool              chunkenc.Pool             // This is used when fetching a chunk from the disk to allocate a chunk.

	// Writer and Reader.
	// 读写相关
	// We flush chunks to disk in batches. Hence, we store them in this buffer
	// from which chunks are served till they are flushed and are ready for m-mapping.
	chunkBuffer *chunkBuffer

	// If 'true', it indicated that the maxt of all the on-disk files were set
	// after iterating through all the chunks in those files.
	fileMaxtSet bool

	closed bool
}

type mmappedChunkFile struct {
	byteSlice ByteSlice // realByteSliceo类型
	maxt      int64
}

// NewChunkDiskMapper returns a new writer against the given directory
// using the default head chunk file duration.
// NOTE: 'IterateAllChunks' method needs to be called at least once after creating ChunkDiskMapper
// to set the maxt of all the file.
func NewChunkDiskMapper(dir string, pool chunkenc.Pool, writeBufferSize int) (*ChunkDiskMapper, error) {
	// Validate write buffer size.
	if writeBufferSize < MinWriteBufferSize || writeBufferSize > MaxWriteBufferSize {
		return nil, errors.Errorf("ChunkDiskMapper write buffer size should be between %d and %d (actual: %d)", MinWriteBufferSize, MaxHeadChunkFileSize, writeBufferSize)
	}
	if writeBufferSize%1024 != 0 {
		return nil, errors.Errorf("ChunkDiskMapper write buffer size should be a multiple of 1024 (actual: %d)", writeBufferSize)
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}

	m := &ChunkDiskMapper{
		dir:             dirFile,
		pool:            pool,
		writeBufferSize: writeBufferSize,
		crc32:           newCRC32(),
		chunkBuffer:     newChunkBuffer(),
	}

	if m.pool == nil {
		m.pool = chunkenc.NewPool()
	}

	return m, m.openMMapFiles()
}

func (cdm *ChunkDiskMapper) openMMapFiles() (returnErr error) {
	cdm.mmappedChunkFiles = map[int]*mmappedChunkFile{}
	cdm.closers = map[int]io.Closer{}
	defer func() {
		if returnErr != nil {
			returnErr = tsdb_errors.NewMulti(returnErr, closeAllFromMap(cdm.closers)).Err()

			cdm.mmappedChunkFiles = nil
			cdm.closers = nil
		}
	}()
	// map[seq]path
	files, err := listChunkFiles(cdm.dir.Name())
	if err != nil {
		return err
	}

	files, err = repairLastChunkFile(files)
	if err != nil {
		return err
	}

	chkFileIndices := make([]int, 0, len(files))
	for seq, fn := range files {
		f, err := fileutil.OpenMmapFile(fn) // ???
		if err != nil {
			return errors.Wrapf(err, "mmap files, file: %s", fn)
		}
		cdm.closers[seq] = f
		cdm.mmappedChunkFiles[seq] = &mmappedChunkFile{byteSlice: realByteSlice(f.Bytes())} // IterateAllChunks中设置maxt
		chkFileIndices = append(chkFileIndices, seq)
	}

	// Check for gaps in the files.
	// 不允许有间隔在序号之间
	sort.Ints(chkFileIndices)
	if len(chkFileIndices) == 0 {
		return nil
	}
	lastSeq := chkFileIndices[0]
	for _, seq := range chkFileIndices[1:] {
		if seq != lastSeq+1 {
			return errors.Errorf("found unsequential head chunk files %s (index: %d) and %s (index: %d)", files[lastSeq], lastSeq, files[seq], seq)
		}
		lastSeq = seq
	}

	// 验证size,magic,version
	for i, b := range cdm.mmappedChunkFiles {
		if b.byteSlice.Len() < HeadChunkFileHeaderSize {
			return errors.Wrapf(errInvalidSize, "%s: invalid head chunk file header", files[i])
		}
		// Verify magic number.
		if m := binary.BigEndian.Uint32(b.byteSlice.Range(0, MagicChunksSize)); m != MagicHeadChunks {
			return errors.Errorf("%s: invalid magic number %x", files[i], m)
		}

		// Verify chunk format version.
		if v := int(b.byteSlice.Range(MagicChunksSize, MagicChunksSize+ChunksFormatVersionSize)[0]); v != chunksFormatV1 {
			return errors.Errorf("%s: invalid chunk format version %d", files[i], v)
		}
	}

	return nil
}

func listChunkFiles(dir string) (map[int]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := map[int]string{}
	for _, fi := range files {
		seq, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}
		res[int(seq)] = filepath.Join(dir, fi.Name())
	}

	return res, nil
}

// repairLastChunkFile deletes the last file if it's empty.
// Because we don't fsync when creating these file, we could end
// up with an empty file at the end during an abrupt shutdown.
// 没有真的实际修复操作只是将大小为零的文件删除掉
func repairLastChunkFile(files map[int]string) (_ map[int]string, returnErr error) {
	lastFile := -1
	for seq := range files {
		if seq > lastFile {
			lastFile = seq
		}
	}

	if lastFile <= 0 {
		return files, nil
	}

	info, err := os.Stat(files[lastFile])
	if err != nil {
		return files, errors.Wrap(err, "file stat during last head chunk file repair")
	}
	if info.Size() == 0 {
		// Corrupt file, hence remove it.
		if err := os.RemoveAll(files[lastFile]); err != nil {
			return files, errors.Wrap(err, "delete corrupted, empty head chunk file during last file repair")
		}
		delete(files, lastFile)
	}

	return files, nil
}

// 与IterateAllChunks函数对应
// WriteChunk writes the chunk to the disk.
// The returned chunk ref is the reference from where the chunk encoding starts for the chunk.
// 文件格式(在chunks_head目录)
// ┌──────────────────────────────┐
// │  magic(0x85BD40DD) <4 byte>  │
// ├──────────────────────────────┤
// │    version(1) <1 byte>       │
// ├──────────────────────────────┤
// │    padding(0) <3 byte>       │
// ├──────────────────────────────┤
// │ ┌──────────────────────────┐ │
// │ │         Chunk 1          │ │
// │ ├──────────────────────────┤ │
// │ │          ...             │ │
// │ ├──────────────────────────┤ │
// │ │         Chunk N          │ │
// │ └──────────────────────────┘ │
// └──────────────────────────────┘
// Chunk格式
// ┌─────────────────────┬───────────────────────┬───────────────────────┬───────────────────┬───────────────┬──────────────┬────────────────┐
// │ series ref <8 byte> │ mint <8 byte, uint64> │ maxt <8 byte, uint64> │ encoding <1 byte> │ len <uvarint> │ data <bytes> │ CRC32 <4 byte> │
// └─────────────────────┴───────────────────────┴───────────────────────┴───────────────────┴───────────────┴──────────────┴────────────────┘
// seriesRef单调递增
func (cdm *ChunkDiskMapper) WriteChunk(seriesRef uint64, mint, maxt int64, chk chunkenc.Chunk) (chkRef uint64, err error) {
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()

	if cdm.closed {
		return 0, ErrChunkDiskMapperClosed
	}

	if cdm.shouldCutNewFile(len(chk.Bytes())) {
		if err := cdm.cut(); err != nil {
			return 0, err
		}
	}

	// if len(chk.Bytes())+MaxHeadChunkMetaSize >= writeBufferSize, it means that chunk >= the buffer size;
	// so no need to flush here, as we have to flush at the end (to not keep partial chunks in buffer).
	// 以下两个条件保证不会出现chunk数据刷新空间分裂(部分在缓冲,另一部分在磁盘)
	// - 如果大于writeBufferSize刷新
	// - 如果小于writeBufferSize但是缓冲空间不足刷新
	// 并且在此基础上尽量减少了刷新操作(减少磁盘I/O)
	if len(chk.Bytes())+MaxHeadChunkMetaSize < cdm.writeBufferSize && cdm.chkWriter.Available() < MaxHeadChunkMetaSize+len(chk.Bytes()) {
		if err := cdm.flushBuffer(); err != nil {
			return 0, err
		}
	}

	cdm.crc32.Reset()
	bytesWritten := 0

	// The upper 4 bytes are for the head chunk file index and
	// the lower 4 bytes are for the head chunk file offset where to start reading this chunk.
	chkRef = chunkRef(uint64(cdm.curFileSequence), uint64(cdm.curFileSize()))

	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], seriesRef) // 序列ID
	bytesWritten += SeriesRefSize
	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], uint64(mint)) // 时间左边界
	bytesWritten += MintMaxtSize
	binary.BigEndian.PutUint64(cdm.byteBuf[bytesWritten:], uint64(maxt)) // 时间右边界
	bytesWritten += MintMaxtSize
	cdm.byteBuf[bytesWritten] = byte(chk.Encoding()) // Chunk编码类型(XOR)
	bytesWritten += ChunkEncodingSize
	n := binary.PutUvarint(cdm.byteBuf[bytesWritten:], uint64(len(chk.Bytes()))) // 数据长度
	bytesWritten += n
	// 将byteBuf中缓存数据(非data部分)写入文件与crc计算缓冲
	if err := cdm.writeAndAppendToCRC32(cdm.byteBuf[:bytesWritten]); err != nil {
		return 0, err
	}
	// 将具体head-chunk数据写入文件与crc计算缓冲
	if err := cdm.writeAndAppendToCRC32(chk.Bytes()); err != nil { // 数据
		return 0, err
	}
	if err := cdm.writeCRC32(); err != nil { // 计算CRC(包含元信息与数据)
		return 0, err
	}
	// 更新最大时间
	if maxt > cdm.curFileMaxt {
		cdm.curFileMaxt = maxt
	}

	// 保证被刷新到写缓冲区的数据在内存中以chunkenc.Chunk形式存在
	// 为了保证未落地磁盘之前数据可以被正常查询
	// 与写缓冲区刷新保持同步一但写缓冲被刷新到磁盘则立即清理
	cdm.chunkBuffer.put(chkRef, chk)

	// 大于writeBufferSize刷新
	// 此种情况不write不会写失败,因为当写入数据大小超过缓冲大小就自动向底层flush数据
	// 每次刷新都会完整flush整个缓冲区但是需要注意最后数据可能会有不满缓冲区的数据残留
	// 在缓存区中所以需要flush一次,保证一个chunk的数据全都flush到磁盘
	if len(chk.Bytes())+MaxHeadChunkMetaSize >= cdm.writeBufferSize {
		// The chunk was bigger than the buffer itself.
		// Flushing to not keep partial chunks in buffer.
		if err := cdm.flushBuffer(); err != nil {
			return 0, err
		}
	}

	return chkRef, nil
}

func chunkRef(seq, offset uint64) (chunkRef uint64) {
	return (seq << 32) | offset // 高4位为文件编号,低4位为文件内偏移
}

// shouldCutNewFile decides the cutting of a new file based on time and size retention.
// Size retention: because depending on the system architecture, there is a limit on how big of a file we can m-map.
// Time retention: so that we can delete old chunks with some time guarantee in low load environments.
func (cdm *ChunkDiskMapper) shouldCutNewFile(chunkSize int) bool {
	// MaxHeadChunkFileSize = 128 * 1024 * 1024 // 128 MiB.
	return cdm.curFileSize() == 0 || // First head chunk file.
		// 当前数据+新增数据+meta数据
		cdm.curFileSize()+int64(chunkSize+MaxHeadChunkMetaSize) > MaxHeadChunkFileSize // Exceeds the max head chunk file size.
}

// CutNewFile creates a new m-mapped file.
func (cdm *ChunkDiskMapper) CutNewFile() (returnErr error) {
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()

	return cdm.cut()
}

// cut creates a new m-mapped file. The write lock should be held before calling this.
func (cdm *ChunkDiskMapper) cut() (returnErr error) {
	// Sync current tail to disk and close.
	if err := cdm.finalizeCurFile(); err != nil {
		return err
	}

	n, newFile, seq, err := cutSegmentFile(cdm.dir, MagicHeadChunks, headChunksFormatV1, HeadChunkFilePreallocationSize)
	if err != nil {
		return err
	}
	defer func() {
		// The file should not be closed if there is no error,
		// its kept open in the ChunkDiskMapper.
		if returnErr != nil {
			returnErr = tsdb_errors.NewMulti(returnErr, newFile.Close()).Err()
		}
	}()

	cdm.curFileNumBytes.Store(int64(n))

	if cdm.curFile != nil {
		cdm.readPathMtx.Lock()
		cdm.mmappedChunkFiles[cdm.curFileSequence].maxt = cdm.curFileMaxt
		cdm.readPathMtx.Unlock()
	}

	mmapFile, err := fileutil.OpenMmapFileWithSize(newFile.Name(), int(MaxHeadChunkFileSize))
	if err != nil {
		return err
	}

	cdm.readPathMtx.Lock()
	cdm.curFileSequence = seq
	cdm.curFile = newFile
	if cdm.chkWriter != nil {
		cdm.chkWriter.Reset(newFile)
	} else {
		cdm.chkWriter = bufio.NewWriterSize(newFile, cdm.writeBufferSize)
	}

	cdm.closers[cdm.curFileSequence] = mmapFile
	cdm.mmappedChunkFiles[cdm.curFileSequence] = &mmappedChunkFile{byteSlice: realByteSlice(mmapFile.Bytes())}
	cdm.readPathMtx.Unlock()

	cdm.curFileMaxt = 0

	return nil
}

// finalizeCurFile writes all pending data to the current tail file,
// truncates its size, and closes it.
func (cdm *ChunkDiskMapper) finalizeCurFile() error {
	if cdm.curFile == nil {
		return nil
	}

	if err := cdm.flushBuffer(); err != nil {
		return err
	}

	if err := cdm.curFile.Sync(); err != nil {
		return err
	}

	return cdm.curFile.Close()
}

func (cdm *ChunkDiskMapper) write(b []byte) error {
	n, err := cdm.chkWriter.Write(b)
	cdm.curFileNumBytes.Add(int64(n))
	return err
}

func (cdm *ChunkDiskMapper) writeAndAppendToCRC32(b []byte) error {
	if err := cdm.write(b); err != nil {
		return err
	}
	_, err := cdm.crc32.Write(b)
	return err
}

func (cdm *ChunkDiskMapper) writeCRC32() error {
	return cdm.write(cdm.crc32.Sum(cdm.byteBuf[:0]))
}

// flushBuffer flushes the current in-memory chunks.
// Assumes that writePathMtx is _write_ locked before calling this method.
func (cdm *ChunkDiskMapper) flushBuffer() error {
	// 数据刷新后立即同步内存(刷新之前内存为空)
	if err := cdm.chkWriter.Flush(); err != nil {
		return err
	}
	cdm.chunkBuffer.clear()
	return nil
}

// Chunk returns a chunk from a given reference.
func (cdm *ChunkDiskMapper) Chunk(ref uint64) (chunkenc.Chunk, error) {
	cdm.readPathMtx.RLock()
	// We hold this read lock for the entire duration because if the Close()
	// is called, the data in the byte slice will get corrupted as the mmapped
	// file will be closed.
	defer cdm.readPathMtx.RUnlock()

	var (
		// Get the upper 4 bytes.
		// These contain the head chunk file index.
		sgmIndex = int(ref >> 32)
		// Get the lower 4 bytes.
		// These contain the head chunk file offset where the chunk starts.
		// We skip the series ref and the mint/maxt beforehand.
		chkStart = int((ref<<32)>>32) + SeriesRefSize + (2 * MintMaxtSize)
		chkCRC32 = newCRC32()
	)

	if cdm.closed {
		return nil, ErrChunkDiskMapperClosed
	}

	// If it is the current open file, then the chunks can be in the buffer too.
	if sgmIndex == cdm.curFileSequence {
		chunk := cdm.chunkBuffer.get(ref)
		if chunk != nil {
			return chunk, nil
		}
	}

	mmapFile, ok := cdm.mmappedChunkFiles[sgmIndex]
	if !ok {
		if sgmIndex > cdm.curFileSequence {
			return nil, &CorruptionErr{
				Dir:       cdm.dir.Name(),
				FileIndex: -1,
				Err:       errors.Errorf("head chunk file index %d more than current open file", sgmIndex),
			}
		}
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       errors.New("head chunk file index %d does not exist on disk"),
		}
	}

	if chkStart+MaxChunkLengthFieldSize > mmapFile.byteSlice.Len() {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       errors.Errorf("head chunk file doesn't include enough bytes to read the chunk size data field - required:%v, available:%v", chkStart+MaxChunkLengthFieldSize, mmapFile.byteSlice.Len()),
		}
	}

	// Encoding.
	chkEnc := mmapFile.byteSlice.Range(chkStart, chkStart+ChunkEncodingSize)[0]

	// Data length.
	// With the minimum chunk length this should never cause us reading
	// over the end of the slice.
	chkDataLenStart := chkStart + ChunkEncodingSize
	c := mmapFile.byteSlice.Range(chkDataLenStart, chkDataLenStart+MaxChunkLengthFieldSize)
	chkDataLen, n := binary.Uvarint(c)
	if n <= 0 {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       errors.Errorf("reading chunk length failed with %d", n),
		}
	}

	// Verify the chunk data end.
	chkDataEnd := chkDataLenStart + n + int(chkDataLen)
	if chkDataEnd > mmapFile.byteSlice.Len() {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       errors.Errorf("head chunk file doesn't include enough bytes to read the chunk - required:%v, available:%v", chkDataEnd, mmapFile.byteSlice.Len()),
		}
	}

	// Check the CRC.
	sum := mmapFile.byteSlice.Range(chkDataEnd, chkDataEnd+CRCSize)
	if _, err := chkCRC32.Write(mmapFile.byteSlice.Range(chkStart-(SeriesRefSize+2*MintMaxtSize), chkDataEnd)); err != nil {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       err,
		}
	}
	if act := chkCRC32.Sum(nil); !bytes.Equal(act, sum) {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       errors.Errorf("checksum mismatch expected:%x, actual:%x", sum, act),
		}
	}

	// The chunk data itself.
	chkData := mmapFile.byteSlice.Range(chkDataEnd-int(chkDataLen), chkDataEnd)
	chk, err := cdm.pool.Get(chunkenc.Encoding(chkEnc), chkData)
	if err != nil {
		return nil, &CorruptionErr{
			Dir:       cdm.dir.Name(),
			FileIndex: sgmIndex,
			Err:       err,
		}
	}
	return chk, nil
}

// 与WriteChunk对应
// IterateAllChunks iterates on all the chunks in its byte slices in the order of the head chunk file sequence
// and runs the provided function on each chunk. It returns on the first error encountered.
// NOTE: This method needs to be called at least once after creating ChunkDiskMapper
// to set the maxt of all the file.
func (cdm *ChunkDiskMapper) IterateAllChunks(f func(seriesRef, chunkRef uint64, mint, maxt int64, numSamples uint16) error) (err error) {
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()

	defer func() {
		cdm.fileMaxtSet = true
	}()

	chkCRC32 := newCRC32()

	// Iterate files in ascending order.
	segIDs := make([]int, 0, len(cdm.mmappedChunkFiles))
	for seg := range cdm.mmappedChunkFiles {
		segIDs = append(segIDs, seg)
	}
	sort.Ints(segIDs) // 排序
	for _, segID := range segIDs {
		mmapFile := cdm.mmappedChunkFiles[segID]
		fileEnd := mmapFile.byteSlice.Len()
		if segID == cdm.curFileSequence {
			fileEnd = int(cdm.curFileSize())
		}
		idx := HeadChunkFileHeaderSize
		for idx < fileEnd {
			if fileEnd-idx < MaxHeadChunkMetaSize {
				// Check for all 0s which marks the end of the file.(文件尾部分可能有padding)
				allZeros := true
				for _, b := range mmapFile.byteSlice.Range(idx, fileEnd) {
					if b != byte(0) {
						allZeros = false
						break
					}
				}
				if allZeros {
					// End of segment chunk file content.
					break
				}
				return &CorruptionErr{
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err: errors.Errorf("head chunk file has some unread data, but doesn't include enough bytes to read the chunk header"+
						" - required:%v, available:%v, file:%d", idx+MaxHeadChunkMetaSize, fileEnd, segID),
				}
			}
			chkCRC32.Reset()
			chunkRef := chunkRef(uint64(segID), uint64(idx))

			startIdx := idx
			seriesRef := binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+SeriesRefSize)) // 序列ID
			idx += SeriesRefSize
			mint := int64(binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+MintMaxtSize))) // 时间左边界
			idx += MintMaxtSize
			maxt := int64(binary.BigEndian.Uint64(mmapFile.byteSlice.Range(idx, idx+MintMaxtSize))) // 时间右边界
			idx += MintMaxtSize

			// We preallocate file to help with m-mapping (especially windows systems).
			// As series ref always starts from 1, we assume it being 0 to be the end of the actual file data.
			// We are not considering possible file corruption that can cause it to be 0.
			// Additionally we are checking mint and maxt just to be sure.
			if seriesRef == 0 && mint == 0 && maxt == 0 {
				break
			}

			idx += ChunkEncodingSize                                                                 // Skip encoding.(Chunk编码类型)
			dataLen, n := binary.Uvarint(mmapFile.byteSlice.Range(idx, idx+MaxChunkLengthFieldSize)) // 数据长度
			idx += n

			numSamples := binary.BigEndian.Uint16(mmapFile.byteSlice.Range(idx, idx+2)) // T/V个数(此数据属于XORChunk文件部分)
			idx += int(dataLen)                                                         // Skip the data.(跳过数据)

			// In the beginning we only checked for the chunk meta size.
			// Now that we have added the chunk data length, we check for sufficient bytes again.
			if idx+CRCSize > fileEnd {
				return &CorruptionErr{
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err:       errors.Errorf("head chunk file doesn't include enough bytes to read the chunk header - required:%v, available:%v, file:%d", idx+CRCSize, fileEnd, segID),
				}
			}

			// Check CRC.
			sum := mmapFile.byteSlice.Range(idx, idx+CRCSize)
			if _, err := chkCRC32.Write(mmapFile.byteSlice.Range(startIdx, idx)); err != nil {
				return err
			}
			if act := chkCRC32.Sum(nil); !bytes.Equal(act, sum) {
				return &CorruptionErr{
					Dir:       cdm.dir.Name(),
					FileIndex: segID,
					Err:       errors.Errorf("checksum mismatch expected:%x, actual:%x", sum, act),
				}
			}
			idx += CRCSize

			if maxt > mmapFile.maxt {
				mmapFile.maxt = maxt // 设置maxt(多个Chunks中最大时间)
			}

			if err := f(seriesRef, chunkRef, mint, maxt, numSamples); err != nil {
				if cerr, ok := err.(*CorruptionErr); ok {
					cerr.Dir = cdm.dir.Name()
					cerr.FileIndex = segID
					return cerr
				}
				return err
			}
		}

		if idx > fileEnd {
			// It should be equal to the slice length.
			return &CorruptionErr{
				Dir:       cdm.dir.Name(),
				FileIndex: segID,
				Err:       errors.Errorf("head chunk file doesn't include enough bytes to read the last chunk data - required:%v, available:%v, file:%d", idx, fileEnd, segID),
			}
		}
	}

	return nil
}

// Truncate deletes the head chunk files which are strictly below the mint.
// mint should be in milliseconds.
// 删除已经被合并(mmappedChunkFiles.maxt < mint)文件(head_chunks目录中)
// mmappedChunkFiles.maxt < mint与memSeries.truncateChunksBefore删除条件一致
func (cdm *ChunkDiskMapper) Truncate(mint int64) error {
	if !cdm.fileMaxtSet {
		return errors.New("maxt of the files are not set")
	}
	cdm.readPathMtx.RLock()

	// Sort the file indices, else if files deletion fails in between,
	// it can lead to unsequential files as the map is not sorted.
	chkFileIndices := make([]int, 0, len(cdm.mmappedChunkFiles))
	for seq := range cdm.mmappedChunkFiles {
		chkFileIndices = append(chkFileIndices, seq)
	}
	sort.Ints(chkFileIndices)

	var removedFiles []int
	for _, seq := range chkFileIndices {
		if seq == cdm.curFileSequence || cdm.mmappedChunkFiles[seq].maxt >= mint {
			break
		}
		if cdm.mmappedChunkFiles[seq].maxt < mint {
			removedFiles = append(removedFiles, seq)
		}
	}
	cdm.readPathMtx.RUnlock()

	errs := tsdb_errors.NewMulti()
	// Cut a new file only if the current file has some chunks.
	if cdm.curFileSize() > HeadChunkFileHeaderSize {
		errs.Add(cdm.CutNewFile())
	}
	// 删除mmap-chunk数据
	errs.Add(cdm.deleteFiles(removedFiles))
	return errs.Err()
}

func (cdm *ChunkDiskMapper) deleteFiles(removedFiles []int) error {
	cdm.readPathMtx.Lock()
	for _, seq := range removedFiles {
		if err := cdm.closers[seq].Close(); err != nil {
			cdm.readPathMtx.Unlock()
			return err
		}
		delete(cdm.mmappedChunkFiles, seq)
		delete(cdm.closers, seq)
	}
	cdm.readPathMtx.Unlock()

	// We actually delete the files separately to not block the readPathMtx for long.
	for _, seq := range removedFiles {
		if err := os.Remove(segmentFile(cdm.dir.Name(), seq)); err != nil {
			return err
		}
	}

	return nil
}

// DeleteCorrupted deletes all the head chunk files after the one which had the corruption
// (including the corrupt file).
func (cdm *ChunkDiskMapper) DeleteCorrupted(originalErr error) error {
	err := errors.Cause(originalErr) // So that we can pick up errors even if wrapped.
	cerr, ok := err.(*CorruptionErr)
	if !ok {
		return errors.Wrap(originalErr, "cannot handle error")
	}

	// Delete all the head chunk files following the corrupt head chunk file.
	segs := []int{}
	cdm.readPathMtx.RLock()
	for seg := range cdm.mmappedChunkFiles {
		if seg >= cerr.FileIndex {
			segs = append(segs, seg)
		}
	}
	cdm.readPathMtx.RUnlock()

	return cdm.deleteFiles(segs)
}

// Size returns the size of the chunk files.
func (cdm *ChunkDiskMapper) Size() (int64, error) {
	return fileutil.DirSize(cdm.dir.Name())
}

func (cdm *ChunkDiskMapper) curFileSize() int64 {
	return cdm.curFileNumBytes.Load()
}

// Close closes all the open files in ChunkDiskMapper.
// It is not longer safe to access chunks from this struct after calling Close.
func (cdm *ChunkDiskMapper) Close() error {
	// 'WriteChunk' locks writePathMtx first and then readPathMtx for cutting head chunk file.
	// The lock order should not be reversed here else it can cause deadlocks.
	cdm.writePathMtx.Lock()
	defer cdm.writePathMtx.Unlock()
	cdm.readPathMtx.Lock()
	defer cdm.readPathMtx.Unlock()

	if cdm.closed {
		return nil
	}
	cdm.closed = true

	errs := tsdb_errors.NewMulti(
		closeAllFromMap(cdm.closers),
		cdm.finalizeCurFile(),
		cdm.dir.Close(),
	)
	cdm.mmappedChunkFiles = map[int]*mmappedChunkFile{}
	cdm.closers = map[int]io.Closer{}

	return errs.Err()
}

func closeAllFromMap(cs map[int]io.Closer) error {
	errs := tsdb_errors.NewMulti()
	for _, c := range cs {
		errs.Add(c.Close())
	}
	return errs.Err()
}

const inBufferShards = 128 // 128 is a randomly chosen number.

// chunkBuffer is a thread safe buffer for chunks.
type chunkBuffer struct {
	inBufferChunks     [inBufferShards]map[uint64]chunkenc.Chunk
	inBufferChunksMtxs [inBufferShards]sync.RWMutex // 不需要padding(避免乒乓效应) ???
}

func newChunkBuffer() *chunkBuffer {
	cb := &chunkBuffer{}
	for i := 0; i < inBufferShards; i++ {
		cb.inBufferChunks[i] = make(map[uint64]chunkenc.Chunk)
	}
	return cb
}

func (cb *chunkBuffer) put(ref uint64, chk chunkenc.Chunk) {
	shardIdx := ref % inBufferShards

	cb.inBufferChunksMtxs[shardIdx].Lock()
	cb.inBufferChunks[shardIdx][ref] = chk
	cb.inBufferChunksMtxs[shardIdx].Unlock()
}

func (cb *chunkBuffer) get(ref uint64) chunkenc.Chunk {
	shardIdx := ref % inBufferShards

	cb.inBufferChunksMtxs[shardIdx].RLock()
	defer cb.inBufferChunksMtxs[shardIdx].RUnlock()

	return cb.inBufferChunks[shardIdx][ref]
}

func (cb *chunkBuffer) clear() {
	for i := 0; i < inBufferShards; i++ {
		cb.inBufferChunksMtxs[i].Lock()
		cb.inBufferChunks[i] = make(map[uint64]chunkenc.Chunk)
		cb.inBufferChunksMtxs[i].Unlock()
	}
}
