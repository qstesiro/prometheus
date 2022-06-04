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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It received minor modifications to suit Prometheus's needs.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"encoding/binary"
	"io"
)

// bstream is a stream of bits.
type bstream struct {
	stream []byte // the data stream
	count  uint8  // how many bits are valid in current byte
}

func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

// 代码风格(未使用大写) ???
const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 { // 当前无可用空间增加一个字节
		b.stream = append(b.stream, 0)
		b.count = 8 // 当前可用位数为8
	}

	i := len(b.stream) - 1 // 定位到新的字节空间

	if bit { // 只需写入1(0不需要操作)
		b.stream[i] |= 1 << (b.count - 1) // 写入一位
	}

	b.count-- // 减少可用位数
}

// 按字节写数据时count不会变化(优秀) !!!
func (b *bstream) writeByte(byt byte) {
	if b.count == 0 { // 当前无可用空间增加一个字节
		b.stream = append(b.stream, 0)
		b.count = 8 // 当前可用位数为8
	}

	i := len(b.stream) - 1 // 定位到新的字节空间

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count) // 填充当前可用位

	b.stream = append(b.stream, 0) // 增加一个字节空间
	i++                            // 定位到新的字节空间
	b.stream[i] = byt << b.count   // 写入剩余数据位
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits)) // 将需要写入的位数右移到启始位置
	// 按字节写入
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt) // 写字节
		u <<= 8
		nbits -= 8
	}
	// 按位数写入
	for nbits > 0 {
		b.writeBit((u >> 63) == 1) // 写位数
		u <<= 1
		nbits--
	}
}

type bstreamReader struct {
	// 基础数据
	stream       []byte
	streamOffset int // The offset from which read the next byte from the stream.(可读数据的索引偏移)
	// 缓冲区
	buffer uint64 // The current buffer, filled from the stream, containing up to 8 bytes from which read bits.
	valid  uint8  // The number of bits valid to read (from left) in the current buffer.(当前可用比特位)
}

func newBReader(b []byte) bstreamReader {
	return bstreamReader{
		stream: b,
	}
}

func (b *bstreamReader) readBit() (bit, error) {
	if b.valid == 0 { // 缓冲区数据不够
		if !b.loadNextBuffer(1) {
			return false, io.EOF
		}
	}

	return b.readBitFast()
}

// readBitFast is like readBit but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBit().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitFast() (bit, error) {
	if b.valid == 0 { // 缓冲区数据不够
		return false, io.EOF
	}

	b.valid--                       // 先减少可用位数
	bitmask := uint64(1) << b.valid // 左移数据位
	return (b.buffer & bitmask) != 0, nil
}

func (b *bstreamReader) readBits(nbits uint8) (uint64, error) {
	if b.valid == 0 { // 缓冲区数据不够
		if !b.loadNextBuffer(nbits) {
			return 0, io.EOF
		}
	}

	if nbits <= b.valid { // 缓冲区数据足够
		return b.readBitsFast(nbits)
	}

	// We have to read all remaining valid bits from the current buffer and a part from the next one.
	// 从当前缓冲区中获取部分数据
	bitmask := (uint64(1) << b.valid) - 1 // 长度为b.valid且位全为1
	nbits -= b.valid
	v := (b.buffer & bitmask) << nbits
	b.valid = 0
	// 再读数据填充缓冲区
	if !b.loadNextBuffer(nbits) {
		return 0, io.EOF
	}
	// 从缓冲区中读剩余的数据
	bitmask = (uint64(1) << nbits) - 1 // 长度为b.valid且位全为1
	v = v | ((b.buffer >> (b.valid - nbits)) & bitmask)
	b.valid -= nbits

	return v, nil
}

// readBitsFast is like readBits but can return io.EOF if the internal buffer is empty.
// If it returns io.EOF, the caller should retry reading bits calling readBits().
// This function must be kept small and a leaf in order to help the compiler inlining it
// and further improve performances.
func (b *bstreamReader) readBitsFast(nbits uint8) (uint64, error) {
	if nbits > b.valid {
		return 0, io.EOF
	}

	bitmask := (uint64(1) << nbits) - 1 // 长度为b.valid且位全为1
	b.valid -= nbits

	return (b.buffer >> b.valid) & bitmask, nil
}

func (b *bstreamReader) ReadByte() (byte, error) {
	v, err := b.readBits(8)
	if err != nil {
		return 0, err
	}
	return byte(v), nil
}

// loadNextBuffer loads the next bytes from the stream into the internal buffer.
// The input nbits is the minimum number of bits that must be read, but the implementation
// can read more (if possible) to improve performances.
func (b *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if b.streamOffset >= len(b.stream) {
		return false
	}

	// Handle the case there are more then 8 bytes in the buffer (most common case)
	// in a optimized way. It's guaranteed that this branch will never read from the
	// very last byte of the stream (which suffers race conditions due to concurrent
	// writes).
	// 当前可用数据大于8字节,也就是stream中至少保留了一个字节
	// 因为写bit实际操作的是一个byte所以保留一个byte可以避免并发读写问题
	if b.streamOffset+8 < len(b.stream) {
		b.buffer = binary.BigEndian.Uint64(b.stream[b.streamOffset:])
		b.streamOffset += 8
		b.valid = 64
		return true
	}

	// We're here if the are 8 or less bytes left in the stream. Since this reader needs
	// to handle race conditions with concurrent writes happening on the very last byte
	// we make sure to never over more than the minimum requested bits (rounded up to
	// the next byte). The following code is slower but called less frequently.
	// 不明白有以下疑问 ???
	// - 为什么计算nbytes要多一个byte
	// - b.streamOffset+nbytes >= len(b.stream)的情况读的所有可用数据不保留最后byte如何保证并发
	nbytes := int((nbits / 8) + 1)
	if b.streamOffset+nbytes > len(b.stream) {
		nbytes = len(b.stream) - b.streamOffset
	}

	buffer := uint64(0)
	for i := 0; i < nbytes; i++ {
		buffer = buffer | (uint64(b.stream[b.streamOffset+i]) << uint(8*(nbytes-i-1)))
	}

	b.buffer = buffer
	b.streamOffset += nbytes
	b.valid = uint8(nbytes * 8)

	return true
}
