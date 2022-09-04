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
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmap'd
// read-only byte slices.

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
	"math"
	"math/bits"
)

const (
	chunkCompactCapacityThreshold = 32
)

// XORChunk holds XOR encoded sample data.
// 实现Chunk接口
type XORChunk struct {
	b bstream
}

// 资料文章
// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf(论文)
// http://www.nosqlnotes.com/technotes/tsdb/facebook-gorilla-1/
// https://developer.aliyun.com/article/174535
// https://www.jianshu.com/p/0e21343b244d
// https://github.com/chenjiandongx/mandodb

// NewXORChunk returns a new chunk with XOR encoding of the given size.
func NewXORChunk() *XORChunk {
	b := make([]byte, 2, 128) // 前两个字节为T/V对个数(这种初始方式思路清奇)
	return &XORChunk{b: bstream{stream: b, count: 0}}
}

// Encoding returns the encoding type.
func (c *XORChunk) Encoding() Encoding {
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.
func (c *XORChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *XORChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

func (c *XORChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
func (c *XORChunk) Appender() (Appender, error) {
	it := c.iterator(nil)

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &xorAppender{
		b:        &c.b,
		t:        it.t,
		v:        it.val,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if binary.BigEndian.Uint16(a.b.bytes()) == 0 {
		a.leading = 0xff // 初始
	}
	return a, nil
}

func (c *XORChunk) iterator(it Iterator) *xorIterator {
	// Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	// When using striped locks to guard access to chunks, probably yes.
	// Could only copy data if the chunk is not completed yet.
	if xorIter, ok := it.(*xorIterator); ok {
		xorIter.Reset(c.b.bytes())
		return xorIter
	}
	// +----------+--------------------------------+-----------+
	// | 总数(2b) |          已写入数据            |  新数据   |
	// +----------+--------------------------------+-----------+
	// 在获取迭代器的外部会锁定数据阻止并发写入
	// Iter通过记录总数限定迭代数据不会超过"已写入数据"部分
	// 后续解锁后新数据写入只会操作"新数据"部分
	// 所以并发读写的缓冲区中不同的部分也就没有并发问题
	// 有效解决了并发读写值得借鉴 !!!
	return &xorIterator{
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		br:       newBReader(c.b.bytes()[2:]),          // 跳过2个字节
		numTotal: binary.BigEndian.Uint16(c.b.bytes()), // 取前两个字节内容
		t:        math.MinInt64,
	}
}

// Iterator implements the Chunk interface.
func (c *XORChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

// 实现了chunkenc.Appender接口
type xorAppender struct {
	b *bstream

	t      int64
	v      float64
	tDelta uint64

	leading  uint8
	trailing uint8
}

// 整体格式
/*
   +-------------------------------+-------------------+
   |               T1              |         V1        |
   +-------------------------------+-------------------+
   |            TD=T2-T1           |      VD=V2^V1     |
   +-------------------------------+-------------------+
   |         TDOD=T3-T2-TD         |      VD=V3^V2     |
   +-------------------------------+-------------------+
   |              ...              |        ...        |
   +-------------------------------+-------------------+
   | TDOD[n]=T[n]-T[n-1]-TDOD[n-1] | VD[n]=V[n]^V[n-1] |
   +-------------------------------+-------------------+
*/

// Delta-of-Delta格式
/*
  +-+
  |0| DOD = 0      1b
  +-+
  +--+---------+
  |10| 14位DOD |   16b
  +--+---------+
  +---+---------+
  |110| 17位DOD |  20b
  +---+---------+
  +----+---------+
  |1110| 20位DOD | 24b
  +----+---------+
  +----+---------+
  |1111| 64位DOD | 68b
  +---+----------+
*/
func (a *xorAppender) Append(t int64, v float64) {
	var tDelta uint64
	num := binary.BigEndian.Uint16(a.b.bytes())

	if num == 0 {
		// 写入T(num=0与num=1相同的写入方式)
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutVarint(buf, t)] {
			a.b.writeByte(b)
		}
		// 写入V
		// 浮点数二进制表示
		// https://zhuanlan.zhihu.com/p/343033661
		// https://www.h-schmidt.net/FloatConverter/IEEE754.html
		a.b.writeBits(math.Float64bits(v), 64)

	} else if num == 1 {
		tDelta = uint64(t - a.t)
		// 写入T(num=0与num=1相同的写入方式)
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			a.b.writeByte(b)
		}
		// 写入V
		a.writeVDelta(v)

	} else {
		tDelta = uint64(t - a.t)
		dod := int64(tDelta - a.tDelta)
		// 写入T
		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		// 扩大了范围gorilla精度是秒而prometheus精度是毫秒
		switch {
		case dod == 0:
			a.b.writeBit(zero)
		case bitRange(dod, 14):
			a.b.writeBits(0x02, 2) // '10'
			a.b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			a.b.writeBits(0x06, 3) // '110'
			a.b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			a.b.writeBits(0x0e, 4) // '1110'
			a.b.writeBits(uint64(dod), 20)
		default:
			a.b.writeBits(0x0f, 4) // '1111'
			a.b.writeBits(uint64(dod), 64)
		}
		// 写入V
		a.writeVDelta(v)
	}

	a.t = t
	a.v = v
	binary.BigEndian.PutUint16(a.b.bytes(), num+1) // 记录T/V对数
	a.tDelta = tDelta
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

func (a *xorAppender) writeTDelta(t int64) {
	// 是否增加单独的时间写入函数逻辑对称性更好 ???
}

// XOR-Delta格式
/*
   +-+
   |0| Delta = 0
   +-+
   +-+-+----------------------+
   |1|0| 与之前有效数据位相同 |
   +-+-+----------------------+
   +-+-+-----------------+-------------------+----------+
   |1|1| leading长度(5b) |  有效数据位数(6b) | 差值数据 |
   +-+-+-----------------+-------------------+----------+
*/
func (a *xorAppender) writeVDelta(v float64) {
	// 差值运算不是数学差值而是xor运算
	// 浮点数的数学差值不会减少内存使用(不像整数的数学差值数据大小与内存使用成正比)
	// 浮点数无论数据大小都占用固定大小内存所以使用xor
	// 对于相近值xor运行结果中leading与tailing零占比大有效数据占比小
	// https://cloud.tencent.com/developer/article/1473541
	// https://pkg.go.dev/math#pkg-constants
	vDelta := math.Float64bits(v) ^ math.Float64bits(a.v)
	// 与前值相等
	if vDelta == 0 {
		a.b.writeBit(zero)
		return
	}
	// 与前值不相等
	a.b.writeBit(one)
	// +--------------+--------------------+--------------+
	// |  leading(0)  |      有效数据      |  tailing(0)  |
	// +--------------+--------------------+--------------+
	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	// 使用5位存储leading不能超过31(如果超过则收窄这会导致有效差值的长度中包含多余的0)
	if leading >= 32 {
		leading = 31 // 5位存储无符号数最大31
	}
	// a.leading != 0xff 保证之前已经进行过差值运算 (创建appender时初始化a.leading = 0xff)
	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		// +-+-+----------------------+
		// |1|0| 与之前有效数据位相同 |
		// +-+-+----------------------+
		// 因leading >= a.leading && trailing >= a.trailing条件
		// 所以实际保存有效数据盲位的前或后有冗余的0(读时不影响数据还原)
		a.b.writeBit(zero)
		a.b.writeBits(vDelta>>a.trailing, 64-int(a.leading)-int(a.trailing))
	} else {
		a.leading, a.trailing = leading, trailing
		// +-+-+-----------------+-------------------+----------+
		// |1|1| leading长度(5b) |  有效数据位数(6b) | 差值数据 |
		// +-+-+-----------------+-------------------+----------+
		a.b.writeBit(one)
		// leading/tailing两者必须存储之一不然无法计算原始数据
		a.b.writeBits(uint64(leading), 5) // 5位存储无符号数最大31

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		sigbits := 64 - leading - trailing
		a.b.writeBits(uint64(sigbits), 6) // 6位存储无符号数最大63
		a.b.writeBits(vDelta>>trailing, int(sigbits))
	}
}

type xorIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t   int64   // 前一个T
	val float64 // 前一个V

	leading  uint8
	trailing uint8

	tDelta uint64 // 前一个DOD
	err    error
}

func (it *xorIterator) Seek(t int64) bool {
	if it.err != nil {
		return false
	}

	for t > it.t || it.numRead == 0 {
		if !it.Next() {
			return false
		}
	}
	return true
}

func (it *xorIterator) At() (int64, float64) {
	return it.t, it.val
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b) // 取出T/V对数

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.leading = 0
	it.trailing = 0
	it.tDelta = 0
	it.err = nil
}

func (it *xorIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}
	// 第一对T/V
	if it.numRead == 0 {
		t, err := binary.ReadVarint(&it.br)
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t = t
		it.val = math.Float64frombits(v)

		it.numRead++
		return true
	}
	// 第二对T/V
	if it.numRead == 1 {
		tDelta, err := binary.ReadUvarint(&it.br)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = tDelta
		it.t = it.t + int64(it.tDelta)

		return it.readValue()
	}
	var d byte
	// read delta-of-delta(读取TDOD的长度前缀)
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBitFast()
		if err != nil {
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return false
		}
		// 值: 0/10/110/1110/1111
		// 循环4次或是出现0结束循环
		if bit == zero {
			break
		}
		d |= 1
	}
	var sz uint8
	var dod int64
	switch d {
	case 0x00:
		// dod == 0
	case 0x02:
		sz = 14
	case 0x06:
		sz = 17
	case 0x0e:
		sz = 20
	case 0x0f:
		// Do not use fast because it's very unlikely it will succeed.
		bits, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		dod = int64(bits)
	}

	if sz != 0 {
		bits, err := it.br.readBitsFast(sz)
		if err != nil {
			bits, err = it.br.readBits(sz)
		}
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod)
	it.t = it.t + int64(it.tDelta)

	return it.readValue()
}

func (it *xorIterator) readValue() bool {
	bit, err := it.br.readBitFast()
	if err != nil {
		bit, err = it.br.readBit()
	}
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero { // 与前值有效位数相等
		// it.val = it.val
	} else { // 与前值有效位数不相等
		bit, err := it.br.readBitFast()
		if err != nil {
			bit, err = it.br.readBit()
		}
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero { // 复用之前有效位长度
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.readBitsFast(5) // leading位序列
			if err != nil {
				bits, err = it.br.readBits(5)
			}
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits) // 转换位序列

			bits, err = it.br.readBitsFast(6) // 差值长度位序列
			if err != nil {
				bits, err = it.br.readBits(6)
			}
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits) // 转换位序列
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits // 计算tailing
		}

		mbits := 64 - it.leading - it.trailing // 有效位长度
		bits, err := it.br.readBitsFast(mbits) // 获取有效位数据
		if err != nil {
			bits, err = it.br.readBits(mbits)
		}
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)    // 前值转换位序列
		vbits ^= bits << it.trailing         // 还原当前值位序列
		it.val = math.Float64frombits(vbits) // 转换为正常值
	}

	it.numRead++
	return true
}
