// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrBatchCorrupted records reason of batch corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrBatchCorrupted struct {
	Reason string
}

func (e *ErrBatchCorrupted) Error() string {
	return fmt.Sprintf("leveldb: batch corrupted: %s", e.Reason)
}

func newErrBatchCorrupted(reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrBatchCorrupted{reason})
}

const (
	batchHeaderLen = 8 + 4
	batchGrowLimit = 3000
)

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type batchIndex struct {
	keyType keyType
	// keyPos 记录在batch.data[]数组key的index位置
	keyPos, keyLen     int
	valuePos, valueLen int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

// Batch is a write batch.
type Batch struct {

	// 设计的比较巧妙，把数据和索引分开，通过索引可以快速寻址到key的和value，在batch结构这样应用，在leveldb go 项目中，其他很多地方也是这样使用的，比如skiplist，也是通过数组和索引数组
	// 实现的
	data  []byte
	index []batchIndex

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int

	// growLimit is the threshold in order to slow down the memory allocation
	// for batch when the number of accumulated entries exceeds value.
	//
	// batchGrowLimit is used as the default threshold if it's not configured.
	growLimit int
}

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		limit := batchGrowLimit
		if b.growLimit > 0 {
			limit = b.growLimit
		}
		div := 1
		if len(b.index) > limit {
			div = len(b.index) / limit
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

func (b *Batch) appendRec(kt keyType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)

	// kt = update, 则需要添加valueLen、value值
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	// 判断batch 是否需要扩容， 一条记录长度 > batchGrowLimit 时， 需要扩容
	b.grow(n)

	//构建索引的内容 : index: [(keyType->keyPos->keyLen->key->valuePos->valueLen->value),(...)]

	index := batchIndex{keyType: kt}
	// 现在batch.data的长度，应该是0， 因为还没有内容
	o := len(b.data)

	// 复制 b.data 到 data 临时变量
	data := b.data[:o+n]

	// 开始填充batch.data数组内容
	// data[0] = keyType,  可能是 update/delete
	// data[1] = keyLen,  key长度
	// data[2] = key, key的值
	// data[3] = valueLen, value长度
	// data[4] = value, value的值
	data[o] = byte(kt)

	o++
	// 填充 data 数组中key的长度
	o += binary.PutUvarint(data[o:], uint64(len(key)))

	// 填充index 的key 在data的索引位置
	index.keyPos = o

	// 填充index keylenth值
	index.keyLen = len(key)

	// 填充 data[2] key的值
	o += copy(data[o:], key)

	// 判断类型，如果是delete，则不需要valuelen，value值，如果是update，则需要valuelen, vaue值

	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}

	// 在把临时变量data 内容复制给b.data
	b.data = data[:o]

	// 把这组记录添加 b.index list中
	b.index = append(b.index, index)

	// 不管e PUT请求 还是 Batch  请求 都会封装成batch结构, 所以b.internalLen 记录当前操作的总size
	b.internalLen += index.keyLen + index.valueLen + 8
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) {
	b.appendRec(keyTypeVal, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) {
	b.appendRec(keyTypeDel, key, nil)
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
func (b *Batch) Dump() []byte {
	return b.data
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	return b.decode(data, -1)
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	for _, index := range b.index {
		switch index.keyType {
		case keyTypeVal:
			r.Put(index.k(b.data), index.v(b.data))
		case keyTypeDel:
			r.Delete(index.k(b.data))
		}
	}
	return nil
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.index)
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	b.internalLen = 0
}

func (b *Batch) replayInternal(fn func(i int, kt keyType, k, v []byte) error) error {
	for i, index := range b.index {
		if err := fn(i, index.keyType, index.k(b.data), index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) append(p *Batch) {
	ob := len(b.data)
	oi := len(b.index)
	b.data = append(b.data, p.data...)
	b.index = append(b.index, p.index...)
	b.internalLen += p.internalLen

	// Updating index offset.
	if ob != 0 {
		for ; oi < len(b.index); oi++ {
			index := &b.index[oi]
			index.keyPos += ob
			if index.valueLen != 0 {
				index.valuePos += ob
			}
		}
	}
}

func (b *Batch) decode(data []byte, expectedLen int) error {
	b.data = data
	b.index = b.index[:0]
	b.internalLen = 0
	err := decodeBatch(data, func(i int, index batchIndex) error {
		b.index = append(b.index, index)
		b.internalLen += index.keyLen + index.valueLen + 8
		return nil
	})
	if err != nil {
		return err
	}
	if expectedLen >= 0 && len(b.index) != expectedLen {
		return newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", expectedLen, len(b.index)))
	}
	return nil
}

func (b *Batch) putMem(seq uint64, mdb *memdb.DB) error {
	// seq 是 db的seq
	var ik []byte
	// batch index 结构
	// [0] keytype   udpate/delete
	// [1] keypos    key batch.data中的的起始位置，可快速定位key
	// [2] keylength key的长度
	// [3] valuepos  value batch.data中的的起始位置，可快定位value
	// [4] valuelen  value的长度

	for i, index := range b.index {
		// level db中的key 不是用户直接使用的k，而是 一个称作内部的key， 结构是 userKey + db.seq(7字节) + keytype(1字节)
		// keytype update/delete
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)

		// 开始写入到mtable 里, 它是是skiplist结构
		if err := mdb.Put(ik, index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func newBatch() interface{} {
	return &Batch{}
}

// MakeBatch returns empty batch with preallocated buffer.
func MakeBatch(n int) *Batch {
	return &Batch{data: make([]byte, 0, n)}
}

// BatchConfig contains the config options for batch.
type BatchConfig struct {
	// InitialCapacity is the batch initial capacity to preallocate.
	//
	// The default value is 0.
	InitialCapacity int

	// GrowLimit is the limit (in terms of entry) of how much buffer
	// can grow each cycle.
	//
	// Initially the buffer will grow twice its current size until
	// GrowLimit threshold is reached, after that the buffer will grow
	// up to GrowLimit each cycle. This buffer grow size in bytes is
	// loosely calculated from average entry size multiplied by GrowLimit.
	//
	// Generally, the memory allocation step is larger if this value
	// is configured large, vice versa.
	//
	// The default value is 3000.
	GrowLimit int
}

// MakeBatchWithConfig initializes a batch object with the given configs.
func MakeBatchWithConfig(config *BatchConfig) *Batch {
	var batch = new(Batch)
	if config != nil {
		if config.InitialCapacity > 0 {
			batch.data = make([]byte, 0, config.InitialCapacity)
		}
		if config.GrowLimit > 0 {
			batch.growLimit = config.GrowLimit
		}
	}
	return batch
}

func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	var index batchIndex
	for i, o := 0, 0; o < len(data); i++ {
		// Key type.
		index.keyType = keyType(data[o])
		if index.keyType > keyTypeVal {
			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		}
		o++

		// Key.
		x, n := binary.Uvarint(data[o:])
		o += n
		if n <= 0 || o+int(x) > len(data) {
			return newErrBatchCorrupted("bad record: invalid key length")
		}
		index.keyPos = o
		index.keyLen = int(x)
		o += index.keyLen

		// Value.
		if index.keyType == keyTypeVal {
			x, n = binary.Uvarint(data[o:])
			o += n
			if n <= 0 || o+int(x) > len(data) {
				return newErrBatchCorrupted("bad record: invalid value length")
			}
			index.valuePos = o
			index.valueLen = int(x)
			o += index.valueLen
		} else {
			index.valuePos = 0
			index.valueLen = 0
		}

		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	if seq < expectSeq {
		return 0, 0, newErrBatchCorrupted("invalid sequence number")
	}
	data = data[batchHeaderLen:]
	var ik []byte
	var decodedLen int
	err = decodeBatch(data, func(i int, index batchIndex) error {
		if i >= batchLen {
			return newErrBatchCorrupted("invalid records length")
		}
		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.keyType)
		if err := mdb.Put(ik, index.v(data)); err != nil {
			return err
		}
		decodedLen++
		return nil
	})
	if err == nil && decodedLen != batchLen {
		err = newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", batchLen, decodedLen))
	}
	return
}

func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	// batch header 头的长度12
	// [0..7] leveldb.seq
	// [8..12] 所有本次record的个数，一个batch 就是一个record
	dst = ensureBuffer(dst, batchHeaderLen)
	binary.LittleEndian.PutUint64(dst, seq)
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))
	return dst
}

func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	if len(data) < batchHeaderLen {
		return 0, 0, newErrBatchCorrupted("too short")
	}

	seq = binary.LittleEndian.Uint64(data)
	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
	if batchLen < 0 {
		return 0, 0, newErrBatchCorrupted("invalid records length")
	}
	return
}

func batchesLen(batches []*Batch) int {
	batchLen := 0
	for _, batch := range batches {
		batchLen += batch.Len()
	}
	return batchLen
}

func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
	// seq: leveldb seq
	// wr: Single Writer
	// 生产batches header 并写入到journal 日志中
	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
		return err
	}
	// 写入batch data 到journal 日志中
	//
	for _, batch := range batches {
		if _, err := wr.Write(batch.data); err != nil {
			return err
		}
	}
	return nil
}
