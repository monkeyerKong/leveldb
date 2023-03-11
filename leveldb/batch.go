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
/*

 Batch.Data
       ╲─────╲             ┌───────────────────────────key offset────────────────────────────┐
              ╲            │                                                                 │                                              Batch.index
               ╲           │                                                                 │                                                ╱
              ┌────┬───┬──────┬──┬──────┬────┬───┬──────┬──┬──────┐                          │                                     ╱─────────╱
              │ 1  │ 4 │ key1 │6 │value1│ 1  │ 4 │ key2 │6 │value2│                          │                                    ╱
              └────┴───┴──────┴──┴──────┴────┴───┴──────┴──┴──────┘                          ▼                                   ╱
                 │                   │ │   │                   │                ┌────┬───┬──────┬──┬──────┬────┬───┬──────┬──┬──────┐
                 │                   │ │   │                   │                │ 1  │ 4 │  2   │6 │  4   │ 1  │ 4 │  7   │6 │  9   │
                 └──────kv1──────────┘ │   └─────────kv2───────┘                └────┴───┴──────┴──┴──────┴────┴───┴──────┴──┴──────┘
                                       │                                                               ▲
                                       │                                                               │
                                       │                                                               │
                                       └──────────────────────────value offset─────────────────────────┘


*/
type Batch struct {

	// 设计的比较巧妙，把数据和索引分开，通过索引可以快速寻址到key的和value，在batch结构这样应用，在leveldb go 项目中，其他很多地方也是这样使用的，比如skiplist，也是通过数组和索引数组
	// 实现的
	/*

		 Batch.Data
		       ╲─────╲
		              ╲
		               ╲
		          ┌──────────┬──────────┬──────────────┬──────────────┬──────────────┐
		          │ keyType  │keyLength │ key Content  │ value Length │Value Content │
		          └──────────┴──────────┴──────────────┴──────────────┴──────────────┘
		                │
		                │
		                │     ┌───────────┐
		                └─────│  Delete   │
		                      ├───────────┤
		                      │   Value   │
		                      └───────────┘
		eg:

	*/
	data []byte

	/*

	 Batch.index
	        ╲─────╲
	               ╲
	                ╲
	            ┌──────────┬──────────┬──────────────┬──────────────┬──────────────┐
	            │ keyType  │keyLength │    keyPos    │ value Length │   ValuePos   │
	            └──────────┴──────────┴──────────────┴──────────────┴──────────────┘
	*/
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

	// n : key type (1 byte) + 保留空间(5 byte) + keylen + 保留空间(5 byte) + value length

	//当前batch data 的大小
	o := len(b.data)
	// batch.data的free 空间容量不足
	if cap(b.data)-o < n {
		// 设置batch 的 空间上限
		limit := batchGrowLimit
		if b.growLimit > 0 {
			limit = b.growLimit
		}
		// 扩容因子, 根据batch.index 和 空间limit，比较调整因子
		div := 1

		// 如果索引的长度 超过 空间上限
		if len(b.index) > limit {
			// 倍数调整扩容因子div, b.index 容量越大，扩容的速度也就越慢
			div = len(b.index) / limit
		}
		// 扩容因子
		// div = 1 的时候，扩容的容量 = 2 * old data + n
		// div = 2 的时候，扩容的容量 = 1.5 * old data + n
		// div = 3 的时候，扩容的容量 = 1.25 * old data + n

		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

// appendRec 以batch 格式封装kv，并分别append 到 batch.data 和 batch.index 缓存中
/*
内存操作，leveldb 基本都是用append方法
*/
func (b *Batch) appendRec(kt keyType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)

	// kt = update, 则需要添加valueLen、value值
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	// 扩容, 且具有 slow down 特性的扩容
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
	// keyType 1 字节长度
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

// 从batch data中解析k和v
func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	/*
		格式：
			┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
			│key type     │key length   │     key     │value length │    value    │
			│             │             │             │             │             │
			└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
	*/
	var index batchIndex
	// i：表示 第几个batch 或者 batch 的个数
	// o: 表示 在block内的偏移量用于计算key type, keylength等
	for i, o := 0, 0; o < len(data); i++ {
		// Key type. 占用一个字节
		index.keyType = keyType(data[o])
		// 不合法类型, keytype : v 和 d
		if index.keyType > keyTypeVal {
			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		}
		// 移动偏移量 到keylength 位置
		o++

		// Key. 占用 8字节, x: keylength 值,
		x, n := binary.Uvarint(data[o:])

		// 移动偏移量 到key位置
		o += n

		// 不合法 , 数据中没有记录keylength
		if n <= 0 || o+int(x) > len(data) {
			return newErrBatchCorrupted("bad record: invalid key length")
		}
		// 记录key的位置
		index.keyPos = o
		index.keyLen = int(x)
		// 移动偏移量到 vauleLength位置
		o += index.keyLen

		// Value.
		if index.keyType == keyTypeVal {

			// value Length：  占用8字节
			x, n = binary.Uvarint(data[o:])

			// 移动偏移量到 value
			o += n
			// 格式不合法，没有记录value length
			if n <= 0 || o+int(x) > len(data) {
				return newErrBatchCorrupted("bad record: invalid value length")
			}

			// 记录value 的位置
			index.valuePos = o
			index.valueLen = int(x)

			// 移动偏移量到下一个batch
			o += index.valueLen
		} else {
			// 当前的key 是删除的key
			index.valuePos = 0
			index.valueLen = 0
		}

		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

// 从journal block data 的数据部分解析出 kv 并插入到memdb(skiplist) 中
func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
	/*

		Journal 文件格式:
													Full
												First  ╱
												╱Middle─╱
												╱  Last
											╱
			┌────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┐
			│  checkSum  │Data Length │    Type    │ Batch Seq  │ Batch Size │  Key Type  │ Key Length │    Key     │Value Length│   Value    │
			│            │            │            │            │            │            │            │            │            │            │
			└────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┘
				│                         │            │                                                                             │
				│           Block         │            │                                                                             │
				└───────────Header────────┘            └────────────────────────────────────Data─────────────────────────────────────┘
		即 Batch Seq  和 Batch Size
	*/
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	// journal 的batch seq 不合法 或者说 mainfest文件的session record seq 不合法
	if seq < expectSeq {
		return 0, 0, newErrBatchCorrupted("invalid sequence number")
	}
	// 取出batch 数据
	data = data[batchHeaderLen:]
	var ik []byte

	// 记录解析出batch的个数
	var decodedLen int

	// 解析出kv
	err = decodeBatch(data, func(i int, index batchIndex) error {
		// 不合法
		if i >= batchLen {
			return newErrBatchCorrupted("invalid records length")
		}

		// 生成ikey
		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.keyType)

		// 插入到skiplist
		if err := mdb.Put(ik, index.v(data)); err != nil {
			return err
		}
		decodedLen++
		return nil
	})

	// 不合法，解析出的batch个数 和 头记录的个数不一样
	if err == nil && decodedLen != batchLen {
		err = newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", batchLen, decodedLen))
	}
	return
}

// 生成 batch 的头信息, 及batch seq 和 batch size
func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	// batch header 头的长度12
	// [0..7] leveldb.seq
	// [8..12] 所有本次record的个数，一个batch 就是一个record
	dst = ensureBuffer(dst, batchHeaderLen)
	binary.LittleEndian.PutUint64(dst, seq)
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))
	return dst
}

// 从journal block 的数据部分解析出 batch的头 (batch seq, batch的长度)
func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	// 数据不合法，长度 小于 batch header
	if len(data) < batchHeaderLen {
		return 0, 0, newErrBatchCorrupted("too short")
	}

	// seq 占用8字节，取前8个字节 (注: uint64 占用8字节)
	seq = binary.LittleEndian.Uint64(data)
	// batchlen 占用4字节，取后面4个字节
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
