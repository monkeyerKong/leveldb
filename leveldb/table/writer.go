// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func sharedPrefixLen(a, b []byte) int {
	/*
		a 是prev key
		b 是 当前key
	*/
	//n Prev key 的长度
	i, n := 0, len(a)
	// 取 min(len(prevkey), len(key))
	if n > len(b) {
		n = len(b)
	}

	// 计数 prev key 和 key相同的字符
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// leveldb并不会为每一对keyvalue对都存储完整的key值，而是存储与上一个key非共享的部分，避免了key重复内容的存储

/*
此外，第一个restart point为0（偏移量），第二个restart point为16，restart point共有两个，因此一个datablock数据段的末尾添加了下图所示的数据：

	┌──────┬──────┬──────┐
	│  0   │  22  │  2   │
	└──────┴──────┴──────┘
	    │      │      ╲
	    │      │       ╲─╲
	restart points        ╲ restart points length

	尾部数据记录了每一个restart point的值，以及所有restart point的个数。
*/
type blockWriter struct {
	restartInterval int // 默认是16
	buf             util.Buffer
	nEntries        int // 记录当前block中的entry数量

	prevKey []byte // 上一个 key

	// 每间隔若干个keyvalue对，将为该条记录重新存储一个完整的key。重复该过程（默认间隔值为16），每个重新存储完整key的点称之为Restart point
	/* leveldb设计Restart point的目的是在读取sstable内容时，加速查找的过程。
	由于每个Restart point存储的都是完整的key值，因此在sstable中进行数据查找时，可以首先利用restart point点的数据进行键值比较，以便于快速定位目标数据所在的区域；
	当确定目标数据所在区域时，再依次对区间内所有数据项逐项比较key值，进行细粒度地查找；
	该思想有点类似于跳表中利用高层数据迅速定位，底层数据详细查找的理念，降低查找的复杂度。 */
	restarts []uint32
	/*
				datablock 的entry 的格式如下:
					┌────────────────────┬────────────────────┬─────────────┬────────────────────┬─────┐
					│ shared key length  │unshared key length │value length │unshared key content│value│
					└────────────────────┴────────────────────┴─────────────┴────────────────────┴─────┘
				一个entry分为5部分内容：
					1. 与前一条记录key共享部分的长度；
					2. 与前一条记录key不共享部分的长度；
					3. value长度
					4.与前一条记录key非共享的内容；
					5.value内容；

				restart_interval=2
					entry one  : key=dick,value=miss
					entry two  : key=diss,value=wonder
					entry three: key=locker,value=no


			 restart ponit (offset=0)                                        restart ponit (offset=22)
		 	  ╱                                                              ╱
		 ╱─────╱                                                        ╱─────╱
		╱                                                              ╱
		┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬───────┬─────┬─────┬─────┬──────┬───────┐
		│  0  │  4  │  4  │dick │miss │  2  │  2  │  6  │ ss  │wonder │  0  │  6  │  2  │locker│  no   │
		└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴───────┴─────┴─────┴─────┴──────┴───────┘
		   │                       │     │                        │      │                         │
		   │                       │     │                        │      │                         │
		   └──────entry1───────────┘     └─────────entry2─────────┘      └───────────entry3────────┘


				三组entry按上图的格式进行存储。值得注意的是restart_interval为2，因此每隔两个entry都会有一条数据作为restart point点的数据项，存储完整key值。因此entry3存储了完整的key
	*/
	scratch []byte // datablock的entry一部分，包含shared、noshared、value三个部分
}

func (w *blockWriter) append(key, value []byte) (err error) {
	nShared := 0

	// 判断是否是一个restart point 点
	if w.nEntries%w.restartInterval == 0 {

		// 需要打点,记录restart point点, 上图中的offset
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	} else {
		// 不需要打点, 记录当前key 和上一个key的共享字节长度, 看上图
		nShared = sharedPrefixLen(w.prevKey, key)
	}
	// 构造entry, 共享key长度
	n := binary.PutUvarint(w.scratch[0:], uint64(nShared))
	// 非共享key长度
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-nShared))
	// value长度
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	// 写入前3个部分到buffer
	if _, err = w.buf.Write(w.scratch[:n]); err != nil {
		return err
	}
	// 写入非共享key的字节
	if _, err = w.buf.Write(key[nShared:]); err != nil {
		return err
	}
	// 写入value
	if _, err = w.buf.Write(value); err != nil {
		return err
	}
	// 记录当前key
	w.prevKey = append(w.prevKey[:0], key...)
	// block 内entry, 即kv 计数器 自增
	w.nEntries++
	return nil
}

/*
此外，第一个restart point为0（偏移量），第二个restart point为16，restart point共有两个，因此一个datablock数据段的末尾添加了下图所示的数据：

	┌──────┬──────┬──────┐
	│  0   │  22  │  2   │
	└──────┴──────┴──────┘
	    │      │      ╲
	    │      │       ╲─╲
	restart points        ╲ restart points length

	尾部数据记录了每一个restart point的值，以及所有restart point的个数。
	内容都写在datawirte的buf中
*/
func (w *blockWriter) finish() error {
	// Write restarts entry.
	// 可能kv的数据特别大，超过blockdata size 即 4k
	if w.nEntries == 0 {
		// Must have at least one restart entry.
		w.restarts = append(w.restarts, 0)
	}
	// 追加restart points length
	w.restarts = append(w.restarts, uint32(len(w.restarts)))
	for _, x := range w.restarts {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	return nil
}

func (w *blockWriter) reset() {
	w.buf.Reset()
	w.nEntries = 0
	w.restarts = w.restarts[:0]
}

func (w *blockWriter) bytesLen() int {
	restartsLen := len(w.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	// 每一个restart point 占用4个字节，通过restart.buf4 = w.buf.alloc(4) , 最后一个4表示 restartlen 也占用4个字节
	// 一个block块至少 有一个 restart ponit + restart points length
	return w.buf.Len() + 4*restartsLen + 4
}

type filterWriter struct {
	generator filter.FilterGenerator
	buf       util.Buffer
	nKeys     int
	offsets   []uint32
	baseLg    uint
}

func (w *filterWriter) add(key []byte) {
	if w.generator == nil {
		return
	}
	w.generator.Add(key)
	w.nKeys++
}

func (w *filterWriter) flush(offset uint64) {
	if w.generator == nil {
		return
	}
	for x := int(offset / uint64(1<<w.baseLg)); x > len(w.offsets); {
		w.generate()
	}
}

func (w *filterWriter) finish() error {
	if w.generator == nil {
		return nil
	}
	// Generate last keys.

	if w.nKeys > 0 {
		w.generate()
	}
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	for _, x := range w.offsets {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	return w.buf.WriteByte(byte(w.baseLg))
}

func (w *filterWriter) generate() {
	// Record offset.
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	// Generate filters.
	if w.nKeys > 0 {
		w.generator.Generate(&w.buf)
		w.nKeys = 0
	}
}

// Writer is a table writer.
type Writer struct {
	writer io.Writer
	err    error
	// Options
	cmp         comparer.Comparer
	filter      filter.Filter
	compression opt.Compression
	blockSize   int // 默认是4k

	bpool *util.BufferPool

	/* datablock */
	//  ┌──────┬──────────────────┬──────────────────┐
	//  │ data │ compression type │     crc          │
	//  └──────┴──────────────────┴──────────────────┘

	/*  data 部分描述
	   ┌─────────────────┐
	┌─■│  entry01        │
	│  ├─────────────────┤
	│  │  entry02        │■─┐
	│  ├─────────────────┤  │
	│  │  entryn         │  │
	│  ├─────────────────┤  │
	└──│  restart point01│  │
	   ├─────────────────┤  │
	   │  restart point02│──┘
	   └─────────────────┘
	*/
	dataBlock   blockWriter  // 用来存储key value数据对
	indexBlock  blockWriter  // index block中用来存储每个data block的索引信息
	filterBlock filterWriter //来存储一些过滤器相关的数据（布隆过滤器), 如果设置了有数据，没有设置则无数据
	pendingBH   blockHandle  //记录了上一个dataBlock的索引信息(偏移量，长度), blockhandle
	offset      uint64       // 记录sstable中的总字节数, 也会当做上一个block的偏移量
	nEntries    int          // kv 键值对的个数, kv 会被封装在entry结构里
	// Scratch allocated enough for 5 uvarint. Block writer should not use
	// first 20-bytes since it will be used to encode block handle, which
	// then passed to the block writer itself.
	scratch            [50]byte
	comparerScratch    []byte
	compressionScratch []byte
}

// 采取是以一个block大小为单位进行压缩写入sstable文件中
/*
datablock:
	┌──────────────────┬──────────────────┬─────────────┐
	│    datablock     │ compression type │     crc     │
	└──────────────────┴──────────────────┴─────────────┘
	compression type: 占用 1字节
	crc : 占用 4字节

*/
func (w *Writer) writeBlock(buf *util.Buffer, compression opt.Compression) (bh blockHandle, err error) {
	// Compress the buffer if necessary.
	var b []byte
	if compression == opt.SnappyCompression {
		// Allocate scratch enough for compression and block trailer.
		if n := snappy.MaxEncodedLen(buf.Len()) + blockTrailerLen; len(w.compressionScratch) < n {
			w.compressionScratch = make([]byte, n)
		}
		compressed := snappy.Encode(w.compressionScratch, buf.Bytes())
		n := len(compressed)
		b = compressed[:n+blockTrailerLen]
		b[n] = blockTypeSnappyCompression
	} else {
		tmp := buf.Alloc(blockTrailerLen)
		// 压缩type,占用1字节
		tmp[0] = blockTypeNoCompression
		b = buf.Bytes()
	}

	// Calculate the checksum.
	n := len(b) - 4
	checksum := util.NewCRC(b[:n]).Value()
	binary.LittleEndian.PutUint32(b[n:], checksum)

	// Write the buffer to the file.
	_, err = w.writer.Write(b)
	if err != nil {
		return
	}
	bh = blockHandle{w.offset, uint64(len(b) - blockTrailerLen)}
	// 记录当前的offset, 供后面的block使用,
	w.offset += uint64(len(b))
	return
}

/*
把datablock 写入sstable文件后，会记录block handle值, 且需要完成对data block的index block 信息构造。
构造完index block 后，重置block handle, 和datablock 中的 prev key, 因为datablock 结构体的生命周期由blocksize定，
每完成一次datablock size 大小的block，都需要重置datablock结构体
*/
func (w *Writer) flushPendingBH(key []byte) error {

	// 判断是否有data block 写入sstable文件中，==0 表示没有
	if w.pendingBH.length == 0 {
		return nil
	}

	var separator []byte
	if len(key) == 0 {
		// 收尾工作，写入最后一块datablock，触发这里,假如prev key = pick, 则那么separator = q
		separator = w.cmp.Successor(w.comparerScratch[:0], w.dataBlock.prevKey)
	} else {
		// 正常迭代写入key和value, 假设prev key = pick, key = pity, 那么separator = pid
		separator = w.cmp.Separator(w.comparerScratch[:0], w.dataBlock.prevKey, key)
	}
	if separator == nil {
		separator = w.dataBlock.prevKey
	} else {
		w.comparerScratch = separator
	}
	// 前 20个字节，用于保存 block handle, n = 写入tail偏移量
	n := encodeBlockHandle(w.scratch[:20], w.pendingBH)

	// Append the block handle to the index block.
	/*
		   index block entry
		        ╱

		      ┌───────────────────────┬──────────────┬──────────────┐
		      │  data block max key   │    offset    │    length    │
		      └───────────────────────┴──────────────┴──────────────┘
		                                      │              │
		                                      │  block       │
		                                      └──handle──────┘
		                                                    ╲
		                                                     ╲─────╲
		                                                            ╲
		                                                            data block index
			separator: data block max key, 即block中最大的key, index block entry 中的key
			w.scratch[:n]: block handle, offset 和 length, index block entry 中的value

	*/
	// index block entry 保存在w.indexBlock.buf中
	if err := w.indexBlock.append(separator, w.scratch[:n]); err != nil {
		return err
	}
	// Reset prev key of the data block. datablock 信息已经记录在 index block中了，需要重置prev key, 进行一下data block
	w.dataBlock.prevKey = w.dataBlock.prevKey[:0]
	// Clear pending block handle.
	w.pendingBH = blockHandle{}
	return nil
}

// 收尾：写入datablock 的 restart point信息, 并重置datablock
func (w *Writer) finishBlock() error {
	// 追加restart point 尾巴
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	//以block buffer为单位 追加到sstable，并返回block handle，存有当前block 在sstable 的offset 及block 的长度
	bh, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		return err
	}

	// 记录当前的block 的handle，包括offset, length
	w.pendingBH = bh
	// Reset the data block.清空block.buffer,kv计数器,restart ponit 列表, 但是保留block的prevkey
	// 并没有 清除prev key, 主要因为在构建index block 时用到，用于填充 index block entry的max key
	w.dataBlock.reset()
	// Flush the filter block.
	w.filterBlock.flush(w.offset)
	return nil
}

// Append appends key/value pair to the table. The keys passed must
// be in increasing order.
//
// It is safe to modify the contents of the arguments after Append returns.
// 以block 为单位写入(flush)到sstable文件中, block 承载着k，v
/*

datablock 完整的结构
	restart ponit (offset=0)                                        restart ponit (offset=22)
	╱                                                              ╱
	╱─────╱                                                        ╱─────╱
	╱                                                              ╱
	┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬───────┬─────┬─────┬─────┬──────┬───────┌──────┬──────┬──────┐
	│  0  │  4  │  4  │dick │miss │  2  │  2  │  6  │ ss  │wonder │  0  │  6  │  2  │locker│  no   │  0   │  22  │  2   │
	└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴───────┴─────┴─────┴─────┴──────┴───────└──────┴──────┴──────┘
		│                       │     │                        │      │                         │          │      │      ╲
		│                       │     │                        │      │                         │          │      │       ╲─╲
		└──────entry1───────────┘     └─────────entry2─────────┘      └───────────entry3────────┘      restart points        ╲ restart points length

*/
func (w *Writer) Append(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	// 需要严格保证顺序
	if w.nEntries > 0 && w.cmp.Compare(w.dataBlock.prevKey, key) >= 0 {
		w.err = fmt.Errorf("leveldb/table: Writer: keys are not in increasing order: %q, %q", w.dataBlock.prevKey, key)
		return w.err
	}

	// 完成index block 信息构造，重置datablock 的 prevkey, 及block handle,准备构建下一个data block
	if err := w.flushPendingBH(key); err != nil {
		return err
	}
	// Append key/value pair to the data block kv 做为一个entry项加入datablock buf.
	if err := w.dataBlock.append(key, value); err != nil {
		return err
	}
	// Add key to the filter block.
	w.filterBlock.add(key)

	// Finish the data block if block size target reached.
	if w.dataBlock.bytesLen() >= w.blockSize {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	// sstable层面的 kv 计数器
	w.nEntries++
	return nil
}

// BlocksLen returns number of blocks written so far.
func (w *Writer) BlocksLen() int {
	n := w.indexBlock.nEntries
	if w.pendingBH.length > 0 {
		// Includes the pending block.
		n++
	}
	return n
}

// EntriesLen returns number of entries added so far.
func (w *Writer) EntriesLen() int {
	// sstable有多少个kv
	return w.nEntries
}

// BytesLen returns number of bytes written so far.
func (w *Writer) BytesLen() int {
	return int(w.offset)
}

// Close will finalize the table. Calling Append is not possible
// after Close, but calling BlocksLen, EntriesLen and BytesLen
// is still possible.
/*
	1. 写最后的datablck 到sstable文件
	2. 写入indexblock 到sstable文件
	3. 写入最后的filterblock 到sstable文件
	4. 组织footer结构，并写入到sstable文件
		4.1: 写入meta index
		4.2 : 写入index block index
		4.3 : 写入magic
	5. 计算sstalbe 字节长度
*/
func (w *Writer) Close() error {
	defer func() {
		if w.bpool != nil {
			// Buffer.Bytes() returns [offset:] of the buffer.
			// We need to Reset() so that the offset = 0, resulting
			// in buf.Bytes() returning the whole allocated bytes.
			w.dataBlock.buf.Reset()
			// 把buf 重新丢回 sync.pool
			w.bpool.Put(w.dataBlock.buf.Bytes())
		}
	}()

	if w.err != nil {
		return w.err
	}

	// Write the last data block. Or empty data block if there
	// aren't any data blocks at all.
	// datablock收尾工作, 不够一个datablock size 的datablock 及，最后未写入到sstable的数据
	// 并不会填充000...这些值，保障最后一个block也是4k
	if w.dataBlock.nEntries > 0 || w.nEntries == 0 {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	// 写入最后的index block，清空block handle, datablock prev key
	if err := w.flushPendingBH(nil); err != nil {
		return err
	}

	// Write the filter block.
	var filterBH blockHandle
	if err := w.filterBlock.finish(); err != nil {
		return err
	}
	if buf := &w.filterBlock.buf; buf.Len() > 0 {
		filterBH, w.err = w.writeBlock(buf, opt.NoCompression)
		if w.err != nil {
			return w.err
		}
	}

	// Write the metaindex block.
	if filterBH.length > 0 {
		key := []byte("filter." + w.filter.Name())
		n := encodeBlockHandle(w.scratch[:20], filterBH)
		if err := w.dataBlock.append(key, w.scratch[:n]); err != nil {
			return err
		}
	}
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	metaindexBH, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	/*
			index block 的key 和value 如下图:
			   index block entry
			               ╲                  Key                  ┌────value─────┐
			                ╲──╲               │                   │              │
			                    ╲              │                   │              │
			                     ╲ ┌───────────────────────┬──────────────┬──────────────┐
			                       │  data block max key   │    offset    │    length    │
			                       └───────────────────────┴──────────────┴──────────────┘
			                                                       │              │
			                                                       │  block       │
			                                                       └──handle──────┘

		index block 结构 同data block
	*/

	// Write the index block.
	// 写入最后的restart 字段
	if err := w.indexBlock.finish(); err != nil {
		return err
	}
	// 追加index block 到sstable
	indexBH, err := w.writeBlock(&w.indexBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	/*

		   footer ╲
		           ╲──╲
		               ╲
		                ╲
		             ┌─────────────┬──────────────────┬──────────────┐
		             │ meta index  │index block index │    magic     │
		             └─────────────┴──────────────────┴──────────────┘
		meta index: 指的元数据索引在sstable 的位置, 结构都是blockhandle, offset + length
		index block index: 指的index索引在sstable 的位置
		magic: 内容为："http://code.google.com/p/leveldb/"字符串sha1哈希的前8个字节。
	*/
	footer := w.scratch[:footerLen]
	for i := range footer {
		footer[i] = 0
	}
	n := encodeBlockHandle(footer, metaindexBH)
	encodeBlockHandle(footer[n:], indexBH)
	copy(footer[footerLen-len(magic):], magic)
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}
	// 加上 footer的长度
	w.offset += footerLen

	w.err = errors.New("leveldb/table: writer is closed")
	return nil
}

// NewWriter creates a new initialized table writer for the file.
//
// Table writer is not safe for concurrent use.
func NewWriter(f io.Writer, o *opt.Options, pool *util.BufferPool, size int) *Writer {
	var bufBytes []byte
	if pool == nil {
		bufBytes = make([]byte, size)
	} else {
		bufBytes = pool.Get(size)
	}
	bufBytes = bufBytes[:0]

	w := &Writer{
		writer:          f,
		cmp:             o.GetComparer(),
		filter:          o.GetFilter(),
		compression:     o.GetCompression(),
		blockSize:       o.GetBlockSize(),
		comparerScratch: make([]byte, 0),
		bpool:           pool,
		dataBlock:       blockWriter{buf: *util.NewBuffer(bufBytes)},
	}
	// data block
	w.dataBlock.restartInterval = o.GetBlockRestartInterval()
	// The first 20-bytes are used for encoding block handle.
	w.dataBlock.scratch = w.scratch[20:]
	// index block
	w.indexBlock.restartInterval = 1
	w.indexBlock.scratch = w.scratch[20:]
	// filter block
	if w.filter != nil {
		w.filterBlock.generator = w.filter.NewGenerator()
		w.filterBlock.baseLg = uint(o.GetFilterBaseLg())
		w.filterBlock.flush(0)
	}
	return w
}
