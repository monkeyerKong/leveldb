// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import (
	"math/rand"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound
	ErrIterReleased = errors.New("leveldb/memdb: iterator released")
)

const tMaxHeight = 12

type dbIter struct {
	util.BasicReleaser
	p          *DB
	slice      *util.Range
	node       int
	forward    bool
	key, value []byte
	err        error
}

func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	// 就是填充key, value
	if i.node != 0 {
		n := i.p.nodeData[i.node]
		m := n + i.p.nodeData[i.node+nKey]
		// 取出kye
		i.key = i.p.kvData[n:m]
		if i.slice != nil {
			// 设置了start
			switch {
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
				//查找的key 比start 的key 小，则设置node =0
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		i.value = i.p.kvData[m : m+i.p.nodeData[i.node+nVal]]
		return true
	}
bail:
	i.key = nil
	i.value = nil
	return false
}

func (i *dbIter) Valid() bool {
	return i.node != 0
}

func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		// 设置start , 则skiplist查找 start (key)的即next node，
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		// 没有设置start，则skiplist 中的第一个 node
		i.node = i.p.nodeData[nNext]
	}
	return i.fill(false, true)
}

func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)
	} else {
		// level0 的最后一个节点
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

// 顺着当前的level，去下一个节点
func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	// 在当前level取下一个节点, 基本上这个level 都是 level0
	i.node = i.p.nodeData[i.node+nNext]
	return i.fill(false, true)
}

func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	// 取prev node
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

// DB is an in-memory key/value database.
type DB struct {
	cmp comparer.BasicComparer
	rnd *rand.Rand

	mu sync.RWMutex

	// skiplist 存储数据的数组，实现区别教科书上的实现 , kv存储与索引的位置是分开的, 其实batch 也是数据与索引分开的
	// kvData [] = [(key1, value1), (key2, value2),...], 对于数据的操作就是append， 比如常用的方法：l.pushback(). 复杂度O(1), 这样是为什么写的块原因
	kvData []byte

	// 把当前kv 称作 一个节点 ， 即node
	// Node data:
	// [0]         : KV offset, 每个node在kvData中的起始位置
	// [1]         : Key length, 当前node.key的长度
	// [2]         : Value length , 当前node.value的长度
	// [3]         : Height ,当前node的高度
	// [3..height] : Next nodes,

	// 比如：level0 next node
	// level1 next node
	// level2 next node
	// level-height next node
	nodeData []int

	// 当前操作node 节点各个level 中的prev node
	prevNode [tMaxHeight]int

	// skiplist 的最大高度，leveldb = 12
	maxHeight int

	// 统计信息，n表示 节点的个数，kvsize  表示 skiplist kvdata总大小, 不能通过len(kvData)计算，因为删除数据，不会删除kvData中内容，只会删除索引关系
	n      int
	kvSize int
}

func (p *DB) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
// 注意：内部方法，不可直接使用，非线程安全
func (p *DB) findGE(key []byte, prev bool) (int, bool) {
	node := 0
	//从skiplist 的最高level开始遍历
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		cmp := 1
		if next != 0 {
			o := p.nodeData[next]
			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
		}
		if cmp < 0 {
			// Keep searching in this list, 在当前level 中向后遍历
			node = next
		} else {
			if prev {
				// 遍历当前level 链表的时候，保存每个next 的prev node
				p.prevNode[h] = node
				// 在level0上 找到了 节点
			} else if cmp == 0 {
				return next, true
			}
			// 找到了最后一层, 判断是否找到节点
			if h == 0 {
				return next, cmp == 0
			}
			// 调到下一层
			h--
		}
	}
}

// 找到 key的 prev 节点
// 注意：内部方法，不可直接使用，非线程安全
func (p *DB) findLT(key []byte) int {
	node := 0
	// 从skiplist 的最高level开始遍历
	h := p.maxHeight - 1
	for {
		// 找到next node索引
		next := p.nodeData[node+nNext+h]
		// 定位到next node
		o := p.nodeData[next]
		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
			if h == 0 {
				break
			}
			// 调到下一层
			h--
		} else {
			// 向后遍历
			node = next
		}
	}
	return node
}

// 注意：内部方法，不可直接使用，非线程安全
func (p *DB) findLast() int {
	node := 0
	h := p.maxHeight - 1
	// 也是一样，从走最高层一直找到最后一层level0，取node
	for {
		next := p.nodeData[node+nNext+h]
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (p *DB) Put(key []byte, value []byte) error {
	// 操作skiplist 整个skiplist锁保护
	p.mu.Lock()
	defer p.mu.Unlock()

	// 判断是否存在元素, 返回一个node和exact，exact = true ，表示找到， exact = false 表示没有找到，此时node 为l0 上刚好大于key节点，即key会放到它前面
	// 位置应该是这样.prve node->current node->next node. 其实把node 叫next node 更合适
	if node, exact := p.findGE(key, true); exact {
		//发现node，kvdata 只追加不修改, 索引(nodedata)需要重置node 的偏移量(offset)，重新计算value的长度
		kvOffset := len(p.kvData)
		p.kvData = append(p.kvData, key...)
		p.kvData = append(p.kvData, value...)
		p.nodeData[node] = kvOffset
		m := p.nodeData[node+nVal]
		p.nodeData[node+nVal] = len(value)

		// 在上面也提到了，不能用len(kvData) 计算kvsize
		p.kvSize += len(value) - m
		return nil
	}
	// 随机高度，使用的概率问题
	h := p.randHeight()

	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			// 此时prvenode 是当前需要插入的node 在各个level的 prev node, 如果随机出来的高度大于maxHeight, 那么需要初始化后面几个node
			p.prevNode[i] = 0
		}
		p.maxHeight = h
	}

	// 计算 key 的偏移量
	kvOffset := len(p.kvData)
	// 追加key 到kvdata
	p.kvData = append(p.kvData, key...)
	// 追加 value 到kvdata
	p.kvData = append(p.kvData, value...)

	// 计算node 在nodedata 的索引
	node := len(p.nodeData)
	// 把node元数据追加到nodeData中, 元数据 offset,keylen,valuelen,height
	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h)

	// i 表示 跳表的level, l0,l1,l2等等, n表示level对应的 prevnode索引位置
	for i, n := range p.prevNode[:h] {

		// 获取每个level的next ndoe 索引
		m := n + nNext + i

		// 把当前的node next node 指向 prevnode 的next node
		p.nodeData = append(p.nodeData, p.nodeData[m])
		// 当前prevnode next 指向当前节点
		p.nodeData[m] = node

		//  是一个简单 链表节点插入操作，prev, node
		// node->next = prev->next
		// prev->next = node
	}

	p.kvSize += len(key) + len(value)
	p.n++
	return nil
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
// 仅删除索引数据，不会再kvdata中删除任何数据, 这样好处就是快, 在skiplist 已经检索不到改节点了
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exact := p.findGE(key, true)
	if !exact {
		return ErrNotFound
	}

	// 获取当前节点的高度
	h := p.nodeData[node+nHeight]

	for i, n := range p.prevNode[:h] {
		// i表示当前level，即高度. 获取当前level的prevnode 索引
		m := n + nNext + i

		// 相当于 prev->next = node->next
		// nodeData[preNode+nNext+i] 表示prev node的next ,因为nodedata 定义
		// 0: offset
		// 1: keylen
		// 2: vaulelen
		// 3: height
		// 4: level0:next node
		// 5: level1:next node
		// .... leveln :next node
		p.nodeData[m] = p.nodeData[p.nodeData[m]+nNext+i]
	}

	p.kvSize -= p.nodeData[node+nKey] + p.nodeData[node+nVal]
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
func (p *DB) Contains(key []byte) bool {
	// 对外提供的方法，保证线程安全
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *DB) Get(key []byte) (value []byte, err error) {
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		// 这种单纯的查找，不操作prevnode
		o := p.nodeData[node] + p.nodeData[node+nKey]
		value = p.kvData[o : o+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
// 返回k，v, 在skiplist
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData) - len(p.kvData)
}

// Len returns the number of entries in the DB.
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
func (p *DB) Reset() {
	p.mu.Lock()
	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData = p.kvData[:0]
	p.nodeData = p.nodeData[:nNext+tMaxHeight]
	p.nodeData[nKV] = 0
	p.nodeData[nKey] = 0
	p.nodeData[nVal] = 0
	p.nodeData[nHeight] = tMaxHeight
	for n := 0; n < tMaxHeight; n++ {
		p.nodeData[nNext+n] = 0
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0, capacity),
		nodeData:  make([]int, 4+tMaxHeight),
	}
	p.nodeData[nHeight] = tMaxHeight
	return p
}
