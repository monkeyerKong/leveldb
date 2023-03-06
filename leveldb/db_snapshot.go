// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"container/list"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type snapshotElement struct {
	seq uint64        // 用一个seq来代表一个数据库状态。
	ref int           // 快照引用计数
	e   *list.Element // 双向链表元素指针
}

// Acquires a snapshot, based on latest sequence.
// 在leveldb中，用户对同一个key的若干次修改（包括删除）是以维护多条数据项的方式进行存储的（直至进行compaction时才会合并成同一条记录），
// 每条数据项都会被赋予一个序列号，代表这条数据项的新旧状态。一条数据项的序列号越大，表示其中代表的内容为最新值。
func (db *DB) acquireSnapshot() *snapshotElement {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	// 获取当前db 最新的seq
	seq := db.getSeq()

	// 查看当前db 是否有old(历史)快照
	if e := db.snapsList.Back(); e != nil {
		se := e.Value.(*snapshotElement)
		// 如果有old(历史)的快照，判断old 快照的seq 和当前db 的seq是否相等
		if se.seq == seq {
			// 快照引用计数自增
			se.ref++
			// 返回
			return se
		} else if seq < se.seq {
			// 如果出现最新的seq 没有 old 快照的seq 大,肯定是逻辑上的bug了
			panic("leveldb: sequence number is not increasing")
		}
	}
	// 在快照列表中未发现当前seq的快照
	se := &snapshotElement{seq: seq, ref: 1}
	// 把快照对象添加双向链表尾部
	se.e = db.snapsList.PushBack(se)
	return se
}

// Releases given snapshot element. 从快照列表中删除快照
func (db *DB) releaseSnapshot(se *snapshotElement) {
	// 整个快照表锁
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	// 引用计数自减
	se.ref--
	if se.ref == 0 {
		// 待引用计数 =0， 删除快照节点
		db.snapsList.Remove(se.e)
		//释放节点内存
		se.e = nil
	} else if se.ref < 0 {
		panic("leveldb: Snapshot: negative element reference")
	}
}

// Gets minimum sequence that not being snapshotted.
func (db *DB) minSeq() uint64 {
	db.snapsMu.Lock()
	defer db.snapsMu.Unlock()

	if e := db.snapsList.Front(); e != nil {
		return e.Value.(*snapshotElement).seq
	}

	return db.getSeq()
}

// Snapshot is a DB snapshot.
type Snapshot struct {
	db       *DB
	elem     *snapshotElement
	mu       sync.RWMutex
	released bool
}

// Creates new snapshot object.
func (db *DB) newSnapshot() *Snapshot {
	snap := &Snapshot{
		db:   db,
		elem: db.acquireSnapshot(),
	}
	atomic.AddInt32(&db.aliveSnaps, 1)
	runtime.SetFinalizer(snap, (*Snapshot).Release)
	return snap
}

func (snap *Snapshot) String() string {
	return fmt.Sprintf("leveldb.Snapshot{%d}", snap.elem.seq)
}

// Get gets the value for the given key. It returns ErrNotFound if
// the DB does not contains the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
// 根据快照的seq 去根据key 返回value
func (snap *Snapshot) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	err = snap.db.ok()
	if err != nil {
		return
	}
	return snap.db.get(nil, nil, key, snap.elem.seq, ro)
}

// Has returns true if the DB does contains the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (snap *Snapshot) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	err = snap.db.ok()
	if err != nil {
		return
	}
	return snap.db.has(nil, nil, key, snap.elem.seq, ro)
}

// NewIterator returns an iterator for the snapshot of the underlying DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. The resultant key/value pairs are guaranteed to be
// consistent.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Value() methods), its content should not be
// modified unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
// Releasing the snapshot doesn't mean releasing the iterator too, the
// iterator would be still valid until released.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (snap *Snapshot) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	snap.mu.Lock()
	defer snap.mu.Unlock()
	if snap.released {
		return iterator.NewEmptyIterator(ErrSnapshotReleased)
	}
	if err := snap.db.ok(); err != nil {
		return iterator.NewEmptyIterator(err)
	}
	// Since iterator already hold version ref, it doesn't need to
	// hold snapshot ref.
	return snap.db.newIterator(nil, nil, snap.elem.seq, slice, ro)
}

// Release releases the snapshot. This will not release any returned
// iterators, the iterators would still be valid until released or the
// underlying DB is closed.
//
// Other methods should not be called after the snapshot has been released.
func (snap *Snapshot) Release() {
	snap.mu.Lock()
	defer snap.mu.Unlock()

	if !snap.released {
		// Clear the finalizer.
		runtime.SetFinalizer(snap, nil)

		snap.released = true
		snap.db.releaseSnapshot(snap.elem)
		atomic.AddInt32(&snap.db.aliveSnaps, -1)
		snap.db = nil
		snap.elem = nil
	}
}
