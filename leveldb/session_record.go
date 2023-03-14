// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bufio"
	"encoding/binary"
	"io"
	"strings"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type byteReader interface {
	io.Reader
	io.ByteReader
}

// These numbers are written to disk and should not be changed.
const (
	recComparer    = 1
	recJournalNum  = 2
	recNextFileNum = 3
	recSeqNum      = 4
	recCompPtr     = 5
	recDelTable    = 6
	recAddTable    = 7
	// 8 was used for large value refs
	recPrevJournalNum = 9
)

type cpRecord struct {
	level int         // 哪一层level
	ikey  internalKey // ?是最大key 还是最小key
}

// 新增sstable record信息
type atRecord struct {
	level int         // sstable 在哪一层level
	num   int64       // sstable 文件号
	size  int64       // 文件大小
	imin  internalKey // 文件最小key
	imax  internalKey // 文件最大key
}

// 删除sstable record信息
type dtRecord struct {
	level int   //sstable 在哪一层level
	num   int64 //sstable 文件号
}

/*
Manifest session record文件格式:

	┌────────────────────┬──────────────────────────┬────────────────────┐
	│   Type=KComparer   │  kComparer Name Length   │   KComparer Name   │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┼──────────────────────────┼────────────────────┘
	│  Type=KLogNumber   │        Log Number        │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│Type=KPrevLogNumber │     Prev Log Number      │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│Type=KNextFileNumber│     Next File Number     │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│ Type=KLastSequence │   Last Sequence Number   │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┼────────────────────┬──────────────────────────┐
	│Type=KComparerPointe│          Level           │Internal Key Length │       Internal Key       │
	│         r          │        (Varint32)        │     (Varint64)     │         (String)         │
	├────────────────────┴──────────────────────────┴────────────────────┴──────────────────────────┤
	│                                              ...                                              │
	│                                                                                               │
	├────────────────────┬──────────────────────────┬────────────────────┬──────────────────────────┤
	│Type=KComparerPointe│          Level           │Internal Key Length │       Internal Key       │
	│         r          │        (Varint32)        │     (Varint64)     │         (String)         │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┘
	│  Type=KDeleteFile  │          Level           │      File Num      │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┴──────────────────────────┴────────────────────┤
	│                                ...                                 │
	│                                                                    │
	├────────────────────┬──────────────────────────┬────────────────────┤
	│  Type=KDeleteFile  │          Level           │      File Num      │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┐
	│   Type=KNewFile    │          Level           │      File Num      │        File Size         │
	│     (Varint32)     │        (Varint32)        │     (Varint64)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┤
	│Smallest Key Length │       Smallest Key       │ Largest Key Length │       Largest Key        │
	│     (Varint32)     │         (String)         │     (Varint32)     │         (String)         │
	└────────────────────┴──────────────────────────┴────────────────────┴──────────────────────────┘
*/
type sessionRecord struct {
	/*
		hasRec掩码标志位：
			┌─────────────────┬──────────────────┬───────────┬───────────┬───────────┬───────────┬───────────────┬────────────────┬───────────┐
			│recPrevJournalNum│ Large Value Ref  │recAddTable│recDelTable│recCompPtr │ recSeqNum │recNextFileNum │ recJournalNum  │recComparer│
			└─────────────────┴──────────────────┴───────────┴───────────┴───────────┴───────────┴───────────────┴────────────────┴───────────┘
	*/
	hasRec         int        // session record 的掩码
	comparer       string     // compaprer名称
	journalNum     int64      //最新的journal文件的序号
	prevJournalNum int64      // 上一个journal文件序号
	nextFileNum    int64      // 下一个sstable 文件序号
	seqNum         uint64     // 数据库已经持久化数据项中最大的sequence number
	compPtrs       []cpRecord // compaction pointers
	addedTables    []atRecord // 新增sstable 文件信息
	deletedTables  []dtRecord // 删除sstable 文件信息

	scratch [binary.MaxVarintLen64]byte
	err     error
}

func (p *sessionRecord) has(rec int) bool {
	return p.hasRec&(1<<uint(rec)) != 0
}

func (p *sessionRecord) setComparer(name string) {
	p.hasRec |= 1 << recComparer
	p.comparer = name
}

func (p *sessionRecord) setJournalNum(num int64) {
	p.hasRec |= 1 << recJournalNum
	p.journalNum = num
}

func (p *sessionRecord) setPrevJournalNum(num int64) {
	p.hasRec |= 1 << recPrevJournalNum
	p.prevJournalNum = num
}

func (p *sessionRecord) setNextFileNum(num int64) {
	p.hasRec |= 1 << recNextFileNum
	p.nextFileNum = num
}

func (p *sessionRecord) setSeqNum(num uint64) {
	p.hasRec |= 1 << recSeqNum
	p.seqNum = num
}

func (p *sessionRecord) addCompPtr(level int, ikey internalKey) {
	p.hasRec |= 1 << recCompPtr
	p.compPtrs = append(p.compPtrs, cpRecord{level, ikey})
}

// 清除compaction pointers内容及掩码标志位
func (p *sessionRecord) resetCompPtrs() {
	p.hasRec &= ^(1 << recCompPtr)
	p.compPtrs = p.compPtrs[:0]
}

func (p *sessionRecord) addTable(level int, num, size int64, imin, imax internalKey) {
	p.hasRec |= 1 << recAddTable
	p.addedTables = append(p.addedTables, atRecord{level, num, size, imin, imax})
}

// 在session record 的addedTables 中添加 level i中添加文件元数据信
func (p *sessionRecord) addTableFile(level int, t *tFile) {
	p.addTable(level, t.fd.Num, t.size, t.imin, t.imax)
}

// 清除add tables内容及掩码标志位
func (p *sessionRecord) resetAddedTables() {
	p.hasRec &= ^(1 << recAddTable)
	p.addedTables = p.addedTables[:0]
}

func (p *sessionRecord) delTable(level int, num int64) {
	p.hasRec |= 1 << recDelTable
	p.deletedTables = append(p.deletedTables, dtRecord{level, num})
}

// 清除delete tables内容及掩码标志位
func (p *sessionRecord) resetDeletedTables() {
	p.hasRec &= ^(1 << recDelTable)
	p.deletedTables = p.deletedTables[:0]
}

func (p *sessionRecord) putUvarint(w io.Writer, x uint64) {
	if p.err != nil {
		return
	}
	n := binary.PutUvarint(p.scratch[:], x)
	_, p.err = w.Write(p.scratch[:n])
}

func (p *sessionRecord) putVarint(w io.Writer, x int64) {
	if x < 0 {
		panic("invalid negative value")
	}
	p.putUvarint(w, uint64(x))
}

func (p *sessionRecord) putBytes(w io.Writer, x []byte) {
	if p.err != nil {
		return
	}
	p.putUvarint(w, uint64(len(x)))
	if p.err != nil {
		return
	}
	_, p.err = w.Write(x)
}

// 对record 二进制编码，写入record 到manifest文件中
func (p *sessionRecord) encode(w io.Writer) error {
	p.err = nil
	if p.has(recComparer) {
		p.putUvarint(w, recComparer)
		p.putBytes(w, []byte(p.comparer))
	}
	if p.has(recJournalNum) {
		p.putUvarint(w, recJournalNum)
		p.putVarint(w, p.journalNum)
	}
	if p.has(recNextFileNum) {
		p.putUvarint(w, recNextFileNum)
		p.putVarint(w, p.nextFileNum)
	}
	if p.has(recSeqNum) {
		p.putUvarint(w, recSeqNum)
		p.putUvarint(w, p.seqNum)
	}
	for _, r := range p.compPtrs {
		p.putUvarint(w, recCompPtr)
		p.putUvarint(w, uint64(r.level))
		p.putBytes(w, r.ikey)
	}
	for _, r := range p.deletedTables {
		p.putUvarint(w, recDelTable)
		p.putUvarint(w, uint64(r.level))
		p.putVarint(w, r.num)
	}
	for _, r := range p.addedTables {
		p.putUvarint(w, recAddTable)
		p.putUvarint(w, uint64(r.level))
		p.putVarint(w, r.num)
		p.putVarint(w, r.size)
		p.putBytes(w, r.imin)
		p.putBytes(w, r.imax)
	}
	return p.err
}

// 读取Manifest session record  Uvarint 部分内容
/*
比如：
	┌────────────────────┬──────────────────────────┬────────────────────┐
	│   Type=KComparer   │  kComparer Name Length   │   KComparer Name   │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	└────────────────────┴──────────────────────────┴────────────────────┘

读取的是Type=KComparer  或者 kComparer Name Length  返回它们的值. 如果是EOF 错误处理
*/
func (p *sessionRecord) readUvarintMayEOF(field string, r io.ByteReader, mayEOF bool) uint64 {
	if p.err != nil {
		return 0
	}
	x, err := binary.ReadUvarint(r)
	if err != nil {
		if err == io.ErrUnexpectedEOF || (!mayEOF && err == io.EOF) {
			p.err = errors.NewErrCorrupted(storage.FileDesc{}, &ErrManifestCorrupted{field, "short read"})
		} else if strings.HasPrefix(err.Error(), "binary:") {
			p.err = errors.NewErrCorrupted(storage.FileDesc{}, &ErrManifestCorrupted{field, err.Error()})
		} else {
			p.err = err
		}
		return 0
	}
	return x
}

func (p *sessionRecord) readUvarint(field string, r io.ByteReader) uint64 {
	return p.readUvarintMayEOF(field, r, false)
}

func (p *sessionRecord) readVarint(field string, r io.ByteReader) int64 {
	x := int64(p.readUvarintMayEOF(field, r, false))
	if x < 0 {
		p.err = errors.NewErrCorrupted(storage.FileDesc{}, &ErrManifestCorrupted{field, "invalid negative value"})
	}
	return x
}

/*
返回字节序, 即Manifest session record  字符串部分
比如：

	┌────────────────────┬──────────────────────────┬────────────────────┐
	│   Type=KComparer   │  kComparer Name Length   │   KComparer Name   │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	└────────────────────┴──────────────────────────┴────────────────────┘

返回的就是KComparer Name
*/
func (p *sessionRecord) readBytes(field string, r byteReader) []byte {
	if p.err != nil {
		return nil
	}
	// 读filed 长度
	n := p.readUvarint(field, r)
	if p.err != nil {
		return nil
	}
	// 申请 filed长度个空间
	x := make([]byte, n)
	// 从r(reader)中读取filed 名字,保存在x中
	_, p.err = io.ReadFull(r, x)
	if p.err != nil {
		if p.err == io.ErrUnexpectedEOF {
			p.err = errors.NewErrCorrupted(storage.FileDesc{}, &ErrManifestCorrupted{field, "short read"})
		}
		return nil
	}
	return x
}

func (p *sessionRecord) readLevel(field string, r io.ByteReader) int {
	if p.err != nil {
		return 0
	}
	x := p.readUvarint(field, r)
	if p.err != nil {
		return 0
	}
	return int(x)
}

/*
Manifest session record文件格式:

	┌────────────────────┬──────────────────────────┬────────────────────┐
	│   Type=KComparer   │  kComparer Name Length   │   KComparer Name   │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┼──────────────────────────┼────────────────────┘
	│  Type=KLogNumber   │        Log Number        │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│Type=KPrevLogNumber │     Prev Log Number      │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│Type=KNextFileNumber│     Next File Number     │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┤
	│ Type=KLastSequence │   Last Sequence Number   │
	│     (Varint32)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┼────────────────────┬──────────────────────────┐
	│Type=KComparerPointe│          Level           │Internal Key Length │       Internal Key       │
	│         r          │        (Varint32)        │     (Varint64)     │         (String)         │
	├────────────────────┴──────────────────────────┴────────────────────┴──────────────────────────┤
	│                                              ...                                              │
	│                                                                                               │
	├────────────────────┬──────────────────────────┬────────────────────┬──────────────────────────┤
	│Type=KComparerPointe│          Level           │Internal Key Length │       Internal Key       │
	│         r          │        (Varint32)        │     (Varint64)     │         (String)         │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┘
	│  Type=KDeleteFile  │          Level           │      File Num      │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┴──────────────────────────┴────────────────────┤
	│                                ...                                 │
	│                                                                    │
	├────────────────────┬──────────────────────────┬────────────────────┤
	│  Type=KDeleteFile  │          Level           │      File Num      │
	│     (Varint32)     │        (Varint32)        │      (String)      │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┐
	│   Type=KNewFile    │          Level           │      File Num      │        File Size         │
	│     (Varint32)     │        (Varint32)        │     (Varint64)     │        (Varint64)        │
	├────────────────────┼──────────────────────────┼────────────────────┼──────────────────────────┤
	│Smallest Key Length │       Smallest Key       │ Largest Key Length │       Largest Key        │
	│     (Varint32)     │         (String)         │     (Varint32)     │         (String)         │
	└────────────────────┴──────────────────────────┴────────────────────┴──────────────────────────┘
*/
func (p *sessionRecord) decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	p.err = nil
	for p.err == nil {
		rec := p.readUvarintMayEOF("field-header", br, true)
		if p.err != nil {
			if p.err == io.EOF {
				return nil
			}
			return p.err
		}
		switch rec {
		case recComparer:
			x := p.readBytes("comparer", br)
			if p.err == nil {
				// 设置Comparer名字
				p.setComparer(string(x))
			}
		case recJournalNum:
			x := p.readVarint("journal-num", br)
			if p.err == nil {
				// 设置JournalNum
				p.setJournalNum(x)
			}
		case recPrevJournalNum:
			x := p.readVarint("prev-journal-num", br)
			if p.err == nil {
				p.setPrevJournalNum(x)
			}
		case recNextFileNum:
			x := p.readVarint("next-file-num", br)
			if p.err == nil {
				p.setNextFileNum(x)
			}
		case recSeqNum:
			x := p.readUvarint("seq-num", br)
			if p.err == nil {
				p.setSeqNum(x)
			}
		case recCompPtr:
			level := p.readLevel("comp-ptr.level", br)
			ikey := p.readBytes("comp-ptr.ikey", br)
			if p.err == nil {
				p.addCompPtr(level, internalKey(ikey))
			}
		case recAddTable:
			level := p.readLevel("add-table.level", br)
			num := p.readVarint("add-table.num", br)
			size := p.readVarint("add-table.size", br)
			imin := p.readBytes("add-table.imin", br)
			imax := p.readBytes("add-table.imax", br)
			if p.err == nil {
				p.addTable(level, num, size, imin, imax)
			}
		case recDelTable:
			level := p.readLevel("del-table.level", br)
			num := p.readVarint("del-table.num", br)
			if p.err == nil {
				p.delTable(level, num)
			}
		}
	}

	return p.err
}
