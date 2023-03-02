// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package comparer

import "bytes"

type bytesComparer struct{}

func (bytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytesComparer) Name() string {
	return "leveldb.BytewiseComparator"
}

/*
比较a，b值，a 相对于prev key, b 相当于key, prev key 和 key 如果出现相同部分，且从不不同部分开始比较，prev key 不同的第一个字符 需要小于key 不同的第一个字符，返回相同部分且第一个不同的字符++ ,否则返回nil
eg:

	a = "pick"
	b = "pity"

返回 ：pid
*/
func (bytesComparer) Separator(dst, a, b []byte) []byte {
	i, n := 0, len(a)
	// n = min(len(a), len(b))
	if n > len(b) {
		n = len(b)
	}
	// 计数a, b字节序 相同的offset
	for ; i < n && a[i] == b[i]; i++ {
	}

	if i >= n {
		//  a 或者 b 是其中一个子串
		// Do not shorten if one string is a prefix of the other
	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		dst = append(dst, a[:i+1]...)
		dst[len(dst)-1]++
		return dst
	}
	return nil
}

/*
b 如果第一个字符不是0xff, 则返回第一个字符++, ,如果都是0xff, 则返回nil
eg:

	a = "pick"

返回 q
*/
func (bytesComparer) Successor(dst, b []byte) []byte {
	for i, c := range b {
		if c != 0xff {
			dst = append(dst, b[:i+1]...)
			dst[len(dst)-1]++
			return dst
		}
	}
	return nil
}

// DefaultComparer are default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer = bytesComparer{}
