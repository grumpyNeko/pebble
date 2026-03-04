// Copyright 2024 The Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pmtformat

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestPMTFormat(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, w.Add(100, 1000))
	require.NoError(t, w.Add(200, 2000))
	require.NoError(t, w.Add(300, 3000))
	require.NoError(t, w.Close())

	data := buf.Bytes()
	iter := newPMTIter(t, data)

	kv := iter.First()
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodePMTTestKey(kv))
	require.Equal(t, uint64(1000), decodePMTTestValue(t, kv))

	kv = iter.Next()
	require.NotNil(t, kv)
	require.Equal(t, uint64(200), decodePMTTestKey(kv))
	require.Equal(t, uint64(2000), decodePMTTestValue(t, kv))

	kv = iter.Next()
	require.NotNil(t, kv)
	require.Equal(t, uint64(300), decodePMTTestKey(kv))
	require.Equal(t, uint64(3000), decodePMTTestValue(t, kv))
	require.Nil(t, iter.Next())

	index := decodeIndexKeys(data)
	require.Equal(t, 1, len(index))
	require.Equal(t, uint64(300), index[0])

	val, ok := seekExact(iter, 100)
	require.True(t, ok)
	require.Equal(t, uint64(1000), val)

	val, ok = seekExact(iter, 200)
	require.True(t, ok)
	require.Equal(t, uint64(2000), val)

	val, ok = seekExact(iter, 300)
	require.True(t, ok)
	require.Equal(t, uint64(3000), val)

	_, ok = seekExact(iter, 999)
	require.False(t, ok)
}

func TestPMTFormatEmpty(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, w.Close())

	data := buf.Bytes()
	iter := newPMTIter(t, data)

	require.Nil(t, iter.First())
	require.Nil(t, iter.Last())
	require.Nil(t, iter.SeekGE(bePMT(1), base.SeekGEFlagsNone))
	require.Nil(t, iter.SeekLT(bePMT(1), base.SeekLTFlagsNone))
	require.Equal(t, 4096, len(data))
	require.Empty(t, decodeIndexKeys(data))
}

func TestPMTFormatMultiplePages(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	numEntries := 600
	for i := 0; i < numEntries; i++ {
		require.NoError(t, w.Add(uint64(i*10), uint64(i*100)))
	}
	require.NoError(t, w.Close())

	data := buf.Bytes()
	iter := newPMTIter(t, data)

	count := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		wantKey := uint64(count * 10)
		wantVal := uint64(count * 100)
		require.Equal(t, wantKey, decodePMTTestKey(kv))
		require.Equal(t, wantVal, decodePMTTestValue(t, kv))
		count++
	}
	require.Equal(t, numEntries, count)

	index := decodeIndexKeys(data)
	require.Equal(t, 3, len(index))
	require.Equal(t, uint64(255*10), index[0])
	require.Equal(t, uint64(511*10), index[1])
	require.Equal(t, uint64(599*10), index[2])

	val, ok := seekExact(iter, uint64(100*10))
	require.True(t, ok)
	require.Equal(t, uint64(100*100), val)

	val, ok = seekExact(iter, uint64(300*10))
	require.True(t, ok)
	require.Equal(t, uint64(300*100), val)

	val, ok = seekExact(iter, uint64(550*10))
	require.True(t, ok)
	require.Equal(t, uint64(550*100), val)
}

func TestPMTFormat4KBPage(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	require.NoError(t, w.Add(100, 1000))
	require.NoError(t, w.Add(200, 2000))
	require.NoError(t, w.Close())

	data := buf.Bytes()
	expectedSize := 4096 + 4096
	require.Equal(t, expectedSize, len(data))
	require.Equal(t, 4096, len(data[:4096]))
}

func TestPMTFormatTableFull(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	for i := 0; i < MaxEntriesPerTable(); i++ {
		require.NoError(t, w.Add(uint64(i+1), uint64(i+1)))
	}
	require.ErrorContains(t, w.Add(uint64(MaxEntriesPerTable()+1), 1), "table is full")
}

func newPMTIter(t *testing.T, data []byte) *Iter {
	t.Helper()
	iter, err := NewIter(pmtReadableForTest{r: bytes.NewReader(data)}, int64(len(data)), base.SeqNum(1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, iter.Close())
	})
	return iter
}

func seekExact(iter *Iter, key uint64) (uint64, bool) {
	kv := iter.SeekGE(bePMT(key), base.SeekGEFlagsNone)
	if kv == nil {
		return 0, false
	}
	if gotKey := decodePMTTestKey(kv); gotKey != key {
		return 0, false
	}
	v, _, err := kv.Value(nil)
	if err != nil {
		return 0, false
	}
	return binary.BigEndian.Uint64(v), true
}

func decodeIndexKeys(data []byte) []uint64 {
	totalPages := len(data) / dataPageSize
	numDataPages := totalPages - 1
	indexPageOffset := numDataPages * dataPageSize
	indexPage := data[indexPageOffset : indexPageOffset+indexPageSize]
	keys := make([]uint64, numDataPages)
	for i := 0; i < numDataPages; i++ {
		offset := i * indexEntrySize
		keys[i] = binary.BigEndian.Uint64(indexPage[offset : offset+indexEntrySize])
	}
	return keys
}

func bePMT(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodePMTTestKey(kv *base.InternalKV) uint64 {
	return binary.BigEndian.Uint64(kv.K.UserKey)
}

func decodePMTTestValue(t *testing.T, kv *base.InternalKV) uint64 {
	t.Helper()
	v, _, err := kv.Value(nil)
	require.NoError(t, err)
	return binary.BigEndian.Uint64(v)
}

type pmtReadableForTest struct {
	r *bytes.Reader
}

func (tr pmtReadableForTest) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := tr.r.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (pmtReadableForTest) Close() error {
	return nil
}
