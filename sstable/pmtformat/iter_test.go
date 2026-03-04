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

func TestIterEmpty(t *testing.T) {
	iter := newTestIter(t, nil)

	require.Nil(t, iter.First())
	require.Nil(t, iter.Last())
	require.Nil(t, iter.SeekGE(be(1), base.SeekGEFlagsNone))
	require.Nil(t, iter.SeekLT(be(1), base.SeekLTFlagsNone))
	require.NoError(t, iter.Error())
}

func TestIterSingleAndSeeks(t *testing.T) {
	iter := newTestIter(t, []uint64{100})

	kv := iter.First()
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodeKey(kv))
	require.Equal(t, base.InternalKeyKindSet, kv.K.Kind())
	require.Equal(t, base.SeqNum(7), kv.K.SeqNum())
	require.Equal(t, uint64(1100), decodeValue(t, kv))
	require.Nil(t, iter.Next())

	kv = iter.Last()
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodeKey(kv))
	require.Nil(t, iter.Prev())

	kv = iter.SeekGE(be(99), base.SeekGEFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodeKey(kv))

	kv = iter.SeekGE(be(100), base.SeekGEFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodeKey(kv))

	require.Nil(t, iter.SeekGE(be(101), base.SeekGEFlagsNone))

	kv = iter.SeekLT(be(101), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(100), decodeKey(kv))

	require.Nil(t, iter.SeekLT(be(100), base.SeekLTFlagsNone))
}

func TestIterCrossPageSeekAndNext(t *testing.T) {
	keys := make([]uint64, 257)
	for i := range keys {
		keys[i] = uint64(i)
	}
	iter := newTestIter(t, keys)

	kv := iter.SeekGE(be(255), base.SeekGEFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(255), decodeKey(kv))
	require.Equal(t, 0, iter.pageIdx)

	kv = iter.Next()
	require.NotNil(t, kv)
	require.Equal(t, uint64(256), decodeKey(kv))
	require.Equal(t, 1, iter.pageIdx)
	require.Nil(t, iter.Next())
}

func TestIterOutOfRangeAndSetBounds(t *testing.T) {
	iter := newTestIter(t, []uint64{10, 20, 30, 40})

	require.Nil(t, iter.SeekGE(be(41), base.SeekGEFlagsNone))
	require.Nil(t, iter.SeekLT(be(10), base.SeekLTFlagsNone))

	iter.SetBounds(be(20), be(40))
	require.NoError(t, iter.Error())

	kv := iter.SeekGE(be(10), base.SeekGEFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(20), decodeKey(kv))

	require.Nil(t, iter.SeekGE(be(40), base.SeekGEFlagsNone))
	require.Nil(t, iter.SeekLT(be(20), base.SeekLTFlagsNone))

	kv = iter.SeekLT(be(40), base.SeekLTFlagsNone)
	require.NotNil(t, kv)
	require.Equal(t, uint64(30), decodeKey(kv))
}

func newTestIter(t *testing.T, keys []uint64) *Iter {
	t.Helper()

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, k := range keys {
		require.NoError(t, w.Add(k, k+1000))
	}
	require.NoError(t, w.Close())

	iter, err := NewIter(testReadable{r: bytes.NewReader(buf.Bytes())}, int64(buf.Len()), base.SeqNum(7))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, iter.Close())
	})
	return iter
}

func be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeKey(kv *base.InternalKV) uint64 {
	return binary.BigEndian.Uint64(kv.K.UserKey)
}

func decodeValue(t *testing.T, kv *base.InternalKV) uint64 {
	t.Helper()
	v, _, err := kv.Value(nil)
	require.NoError(t, err)
	return binary.BigEndian.Uint64(v)
}

type testReadable struct {
	r *bytes.Reader
}

func (tr testReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := tr.r.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (testReadable) Close() error {
	return nil
}
