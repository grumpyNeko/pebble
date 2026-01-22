package pebble

import (
	"bytes"
	"context"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// flushableMemIter wraps a simpleMemIter to implement the flushable interface
type flushableMemIter struct {
	iter *simpleMemIter
}

func (f *flushableMemIter) newIter(o *IterOptions) internalIterator {
	// Create a new iterator instance with reset position
	return &simpleMemIter{
		keys: f.iter.keys,
		vals: f.iter.vals,
		pos:  -1,
	}
}

func (f *flushableMemIter) newFlushIter(o *IterOptions) internalIterator {
	// Create a new iterator instance with reset position
	return &simpleMemIter{
		keys: f.iter.keys,
		vals: f.iter.vals,
		pos:  -1,
	}
}

func (f *flushableMemIter) newRangeDelIter(o *IterOptions) keyspan.FragmentIterator {
	return nil
}

func (f *flushableMemIter) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	return nil
}

func (f *flushableMemIter) containsRangeKeys() bool {
	return false
}

func (f *flushableMemIter) inuseBytes() uint64 {
	return uint64(len(f.iter.keys) * 16) // approximate
}

func (f *flushableMemIter) totalBytes() uint64 {
	return f.inuseBytes()
}

func (f *flushableMemIter) readyForFlush() bool {
	return true
}

func (f *flushableMemIter) computePossibleOverlaps(fn func(bounded) shouldContinue, bounded ...bounded) {
	// For simplicity, assume potential overlap with everything
	for _, b := range bounded {
		if fn(b) == stopIteration {
			return
		}
	}
}

// simpleMemIter is a simple in-memory iterator implementation
type simpleMemIter struct {
	keys []base.InternalKey
	vals [][]byte
	pos  int
	kv   base.InternalKV
}

// newSimpleMemIter creates an iterator from keys and values
func newSimpleMemIter(userKeys []uint64, value uint64, seqNum base.SeqNum) *simpleMemIter {
	iter := &simpleMemIter{
		keys: make([]base.InternalKey, len(userKeys)),
		vals: make([][]byte, len(userKeys)),
		pos:  -1,
	}
	valBytes := BigEndian(value)
	for i, k := range userKeys {
		iter.keys[i] = base.MakeInternalKey(BigEndian(k), seqNum, base.InternalKeyKindSet)
		iter.vals[i] = valBytes
	}
	return iter
}

func (i *simpleMemIter) String() string {
	return "simple-mem"
}

func (i *simpleMemIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	i.pos = 0
	for i.pos < len(i.keys) && i.keys[i.pos].UserKey != nil {
		if base.InternalCompare(bytes.Compare, i.keys[i.pos], base.MakeSearchKey(key)) >= 0 {
			i.kv.K = i.keys[i.pos]
			i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
			return &i.kv
		}
		i.pos++
	}
	return nil
}

func (i *simpleMemIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.SeekGE(key, flags)
}

func (i *simpleMemIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	i.pos = len(i.keys) - 1
	for i.pos >= 0 && i.keys[i.pos].UserKey != nil {
		if base.InternalCompare(bytes.Compare, i.keys[i.pos], base.MakeSearchKey(key)) < 0 {
			i.kv.K = i.keys[i.pos]
			i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
			return &i.kv
		}
		i.pos--
	}
	return nil
}

func (i *simpleMemIter) First() *base.InternalKV {
	if len(i.keys) == 0 {
		return nil
	}
	i.pos = 0
	i.kv.K = i.keys[i.pos]
	i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
	return &i.kv
}

func (i *simpleMemIter) Last() *base.InternalKV {
	if len(i.keys) == 0 {
		return nil
	}
	i.pos = len(i.keys) - 1
	i.kv.K = i.keys[i.pos]
	i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
	return &i.kv
}

func (i *simpleMemIter) Next() *base.InternalKV {
	i.pos++
	if i.pos >= len(i.keys) {
		return nil
	}
	i.kv.K = i.keys[i.pos]
	i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
	return &i.kv
}

func (i *simpleMemIter) NextPrefix(succKey []byte) *base.InternalKV {
	return i.Next()
}

func (i *simpleMemIter) Prev() *base.InternalKV {
	i.pos--
	if i.pos < 0 {
		return nil
	}
	i.kv.K = i.keys[i.pos]
	i.kv.V = base.MakeInPlaceValue(i.vals[i.pos])
	return &i.kv
}

func (i *simpleMemIter) Error() error {
	return nil
}

func (i *simpleMemIter) Close() error {
	return nil
}

func (i *simpleMemIter) SetBounds(lower, upper []byte) {
	// no-op for simplicity
}

func (i *simpleMemIter) SetContext(ctx context.Context) {
	// no-op
}

func (i *simpleMemIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}
