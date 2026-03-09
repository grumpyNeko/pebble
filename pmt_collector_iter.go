package pebble

import (
	"context"
	"encoding/binary"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

type collectorIter struct {
	state Collector
	pos   int
	kv    base.InternalKV
	lower uint64
	upper uint64
	hasLB bool
	hasUB bool
}

func (i *collectorIter) String() string {
	return "collector"
}

func (i *collectorIter) kvAt(pos int) *base.InternalKV {
	if pos < 0 || pos >= len(i.state.Keys) {
		return nil
	}
	if !i.inBounds(i.state.Keys[pos]) {
		return nil
	}
	i.pos = pos
	i.kv.K = base.MakeInternalKey(BigEndian(i.state.Keys[pos]), 0, base.InternalKeyKindSet)
	i.kv.V = base.MakeInPlaceValue(BigEndian(i.state.Vals[pos]))
	return &i.kv
}

func (i *collectorIter) inBounds(k uint64) bool {
	if i.hasLB && k < i.lower {
		return false
	}
	if i.hasUB && k >= i.upper {
		return false
	}
	return true
}

func (i *collectorIter) seekKey(key []byte) uint64 {
	if len(key) != 8 {
		panic("collector key len")
	}
	return binary.BigEndian.Uint64(key)
}

func (i *collectorIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	target := i.seekKey(key)
	if i.hasLB && target < i.lower {
		target = i.lower
	}
	idx := collectorFind(i.state.Keys, target)
	_ = flags
	if idx >= len(i.state.Keys) {
		return nil
	}
	if !i.inBounds(i.state.Keys[idx]) {
		return nil
	}
	return i.kvAt(idx)
}

func (i *collectorIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	_ = prefix
	return i.SeekGE(key, flags)
}

func (i *collectorIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	target := i.seekKey(key)
	if i.hasUB && target > i.upper {
		target = i.upper
	}
	idx := collectorFind(i.state.Keys, target) - 1
	_ = flags
	if idx < 0 {
		return nil
	}
	if !i.inBounds(i.state.Keys[idx]) {
		return nil
	}
	return i.kvAt(idx)
}

func (i *collectorIter) First() *base.InternalKV {
	start := 0
	if i.hasLB {
		start = collectorFind(i.state.Keys, i.lower)
	}
	return i.kvAt(start)
}

func (i *collectorIter) Last() *base.InternalKV {
	end := len(i.state.Keys) - 1
	if i.hasUB {
		end = collectorFind(i.state.Keys, i.upper) - 1
	}
	return i.kvAt(end)
}

func (i *collectorIter) Next() *base.InternalKV {
	for pos := i.pos + 1; pos < len(i.state.Keys); pos++ {
		if i.inBounds(i.state.Keys[pos]) {
			return i.kvAt(pos)
		}
		if i.hasUB && i.state.Keys[pos] >= i.upper {
			return nil
		}
	}
	return nil
}

func (i *collectorIter) NextPrefix(succKey []byte) *base.InternalKV {
	_ = succKey
	return i.Next()
}

func (i *collectorIter) Prev() *base.InternalKV {
	for pos := i.pos - 1; pos >= 0; pos-- {
		if i.inBounds(i.state.Keys[pos]) {
			return i.kvAt(pos)
		}
		if i.hasLB && i.state.Keys[pos] < i.lower {
			return nil
		}
	}
	return nil
}

func (i *collectorIter) Error() error {
	return nil
}

func (i *collectorIter) Close() error {
	return nil
}

func (i *collectorIter) SetBounds(lower, upper []byte) {
	i.hasLB = lower != nil
	i.hasUB = upper != nil
	if i.hasLB {
		if len(lower) != 8 {
			panic("collector lower len")
		}
		i.lower = binary.BigEndian.Uint64(lower)
	}
	if i.hasUB {
		if len(upper) != 8 {
			panic("collector upper len")
		}
		i.upper = binary.BigEndian.Uint64(upper)
	}
	i.pos = -1
}

func (i *collectorIter) SetContext(ctx context.Context) {
	_ = ctx
}

func (i *collectorIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}
