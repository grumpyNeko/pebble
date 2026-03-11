package pebble

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble/internal/keyspan"
)

type flushableCollectorIter struct {
	lower []byte
	upper []byte
}

func (f *flushableCollectorIter) newIter(o *IterOptions) internalIterator {
	it := collectorNewIter()
	if it == nil {
		it = &collectorIter{
			state: Collector{},
			pos:   -1,
		}
	}
	it.SetBounds(f.lower, f.upper)
	return it
}

func (f *flushableCollectorIter) newFlushIter(o *IterOptions) internalIterator {
	return f.newIter(o)
}

func (f *flushableCollectorIter) newRangeDelIter(o *IterOptions) keyspan.FragmentIterator {
	return nil
}

func (f *flushableCollectorIter) newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator {
	return nil
}

func (f *flushableCollectorIter) containsRangeKeys() bool {
	return false
}

func (f *flushableCollectorIter) inuseBytes() uint64 {
	low, high := f.bounds()
	return uint64(collectorKVCountInRange(low, high)) * 16
}

func (f *flushableCollectorIter) totalBytes() uint64 {
	return f.inuseBytes()
}

func (f *flushableCollectorIter) readyForFlush() bool {
	return true
}

func (f *flushableCollectorIter) computePossibleOverlaps(fn func(bounded) shouldContinue, bounded ...bounded) {
	for _, b := range bounded {
		if fn(b) == stopIteration {
			return
		}
	}
}

func (f *flushableCollectorIter) bounds() (uint64, uint64) {
	low := uint64(0)
	high := uint64(math.MaxUint64)
	if f.lower != nil {
		if len(f.lower) != 8 {
			panic("collector lower len")
		}
		low = binary.BigEndian.Uint64(f.lower)
	}
	if f.upper != nil {
		if len(f.upper) != 8 {
			panic("collector upper len")
		}
		upper := binary.BigEndian.Uint64(f.upper)
		if upper == 0 {
			panic("collector upper zero")
		}
		high = upper - 1
	}
	return low, high
}
