package pebble

import (
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/internal/pmtinternal"
)

type Collector struct {
	Keys []uint64
	Vals []uint64
}

var collectorMu sync.Mutex
var CurrCollector = Collector{}
var NextCollector = Collector{} // 每次flush都重建

func collectorEnabled() bool {
	return pmtinternal.EnablePMT && pmtinternal.EnableCollector
}

func collectorMaxCount() int {
	if pmtinternal.CollectorMaxBytes < 16 {
		panic("collector max bytes")
	}
	return int(pmtinternal.CollectorMaxBytes / 16)
}

func collectorFind(keys []uint64, key uint64) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i] >= key
	})
}

func collectorNewIter() internalIterator {
	if !collectorEnabled() {
		panic("collector disabled")
	}
	collectorMu.Lock()
	state := CurrCollector
	collectorMu.Unlock()
	if len(state.Keys) == 0 {
		return nil
	}
	return &collectorIter{
		state: state,
		pos:   -1,
	}
}

func collectorRangeBounds(keys []uint64, low, high uint64) (start int, end int) {
	start = sort.Search(len(keys), func(i int) bool {
		return keys[i] >= low
	})
	end = sort.Search(len(keys), func(i int) bool {
		return keys[i] > high
	})
	return start, end
}

func collectorKVCountInRange(low, high uint64) int {
	if !collectorEnabled() {
		return 0
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	start, end := collectorRangeBounds(CurrCollector.Keys, low, high)
	return end - start
}

func collectorMinMaxInRange(low, high uint64) (minK uint64, maxK uint64, ok bool) {
	if !collectorEnabled() {
		return 0, 0, false
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	start, end := collectorRangeBounds(CurrCollector.Keys, low, high)
	if start >= end {
		return 0, 0, false
	}
	return CurrCollector.Keys[start], CurrCollector.Keys[end-1], true
}

// 临时调试: 校验[low,high]范围内collector数据有序且无重复.
func collectorValidateRange(low, high uint64) {
	if !collectorEnabled() {
		return
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	start, end := collectorRangeBounds(CurrCollector.Keys, low, high)
	if start >= end {
		return
	}
	prev := CurrCollector.Keys[start]
	if prev < low || prev > high {
		panic("collector range")
	}
	for i := start + 1; i < end; i++ {
		k := CurrCollector.Keys[i]
		if k < low || k > high {
			panic("collector range")
		}
		if k <= prev {
			panic("collector order or dup")
		}
		prev = k
	}
}

func collectorPagesInRange(keys []uint64, low, high uint64) int {
	start, end := collectorRangeBounds(keys, low, high)
	count := end - start
	if count == 0 {
		return 0
	}
	return (count + KVPerPage - 1) / KVPerPage
}

func _collectorFlushBegin() {
	if !collectorEnabled() {
		return
	}
	collectorMu.Lock()
	NextCollector = Collector{}
	collectorMu.Unlock()
}

func _collectorFlushEnd() {
	if !collectorEnabled() {
		return
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	if len(NextCollector.Keys) != len(NextCollector.Vals) {
		panic("collector corrupted")
	}
	// Rebuild key/value pairs after sort and deduplicate by key.
	// For duplicate keys, keep the latest value written in this flush cycle.
	type kv struct {
		k uint64
		v uint64
	}
	tmp := make([]kv, len(NextCollector.Keys))
	for i := range NextCollector.Keys {
		tmp[i] = kv{k: NextCollector.Keys[i], v: NextCollector.Vals[i]}
	}
	sort.SliceStable(tmp, func(i, j int) bool {
		return tmp[i].k < tmp[j].k
	})
	keys := make([]uint64, 0, len(tmp))
	vals := make([]uint64, 0, len(tmp))
	for i := 0; i < len(tmp); {
		j := i + 1
		lastV := tmp[i].v
		for j < len(tmp) && tmp[j].k == tmp[i].k {
			lastV = tmp[j].v
			j++
		}
		keys = append(keys, tmp[i].k)
		vals = append(vals, lastV)
		i = j
	}
	if len(keys) > collectorMaxCount() {
		panic("collector max bytes")
	}
	CurrCollector = Collector{Keys: keys, Vals: vals}
	NextCollector = Collector{}
}

func collectorAppendNext(keys []uint64, vals []uint64) {
	if !collectorEnabled() {
		return
	}
	if len(keys) == 0 {
		return
	}
	if len(vals) != len(keys) {
		panic("collector vals len")
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	NextCollector.Keys = append(NextCollector.Keys, keys...)
	NextCollector.Vals = append(NextCollector.Vals, vals...)
	if len(NextCollector.Keys) != len(NextCollector.Vals) {
		panic("collector corrupted")
	}
	if len(NextCollector.Keys) > collectorMaxCount() {
		panic("collector max bytes")
	}
}

func collectorAppendNextRange(low, high uint64) {
	if !collectorEnabled() {
		return
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	start, end := collectorRangeBounds(CurrCollector.Keys, low, high)
	if start >= end {
		return
	}
	NextCollector.Keys = append(NextCollector.Keys, CurrCollector.Keys[start:end]...)
	NextCollector.Vals = append(NextCollector.Vals, CurrCollector.Vals[start:end]...)
	if len(NextCollector.Keys) != len(NextCollector.Vals) {
		panic("collector corrupted")
	}
	if len(NextCollector.Keys) > collectorMaxCount() {
		panic("collector max bytes")
	}
}

func collectorAppendNextConst(keys []uint64, val uint64) {
	if len(keys) == 0 {
		return
	}
	vals := make([]uint64, len(keys))
	for i := range vals {
		vals[i] = val
	}
	collectorAppendNext(keys, vals)
}

func collectorGet(k uint64) (uint64, bool) {
	if !collectorEnabled() {
		return 0, false
	}
	collectorMu.Lock()
	defer collectorMu.Unlock()
	idx := collectorFind(CurrCollector.Keys, k)
	if idx >= len(CurrCollector.Keys) || CurrCollector.Keys[idx] != k {
		return 0, false
	}
	return CurrCollector.Vals[idx], true
}
