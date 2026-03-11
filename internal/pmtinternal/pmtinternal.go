package pmtinternal

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"math"
	"runtime"
	"sync"
)

var EnablePMT = true
var EnablePMTTableFormat = false
var EnableCollector = true
var CollectorTriggerPages = 3
var CollectorMaxBytes uint64 = 64 << 20

type FlushExtraParams struct {
	Low  uint64
	High uint64
}

// FlushExtraParamsByGoID stores per-goroutine extra params for passing data
// without changing function signatures.
var FlushExtraParamsByGoID sync.Map // map[uint64]FlushExtraParams
var CollectorDoneByGoID sync.Map    // map[uint64]bool

func currentGoID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) {
		panic("goid")
	}
	var id uint64
	for i := len(prefix); i < n; i++ {
		c := buf[i]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	if id == 0 {
		panic("goid")
	}
	return id
}

func SetFlushExtraParams(low uint64, high uint64) {
	FlushExtraParamsByGoID.Store(currentGoID(), FlushExtraParams{
		Low:  low,
		High: high,
	})
}

func GetFlushExtraParams() (low uint64, high uint64, ok bool) {
	v, ok := FlushExtraParamsByGoID.Load(currentGoID())
	if !ok {
		return 0, 0, false
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High, true
}

func GetAndDelFlushExtraParams() (low uint64, high uint64) {
	v, ok := FlushExtraParamsByGoID.LoadAndDelete(currentGoID())
	if !ok {
		panic("ExtraParams !ok")
	}
	p, ok := v.(FlushExtraParams)
	if !ok {
		panic("flush extra params type")
	}
	return p.Low, p.High
}

func ClearFlushExtraParams() {
	FlushExtraParamsByGoID.Delete(currentGoID())
}

func SetCollectorDone(done bool) {
	CollectorDoneByGoID.Store(currentGoID(), done)
}

func GetAndDelCollectorDone() (done bool, ok bool) {
	v, ok := CollectorDoneByGoID.LoadAndDelete(currentGoID())
	if !ok {
		return false, false
	}
	done, ok = v.(bool)
	if !ok {
		panic("collector done type")
	}
	return done, true
}

const PMTPartIdxFilename = "PartIdx.json"
const PMTFlushHistoryDirname = "flush_history"

type PlanStep1Method uint8

const (
	PlanStep1Simple PlanStep1Method = iota
	PlanStep1V4
)

var Step1Method = PlanStep1V4
var Step1V4RewriteFactor = 1.0
var Step1V4NewWeight = 0.9
var Step1V4OldWeight = 0.1

// If totalWriteExpected is smaller than this threshold,
// may advance compaction to help rebalance.
var WriteThresholdInPages0 uint64 = 5500

// If totalWriteExpected is larger than this threshold,
// small wt0 compactions may be delayed.
var WriteThresholdInPages1 uint64 = 6000

// When delaying compaction above WriteThresholdInPages1,
// only wt0 parts with NewPages smaller than this threshold are delayed.
var DelayCompactNewPagesThreshold int = 50
var NoActiveMergeUntil = 64 // 64次multiflush之前不提前压实

func SetStep1Method(m PlanStep1Method) {
	switch m {
	case PlanStep1Simple, PlanStep1V4:
		Step1Method = m
	default:
		panic(fmt.Sprintf("SetStep1Method: unknown method %d", m))
	}
}

var PartIdx []Part = []Part{
	Part{
		Low:   0,
		High:  math.MaxUint64,
		Stack: nil,
	},
}

// todo: First,Last
type Part struct {
	Low   uint64 // inclusive lower bound (minimum key that can be contained)
	High  uint64 // inclusive upper bound (maximum key that can be contained)
	Stack []base.FileNum
	Tmp   []base.FileNum // for replace
}

func GetPartContain(k uint64) int {
	// Binary search for the first partition where High >= k
	// Then verify that Low <= k <= High
	low, high := 0, len(PartIdx)
	for high > low {
		mid := int(uint(low+high) >> 1)
		if PartIdx[mid].High < k {
			low = mid + 1
		} else {
			high = mid
		}
	}
	// Verify that the found partition actually contains k
	if low < len(PartIdx) && PartIdx[low].Low <= k && k <= PartIdx[low].High {
		return low
	}
	panic(fmt.Sprintf("no partition contains key %d", k))
}

func Replace(idx int, newParts []Part) {
	newIdx := make([]Part, 0, len(newParts))
	for i := 0; i < idx; i++ {
		newIdx = append(newIdx, PartIdx[i])
	}
	newIdx = append(newIdx, newParts...)
	for i := idx + 1; i < len(PartIdx); i++ {
		newIdx = append(newIdx, PartIdx[i])
	}
}

var SstMap map[uint64]SstInfo = make(map[uint64]SstInfo, 1024)

type SstInfo struct {
	Size     uint64 // in bytes
	Smallest uint64
	Largest  uint64
}

func AddToMap(output uint64, info SstInfo) {
	if _, ok := SstMap[output]; ok {
		panic(fmt.Sprintf("file %d already exists", output))
	}
	SstMap[output] = info
}

func RemoveFromMap(input uint64) {
	if _, ok := SstMap[uint64(input)]; !ok {
		panic(fmt.Sprintf("file %d not exists", input)) // partial?
	}
	delete(SstMap, uint64(input))
}
