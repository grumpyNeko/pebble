package pmtinternal

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"math"
)

var EnablePMT = true
var EnablePMTTableFormat = false

type PlanStep1Method uint8

const (
	PlanStep1Simple PlanStep1Method = iota
	PlanStep1V1
	PlanStep1V2
)

var Step1Method = PlanStep1V1
var Step1V2ChunkSize = 64
var ActiveMergeBudgetBytes uint64 = 5500 * 4096
var NoActiveMergeUntil = 64 // 64次multiflush之前不提前压实

func SetStep1Method(m PlanStep1Method) {
	switch m {
	case PlanStep1Simple, PlanStep1V1, PlanStep1V2:
		Step1Method = m
	default:
		panic(fmt.Sprintf("SetStep1Method: unknown method %d", m))
	}
}

func SetStep1V2ChunkSize(chunkSize int) {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("SetStep1V2ChunkSize: invalid chunkSize %d", chunkSize))
	}
	Step1V2ChunkSize = chunkSize
}

func SetActiveMergeBudgetBytes(budgetBytes uint64) {
	ActiveMergeBudgetBytes = budgetBytes
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
