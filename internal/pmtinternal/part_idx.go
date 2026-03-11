package pmtinternal

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"math"
)

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
