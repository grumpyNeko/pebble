package pmtinternal

import (
	"encoding/binary"
	"fmt"
)

var EnablePMT = true
var EnablePMTTableFormat = true
var LogicDel = false
var EnableCollector = false
var CollectorTriggerPages = 0
var CollectorMaxBytes uint64 = 16 << 20

const PMTPartIdxFilename = "PartIdx.json"
const PMTFlushHistoryDirname = "flush_history"

type PlanStep1Method uint8

const (
	PlanStep1Simple PlanStep1Method = iota
	PlanStep1V4
)

var Step1Method = PlanStep1V4
var Step1V4RewriteFactor = 1.0
var Step1V4NewWeight = 1.0
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

func IsLogicDelValue(v []byte) bool {
	return len(v) == 8 && binary.BigEndian.Uint64(v) == 999
}
