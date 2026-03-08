package pebble

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	KVPerPage = 4096 / 16
	PageSize  = 4096
)

type PartPlanStat struct {
	PartLow  uint64
	PartHigh uint64
	NewPages int
	WriteTo  uint16
	Stack    []uint64 // file size in page
	Reason   string
}

type FlushPlanStat struct {
	passiveMergeCount  int // todo
	activeMergeCount   int // todo
	delayCompactCount  int
	totalWriteExpected uint64
	totalExtraWrite    uint64
	totalReducedWrite  uint64
	wt0                []int
	plans              []PartPlanStat
}

var flushHistory []FlushPlanStat
var totalWriteExpectedList []uint64

type FlushPlan struct {
	passiveMergeCount  int
	activeMergeCount   int
	delayCompactCount  int
	totalWriteExpected uint64
	totalExtraWrite    uint64
	totalReducedWrite  uint64
	wt0                []int // writeTo==0
	planList           []PartPlan
}

// todo: 改成并发的
func planStep1(newKeys []uint64) FlushPlan {
	switch pmtinternal.Step1Method {
	case pmtinternal.PlanStep1Simple:
		return step1Simple()
	case pmtinternal.PlanStep1V1:
		return step1V1(newKeys)
	case pmtinternal.PlanStep1V2:
		return step1V2(newKeys)
	case pmtinternal.PlanStep1V3:
		return step1V3(newKeys)
	case pmtinternal.PlanStep1V4:
		return step1V4(newKeys)
	default:
		panic(fmt.Sprintf("planStep1: unknown step1 method %d", pmtinternal.Step1Method))
	}
}

func plan(newKeys []uint64) FlushPlan {
	flushPlan := planStep1(newKeys)
	flushPlan = mergeAdjacent_wt0(flushPlan)
	if len(flushHistory) > pmtinternal.NoActiveMergeUntil {
		if flushPlan.totalWriteExpected < pmtinternal.WriteThresholdInPages0 {
			extraWriteThreshold := pmtinternal.WriteThresholdInPages0 - flushPlan.totalWriteExpected
			flushPlan = activeMergePlan(flushPlan, extraWriteThreshold)
			flushPlan = mergeAdjacent_wt0(flushPlan)
		}
		// todo: 推迟也不能只追加啊
		//if flushPlan.totalWriteExpected > pmtinternal.WriteThresholdInPages1 {
		//	flushPlan = delaySmallCompaction(flushPlan)
		//}
	}
	recordFlushPlan(flushPlan) // todo: 不应该在这里
	return flushPlan
}

func step1Simple() FlushPlan {
	const threshold = 3

	pList := make([]PartPlan, 0, 256)
	var totalWriteExpected uint64
	for _, p := range pmtinternal.PartIdx {
		sp := PartPlan{
			High:     p.High,
			low:      p.Low,
			NewPages: 0,
			WriteTo:  uint16(len(p.Stack)),
			Stack:    p.Stack,
			Outputs:  nil,
		}
		writeTo := len(sp.Stack)
		reason := "append_only"
		if len(sp.Stack) > threshold {
			writeTo = 0
			reason = "stack_len_reach_threshold"
		}
		if writeTo >= manifest.NumLevels {
			panic(fmt.Sprintf("writeTo >= manifest.NumLevels"))
		}
		sp.WriteTo = uint16(writeTo)
		sp.Reason = reason
		totalWriteExpected += sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
		pList = append(pList, sp)
	}

	return FlushPlan{
		passiveMergeCount:  0,
		activeMergeCount:   0,
		delayCompactCount:  0,
		totalWriteExpected: totalWriteExpected,
		totalExtraWrite:    0,
		totalReducedWrite:  0,
		wt0:                collectWt0(pList),
		planList:           pList,
	}
}

func step1V1(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	pList := make([]PartPlan, 0, len(parts))
	var totalWriteExpected uint64

	// Two pointers over sorted newKeys + sorted PartIdx range to get O(P+N).
	keyL, keyR := 0, 0
	for _, p := range parts {
		for keyL < len(newKeys) && newKeys[keyL] < p.Low {
			keyL++
		}
		if keyR < keyL {
			keyR = keyL
		}
		for keyR < len(newKeys) && newKeys[keyR] <= p.High {
			keyR++
		}
		newKVCount := keyR - keyL
		newPages := (newKVCount + KVPerPage - 1) / KVPerPage
		sp, writeExpected := buildStep1V1PartPlan(p, newPages)
		totalWriteExpected += writeExpected
		pList = append(pList, sp)
	}

	return FlushPlan{
		passiveMergeCount:  0,
		activeMergeCount:   0,
		delayCompactCount:  0,
		totalWriteExpected: totalWriteExpected,
		totalExtraWrite:    0,
		totalReducedWrite:  0,
		wt0:                collectWt0(pList),
		planList:           pList,
	}
}

func step1V2(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	partCount := len(parts)
	pList := make([]PartPlan, partCount)
	chunkSize := pmtinternal.Step1V2ChunkSize
	if chunkSize <= 0 {
		panic(fmt.Sprintf("step1V2: invalid chunkSize %d", chunkSize))
	}
	workers := partCount / chunkSize
	maxWorkers := runtime.GOMAXPROCS(0)
	if workers > maxWorkers {
		workers = maxWorkers
	}
	if workers < 1 {
		workers = 1
	}

	var wg sync.WaitGroup
	workerWriteExpected := make([]uint64, workers)
	for wid := 0; wid < workers; wid++ {
		start := wid * partCount / workers
		end := (wid + 1) * partCount / workers
		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			if start >= end {
				return
			}
			// Find first key index that may hit this worker segment.
			keyL := sort.Search(len(newKeys), func(i int) bool {
				return newKeys[i] >= parts[start].Low
			})
			keyR := keyL
			var localWriteExpected uint64
			for i := start; i < end; i++ {
				p := parts[i]
				for keyL < len(newKeys) && newKeys[keyL] < p.Low {
					keyL++
				}
				if keyR < keyL {
					keyR = keyL
				}
				for keyR < len(newKeys) && newKeys[keyR] <= p.High {
					keyR++
				}
				newKVCount := keyR - keyL
				newPages := (newKVCount + KVPerPage - 1) / KVPerPage
				sp, writeExpected := buildStep1V1PartPlan(p, newPages)
				pList[i] = sp
				localWriteExpected += writeExpected
			}
			workerWriteExpected[workerID] = localWriteExpected
		}(wid, start, end)
	}
	wg.Wait()

	var totalWriteExpected uint64
	for _, v := range workerWriteExpected {
		totalWriteExpected += v
	}

	return FlushPlan{
		passiveMergeCount:  0,
		activeMergeCount:   0,
		delayCompactCount:  0,
		totalWriteExpected: totalWriteExpected,
		totalExtraWrite:    0,
		totalReducedWrite:  0,
		wt0:                collectWt0(pList),
		planList:           pList,
	}
}

func step1V3(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	pList := make([]PartPlan, 0, len(parts))
	var totalWriteExpected uint64

	keyL, keyR := 0, 0
	for _, p := range parts {
		for keyL < len(newKeys) && newKeys[keyL] < p.Low {
			keyL++
		}
		if keyR < keyL {
			keyR = keyL
		}
		for keyR < len(newKeys) && newKeys[keyR] <= p.High {
			keyR++
		}
		newKVCount := keyR - keyL
		newPages := (newKVCount + KVPerPage - 1) / KVPerPage
		sp, writeExpected := buildStep1V3PartPlan(p, newPages)
		totalWriteExpected += writeExpected
		pList = append(pList, sp)
	}

	return FlushPlan{
		passiveMergeCount:  0,
		activeMergeCount:   0,
		delayCompactCount:  0,
		totalWriteExpected: totalWriteExpected,
		totalExtraWrite:    0,
		totalReducedWrite:  0,
		wt0:                collectWt0(pList),
		planList:           pList,
	}
}

func step1V4(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	pList := make([]PartPlan, 0, len(parts))
	var totalWriteExpected uint64

	keyL, keyR := 0, 0
	for _, p := range parts {
		for keyL < len(newKeys) && newKeys[keyL] < p.Low {
			keyL++
		}
		if keyR < keyL {
			keyR = keyL
		}
		for keyR < len(newKeys) && newKeys[keyR] <= p.High {
			keyR++
		}
		newKVCount := keyR - keyL
		newPages := (newKVCount + KVPerPage - 1) / KVPerPage

		sp, writeExpected := buildStep1V4PartPlan(p, newPages)
		totalWriteExpected += writeExpected
		pList = append(pList, sp)
	}

	return FlushPlan{
		passiveMergeCount:  0,
		activeMergeCount:   0,
		delayCompactCount:  0,
		totalWriteExpected: totalWriteExpected,
		totalExtraWrite:    0,
		totalReducedWrite:  0,
		wt0:                collectWt0(pList),
		planList:           pList,
	}
}

func collectWt0(planList []PartPlan) []int {
	ret := make([]int, 0, len(planList))
	for i := range planList {
		if planList[i].WriteTo == 0 {
			ret = append(ret, i)
		}
	}
	return ret
}

func stackPagesFromFiles(stack []FileNum) []uint64 {
	pages := make([]uint64, 0, len(stack))
	for _, fn := range stack {
		info, ok := pmtinternal.SstMap[uint64(fn)]
		if !ok {
			panic(fmt.Sprintf("plan snapshot: file %d not found in SstMap", fn))
		}
		pages = append(pages, sstSizeInPages(info.Size))
	}
	return pages
}

func partPlanStatFromPlan(pp PartPlan) PartPlanStat {
	return PartPlanStat{
		PartLow:  pp.low,
		PartHigh: pp.High,
		NewPages: pp.NewPages,
		WriteTo:  pp.WriteTo,
		Stack:    stackPagesFromFiles(pp.Stack),
		Reason:   pp.Reason,
	}
}

func snapshotPartPlanStats(planList []PartPlan) []PartPlanStat {
	stats := make([]PartPlanStat, 0, len(planList))
	for _, pp := range planList {
		stats = append(stats, partPlanStatFromPlan(pp))
	}
	return stats
}

func buildStep1V1PartPlan(p pmtinternal.Part, newPages int) (PartPlan, uint64) {
	const plan1A = 1.0
	const plan1B = 2.0
	const plan1WriteToBoundary = 4

	sp := PartPlan{
		High:     p.High,
		low:      p.Low,
		NewPages: newPages,
		WriteTo:  uint16(len(p.Stack)),
		Stack:    p.Stack,
		Outputs:  nil,
	}
	threshold := plan1A*float64(newPages) + plan1B
	writeTo := len(sp.Stack)
	rewritePages := 0
	reason := "stack_empty"
	if len(sp.Stack) > 0 {
		reason = "no_threshold_limit"
	}
	for i := len(sp.Stack) - 1; i >= 0; i-- {
		info, ok := pmtinternal.SstMap[uint64(sp.Stack[i])]
		if !ok {
			panic(fmt.Sprintf("planStep1: file %d not found in SstMap", sp.Stack[i]))
		}
		nextFilePages := int(info.Size / PageSize)
		if float64(rewritePages+nextFilePages) > threshold {
			reason = "reach_threshold"
			break
		}
		rewritePages += nextFilePages
		writeTo = i
	}
	if writeTo == plan1WriteToBoundary {
		writeTo--
		reason = "reach_cap"
	}
	if writeTo >= manifest.NumLevels {
		writeTo = manifest.NumLevels - 1
		reason = reason + "_clamp_levels"
	}
	sp.WriteTo = uint16(writeTo)
	sp.Reason = reason
	writeExpected := uint64(newPages) + sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
	return sp, writeExpected
}

func buildStep1V3PartPlan(p pmtinternal.Part, newPages int) (PartPlan, uint64) {
	const plan1WriteToBoundary = 4

	sp := PartPlan{
		High:     p.High,
		low:      p.Low,
		NewPages: newPages,
		WriteTo:  uint16(len(p.Stack)),
		Stack:    p.Stack,
		Outputs:  nil,
	}
	writeTo := len(sp.Stack)
	rewritePages := 0
	reason := "stack_empty"
	if len(sp.Stack) > 0 {
		reason = "no_threshold_limit_v3"
	}
	remainingSlots := plan1WriteToBoundary - len(sp.Stack)
	if remainingSlots < 0 {
		remainingSlots = 0
	}
	threshold := step1V3RewriteThreshold(newPages, remainingSlots)
	for i := len(sp.Stack) - 1; i >= 0; i-- {
		info, ok := pmtinternal.SstMap[uint64(sp.Stack[i])]
		if !ok {
			panic(fmt.Sprintf("planStep1V3: file %d not found in SstMap", sp.Stack[i]))
		}
		nextFilePages := int(info.Size / PageSize)
		if float64(rewritePages+nextFilePages) > threshold {
			reason = fmt.Sprintf("reach_threshold_v3_slots_%d", remainingSlots)
			break
		}
		rewritePages += nextFilePages
		writeTo = i
	}
	if writeTo == plan1WriteToBoundary {
		writeTo--
		reason = "reach_cap_v3"
	}
	if writeTo >= manifest.NumLevels {
		writeTo = manifest.NumLevels - 1
		reason = reason + "_clamp_levels"
	}
	sp.WriteTo = uint16(writeTo)
	sp.Reason = reason
	writeExpected := uint64(newPages) + sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
	return sp, writeExpected
}

func costV4(part pmtinternal.Part, newPages int, writeTo int) float64 {
	tablePages := stackPagesFromFiles(part.Stack)
	if writeTo < 0 || writeTo > len(tablePages) {
		panic("costV4: invalid writeTo")
	}
	if writeTo == len(tablePages) {
		return 0
	}
	var oldCost uint64
	for j := writeTo + 1; j < len(tablePages); j++ {
		oldCost += uint64(j-writeTo) * tablePages[j]
	}
	tablePagesI := float64(tablePages[writeTo])
	slotsFromI := float64(len(tablePages) - writeTo)
	return pmtinternal.Step1V4RewriteFactor*tablePagesI -
		pmtinternal.Step1V4NewWeight*float64(newPages)*slotsFromI -
		pmtinternal.Step1V4OldWeight*float64(oldCost)
}

func buildStep1V4PartPlan(p pmtinternal.Part, newPages int) (PartPlan, uint64) {
	sp := PartPlan{
		High:     p.High,
		low:      p.Low,
		NewPages: newPages,
		WriteTo:  uint16(len(p.Stack)),
		Stack:    p.Stack,
		Outputs:  nil,
	}
	writeTo := len(sp.Stack)
	reason := "stack_empty"
	if len(sp.Stack) > 0 {
		reason = "append_only_v4_min_cost_non_negative"
	}
	tablePages := stackPagesFromFiles(sp.Stack)
	n := len(tablePages)
	bestCost := 0.0
	bestI := writeTo
	for i := n - 1; i >= 0; i-- {
		cost := costV4(p, newPages, i)
		if cost < bestCost {
			bestCost = cost
			bestI = i
		}
	}
	writeTo = bestI
	if bestI < len(sp.Stack) {
		reason = fmt.Sprintf("rewrite_v4_min_cost_i_%d", bestI)
	}
	if writeTo >= manifest.NumLevels {
		writeTo = manifest.NumLevels - 1
		reason = reason + "_clamp_levels"
	}
	sp.WriteTo = uint16(writeTo)
	sp.Reason = reason
	writeExpected := uint64(newPages) + sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
	return sp, writeExpected
}

func step1V3RewriteThreshold(newPages int, remainingSlots int) float64 {
	if remainingSlots < 0 {
		panic("step1V3RewriteThreshold: remainingSlots < 0")
	}
	switch {
	case remainingSlots >= 2:
		return 0.35*float64(newPages) + 1
	case remainingSlots == 1:
		return 0.7*float64(newPages) + 2
	default:
		return 1.0*float64(newPages) + 2
	}
}

func sumStackPagesFrom(stack []FileNum, from int) uint64 {
	if from < 0 || from > len(stack) {
		panic("invalid from")
	}
	var pages uint64
	for i := from; i < len(stack); i++ {
		info, ok := pmtinternal.SstMap[uint64(stack[i])]
		if !ok {
			panic(fmt.Sprintf("file %d not found in SstMap", stack[i]))
		}
		pages += sstSizeInPages(info.Size)
	}
	return pages
}

func sstSizeInPages(size uint64) uint64 {
	if size == 0 {
		return 0
	}
	return (size + uint64(PageSize) - 1) / uint64(PageSize)
}

func recordFlushPlan(flushPlan FlushPlan) {
	totalWriteExpectedList = append(totalWriteExpectedList, flushPlan.totalWriteExpected)
	wt0 := append([]int(nil), flushPlan.wt0...)
	flushHistory = append(flushHistory, FlushPlanStat{
		passiveMergeCount:  flushPlan.passiveMergeCount,
		activeMergeCount:   flushPlan.activeMergeCount,
		delayCompactCount:  flushPlan.delayCompactCount,
		totalWriteExpected: flushPlan.totalWriteExpected,
		totalExtraWrite:    flushPlan.totalExtraWrite,
		totalReducedWrite:  flushPlan.totalReducedWrite,
		wt0:                wt0,
		plans:              snapshotPartPlanStats(flushPlan.planList),
	})
}

func mergeAdjacent_wt0(flushPlan FlushPlan) FlushPlan {
	merged := make([]PartPlan, 0, len(flushPlan.planList))
	passiveMergeCount := 0
	for i := 0; i < len(flushPlan.planList); { // 双指针
		cur := flushPlan.planList[i]
		if cur.WriteTo != 0 {
			merged = append(merged, cur)
			i++
			continue
		}
		j := i + 1
		for ; j < len(flushPlan.planList) && flushPlan.planList[j].WriteTo == 0; j++ {
			next := flushPlan.planList[j]
			// no need to check keyrange
			cur.High = next.High
			cur.NewPages += next.NewPages
			cur.Stack = append(cur.Stack, next.Stack...)
			passiveMergeCount++
		}
		merged = append(merged, cur)
		i = j
	}
	flushPlan.passiveMergeCount += passiveMergeCount
	flushPlan.planList = merged
	flushPlan.wt0 = collectWt0(merged)
	return flushPlan
}

func adjustedReasonForWriteToChange(from, to int) string {
	if from == to {
		panic("adjustedReasonForWriteToChange: from == to")
	}
	if from < to {
		return fmt.Sprintf("delayFrom%d", from)
	}
	return fmt.Sprintf("advanceFrom%d", from)
}

func delaySmallCompaction(flushPlan FlushPlan) FlushPlan {
	for _, idx := range flushPlan.wt0 {
		if idx < 0 || idx >= len(flushPlan.planList) {
			panic("delaySmallCompaction: invalid wt0 idx")
		}
		pp := &flushPlan.planList[idx]
		if pp.WriteTo != 0 {
			panic("delaySmallCompaction: pp.WriteTo != 0")
		}
		if pp.NewPages >= pmtinternal.DelayCompactNewPagesThreshold {
			continue
		}
		if len(pp.Stack) >= manifest.NumLevels {
			panic("writeTo would be too large")
		}
		reducedWrite := sumStackPagesFrom(pp.Stack, 0)
		if reducedWrite == 0 {
			continue
		}
		if flushPlan.totalWriteExpected < reducedWrite {
			panic("delaySmallCompaction: totalWriteExpected underflow")
		}
		oldWriteTo := int(pp.WriteTo)
		newWriteTo := len(pp.Stack)
		pp.WriteTo = uint16(newWriteTo)
		pp.Reason = adjustedReasonForWriteToChange(oldWriteTo, newWriteTo)
		flushPlan.totalWriteExpected -= reducedWrite
		flushPlan.totalReducedWrite += reducedWrite
		flushPlan.delayCompactCount++
	}
	flushPlan.wt0 = collectWt0(flushPlan.planList)
	return flushPlan
}

// todo: 不一定要把pp.WriteTo提前到0
func extraWriteExpected(pp PartPlan) uint64 {
	if int(pp.WriteTo) > len(pp.Stack) {
		panic("pp.WriteTo > len(pp.Stack)")
	}
	if pp.WriteTo == 0 {
		panic("pp.WriteTo == 0")
	}
	return sumStackPagesFrom(pp.Stack, 0) - sumStackPagesFrom(pp.Stack, int(pp.WriteTo))
}

func chooseTryPushIdx(flushPlan FlushPlan, wt0Idx int, wt0Set map[int]struct{}) (int, uint64, bool) {
	prevIdx, hasPrev := wt0Idx-1, wt0Idx > 0
	succIdx, hasSucc := wt0Idx+1, wt0Idx+1 < len(flushPlan.planList)
	if !hasPrev && !hasSucc {
		return 0, 0, false
	}
	if !hasPrev {
		if _, ok := wt0Set[succIdx]; ok {
			panic("chooseTryPushIdx: succ in wt0")
		}
		return succIdx, extraWriteExpected(flushPlan.planList[succIdx]), true
	}
	if !hasSucc {
		if _, ok := wt0Set[prevIdx]; ok {
			panic("chooseTryPushIdx: prev in wt0")
		}
		return prevIdx, extraWriteExpected(flushPlan.planList[prevIdx]), true
	}
	if _, ok := wt0Set[prevIdx]; ok {
		panic("chooseTryPushIdx: prev in wt0")
	}
	if _, ok := wt0Set[succIdx]; ok {
		panic("chooseTryPushIdx: succ in wt0")
	}
	prevWrite := extraWriteExpected(flushPlan.planList[prevIdx])
	succWrite := extraWriteExpected(flushPlan.planList[succIdx])
	if prevWrite < succWrite {
		return prevIdx, prevWrite, true
	}
	return succIdx, succWrite, true
}

func activeMergePlan(flushPlan FlushPlan, extraWriteThreshold uint64) FlushPlan {
	if flushPlan.wt0 == nil {
		panic("flushPlan.wt0 == nil")
	}
	if len(flushPlan.planList) <= 1 {
		return flushPlan
	}
	if extraWriteThreshold == 0 {
		return flushPlan
	}

	type tryPushCandidate struct {
		ppIdx      int
		extraWrite uint64
	}
	wt0Set := make(map[int]struct{}, len(flushPlan.wt0))
	for _, idx := range flushPlan.wt0 {
		wt0Set[idx] = struct{}{}
	}
	tryPushMap := make(map[int]tryPushCandidate, len(flushPlan.wt0))
	tryPushList := make([]tryPushCandidate, 0, len(flushPlan.wt0))
	for _, wt0Idx := range flushPlan.wt0 {
		tryPushIdx, extraWrite, ok := chooseTryPushIdx(flushPlan, wt0Idx, wt0Set)
		if !ok {
			continue
		}
		if _, exists := wt0Set[tryPushIdx]; exists {
			panic("activeMergePlan: tryPush in wt0")
		}
		if _, exists := tryPushMap[tryPushIdx]; exists {
			continue
		}
		candidate := tryPushCandidate{
			ppIdx:      tryPushIdx,
			extraWrite: extraWrite,
		}
		tryPushMap[tryPushIdx] = candidate
		tryPushList = append(tryPushList, candidate)
	}
	sort.Slice(tryPushList, func(i, j int) bool {
		if tryPushList[i].extraWrite != tryPushList[j].extraWrite {
			return tryPushList[i].extraWrite < tryPushList[j].extraWrite
		}
		return tryPushList[i].ppIdx < tryPushList[j].ppIdx
	})
	totalExtraWrite := uint64(0)
	for _, tryPush := range tryPushList {
		if totalExtraWrite+tryPush.extraWrite > extraWriteThreshold {
			break
		}
		oldWriteTo := int(flushPlan.planList[tryPush.ppIdx].WriteTo)
		flushPlan.wt0 = append(flushPlan.wt0, tryPush.ppIdx)
		flushPlan.planList[tryPush.ppIdx].WriteTo = 0
		flushPlan.planList[tryPush.ppIdx].Reason = adjustedReasonForWriteToChange(oldWriteTo, 0)
		flushPlan.totalWriteExpected += tryPush.extraWrite
		totalExtraWrite += tryPush.extraWrite
		flushPlan.totalExtraWrite += tryPush.extraWrite
		flushPlan.activeMergeCount++
	}
	sort.Ints(flushPlan.wt0)
	return flushPlan
}

func dumpFlushHistory(startFrom int, path string) {
	if path == "" {
		panic("dumpFlushHistory: empty path")
	}
	path = filepath.Join(path, pmtinternal.PMTFlushHistoryDirname)
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(err)
	}
	if startFrom > len(flushHistory) {
		panic("startFrom > len(flushHistory)")
	}
	for flushID := startFrom; flushID < len(flushHistory); flushID++ {
		f, err := os.Create(filepath.Join(path, fmt.Sprintf("flush_%06d.plan", flushID)))
		if err != nil {
			panic(err)
		}
		_, err = f.WriteString(fmt.Sprintf(
			"------totalWriteExpected: %d\n"+
				"------totalExtraWrite: %d\n"+
				"------totalReducedWrite: %d\n"+
				"------passiveMergeCount: %d\n"+
				"------activeMergeCount: %d\n"+
				"------delayCompactCount: %d\n"+
				"------wt0: %v\n",
			flushHistory[flushID].totalWriteExpected,
			flushHistory[flushID].totalExtraWrite,
			flushHistory[flushID].totalReducedWrite,
			flushHistory[flushID].passiveMergeCount,
			flushHistory[flushID].activeMergeCount,
			flushHistory[flushID].delayCompactCount,
			flushHistory[flushID].wt0,
		))
		if err != nil {
			_ = f.Close()
			panic(err)
		}
		for _, e := range flushHistory[flushID].plans {
			_, err := f.WriteString(fmt.Sprintf(
				"# part:[%d,%d], newPages:%d, writeTo:%d, stack: %s, reason: %s\n",
				e.PartLow, e.PartHigh, e.NewPages, e.WriteTo, formatPlanStack(e.Stack), e.Reason,
			))
			if err != nil {
				_ = f.Close()
				panic(err)
			}
		}
		if err := f.Close(); err != nil {
			panic(err)
		}
	}
}

func printTotalWriteExpectedList() {
	var b strings.Builder
	b.WriteString("totalWriteExpectedList=[")
	var sum uint64
	for i, v := range totalWriteExpectedList {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%d", v))
		sum += v
	}
	b.WriteString("]")
	println(b.String())
	if len(totalWriteExpectedList) == 0 {
		println("avgTotalWrite=0")
		return
	}
	avg := float64(sum) / float64(len(totalWriteExpectedList))
	println(fmt.Sprintf("avgTotalWrite=%.2f", avg))
}

// [1000,500,155,100]
func formatPlanStack(stack []uint64) string {
	if len(stack) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, f := range stack {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%d", f))
	}
	b.WriteString("]")
	return b.String()
}

type PartPlan struct {
	High     uint64
	low      uint64 // what use?
	NewPages int
	WriteTo  uint16 // 0 means rewrite all, stack.len means just flush no rewrite
	Reason   string
	Stack    []FileNum
	Outputs  []uint64
}

func newPartIdxFrom(pList []PartPlan) []pmtinternal.Part {
	ret := make([]pmtinternal.Part, 0, len(pList))
	nextLow := uint64(0)
	for _, p := range pList {
		if int(p.WriteTo) > len(p.Stack) {
			panic("p.WriteTo > len(p.Stack)")
		}
		if p.low < nextLow {
			panic("p.low < nextLow")
		}
		if len(p.Outputs) == 0 && p.WriteTo == 0 {
			// delete part: keep nextLow unchanged so following parts absorb this range.
			continue
		}
		if p.WriteTo == 0 {
			outs := append([]uint64(nil), p.Outputs...)
			for _, fn := range outs {
				if _, ok := pmtinternal.SstMap[fn]; !ok {
					panic(fmt.Sprintf("newPartIdxFrom: output file %d not found in SstMap", fn))
				}
			}
			sort.Slice(outs, func(i, j int) bool {
				return pmtinternal.SstMap[outs[i]].Largest < pmtinternal.SstMap[outs[j]].Largest
			})
			currentLow := nextLow
			for _, fn := range outs {
				info := pmtinternal.SstMap[fn]
				if info.Largest < currentLow {
					panic("info.Largest < currentLow")
				}
				ret = append(ret, pmtinternal.Part{
					Low:   currentLow,
					High:  info.Largest,
					Stack: []base.FileNum{base.FileNum(fn)},
					Tmp:   nil,
				})
				currentLow = info.Largest + 1
			}
			nextLow = currentLow
			continue
		}

		newStack := append([]base.FileNum{}, p.Stack[:int(p.WriteTo)]...)
		for _, fn := range p.Outputs {
			newStack = append(newStack, base.FileNum(fn))
		}
		ret = append(ret, pmtinternal.Part{
			Low:   nextLow,
			High:  p.High,
			Stack: newStack,
			Tmp:   nil,
		})
		if p.High != math.MaxUint64 {
			nextLow = p.High + 1
		}
	}
	if len(ret) == 0 {
		ret = append(ret, pmtinternal.Part{
			Low:   0,
			High:  math.MaxUint64,
			Stack: nil,
			Tmp:   nil,
		})
		return ret
	}
	if ret[len(ret)-1].High != math.MaxUint64 {
		ret[len(ret)-1].High = math.MaxUint64
	}
	return ret
}
