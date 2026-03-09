package pebble

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	//Collect  bool     // 写入collector
	Reason string
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

func planStep1(newKeys []uint64) FlushPlan {
	switch pmtinternal.Step1Method {
	case pmtinternal.PlanStep1Simple:
		return step1Simple()
	case pmtinternal.PlanStep1V4:
		return step1V4(newKeys)
	default:
		panic(fmt.Sprintf("planStep1: unknown step1 method %d", pmtinternal.Step1Method))
	}
}

func plan(newKeys []uint64) FlushPlan {
	flushPlan := planStep1(newKeys)
	newPlanList, mergeCount := mergeAdjacent_wt0(flushPlan.planList)
	flushPlan.planList = newPlanList
	flushPlan.passiveMergeCount = mergeCount
	flushPlan.wt0 = collectWt0(newPlanList)
	if len(flushHistory) > pmtinternal.NoActiveMergeUntil {
		if flushPlan.totalWriteExpected < pmtinternal.WriteThresholdInPages0 {
			extraWriteThreshold := pmtinternal.WriteThresholdInPages0 - flushPlan.totalWriteExpected
			flushPlan = activeMergePlan(flushPlan, extraWriteThreshold)
			newPlanList, mergeCount = mergeAdjacent_wt0(flushPlan.planList)
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
		plan := PartPlan{
			High:    p.High,
			low:     p.Low,
			WriteTo: uint16(len(p.Stack)),
			Stack:   p.Stack,
			Outputs: nil,
		}
		writeTo := len(plan.Stack)
		reason := "append_only"
		if len(plan.Stack) > threshold {
			writeTo = 0
			reason = "stack_len_reach_threshold"
		}
		if writeTo >= manifest.NumLevels {
			panic(fmt.Sprintf("writeTo >= manifest.NumLevels"))
		}
		plan.WriteTo = uint16(writeTo)
		plan.Reason = reason
		totalWriteExpected += sumStackPagesFrom(plan.Stack, int(plan.WriteTo))
		pList = append(pList, plan)
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

// mem.keys[idxL, idxR]包含low和high
func seekFakeMemTable(mem fakeMemTable, low uint64, high uint64) (idxL int, idxR int) {
	idxL = sort.Search(len(mem.keys), func(i int) bool {
		return mem.keys[i] >= low
	})
	idxR = sort.Search(len(mem.keys), func(i int) bool {
		return mem.keys[i] > high
	})
	return idxL, idxR
}

// mem.keys[idxL, idxR]包含low和high
func seekKeys(keys []uint64, low uint64, high uint64) (idxL int, idxR int) {
	idxL = sort.Search(len(keys), func(i int) bool {
		return keys[i] >= low
	})
	idxR = sort.Search(len(keys), func(i int) bool {
		return keys[i] > high
	})
	return idxL, idxR
}

func kvCountToPageCount(kvCount int) int {
	return (kvCount + KVPerPage - 1) / KVPerPage
}

func step1V4(newKeys []uint64) FlushPlan {
	pList := make([]PartPlan, 0, len(pmtinternal.PartIdx))
	var totalWriteExpected uint64

	for _, part := range pmtinternal.PartIdx {
		idxL, idxR := seekKeys(newKeys, part.Low, part.High)
		newKVCount := idxR - idxL
		if collectorEnabled() {
			newKVCount += collectorKVCountInRange(part.Low, part.High)
		}
		newPages := kvCountToPageCount(newKVCount)
		writeTo, reason, writeExpected := doStep1V4PartPlan(part, newPages)
		totalWriteExpected += writeExpected
		pList = append(pList, PartPlan{
			High: part.High,
			low:  part.Low,
			//MemTableIdxL:    idxL,
			//MemTableKVCount: idxR - idxL,
			NewPageCount: newPages,
			WriteTo:      writeTo,
			Reason:       reason,
			Stack:        part.Stack,
		})
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
		NewPages: pp.NewPageCount,
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

func costV4(stack []base.FileNum, newPages int, writeTo int) float64 {
	const (
		C = 5
		B = 50
	)
	tablePages := stackPagesFromFiles(stack)
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

	ret := pmtinternal.Step1V4RewriteFactor*tablePagesI -
		pmtinternal.Step1V4NewWeight*float64(newPages)*slotsFromI -
		pmtinternal.Step1V4OldWeight*float64(oldCost)
	if len(stack) >= C && writeTo < C {
		ret -= B
	}
	return ret
}

func doStep1V4PartPlan(p pmtinternal.Part, newPages int) (uint16, string, uint64) {
	writeTo := len(p.Stack)
	reason := "stack_empty"
	if len(p.Stack) > 0 {
		reason = "append_only_v4_min_cost_non_negative"
	}
	tablePages := stackPagesFromFiles(p.Stack)
	n := len(tablePages)
	bestCost := 0.0
	bestWt := writeTo
	for i := n - 1; i >= 0; i-- {
		cost := costV4(p.Stack, newPages, i)
		if cost < bestCost {
			bestCost = cost
			bestWt = i
		}
	}
	writeTo = bestWt
	if bestWt < len(p.Stack) {
		reason = fmt.Sprintf("rewrite_v4_min_cost_i_%d", bestWt)
	}
	if writeTo >= manifest.NumLevels {
		writeTo = manifest.NumLevels - 1
		reason = reason + "_clamp_levels"
	}
	writeExpected := uint64(newPages) + sumStackPagesFrom(p.Stack, writeTo)
	return uint16(writeTo), reason, writeExpected
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

func mergeAdjacent_wt0(planList []PartPlan) ([]PartPlan, int) {
	newPlanList := make([]PartPlan, 0, len(planList))
	mergeCount := 0
	for _, p := range planList {
		if p.WriteTo != 0 {
			newPlanList = append(newPlanList, p)
			continue
		}
		if len(newPlanList) == 0 || newPlanList[len(newPlanList)-1].WriteTo != 0 {
			newPlanList = append(newPlanList, p)
			continue
		}
		last := &newPlanList[len(newPlanList)-1]
		// no need to check keyrange
		last.High = p.High
		last.NewPageCount += p.NewPageCount // todo: 改成MemTableIdxL了, 先别管
		last.Stack = append(last.Stack, p.Stack...)
		mergeCount++
	}
	return newPlanList, mergeCount
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
		if pp.NewPageCount >= pmtinternal.DelayCompactNewPagesThreshold {
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

// todo: 以后不一定要把pp.WriteTo提前到0, 提前到1就行了
func extraWriteExpected(pp PartPlan) uint64 {
	if int(pp.WriteTo) > len(pp.Stack) {
		panic("pp.WriteTo > len(pp.Stack)")
	}
	if pp.WriteTo == 0 {
		panic("pp.WriteTo == 0")
	}
	// TODO: 应该涉及cost, ..
	//oldCost := costV4(pp.Stack, pp.NewPageCount, int(pp.WriteTo))
	//newCost := costV4(pp.Stack, pp.NewPageCount, 0)
	return sumStackPagesFrom(pp.Stack, 0) - sumStackPagesFrom(pp.Stack, int(pp.WriteTo))
}

func chooseTryPushIdx(flushPlan FlushPlan, wt0Idx int) (int, uint64, bool) {
	prevIdx, hasPrev := wt0Idx-1, wt0Idx > 0
	succIdx, hasSucc := wt0Idx+1, wt0Idx+1 < len(flushPlan.planList)
	if !hasPrev && !hasSucc {
		return 0, 0, false
	}
	if !hasPrev {
		return succIdx, extraWriteExpected(flushPlan.planList[succIdx]), true
	}
	if !hasSucc {
		return prevIdx, extraWriteExpected(flushPlan.planList[prevIdx]), true
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
	tryPushMap := make(map[int]tryPushCandidate, len(flushPlan.wt0))
	tryPushList := make([]tryPushCandidate, 0, len(flushPlan.wt0))
	for _, wt0Idx := range flushPlan.wt0 {
		tryPushIdx, extraWrite, ok := chooseTryPushIdx(flushPlan, wt0Idx)
		if !ok {
			continue
		}
		if extraWrite > 150 { // TODO: 不只extraWriteThreshold
			continue
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
		flushPlan.wt0 = append(flushPlan.wt0, tryPush.ppIdx)
		flushPlan.planList[tryPush.ppIdx].WriteTo = 0
		flushPlan.planList[tryPush.ppIdx].Reason = adjustedReasonForWriteToChange(int(flushPlan.planList[tryPush.ppIdx].WriteTo), 0)
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
	High uint64
	low  uint64 // what use?
	// todo: 也许为Memtable和Collector留下线索
	//MemTableIdxL    int
	//MemTableKVCount int
	NewPageCount int
	WriteTo      uint16 // 0 means rewrite all, stack.len means just flush no rewrite
	Reason       string
	Stack        []FileNum
	Outputs      []uint64
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
