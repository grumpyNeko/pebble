package pebble

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
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
	KVPerPage   = 4096 / 16
	PageSize    = 4096
	MaxStackLen = 7
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
	passiveMergeCount       int
	activeMergeCount        int
	step1TotalWriteExpected uint64
	step2TotalWriteExpected uint64
	plans                   []PartPlanStat
}

var flushHistory []FlushPlanStat
var step2TotalWriteExpectedList []uint64

type FlushPlan struct {
	passiveMergeCount       int
	activeMergeCount        int
	step1TotalWriteExpected uint64
	step2TotalWriteExpected uint64
	planList                []PartPlan
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
	default:
		panic(fmt.Sprintf("planStep1: unknown step1 method %d", pmtinternal.Step1Method))
	}
}

func step1Simple() FlushPlan {
	const threshold = 3

	pList := make([]PartPlan, 0, 256)
	tmp := make([]PartPlanStat, 0, len(pmtinternal.PartIdx))
	var step1TotalWriteExpected uint64
	for _, p := range pmtinternal.PartIdx {
		sp := PartPlan{
			High:    p.High,
			low:     p.Low,
			WriteTo: uint16(len(p.Stack)),
			Stack:   p.Stack,
			Outputs: nil,
		}
		writeTo := len(sp.Stack)
		reason := "append_only"
		if len(sp.Stack) > threshold {
			writeTo = 0
			reason = "stack_len_reach_threshold"
		}
		if writeTo >= MaxStackLen {
			panic("writeTo >= MaxStackLen")
		}
		sp.WriteTo = uint16(writeTo)
		step1TotalWriteExpected += sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
		stack := make([]uint64, 0, len(sp.Stack))
		for _, fn := range sp.Stack {
			info, ok := pmtinternal.SstMap[uint64(fn)]
			if !ok {
				panic(fmt.Sprintf("planStep1 snapshot: file %d not found in SstMap", fn))
			}
			stack = append(stack, info.Size/uint64(PageSize))
		}
		tmp = append(tmp, PartPlanStat{
			PartLow:  sp.low,
			PartHigh: sp.High,
			NewPages: 0,
			WriteTo:  sp.WriteTo,
			Stack:    stack,
			Reason:   reason,
		})
		pList = append(pList, sp)
	}

	flushHistory = append(flushHistory, FlushPlanStat{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		plans:                   tmp,
	})
	return FlushPlan{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		planList:                pList,
	}
}

func step1V1(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	pList := make([]PartPlan, 0, len(parts))
	tmp := make([]PartPlanStat, 0, len(parts))
	var step1TotalWriteExpected uint64

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
		sp, stat, writeExpected := buildStep1V1PartPlan(p, newPages)
		step1TotalWriteExpected += writeExpected
		tmp = append(tmp, stat)
		pList = append(pList, sp)
	}

	flushHistory = append(flushHistory, FlushPlanStat{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		plans:                   tmp,
	})
	return FlushPlan{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		planList:                pList,
	}
}

func step1V2(newKeys []uint64) FlushPlan {
	parts := pmtinternal.PartIdx
	partCount := len(parts)
	pList := make([]PartPlan, partCount)
	tmp := make([]PartPlanStat, partCount)
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
				sp, stat, writeExpected := buildStep1V1PartPlan(p, newPages)
				pList[i] = sp
				tmp[i] = stat
				localWriteExpected += writeExpected
			}
			workerWriteExpected[workerID] = localWriteExpected
		}(wid, start, end)
	}
	wg.Wait()

	var step1TotalWriteExpected uint64
	for _, v := range workerWriteExpected {
		step1TotalWriteExpected += v
	}

	flushHistory = append(flushHistory, FlushPlanStat{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		plans:                   tmp,
	})
	return FlushPlan{
		passiveMergeCount:       0,
		activeMergeCount:        0,
		step1TotalWriteExpected: step1TotalWriteExpected,
		step2TotalWriteExpected: step1TotalWriteExpected,
		planList:                pList,
	}
}

func buildStep1V1PartPlan(p pmtinternal.Part, newPages int) (PartPlan, PartPlanStat, uint64) {
	const plan1A = 1.0
	const plan1B = 2.0
	const plan1WriteToBoundary = 4

	sp := PartPlan{
		High:    p.High,
		low:     p.Low,
		WriteTo: uint16(len(p.Stack)),
		Stack:   p.Stack,
		Outputs: nil,
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
	if writeTo >= MaxStackLen {
		panic("writeTo >= MaxStackLen")
	}
	sp.WriteTo = uint16(writeTo)
	writeExpected := uint64(newPages) + sumStackPagesFrom(sp.Stack, int(sp.WriteTo))
	stack := make([]uint64, 0, len(sp.Stack))
	for _, fn := range sp.Stack {
		info, ok := pmtinternal.SstMap[uint64(fn)]
		if !ok {
			panic(fmt.Sprintf("planStep1 snapshot: file %d not found in SstMap", fn))
		}
		stack = append(stack, info.Size/uint64(PageSize))
	}
	return sp, PartPlanStat{
		PartLow:  sp.low,
		PartHigh: sp.High,
		NewPages: newPages,
		WriteTo:  sp.WriteTo,
		Stack:    stack,
		Reason:   reason,
	}, writeExpected
}

func sumStackPagesFrom(stack []FileNum, from int) uint64 {
	if from < 0 || from > len(stack) {
		panic("sumStackPagesFrom: invalid from")
	}
	var pages uint64
	for i := from; i < len(stack); i++ {
		info, ok := pmtinternal.SstMap[uint64(stack[i])]
		if !ok {
			panic(fmt.Sprintf("sumStackPagesFrom: file %d not found in SstMap", stack[i]))
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

func recordFlushPlanStep2(flushPlan FlushPlan) {
	step2TotalWriteExpectedList = append(step2TotalWriteExpectedList, flushPlan.step2TotalWriteExpected)
	if len(flushHistory) == 0 {
		return
	}
	last := len(flushHistory) - 1
	flushHistory[last].passiveMergeCount = flushPlan.passiveMergeCount
	flushHistory[last].activeMergeCount = flushPlan.activeMergeCount
	flushHistory[last].step2TotalWriteExpected = flushPlan.step2TotalWriteExpected
}

func passiveMergePlan(flushPlan FlushPlan) FlushPlan {
	flushPlan.step2TotalWriteExpected = flushPlan.step1TotalWriteExpected
	if len(flushPlan.planList) < 2 {
		recordFlushPlanStep2(flushPlan)
		return flushPlan
	}

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
			cur.Stack = append(cur.Stack, next.Stack...)
			passiveMergeCount++
		}
		merged = append(merged, cur)
		i = j
	}
	ret := FlushPlan{
		passiveMergeCount:       flushPlan.passiveMergeCount + passiveMergeCount,
		activeMergeCount:        flushPlan.activeMergeCount,
		step1TotalWriteExpected: flushPlan.step1TotalWriteExpected,
		step2TotalWriteExpected: flushPlan.step1TotalWriteExpected,
		planList:                merged,
	}
	recordFlushPlanStep2(ret)
	return ret
}

func activeMergePlan(flushPlan FlushPlan) FlushPlan {
	// todo: ...
	return nil
}

func dumpFlushHistory(startFrom int, path string) {
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
			"------step1TotalWriteExpected: %d\n"+
				"------step2TotalWriteExpected: %d\n"+
				"------passiveMergeCount: %d\n"+
				"------activeMergeCount: %d\n",
			flushHistory[flushID].step1TotalWriteExpected,
			flushHistory[flushID].step2TotalWriteExpected,
			flushHistory[flushID].passiveMergeCount,
			flushHistory[flushID].activeMergeCount,
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
	b.WriteString("step2TotalWriteExpectedList=[")
	var sum uint64
	for i, v := range step2TotalWriteExpectedList {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%d", v))
		sum += v
	}
	b.WriteString("]")
	println(b.String())
	if len(step2TotalWriteExpectedList) == 0 {
		println("avgstep2TotalWrite=0")
		return
	}
	avg := float64(sum) / float64(len(step2TotalWriteExpectedList))
	println(fmt.Sprintf("avgstep2TotalWrite=%.2f", avg))
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
	High    uint64
	low     uint64 // what use?
	WriteTo uint16 // 0 means rewrite all, stack.len means just flush no rewrite
	Stack   []FileNum
	Outputs []uint64
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
