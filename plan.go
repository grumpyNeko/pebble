package pebble

import (
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	KVPerPage            = 4096 / 16
	PageSize             = 4096
	plan1A               = 1.0
	plan1B               = 2.0
	plan1WriteToBoundary = 4
	MaxStackLen          = 7
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
	plans []PartPlanStat
}

var flushHistory []FlushPlanStat

type FlushPlan struct {
	planList []PartPlan
}

// todo: 改成并发的
func planStep1(newKeys []uint64) FlushPlan {
	pList := make([]PartPlan, 0, 256)

	tmp := make([]PartPlanStat, 0, len(pmtinternal.PartIdx))
	for _, p := range pmtinternal.PartIdx {
		sp := PartPlan{
			High:    p.High,
			low:     p.Low,
			WriteTo: uint16(len(p.Stack)),
			Stack:   p.Stack,
			Outputs: nil,
		}

		newKVCount := len(rangeLimit(newKeys, sp.low, sp.High))
		newPages := newKVCount / KVPerPage
		if newKVCount%KVPerPage != 0 {
			newPages++
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
			NewPages: newPages,
			WriteTo:  sp.WriteTo,
			Stack:    stack,
			Reason:   reason,
		})
		pList = append(pList, sp)
	}

	flushHistory = append(flushHistory, FlushPlanStat{plans: tmp})
	return FlushPlan{planList: pList}
}

var mergeCt = 0 // 用来观察, 暂时放在外面, 以后也许..
func passiveMergePlan(flushPlan FlushPlan) FlushPlan {
	if len(flushPlan.planList) < 2 {
		return flushPlan
	}

	merged := make([]PartPlan, 0, len(flushPlan.planList))
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

			mergeCt++
		}
		merged = append(merged, cur)
		i = j
	}
	return FlushPlan{planList: merged}
}

func activeMergePlan(list []PartPlan) []PartPlan {
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
