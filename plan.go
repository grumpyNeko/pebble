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

type PlanStat struct {
	PartLow  uint64
	PartHigh uint64
	NewPages int
	WriteTo  uint16
	Stack    []FileNum
	Reason   string
}

var planHistory [][]PlanStat

func plan(newKeys []uint64) []SubPart {
	ret := make([]SubPart, 0, 256)
	onePlan := make([]PlanStat, 0, len(pmtinternal.PartIdx))
	for _, p := range pmtinternal.PartIdx {
		sp := SubPart{
			High:    p.High,
			low:     p.Low,
			WriteTo: uint16(len(p.Stack)),
			Stack:   p.Stack,
			Outputs: nil,
		}

		// old plan (kept as requested):
		// if len(sp.Stack) > 6 {
		// 	println("len(sp.Stack) > 6")
		// 	sp.WriteTo = 0
		// }

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
				panic(fmt.Sprintf("plan: file %d not found in SstMap", sp.Stack[i]))
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
		onePlan = append(onePlan, PlanStat{
			PartLow:  sp.low,
			PartHigh: sp.High,
			NewPages: newPages,
			WriteTo:  sp.WriteTo,
			Stack:    append([]FileNum(nil), sp.Stack...),
			Reason:   reason,
		})
		ret = append(ret, sp)
	}
	planHistory = append(planHistory, onePlan)
	return ret
}

func mergePlan(list []SubPart) []SubPart {
	if len(list) == 0 {
		return list
	}
	merged := make([]SubPart, 0, len(list))
	for i := 0; i < len(list); i++ {
		cur := list[i]
		if cur.WriteTo != 0 {
			merged = append(merged, cur)
			continue
		}
		seen := make(map[FileNum]struct{}, len(cur.Stack))
		for _, f := range cur.Stack {
			seen[f] = struct{}{}
		}
		for i+1 < len(list) && list[i+1].WriteTo == 0 {
			next := list[i+1]
			if cur.High == math.MaxUint64 {
				panic("mergePlan: found next subpart after MaxUint64")
			}
			if next.low != cur.High+1 {
				panic(fmt.Sprintf("mergePlan: non-adjacent range [%d,%d] then [%d,%d]", cur.low, cur.High, next.low, next.High))
			}
			cur.High = next.High
			for _, f := range next.Stack {
				if _, ok := seen[f]; ok {
					continue
				}
				seen[f] = struct{}{}
				cur.Stack = append(cur.Stack, f)
			}
			i++
		}
		merged = append(merged, cur)
	}
	return merged
}

func dumpPlanHistory(startFrom int, path string) {
	if startFrom < 0 {
		panic("startFrom < 0")
	}
	if path == "" {
		panic("path is empty")
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(err)
	}
	if startFrom > len(planHistory) {
		panic("startFrom > len(planHistory)")
	}
	for flushID := startFrom; flushID < len(planHistory); flushID++ {
		f, err := os.Create(filepath.Join(path, fmt.Sprintf("flush_%06d.plan", flushID)))
		if err != nil {
			panic(err)
		}
		for _, e := range planHistory[flushID] {
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
func formatPlanStack(stack []FileNum) string {
	if len(stack) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, f := range stack {
		if i > 0 {
			b.WriteString(",")
		}
		info, ok := pmtinternal.SstMap[uint64(f)]
		if !ok {
			panic(fmt.Sprintf("formatPlanStack: file %d not found in SstMap", f))
		}
		b.WriteString(fmt.Sprintf("%d", info.Size/uint64(PageSize)))
	}
	b.WriteString("]")
	return b.String()
}

type SubPart struct {
	High    uint64
	low     uint64 // what use?
	WriteTo uint16 // 0 means write base, n means merge Stack[n,]
	Stack   []FileNum
	Outputs []uint64
}

func newPartIdxFrom(spList []SubPart) []pmtinternal.Part {
	var parts []pmtinternal.Part
	for i, e := range pmtinternal.PartIdx {
		// Sentinel part has no corresponding SubPart entry.
		if i >= len(spList) {
			parts = append(parts, e)
			continue
		}
		sp := spList[i]
		outs := append([]uint64(nil), sp.Outputs...)
		sort.Slice(outs, func(i, j int) bool {
			return pmtinternal.SstMap[outs[i]].Largest < pmtinternal.SstMap[outs[j]].Largest
		})
		prefix := e.Stack[:int(sp.WriteTo)]
		if len(outs) == 0 {
			parts = append(parts, e)
			continue
		}
		if len(prefix) == 0 { // split part by outputs
			currentLow := e.Low
			for _, fn := range outs {
				info := pmtinternal.SstMap[fn]
				parts = append(parts, pmtinternal.Part{
					Low:   currentLow,
					High:  info.Largest,
					Stack: []base.FileNum{base.FileNum(fn)},
					Tmp:   nil,
				})
				currentLow = info.Largest + 1
			}
			continue
		}
		// append outs to prefix stack
		newStack := append([]base.FileNum{}, prefix...)
		for _, fn := range outs {
			newStack = append(newStack, base.FileNum(fn))
		}
		parts = append(parts, pmtinternal.Part{
			Low:   e.Low,
			High:  e.High,
			Stack: newStack,
			Tmp:   nil,
		})
	}
	n := len(parts)
	if n == 0 {
		panic("why len(newPartIdxFrom) == 0")
	}
	if parts[n-1].High != math.MaxUint64 {
		parts = append(parts, pmtinternal.Part{Low: parts[n-1].High + 1, High: math.MaxUint64})
	}
	return parts
}
