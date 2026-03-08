package pebble

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
)

// 这测试重点是?
// plan.WriteTo == 0, p.Outputs.len == 0
// 空Part, 要删除
// 恰好是最后一个Part
func Test_newPartIdxFrom_DeletePartNoGap(t *testing.T) {
	// before:
	// [0..9]            stack=[11]
	// [10..19]          stack=[33]
	// [20..MaxUint64]   stack=[22]
	//
	// planStep1:
	// p0 WriteTo=1 Outputs=[101] -> [11,101]
	// p1 WriteTo=1 Outputs=[102] -> [33,102]
	// p2 WriteTo=0 Outputs=nil   -> delete last part
	//
	// expect:
	// [0..9]            stack=[11,101]
	// [10..MaxUint64]   stack=[33,102]
	pmtinternal.SstMap = map[uint64]pmtinternal.SstInfo{
		11:  {Largest: 9},
		33:  {Largest: 19},
		22:  {Largest: math.MaxUint64},
		101: {Largest: 9},
		102: {Largest: 19},
	}

	got := newPartIdxFrom([]PartPlan{
		{low: 0, High: 9, WriteTo: 1, Stack: []FileNum{11}, Outputs: []uint64{101}},
		{low: 10, High: 19, WriteTo: 1, Stack: []FileNum{33}, Outputs: []uint64{102}},
		{low: 20, High: math.MaxUint64, WriteTo: 0, Stack: []FileNum{22}, Outputs: nil}, // delete
	})

	if len(got) != 2 {
		t.Fatalf("part count=%d, want=2, got=%+v", len(got), got)
	}
	if got[0].Low != 0 || got[0].High != 9 {
		t.Fatalf("part[0]=[%d,%d], want=[0,9]", got[0].Low, got[0].High)
	}
	if got[1].Low != 10 || got[1].High != math.MaxUint64 {
		t.Fatalf("part[1]=[%d,%d], want=[10,%d]", got[1].Low, got[1].High, uint64(math.MaxUint64))
	}
	if len(got[0].Stack) != 2 || got[0].Stack[0] != base.FileNum(11) || got[0].Stack[1] != base.FileNum(101) {
		t.Fatalf("part[0].stack=%v, want=[11 101]", got[0].Stack)
	}
	if len(got[1].Stack) != 2 || got[1].Stack[0] != base.FileNum(33) || got[1].Stack[1] != base.FileNum(102) {
		t.Fatalf("part[1].stack=%v, want=[33 102]", got[1].Stack)
	}
}

func Test_activeMergePlan(t *testing.T) {
	pmtinternal.SstMap = map[uint64]pmtinternal.SstInfo{
		1:  {Size: uint64(1 * PageSize)},
		2:  {Size: uint64(8 * PageSize)},
		3:  {Size: uint64(5 * PageSize)},
		4:  {Size: uint64(4 * PageSize)},
		5:  {Size: uint64(7 * PageSize)},
		6:  {Size: uint64(1 * PageSize)},
		7:  {Size: uint64(9 * PageSize)},
		8:  {Size: uint64(6 * PageSize)},
		9:  {Size: uint64(1 * PageSize)},
		10: {Size: uint64(2 * PageSize)},
		11: {Size: uint64(3 * PageSize)},
		12: {Size: uint64(1 * PageSize)},
		13: {Size: uint64(4 * PageSize)},
		14: {Size: uint64(3 * PageSize)},
		15: {Size: uint64(5 * PageSize)},
		16: {Size: uint64(10 * PageSize)},
		17: {Size: uint64(2 * PageSize)},
	}

	newPartPlan := func(low, high uint64, writeTo uint16, files ...uint64) PartPlan {
		stack := make([]base.FileNum, 0, len(files))
		for _, fn := range files {
			stack = append(stack, base.FileNum(fn))
		}
		return PartPlan{
			High:    high,
			low:     low,
			WriteTo: writeTo,
			Reason:  "test",
			Stack:   stack,
		}
	}
	newFlushPlan := func(step1Total uint64, planList []PartPlan) FlushPlan {
		return FlushPlan{
			totalWriteExpected: step1Total,
			wt0:                collectWt0(planList),
			planList:           planList,
		}
	}

	cases := []struct {
		name      string
		budget    uint64
		flushPlan FlushPlan
		want      struct {
			planLen            int
			activeMergeCount   int
			totalWriteExpected uint64
			firstHigh          uint64
			writeToZeroIdx     []int
			panic              string
		}
	}{
		{
			name:   "single_side_neighbor",
			budget: 101,
			flushPlan: newFlushPlan(100, []PartPlan{
				newPartPlan(0, 9, 1, 1, 2),
				newPartPlan(10, 19, 0, 3),
				newPartPlan(20, math.MaxUint64, 1, 4, 5),
			}),
			want: struct {
				planLen            int
				activeMergeCount   int
				totalWriteExpected uint64
				firstHigh          uint64
				writeToZeroIdx     []int
				panic              string
			}{
				planLen:            3,
				activeMergeCount:   1,
				totalWriteExpected: 101,
				firstHigh:          9,
				writeToZeroIdx:     []int{0, 1},
			},
		},
		{
			name:   "三合一",
			budget: 55,
			flushPlan: newFlushPlan(50, []PartPlan{
				newPartPlan(0, 9, 0, 6),
				newPartPlan(10, 19, 1, 10, 11),
				newPartPlan(20, 29, 0, 9),
				newPartPlan(30, math.MaxUint64, 1, 7, 8),
			}),
			want: struct {
				planLen            int
				activeMergeCount   int
				totalWriteExpected uint64
				firstHigh          uint64
				writeToZeroIdx     []int
				panic              string
			}{
				planLen:            4,
				activeMergeCount:   1,
				totalWriteExpected: 52,
				firstHigh:          9,
				writeToZeroIdx:     []int{0, 1, 2},
			},
		},
		{
			name:   "budget_truncation",
			budget: 201,
			flushPlan: newFlushPlan(200, []PartPlan{
				newPartPlan(0, 9, 1, 12, 13),
				newPartPlan(10, 19, 0, 6),
				newPartPlan(20, 29, 1, 14, 15),
				newPartPlan(30, 39, 0, 9),
				newPartPlan(40, math.MaxUint64, 1, 16, 17),
			}),
			want: struct {
				planLen            int
				activeMergeCount   int
				totalWriteExpected uint64
				firstHigh          uint64
				writeToZeroIdx     []int
				panic              string
			}{
				planLen:            5,
				activeMergeCount:   1,
				totalWriteExpected: 201,
				firstHigh:          9,
				writeToZeroIdx:     []int{0, 1, 3},
			},
		},
		{
			name:   "panic_when_相邻_merge_candidate",
			budget: 1000,
			flushPlan: newFlushPlan(10, []PartPlan{
				newPartPlan(0, 9, 0, 6),
				newPartPlan(10, 19, 0, 9),
				newPartPlan(20, math.MaxUint64, 1, 12, 13),
			}),
			want: struct {
				planLen            int
				activeMergeCount   int
				totalWriteExpected uint64
				firstHigh          uint64
				writeToZeroIdx     []int
				panic              string
			}{
				panic: "chooseTryPushIdx: succ in wt0",
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pmtinternal.WriteThresholdInPages0 = tc.budget
			if tc.want.panic != "" {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected panic %q", tc.want.panic)
					}
					if got := fmt.Sprint(r); got != tc.want.panic {
						t.Fatalf("panic=%q want=%q", got, tc.want.panic)
					}
				}()
				_ = activeMergePlan(tc.flushPlan, tc.budget-tc.flushPlan.totalWriteExpected)
				return
			}

			got := activeMergePlan(tc.flushPlan, tc.budget-tc.flushPlan.totalWriteExpected)
			if got.activeMergeCount != tc.want.activeMergeCount {
				t.Fatalf("activeMergeCount=%d want=%d", got.activeMergeCount, tc.want.activeMergeCount)
			}
			if got.totalWriteExpected != tc.want.totalWriteExpected {
				t.Fatalf("totalWriteExpected=%d want=%d", got.totalWriteExpected, tc.want.totalWriteExpected)
			}
			if len(got.planList) != tc.want.planLen {
				t.Fatalf("len(planList)=%d want=%d", len(got.planList), tc.want.planLen)
			}
			if got.planList[0].High != tc.want.firstHigh {
				t.Fatalf("planList[0].High=%d want=%d", got.planList[0].High, tc.want.firstHigh)
			}
			var gotWriteToZeroIdx []int
			for i, pp := range got.planList {
				if pp.WriteTo == 0 {
					gotWriteToZeroIdx = append(gotWriteToZeroIdx, i)
				}
			}
			if fmt.Sprint(gotWriteToZeroIdx) != fmt.Sprint(tc.want.writeToZeroIdx) {
				t.Fatalf("writeToZeroIdx=%v want=%v", gotWriteToZeroIdx, tc.want.writeToZeroIdx)
			}
		})
	}
}
