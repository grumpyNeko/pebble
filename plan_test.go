package pebble

import (
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
