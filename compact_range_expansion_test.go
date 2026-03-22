package pebble

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type compactRangeExpansionTarget struct {
	low       uint64
	high      uint64
	partIdx   int
	partDepth int
}

type compactRangeTargets struct {
	first   compactRangeExpansionTarget
	deepest compactRangeExpansionTarget
	last    compactRangeExpansionTarget
}

func Test_compact_range(t *testing.T) {
	dataset := "normal_plus" // normal_plus or uniform
	roundsNs := []int{5, 6, 7, 8}
	lines := make([]string, 0, 32)
	for _, roundsN := range roundsNs {
		rounds := loadRounds(t, dataset, roundsN)
		globalDB := buildGlobalState(t, rounds)
		// 先跑PMT确定范围
		targets := pickTargets(t)
		lines = append(lines, fmt.Sprintf("dataset=%s rounds=%d", dataset, len(rounds)))
		cases := []struct {
			name   string
			target compactRangeExpansionTarget
		}{
			{name: "deepest", target: targets.deepest},
			//{name: "first", target: targets.first},
			//{name: "last", target: targets.last},
		}
		for _, tc := range cases {
			globalBytes, globalSizes := pmtCompactRange(tc.target.low, tc.target.high)
			layered2Bytes, layered2Low, layered2High, layered2Sizes := measureLayered(t, rounds, 2<<20, tc.target.low, tc.target.high)
			layered4Bytes, layered4Low, layered4High, layered4Sizes := measureLayered(t, rounds, 4<<20, tc.target.low, tc.target.high)

			lines = append(lines, fmt.Sprintf("%s partIdx=%d partDepth=%d range=[%d,%d]", tc.name, tc.target.partIdx, tc.target.partDepth, tc.target.low, tc.target.high))
			lines = append(lines, fmt.Sprintf("layered-2MB\tinputMB=%.2f range=[%d,%d] inputs=[%s]", bytesToMB(layered2Bytes), layered2Low, layered2High, formatSizes(layered2Sizes)))
			lines = append(lines, fmt.Sprintf("layered-4MB\tinputMB=%.2f range=[%d,%d] inputs=[%s]", bytesToMB(layered4Bytes), layered4Low, layered4High, formatSizes(layered4Sizes)))
			lines = append(lines, fmt.Sprintf("global-4MB\tinputMB=%.2f range=[%d,%d] inputs=[%s]", bytesToMB(globalBytes), tc.target.low, tc.target.high, formatSizes(globalSizes)))
		}
		lines = append(lines, "")
		if err := globalDB.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeReport(t, dataset, lines)
	for _, line := range lines {
		t.Log(line)
	}
}

func loadRounds(t *testing.T, dataset string, roundsN int) [][]uint64 {
	t.Helper()

	ret := make([][]uint64, 0, roundsN)
	for i := 0; i < roundsN; i++ {
		path := filepath.Join(datasetPath, fmt.Sprintf("%s_round_%03d.bin", dataset, i))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing dataset %s; run Test_gen_data", path)
		}
		ret = append(ret, LoadDataFile(path).Keys)
	}
	return ret
}

func buildGlobalState(t *testing.T, rounds [][]uint64) *DB {
	t.Helper()

	initPMTState()
	memFS := vfs.NewMem()
	db := MustDB("compact-range-expansion-global", false, func(opts *Options) *Options {
		opts.FS = memFS
		opts.DisableAutomaticCompactions = true
		opts.FileFormat = sstable.TableFormatLevelDB
		return setTargetFileSize(opts, 4<<20)
	})
	for i, keys := range rounds {
		flushPlan := plan(keys)
		multilevelFlushConcurrent(db, keys, uint64(i), flushPlan.planList, 1)
		pmtinternal.PartIdx = newPartIdxFrom(flushPlan.planList)
	}
	return db
}

func measureLayered(
	t *testing.T, rounds [][]uint64, targetFileSize int64, low uint64, high uint64,
) (uint64, uint64, uint64, []uint64) {
	t.Helper()

	memFS := vfs.NewMem()
	db := MustDB("compact-range-expansion-layered", false, EnablePebble, func(opts *Options) *Options {
		opts.FS = memFS
		return setTargetFileSize(opts, targetFileSize)
	})
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	writeRounds(db, rounds)
	waitCompactions(t, db)
	inputFiles, inputBytes, finalLow, finalHigh, sizes := layeredInputs(db, low, high)
	use(inputFiles)
	return inputBytes, finalLow, finalHigh, sizes
}

func writeRounds(db *DB, rounds [][]uint64) {
	for i, keys := range rounds {
		batchWrite(db, keys, uint64(i))
	}
}

func waitCompactions(t *testing.T, db *DB) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for isCompacting(db) {
		if time.Now().After(deadline) {
			t.Fatal("waitCompactRangeExpansionCompactions timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func setTargetFileSize(opts *Options, targetFileSize int64) *Options {
	opts.FlushSplitBytes = targetFileSize
	for i := range opts.Levels {
		opts.Levels[i].TargetFileSize = targetFileSize
	}
	return opts
}

func initPMTState() {
	pmtinternal.EnablePMT = true
	pmtinternal.EnablePMTTableFormat = false
	pmtinternal.EnableCollector = true
	pmtinternal.CollectorTriggerPages = 3
	pmtinternal.LogicDel = false
	pmtinternal.SetStep1Method(pmtinternal.PlanStep1V4)
	pmtinternal.PartIdx = []pmtinternal.Part{{
		Low:   0,
		High:  math.MaxUint64,
		Stack: nil,
	}}
	pmtinternal.SstMap = make(map[uint64]pmtinternal.SstInfo, 1024)
}

func collectTargets(t *testing.T) []compactRangeExpansionTarget {
	t.Helper()

	ret := make([]compactRangeExpansionTarget, 0, len(pmtinternal.PartIdx))
	for i, part := range pmtinternal.PartIdx {
		if len(part.Stack) == 0 {
			continue
		}
		ret = append(ret, compactRangeExpansionTarget{
			low:       part.Low,
			high:      part.High,
			partIdx:   i,
			partDepth: len(part.Stack),
		})
	}
	if len(ret) == 0 {
		t.Fatal("collectTargets: no non-empty part")
	}
	return ret
}

func pickTargets(t *testing.T) compactRangeTargets {
	t.Helper()

	targets := collectTargets(t)
	return compactRangeTargets{
		first:   targets[0],
		deepest: deepestTarget(targets),
		last:    targets[len(targets)-1],
	}
}

func deepestTarget(targets []compactRangeExpansionTarget) compactRangeExpansionTarget {
	bestIdx := 0
	for i := 1; i < len(targets); i++ {
		if targets[i].partDepth > targets[bestIdx].partDepth {
			bestIdx = i
		}
	}
	return targets[bestIdx]
}

func bytesToMB(v uint64) float64 {
	return float64(v) / float64(1<<20)
}

// layeredInputs walks every LSM level once.
// Each level contributes the files overlapping the current range, then the range
// moves to the selected files' actual bounds before descending to the next level.
func layeredInputs(db *DB, low uint64, high uint64) (int, uint64, uint64, uint64, []uint64) {
	readState := db.loadReadState()
	defer readState.unref()

	totalFiles := 0
	var totalBytes uint64
	var sizes []uint64
	curLow := low
	curHigh := high
	for level := 0; level < numLevels; level++ {
		files := readState.current.Overlaps(level, base.UserKeyBoundsInclusive(BigEndian(curLow), BigEndian(curHigh)))
		if files.Empty() {
			continue
		}
		fileCount, bytes, nextLow, nextHigh, pickedSizes := sumLevelFiles(files)
		totalFiles += fileCount
		totalBytes += bytes
		sizes = append(sizes, pickedSizes...)
		curLow = nextLow
		curHigh = nextHigh
	}
	return totalFiles, totalBytes, curLow, curHigh, sizes
}

func sumLevelFiles(files manifest.LevelSlice) (int, uint64, uint64, uint64, []uint64) {
	iter := files.Iter()
	first := iter.First()
	if first == nil {
		panic("sumLevelFiles: empty level slice")
	}
	totalFiles := 0
	var totalBytes uint64
	sizes := make([]uint64, 0, 8)
	low := binary.BigEndian.Uint64(first.SmallestPointKey.UserKey)
	high := binary.BigEndian.Uint64(first.LargestPointKey.UserKey)
	for file := first; file != nil; file = iter.Next() {
		totalFiles++
		totalBytes += file.Size
		sizes = append(sizes, file.Size)
		fileLow := binary.BigEndian.Uint64(file.SmallestPointKey.UserKey)
		fileHigh := binary.BigEndian.Uint64(file.LargestPointKey.UserKey)
		if fileLow < low {
			low = fileLow
		}
		if fileHigh > high {
			high = fileHigh
		}
	}
	return totalFiles, totalBytes, low, high, sizes
}

func formatSizes(sizes []uint64) string {
	if len(sizes) == 0 {
		return ""
	}
	ret := fmt.Sprintf("%.2f", bytesToMB(sizes[0]))
	for i := 1; i < len(sizes); i++ {
		ret += fmt.Sprintf(",%.2f", bytesToMB(sizes[i]))
	}
	return ret
}

func writeReport(t *testing.T, dataset string, lines []string) {
	t.Helper()

	reportPath := filepath.Join("ww", fmt.Sprintf("compact_range_expansion_%s.txt", dataset))
	if err := os.MkdirAll(filepath.Dir(reportPath), 0755); err != nil {
		t.Fatal(err)
	}
	data := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(reportPath, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}
