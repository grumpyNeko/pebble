package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
	"github.com/cockroachdb/pebble/vfs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeBarrierFS blocks the first write to each sstable until N writers arrive.
// This is a focused way to prove concurrent "write to disk" overlap.
type writeBarrierFS struct {
	vfs.FS
	b *writeBarrier
}

type writeBarrier struct {
	target int32
	count  int32
	ch     chan struct{}
	once   sync.Once
}

func newWriteBarrier(target int32) *writeBarrier {
	return &writeBarrier{
		target: target,
		ch:     make(chan struct{}),
	}
}

func (b *writeBarrier) arriveAndWait() {
	if atomic.AddInt32(&b.count, 1) == b.target {
		b.once.Do(func() { close(b.ch) })
	}
	<-b.ch
}

type barrierFile struct {
	vfs.File
	b    *writeBarrier
	once sync.Once
}

func (f *barrierFile) waitOnce() {
	f.once.Do(func() { f.b.arriveAndWait() })
}

func (fs *writeBarrierFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, ".sst") {
		return &barrierFile{File: f, b: fs.b}, nil
	}
	return f, nil
}

func (fs *writeBarrierFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(newname, ".sst") {
		return &barrierFile{File: f, b: fs.b}, nil
	}
	return f, nil
}

func (f *barrierFile) Write(p []byte) (int, error) {
	f.waitOnce()
	return f.File.Write(p)
}

func (f *barrierFile) WriteAt(p []byte, ofs int64) (int, error) {
	f.waitOnce()
	return f.File.WriteAt(p, ofs)
}

/*
老代码暂时保留
batchWrite(db, []uint64{2, 10}, 0)
batchWrite(db, []uint64{15, 99}, 0)
batchWrite(db, []uint64{1, 20}, 0)
println(db.LSMViewURL())
db.manualCompact(BigEndian(2), BigEndian(10), 0, false)
println(db.LSMViewURL())
*/
func Test_pmt_basic(t *testing.T) {
	db := MustDB("tmp_db/pmt_basic", true, func(options *Options) *Options {
		pmtinternal.EnablePMTTableFormat = true
		options.FileFormat = sstable.TableFormatPMT0
		return options
	})
	defer db.Close()

	path := filepath.Join("pmttestdata", "normal_plus_round_000.bin")
	d := LoadDataFile(path)
	keys := d.Keys

	flushPlan := planStep1(keys)
	multilevelFlushConcurrent(db, keys, uint64(17), flushPlan.planList, 2)
	pmtinternal.PartIdx = newPartIdxFrom(flushPlan.planList)

	metrics := db.Metrics()
	if len(pmtinternal.PartIdx) == 0 {
		t.Fatalf("PartIdx is empty after multilevel flush")
	}
	use(metrics)
	use(pmtinternal.PartIdx)

	for _, k := range keys {
		val, _ := db.MustGet(BigEndian(k))
		if len(val) != 8 {
			t.Fatalf("MustGet(%d) value len=%d", k, len(val))
		}
		if got := binary.BigEndian.Uint64(val); got != 17 {
			t.Fatalf("MustGet(%d) value=%d want=17", k, got)
		}
	}

	iter, err := db.NewIter(&IterOptions{})
	if err != nil {
		t.Fatalf("NewIter failed: %v", err)
	}
	defer func() {
		if cerr := iter.Close(); cerr != nil {
			t.Fatalf("iter close failed: %v", cerr)
		}
	}()

	var (
		prev    uint64
		hasPrev bool
		seen    int
	)
	for iter.First(); iter.Valid(); iter.Next() {
		k := binary.BigEndian.Uint64(iter.Key())
		if hasPrev && k <= prev {
			t.Fatalf("range read not strictly increasing: prev=%d current=%d", prev, k)
		}
		if _, ok := d.M[k]; !ok {
			t.Fatalf("range read key=%d not in source dataset", k)
		}
		prev = k
		hasPrev = true
		seen++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if seen != len(d.M) {
		t.Fatalf("range read count mismatch: seen=%d want=%d", seen, len(d.M))
	}
}

func Test_PMTGet(t *testing.T) {
	db := MustDB("tmp_db/pmt_get", true, func(options *Options) *Options {
		pmtinternal.EnablePMTTableFormat = true
		options.FileFormat = sstable.TableFormatPMT0
		options.DisableAutomaticCompactions = true
		return options
	})
	defer db.Close()

	// Two flushes for the same key. PMTGet should probe newest table first.
	batchWrite(db, []uint64{2}, 1)
	batchWrite(db, []uint64{2}, 9)

	v, found, tableCt := db.PMTGet(2)
	if !found {
		t.Fatalf("PMTGet(2) not found")
	}
	if v != 9 {
		t.Fatalf("PMTGet(2) expected 9, got %d", v)
	}
	if tableCt < 1 {
		t.Fatalf("PMTGet(2) expected tableCt >= 1, got %d", tableCt)
	}

	v, found, _ = db.PMTGet(999)
	if found {
		t.Fatalf("PMTGet(999) expected not found, got value=%d", v)
	}
}

/*
用pmtformat.NewWriter写
用pmtformat.NewIter直接读
*/
func Test_PMT_NewIters_Point(t *testing.T) {
	var buf bytes.Buffer
	w := pmtformat.NewWriter(&buf)
	keys := []uint64{1, 3, 7, 8, 21, 34}
	for _, k := range keys {
		if err := w.Add(k, k); err != nil {
			t.Fatalf("Add(%d) failed: %v", k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close failed: %v", err)
	}

	data := buf.Bytes()
	it, err := pmtformat.NewIter(pmtTestReadable{r: bytes.NewReader(data)}, int64(len(data)), base.SeqNum(1))
	if err != nil {
		t.Fatalf("new PMT iter failed: %v", err)
	}

	tests := []struct {
		seek  uint64
		want  uint64
		found bool
	}{
		{seek: 0, want: 1, found: true},
		{seek: 1, want: 1, found: true},
		{seek: 2, want: 3, found: true},
		{seek: 7, want: 7, found: true},
		{seek: 9, want: 21, found: true},
		{seek: 34, want: 34, found: true},
		{seek: 35, found: false},
	}
	for _, tc := range tests {
		kv := it.SeekGE(BigEndian(tc.seek), base.SeekGEFlagsNone)
		if !tc.found {
			if kv != nil {
				got := binary.BigEndian.Uint64(kv.K.UserKey)
				t.Fatalf("SeekGE(%d) got key=%d, want nil", tc.seek, got)
			}
			continue
		}
		if kv == nil {
			t.Fatalf("SeekGE(%d) returned nil", tc.seek)
		}
		gotKey := binary.BigEndian.Uint64(kv.K.UserKey)
		if gotKey != tc.want {
			t.Fatalf("SeekGE(%d) got key=%d want=%d", tc.seek, gotKey, tc.want)
		}
		val, _, err := kv.Value(nil)
		if err != nil {
			t.Fatalf("SeekGE(%d) value error: %v", tc.seek, err)
		}
		gotVal := binary.BigEndian.Uint64(val)
		if gotVal != tc.want {
			t.Fatalf("SeekGE(%d) got value=%d want=%d", tc.seek, gotVal, tc.want)
		}
	}
	if err := it.Close(); err != nil {
		t.Fatalf("point iterator close failed: %v", err)
	}
}

type pmtTestReadable struct {
	r *bytes.Reader
}

func (tr pmtTestReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	_, err := tr.r.ReadAt(p, off)
	return err
}

func (pmtTestReadable) Close() error {
	return nil
}

func Test_PMT_CompactAndWrite(t *testing.T) {
	// todo: ..
}

func Test_split(t *testing.T) {
	//db := PMT()
	db := MustDB("test-db", true)

	var metrics *Metrics
	// 手动增加划分点100
	batchWrite(db, []uint64{1, 90, 100, 200, 250}, 0)
	metrics = stat(db)
	println(db.LSMViewURL())
	use(metrics)
}

func Test_sst_meta(t *testing.T) {
	db := MustDB("test-db", true)
	batchWrite(db, []uint64{1, 20}, 0)
	batchWrite(db, []uint64{1, 20}, 0)
	batchWrite(db, []uint64{1, 20}, 0)
	db.MyCompact(BigEndian(1), BigEndian(20), 0)
	batchWrite(db, []uint64{1, 20}, 0)
	println(db.LSMViewURL())
	v := db.DebugCurrentVersion()
	for i := 0; i < 7; i++ {
		if v.Levels[i].Len() == 0 {
			continue
		}
		it := v.Levels[i].Iter()
		for m := it.First(); m != nil; m = it.Next() {
			println(fmt.Sprintf("L%d %+v", i, m))
		}
	}
	// L0 000008
	// L6 000007
}

func Test_MyGet(t *testing.T) {
	db := MustDB("test-db", true, EnablePebble, func(options *Options) *Options {
		options.DisableAutomaticCompactions = true
		return options
	})
	batchWrite(db, []uint64{1, 100}, uint64(1))
	batchWrite(db, []uint64{0, 101}, uint64(2))
	var tables int
	_, tables = db.MustGet(BigEndian(0))
	_, tables = db.MustGet(BigEndian(101))
	_, tables = db.MustGet(BigEndian(1))
	_, tables = db.MustGet(BigEndian(100))
	use(tables)
}

// L0CompactionThreshold = 3, Normal+Uniform+MinMax, deviation=1 << 30
// 128, 5.65 | 5.41,avg:3.829685 | 5.92

// L0CompactionThreshold = 3, Normal+Uniform+MinMax, deviation=1 << 31
// 32, 3.96,
// 64,
// 96,
// 128, 5.66, avg:3.827046 |

// L0CompactionThreshold = 3, Normal+Uniform+MinMax, deviation=1 << 32
// 32, 3.99
// 64, 4.53
// 96, 5.05
// 128, 5.45, avg:3.827048

// L0CompactionThreshold = 3, Uniform
// 32, 5.5, avg=3.624498
// 64, 6.6, avg:3.756997
// 96, 7.44, avg:3.801390
// 128, 7.72, avg:3.84,

// normal_plus
// 64, 耗时: 91759+77551=169310=>396.37 Kops
// 128,
func Test_pebble_wa(t *testing.T) {
	db := MustDB("mybench_pmt", true, EnablePebble, func(options *Options) *Options {
		options.FS = vfs.Default
		options.DisableAutomaticCompactions = false

		options.FileFormat = sstable.TableFormatLevelDB
		pagesize := 4 << 10                             // 4KB
		options.CacheSize = int64(1024 * 16 * pagesize) //
		options.MaxConcurrentCompactions = func() int { return 8 }
		return options
	})

	times := 128 // 48
	datas := make([]uint64, 0, times<<20)
	for i := 0; i < times; i++ {
		path := filepath.Join("pmttestdata", fmt.Sprintf("normal_plus_round_%03d.bin", i))
		d := LoadDataFile(path)
		datas = append(datas, d.Keys...)
	}

	writeStart := time.Now()
	for i := 0; i < times; i++ {
		keys := datas[i<<20 : (i+1)<<20]
		batchWrite(db, keys, uint64(i))
		println(fmt.Sprintf("done %d", i))
	}
	println(fmt.Sprintf("写入阶段耗时(ms): %d", time.Since(writeStart).Milliseconds()))

	compactWaitStart := time.Now()
	for isCompacting(db) {
		println("wait for compact")
		time.Sleep(500 * time.Millisecond)
	}
	println(fmt.Sprintf("等待压实耗时(ms): %d", time.Since(compactWaitStart).Milliseconds()))

	benchmarkRandomReadMultiThread(datas, db, 1)
	benchmarkRandomReadMultiThread(datas, db, 4)
	benchmarkRandomReadMultiThread(datas, db, 8)
	benchmarkRandomReadMultiThread(datas, db, 12)
	benchmarkRandomReadMultiThread(datas, db, 16)
	benchmarkRandomReadMultiThread(datas, db, 24)
	benchmarkRandomReadMultiThread(datas, db, 32)
	benchmarkRandomReadMultiThread(datas, db, 48)
	benchmarkRandomReadMultiThread(datas, db, 64)

	metrics := stat(db)
	use(metrics)
	use(pmtinternal.PartIdx)
	println(fmt.Sprintf("total bytes in: %d", metrics.Total().BytesIn))

	// 512page
	//start random read benchmark, concurrency=1
	//random read cost 110631ms
	//start random read benchmark, concurrency=4
	//random read cost 34848ms
	//start random read benchmark, concurrency=8
	//random read cost 25173ms
	//start random read benchmark, concurrency=12
	//random read cost 20969ms
	//start random read benchmark, concurrency=16
	//random read cost 15841ms
	//start random read benchmark, concurrency=24
	//random read cost 17974ms
	//start random read benchmark, concurrency=32
	//random read cost 19796ms
	//start random read benchmark, concurrency=48
	//random read cost 17226ms
	//start random read benchmark, concurrency=64
	//random read cost 17620ms

	// 4096page
	//start random read benchmark, concurrency=1
	//random read cost 170569ms
	//start random read benchmark, concurrency=4
	//random read cost 39710ms
	//start random read benchmark, concurrency=8
	//random read cost 25718ms
	//start random read benchmark, concurrency=12
	//random read cost 21013ms
	//start random read benchmark, concurrency=16
	//random read cost 18012ms
	//start random read benchmark, concurrency=24
	//random read cost 17086ms
	//start random read benchmark, concurrency=32
	//random read cost 16808ms
	//start random read benchmark, concurrency=48
	//random read cost 16638ms
	//start random read benchmark, concurrency=64
	//random read cost 17269ms

	// 4096page * 16 * 16
	//start random read benchmark, concurrency=1
	//random read cost 79698ms
	//start random read benchmark, concurrency=4
	//random read cost 21925ms
	//start random read benchmark, concurrency=8
	//random read cost 13801ms
	//start random read benchmark, concurrency=12
	//random read cost 10643ms
	//start random read benchmark, concurrency=16
	//random read cost 9957ms
	//start random read benchmark, concurrency=24
	//random read cost 9962ms
	//start random read benchmark, concurrency=32
	//random read cost 9098ms
	//start random read benchmark, concurrency=48
	//random read cost 9922ms
	//start random read benchmark, concurrency=64
	//random read cost 9062ms
}

func Test_pebble_r(t *testing.T) {
	// 打开已经生成的数据
	db := MustDB("test-db", false, EnablePebble, func(options *Options) *Options {
		options.FS = vfs.Default
		options.DisableAutomaticCompactions = false

		options.FileFormat = sstable.TableFormatLevelDB
		pagesize := 4 << 10 // 4KB
		options.CacheSize = int64(4096 * 16 * 16 * pagesize)
		options.MaxConcurrentCompactions = func() int { return 8 }

		options.ReadOnly = true // important
		return options
	})

	times := 128 // 48
	datas := make([]uint64, 0, times<<20)
	for i := 0; i < times; i++ {
		path := filepath.Join("pmttestdata", fmt.Sprintf("normal_plus_round_%03d.bin", i))
		d := LoadDataFile(path)
		datas = append(datas, d.Keys...)
	}

	//avg(db)
	benchmarkRandomReadMultiThread(datas, db, 1)
	benchmarkRandomReadMultiThread(datas, db, 4)
	benchmarkRandomReadMultiThread(datas, db, 8)
	benchmarkRandomReadMultiThread(datas, db, 12)
	benchmarkRandomReadMultiThread(datas, db, 16)
	benchmarkRandomReadMultiThread(datas, db, 24)
	benchmarkRandomReadMultiThread(datas, db, 32)
	benchmarkRandomReadMultiThread(datas, db, 48)
	benchmarkRandomReadMultiThread(datas, db, 64)
}

// normal_plus
// 64, 写耗时: 84448,81142,97571=>687.79 Kops; 点读耗时=
// 128, 写耗时: 195638,269629,312484,276104,225041=>596.42 Kops
func Test_pmt_wa(t *testing.T) {
	println(fmt.Sprintf("GOMAXPROCS=%d", runtime.GOMAXPROCS(0)))
	db := MustDB("mybench_pmt", true, func(options *Options) *Options {
		options.FS = vfs.Default

		pmtinternal.EnablePMTTableFormat = true
		options.FileFormat = sstable.TableFormatPMT0
		//options.FileFormat = sstable.TableFormatPebblev6

		pmtinternal.SetStep1Method(pmtinternal.PlanStep1Simple)

		pagesize := 4 << 10                             // 4KB
		options.CacheSize = int64(1024 * 16 * pagesize) //
		options.DisableAutomaticCompactions = true
		options.MaxConcurrentCompactions = func() int { return 8 }
		return options
	})

	const flushConcurrency = 4
	times := 128 // 128
	datas := make([]uint64, 0, times<<20)
	for i := 0; i < times; i++ {
		path := filepath.Join("pmttestdata", fmt.Sprintf("normal_plus_round_%03d.bin", i))
		d := LoadDataFile(path)
		datas = append(datas, d.Keys...)
	}
	writeStart := time.Now()
	for i := 0; i < times; i++ {
		keys := datas[i<<20 : (i+1)<<20]
		flushPlan := planStep1(keys)
		flushPlan = passiveMergePlan(flushPlan) // current testing

		//for j, sp := range pList {
		//	mem := fakeMemTable{
		//		keys: rangeLimit(d.Keys, sp.low, sp.High),
		//		v:    uint64(i),
		//	}
		//	pList[j].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], int(manifest.NumLevels-1-sp.WriteTo))
		//}
		multilevelFlushConcurrent(db, keys, uint64(i), flushPlan.planList, flushConcurrency)
		pmtinternal.PartIdx = newPartIdxFrom(flushPlan.planList) // TODO: 不是通过CompactionEnd/FlushEnd更新PartIdx
		println(fmt.Sprintf("done %d", i))
	}
	println(fmt.Sprintf("写入阶段耗时(ms): %d", time.Since(writeStart).Milliseconds()))

	dumpFlushHistory(126, "w_flushhistory")

	metrics := stat(db)
	use(metrics)
	printPartStat()
	println(mergeCt)
	printTotalWriteExpectedList()
	// ------------------------------
	// disk 128pagescache nocompression 128round, 0.129ms
	// disk 512pagescache nocompression 128round, 0.112ms
	// disk 256pagescache nocompression 32round, 0.0297
	// disk 128pagescache nocompression 32round, 0.0613

	// pmt 256pagescache nocompression 64round, 0.037516

	// pmt 256pagescache nocompression 128round, 47.1us
	//benchmarkRandomRead(datas, db)

	benchmarkRandomReadMultiThread(datas, db, 1)
	benchmarkRandomReadMultiThread(datas, db, 4)
	benchmarkRandomReadMultiThread(datas, db, 8)
	benchmarkRandomReadMultiThread(datas, db, 12)
	benchmarkRandomReadMultiThread(datas, db, 16)
	benchmarkRandomReadMultiThread(datas, db, 24)
	benchmarkRandomReadMultiThread(datas, db, 32)
	benchmarkRandomReadMultiThread(datas, db, 48)
	benchmarkRandomReadMultiThread(datas, db, 64)

	//TableFormatPebblev6-----------------------------------, avg filesAccessed 6.78
	//start random read benchmark, concurrency=1
	//random read cost 387120ms
	//start random read benchmark, concurrency=4
	//random read cost 128012ms
	//start random read benchmark, concurrency=8
	//random read cost 81403ms
	//start random read benchmark, concurrency=12
	//random read cost 62984ms
	//start random read benchmark, concurrency=16
	//random read cost 58804ms
	//start random read benchmark, concurrency=24
	//random read cost 58741ms
	//start random read benchmark, concurrency=32
	//random read cost 58445ms
	//start random read benchmark, concurrency=48
	//random read cost 57948ms
	//start random read benchmark, concurrency=64
	//random read cost 59463ms

	//TableFormatPMT-----------------------------------512page, avg filesAccessed 6.78
	//start random read benchmark, concurrency=1
	//random read cost 271713ms
	//start random read benchmark, concurrency=4
	//random read cost 95436ms
	//start random read benchmark, concurrency=8
	//random read cost 63884ms
	//start random read benchmark, concurrency=12
	//random read cost 51712ms
	//start random read benchmark, concurrency=16
	//random read cost 47035ms
	//start random read benchmark, concurrency=24
	//random read cost 44765ms
	//start random read benchmark, concurrency=32
	//random read cost 44406ms
	//start random read benchmark, concurrency=48
	//random read cost 44782ms
	//start random read benchmark, concurrency=64
	//random read cost 45203ms
}

func printPartStat() {
	const (
		smallFileThreshold = uint64(4 * PageSize)
		smallPartThreshold = uint64(512 * PageSize)
		largePartThreshold = uint64(512 * 3 * PageSize)
	)

	partCount := len(pmtinternal.PartIdx)
	smallPartCount := 0
	largePartCount := 0
	fileCount := 0
	smallFileCount := 0

	for _, part := range pmtinternal.PartIdx {
		var partSize uint64
		for _, fileNum := range part.Stack {
			info, ok := pmtinternal.SstMap[uint64(fileNum)]
			if !ok {
				panic(fmt.Sprintf("printPartStat: file %d not found in SstMap", fileNum))
			}
			partSize += info.Size
			fileCount++
			if info.Size < smallFileThreshold {
				smallFileCount++
			}
		}

		if partSize < smallPartThreshold {
			smallPartCount++
		}
		if partSize > largePartThreshold {
			largePartCount++
		}
	}

	println(fmt.Sprintf("partCount=%d, smallPartCount=%d, largePartCount=%d, fileCount=%d, smallFileCount=%d", partCount, smallPartCount, largePartCount, fileCount, smallFileCount))
}

// normal_plus, 128,
func Test_pmt_r(t *testing.T) {
	println(fmt.Sprintf("GOMAXPROCS=%d", runtime.GOMAXPROCS(0)))
	db := MustDB("mybench_pmt", false, func(options *Options) *Options {
		options.FS = vfs.Default

		pmtinternal.EnablePMTTableFormat = true
		options.FileFormat = sstable.TableFormatPMT0
		//options.FileFormat = sstable.TableFormatPebblev6

		pagesize := 4 << 10                                  // 4KB
		options.CacheSize = int64(4096 * 16 * 16 * pagesize) //
		options.DisableAutomaticCompactions = true
		options.MaxConcurrentCompactions = func() int { return 8 }
		options.ReadOnly = true // important
		return options
	})

	times := 8 // 128
	datas := make([]uint64, 0, times<<20)
	for i := 0; i < times; i++ {
		path := filepath.Join("pmttestdata", fmt.Sprintf("normal_plus_round_%03d.bin", i))
		d := LoadDataFile(path)
		datas = append(datas, d.Keys...)
	}

	benchmarkRandomReadMultiThread(datas, db, 1)
	benchmarkRandomReadMultiThread(datas, db, 4)
	benchmarkRandomReadMultiThread(datas, db, 8)
	benchmarkRandomReadMultiThread(datas, db, 12)
	benchmarkRandomReadMultiThread(datas, db, 16)
	benchmarkRandomReadMultiThread(datas, db, 24)
	benchmarkRandomReadMultiThread(datas, db, 32)
	benchmarkRandomReadMultiThread(datas, db, 48)
	benchmarkRandomReadMultiThread(datas, db, 64)
}

// 可能需要用全局变量判断, 目前只用writeBarrierFS
// 如果写盘不并发，Test_concurrent会卡住
func Test_concurrent(t *testing.T) {
	println(fmt.Sprintf("GOMAXPROCS=%d", runtime.GOMAXPROCS(0)))
	barrier := newWriteBarrier(2)
	db := MustDB("test-db", true, func(options *Options) *Options {
		// Block on first sstable write to verify concurrent "write to disk".
		options.FS = &writeBarrierFS{FS: vfs.Default, b: barrier}
		pagesize := 4 << 10                       // 4KB
		options.CacheSize = int64(512 * pagesize) //
		options.DisableAutomaticCompactions = true
		options.MaxConcurrentCompactions = func() int { return 8 }
		return options
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		mem := fakeMemTable{
			keys: []uint64{1, 2, 3},
			v:    1,
		}
		multilevelFlushWithResult(
			db,
			mem,
			nil,
			0,
		)
	}()
	go func() {
		defer wg.Done()
		<-start
		mem := fakeMemTable{
			keys: []uint64{100, 101, 102},
			v:    2,
		}
		multilevelFlushWithResult(
			db,
			mem,
			nil,
			0,
		)
	}()
	close(start) // 同时起跑
	wg.Wait()
}

func benchmarkRandomRead(datas []uint64, db *DB) {
	rand.Shuffle(len(datas), func(i, j int) {
		datas[i], datas[j] = datas[j], datas[i]
	})
	println("start random read benchmark")
	start := time.Now()
	for i := 0; i < 1<<20; i++ {
		db.MustGet(BigEndian(datas[i]))
	}
	println(fmt.Sprintf("random read cost %dms", time.Since(start).Milliseconds()))
}

func benchmarkRandomReadMultiThread(datas []uint64, db *DB, concurrency int) {
	if concurrency < 1 {
		panic("concurrency < 1")
	}
	startMetrics := db.Metrics()
	startBlockHits := startMetrics.BlockCache.Hits
	startBlockMisses := startMetrics.BlockCache.Misses

	readN := 1 << 20
	rand.Shuffle(len(datas), func(i, j int) {
		datas[i], datas[j] = datas[j], datas[i]
	})
	println(fmt.Sprintf("start random read benchmark, concurrency=%d", concurrency))
	start := time.Now()
	var wg sync.WaitGroup
	var totalFilesAccessed atomic.Int64
	var totalReads atomic.Int64
	chunk := (readN + concurrency - 1) / concurrency
	for g := 0; g < concurrency; g++ {
		begin := g * chunk
		end := begin + chunk
		if end > readN {
			end = readN
		}
		if begin >= end {
			continue
		}
		keys := datas[begin:end]
		wg.Add(1)
		go func(keys []uint64) {
			defer wg.Done()
			var localFilesAccessed int64
			for _, k := range keys {
				_, filesAccessed := db.MustGet(BigEndian(k))
				localFilesAccessed += int64(filesAccessed)
			}
			totalFilesAccessed.Add(localFilesAccessed)
			totalReads.Add(int64(len(keys)))
		}(keys)
	}
	wg.Wait()
	println(fmt.Sprintf("random read cost %dms", time.Since(start).Milliseconds()))
	avgFilesAccessed := float64(totalFilesAccessed.Load()) / float64(totalReads.Load())
	println(fmt.Sprintf("avg filesAccessed %.4f", avgFilesAccessed))

	endMetrics := db.Metrics()
	deltaBlockHits := endMetrics.BlockCache.Hits - startBlockHits
	deltaBlockMisses := endMetrics.BlockCache.Misses - startBlockMisses
	println(fmt.Sprintf(
		"blockcache during benchmark: hits=%d misses=%d hitRate=%.2f%%",
		deltaBlockHits, deltaBlockMisses, hitRate(deltaBlockHits, deltaBlockMisses),
	))
}

func benchmarkRandomReadWithCPUProfile(t *testing.T, datas []uint64, db *DB, profPath string) {
	t.Helper()
	_ = os.Remove(profPath)
	f, err := os.Create(profPath)
	if err != nil {
		t.Fatalf("create pprof file: %v", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		t.Fatalf("start cpu profile: %v", err)
	}
	defer func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}()
	benchmarkRandomRead(datas, db)
	t.Logf("pprof cpu profile written to %s", profPath)
}

func Test_gen_data(t *testing.T) {
	const times = 128
	deviation := uint64(1 << 32)
	dir := "pmttestdata"
	for i := 0; i < times; i++ {
		d := NewData()
		mean := 1024*math.MaxUint32 + uint64(i)*(math.MaxUint32)
		d = d.AddNormal(mean, deviation, 1<<20, MinKey+1, MaxKey-1)
		d = d.AddUniform(MinKey+1, MaxKey-1, 1024)
		d = d.AddMinMax()
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		path := filepath.Join(dir, fmt.Sprintf("normal_plus_round_%03d.bin", i))
		SaveDataFile(path, d)
	}
	for i := 0; i < times; i++ {
		d := NewData()
		d = d.AddUniform(MinKey, MaxKey, 1<<20)
		d = d.AddMinMax()
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		path := filepath.Join(dir, fmt.Sprintf("uniform_round_%03d.bin", i))
		SaveDataFile(path, d)
	}
}

func multilevelFlushConcurrent(db *DB, keys []uint64, v uint64, spList []PartPlan, concurrency int) {
	if len(spList) == 0 {
		return
	}
	if concurrency < 1 {
		panic(`concurrency < 1`)
	}
	if len(spList) == 1 {
		concurrency = 1 // TODO: 这么做不优雅...
	}
	if concurrency > len(spList) {
		panic(`concurrency > len(spList)`)
	}
	var wg sync.WaitGroup
	size := (len(spList) + concurrency - 1) / concurrency
	for g := 0; g < concurrency; g++ {
		start := g * size
		end := start + size
		if end > len(spList) {
			end = len(spList)
		}
		if start >= end {
			continue
		}
		list := spList[start:end]
		wg.Add(1)
		go func(list []PartPlan) {
			defer wg.Done()
			for i := range list {
				sp := list[i]
				mem := fakeMemTable{
					keys: rangeLimit(keys, sp.low, sp.High),
					v:    v,
				}
				list[i].Outputs = multilevelFlushWithResult(
					db,
					mem,
					sp.Stack[sp.WriteTo:],
					outputLevelForWriteTo(sp.WriteTo),
				)
			}
		}(list)
	}
	wg.Wait()
}

func outputLevelForWriteTo(writeTo uint16) int {
	if writeTo >= manifest.NumLevels {
		panic("writeTo >= manifest.NumLevels")
	}
	return int(manifest.NumLevels - 1 - writeTo)
}

/*
deviation=1 << 32，第一次写入后的划分点
4393177899486
4395245266984
4396802398083
4398212853185
4399636134258
4401259616150
4403587337208
18446744073709551615
*/
// 压实第一个区间
// 3: 34 | 2.2, 48,
// 4: 16+32 | 2.2, 64
// 5: 38+4+32 | 2.2,
// 6: 52+4+32 | 2.2,

// 全局划分，文件数量，可是为什么层内划分会产生碎片啊？
// 3: 67/0
// 4: 97/4
// 5: 126/12
func Test_pebble_1(t *testing.T) {
	db := MustDB("test-db", true, EnablePebble, func(options *Options) *Options {
		options.DisableAutomaticCompactions = true
		return options
	})

	times := 3 // 48
	deviation := uint64(1 << 32)
	for i := 0; i < times; i++ {
		d := NewData()
		mean := 1024*math.MaxUint32 + uint64(i)*(math.MaxUint32)
		d = d.AddNormal(mean, deviation, 1<<20, MinKey+1, MaxKey-1)
		d = d.AddUniform(MinKey+1, MaxKey-1, 1024)
		d = d.AddMinMax()
		//d = d.AddUniform(MinKey, MaxKey, 1<<20)
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		batchWrite(db, d.Keys, uint64(i))
		println(fmt.Sprintf("done %d", i))
	}
	stat(db)

	scanFiles(db)
	err := db.Compact(BigEndian(0), BigEndian(4393177899486), false)
	if err != nil {
		panic(err)
	}
	time.Sleep(1000)
}

// 生成SST
func Test_make_sst(t *testing.T) {
	//db := PMT()
	//m, tw := db.my_newCompactionOutput(db.newJobID(), 0)
	//tw.Add()
	//use(m, tw)

	//db := PMT()
	db := MustDB("test-db", true)
	meta := makeSST(db, vfs.Default.PathJoin("tmp", "a0"), []uint64{1, 10})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a1"), []uint64{1, 11})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a2"), []uint64{10, 15})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a3"), []uint64{10, 15})

	use(meta)
}

// bypass plan
func Test_multilevelFlush(t *testing.T) {
	db := MustDB("test-db", true)

	pList := []PartPlan{
		PartPlan{
			High:    math.MaxUint64,
			low:     0,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys := []uint64{0, 20}
	for i, sp := range pList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    1,
		}
		pList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], outputLevelForWriteTo(sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFrom(pList)
	println(db.LSMViewURL())

	pList = []PartPlan{
		PartPlan{
			High:    20,
			low:     0,
			WriteTo: 0,
			Stack:   []FileNum{4},
			Outputs: nil,
		},
		PartPlan{
			High:    math.MaxUint64,
			low:     21,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys = []uint64{1, 2, 32}
	for i, sp := range pList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    2,
		}
		pList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], outputLevelForWriteTo(sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFrom(pList)
	println(db.LSMViewURL())

	pList = []PartPlan{
		PartPlan{
			High:    20,
			low:     0,
			WriteTo: 1,
			Stack:   []FileNum{5},
			Outputs: nil,
		},
		PartPlan{
			High:    32,
			low:     21,
			WriteTo: 1,
			Stack:   []FileNum{6},
			Outputs: nil,
		},
		PartPlan{
			High:    math.MaxUint64,
			low:     33,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys = []uint64{16, 32}
	for i, sp := range pList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    3,
		}
		pList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], outputLevelForWriteTo(sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFrom(pList)
	println(db.LSMViewURL())
	use(pmtinternal.PartIdx)
}

func Test_multilevelFlush_pprof(t *testing.T) {
	if testing.Short() {
		t.Skip("pprof test skipped in short mode")
	}

	origPartIdx := pmtinternal.PartIdx
	origSstMap := pmtinternal.SstMap
	pmtinternal.PartIdx = []pmtinternal.Part{
		{
			Low:   0,
			High:  math.MaxUint64,
			Stack: nil,
		},
	}
	pmtinternal.SstMap = make(map[uint64]pmtinternal.SstInfo, 1024)
	defer func() {
		pmtinternal.PartIdx = origPartIdx
		pmtinternal.SstMap = origSstMap
	}()

	profPath := "multilevel_flush.cpu.prof"
	_ = os.Remove(profPath)
	f, err := os.Create(profPath)
	if err != nil {
		t.Fatalf("create pprof file: %v", err)
	}
	runtime.SetMutexProfileFraction(1000)
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		t.Fatalf("start cpu profile: %v", err)
	}
	defer func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}()

	db := MustDB("test-db", true, func(options *Options) *Options {
		options.FS = vfs.Default
		pagesize := 4 << 10
		options.CacheSize = int64(512 * pagesize)
		options.DisableAutomaticCompactions = true
		options.MaxConcurrentCompactions = func() int { return 8 }
		return options
	})
	defer db.Close()

	const rounds = 64
	const flushConcurrency = 4
	dir := "pmttestdata"
	for r := 0; r < rounds; r++ {
		path := filepath.Join(dir, fmt.Sprintf("normal_plus_round_%03d.bin", r))
		d := LoadDataFile(path)
		flushPlan := planStep1(d.Keys)
		multilevelFlushConcurrent(db, d.Keys, uint64(r), flushPlan.planList, flushConcurrency)
		pmtinternal.PartIdx = newPartIdxFrom(flushPlan.planList)
	}
	t.Logf("pprof cpu profile written to %s", profPath)
}

// 有多少碎片文件
func scanFiles(db *DB) {
	table0 := 0
	ct := 0
	for _, l := range db.DebugCurrentVersion().Levels {
		it := l.Iter()
		for table := it.First(); table != nil; table = it.Next() {
			ct++
			if table.Size <= 4096 {
				table0++
			}
		}
	}
	println(fmt.Sprintf("tableCt:%d table0Ct:%d", ct, table0))
}

// Test_PMT_Format_Basic 演示 PMT 格式的基本用法
/*
keys <- normal_plus_round_000
tableBuf <- keys
check all point read
check all range read
*/
func Test_PMT_Format_Basic(t *testing.T) {
	path := filepath.Join("pmttestdata", "normal_plus_round_000.bin")
	d := LoadDataFile(path)
	keys := d.Keys
	if len(keys) > pmtformat.MaxEntriesPerTable() {
		keys = keys[:pmtformat.MaxEntriesPerTable()]
	}

	var tableBuf bytes.Buffer
	w := pmtformat.NewWriter(&tableBuf)
	for _, k := range keys {
		// value=key 便于校验 point/range read
		if err := w.Add(k, k); err != nil {
			t.Fatalf("pmt add failed (key=%d): %v", k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("pmt close failed: %v", err)
	}

	it, err := pmtformat.NewIter(
		pmtTestReadable{r: bytes.NewReader(tableBuf.Bytes())},
		int64(tableBuf.Len()),
		base.SeqNum(1),
	)
	if err != nil {
		t.Fatalf("pmt iter create failed: %v", err)
	}
	defer func() {
		if err := it.Close(); err != nil {
			t.Fatalf("pmt iter close failed: %v", err)
		}
	}()

	// check all point read
	for _, k := range keys {
		kv := it.SeekGE(BigEndian(k), base.SeekGEFlagsNone)
		if kv == nil {
			t.Fatalf("point read failed: key=%d, got nil", k)
		}
		gotKey := binary.BigEndian.Uint64(kv.K.UserKey)
		if gotKey != k {
			t.Fatalf("point read failed: key=%d, got key=%d", k, gotKey)
		}
		v, _, err := kv.Value(nil)
		if err != nil {
			t.Fatalf("point read value decode failed: key=%d, err=%v", k, err)
		}
		gotVal := binary.BigEndian.Uint64(v)
		if gotVal != k {
			t.Fatalf("point read failed: key=%d, got val=%d", k, gotVal)
		}
	}

	// check all range read
	i := 0
	for kv := it.First(); kv != nil; kv = it.Next() {
		if i >= len(keys) {
			t.Fatalf("range read has extra entries, first extra key=%d", binary.BigEndian.Uint64(kv.K.UserKey))
		}
		gotKey := binary.BigEndian.Uint64(kv.K.UserKey)
		if gotKey != keys[i] {
			t.Fatalf("range read key mismatch at %d: expected %d, got %d", i, keys[i], gotKey)
		}
		v, _, err := kv.Value(nil)
		if err != nil {
			t.Fatalf("range read value decode failed at %d: %v", i, err)
		}
		gotVal := binary.BigEndian.Uint64(v)
		if gotVal != keys[i] {
			t.Fatalf("range read value mismatch at %d: expected %d, got %d", i, keys[i], gotVal)
		}
		i++
	}
	if i != len(keys) {
		t.Fatalf("entries len mismatch: expected %d, got %d", len(keys), i)
	}
}

// 测试PMT格式的性能
func Test_PMT_Format_Performance(t *testing.T) {
	path := filepath.Join("pmttestdata", "normal_plus_round_000.bin")
	d := LoadDataFile(path)
	keys := d.Keys
	if len(keys) > pmtformat.MaxEntriesPerTable() {
		keys = keys[:pmtformat.MaxEntriesPerTable()]
	}

	// Build PMT0 format in-memory.
	var pmtBuf bytes.Buffer
	pmtWriter := pmtformat.NewWriter(&pmtBuf)
	for _, k := range keys {
		if err := pmtWriter.Add(k, 1); err != nil {
			t.Fatalf("pmt add failed (key=%d): %v", k, err)
		}
	}
	if err := pmtWriter.Close(); err != nil {
		t.Fatalf("pmt close failed: %v", err)
	}
	pmtSize := pmtBuf.Len()

	// Build old format on an in-memory FS.
	mem := vfs.NewMem()
	const oldName = "old_format.sst"
	oldFile, err := mem.Create(oldName, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("create old format file failed: %v", err)
	}
	oldWriter := sstable.NewWriter(objstorageprovider.NewFileWritable(oldFile), sstable.WriterOptions{
		TableFormat: sstable.TableFormatPebblev1,
		Compression: NoCompression,
		BlockSize:   4096,
		//FilterPolicy: bloom.FilterPolicy(10), // 10 bits used per key
		//FilterType:   TableFilter,
	})
	var keyBuf [8]byte
	var valBuf [8]byte

	for _, k := range keys {
		binary.BigEndian.PutUint64(valBuf[:], rand.Uint64())
		binary.BigEndian.PutUint64(keyBuf[:], k)
		if err := oldWriter.Set(keyBuf[:], valBuf[:]); err != nil {
			t.Fatalf("old format set failed (key=%d): %v", k, err)
		}
	}
	if err := oldWriter.Close(); err != nil {
		t.Fatalf("old format close failed: %v", err)
	}
	fi, err := mem.Stat(oldName)
	if err != nil {
		t.Fatalf("stat old format file failed: %v", err)
	}
	oldSize := fi.Size()

	ratio := float64(pmtSize) / float64(oldSize)
	t.Logf("dataset: %s", path)
	t.Logf("keys: %d", len(keys))
	t.Logf("pmtformat size: %d bytes", pmtSize)
	t.Logf("oldformat(v1, no compression) size: %d bytes", oldSize)
	t.Logf("size ratio (pmt/old): %.4f", ratio)
}
