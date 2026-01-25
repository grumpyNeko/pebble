package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"github.com/cockroachdb/pebble/vfs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func Test_pmt_basic(t *testing.T) {
	db := MustDB()
	batchWrite(db, []uint64{2, 10}, 0)
	batchWrite(db, []uint64{15, 99}, 0)
	batchWrite(db, []uint64{1, 20}, 0)
	println(db.LSMViewURL())
	db.manualCompact(BigEndian(2), BigEndian(10), 0, false)
	println(db.LSMViewURL())
	metrics := db.Metrics()
	println(fmt.Sprintf("%+v", metrics))
	use(metrics)
	use(pmtinternal.PartIdx)
}

func Test_split(t *testing.T) {
	//db := PMT()
	db := MustDB()

	var metrics *Metrics
	// 手动增加划分点100
	batchWrite(db, []uint64{1, 90, 100, 200, 250}, 0)
	metrics = stat(db)
	println(db.LSMViewURL())
	use(metrics)
}

func Test_sst_meta(t *testing.T) {
	db := MustDB()
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
	db := MustDB(EnablePebble, func(options *Options) *Options {
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
// 128, 7.72, avg:3.84
func Test_pebble_wa(t *testing.T) {
	db := MustDB(EnablePebble, func(options *Options) *Options {
		options.DisableAutomaticCompactions = false
		return options
	})

	times := 32 // 48
	deviation := uint64(1 << 30)
	for i := 0; i < times; i++ {
		d := NewData()
		mean := 1024*math.MaxUint32 + uint64(i)*(math.MaxUint32)
		use(mean, deviation)
		d = d.AddNormal(mean, deviation, 1<<20, MinKey+1, MaxKey-1)
		d = d.AddUniform(MinKey+1, MaxKey-1, 1024)
		d = d.AddMinMax()
		//d = d.AddUniform(MinKey, MaxKey, 1<<20)
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		batchWrite(db, d.Keys, uint64(i))
		println(fmt.Sprintf("done %d", i))
	}
	for isCompacting(db) {
		println("wait for compact")
		time.Sleep(10 * time.Second)
	}

	metrics := stat(db)
	use(metrics)
	use(pmtinternal.PartIdx)
	println(metrics.Total().BytesIn)
	//avg(db)

	//println("-------------------------------")
	//printNextFileNum(db)
	//err := db.Compact(BigEndian(0), BigEndian(4391513256358), false)
	//if err != nil {
	//	panic(err)
	//}
	//printNextFileNum(db)
	//time.Sleep(1000)
}

func Test_pmt_wa(t *testing.T) {
	db := MustDB(func(options *Options) *Options {
		options.FS = vfs.Default
		pagesize := 4 << 10                       // 4KB
		options.CacheSize = int64(512 * pagesize) //
		options.DisableAutomaticCompactions = false
		return options
	})

	const flushConcurrency = 4
	times := 32 // 48
	deviation := uint64(1 << 32)
	datas := []uint64{}
	writeStart := time.Now()
	for i := 0; i < times; i++ {
		d := NewData()
		mean := 1024*math.MaxUint32 + uint64(i)*(math.MaxUint32)
		use(mean, deviation)
		d = d.AddNormal(mean, deviation, 1<<20, MinKey+1, MaxKey-1)
		d = d.AddUniform(MinKey+1, MaxKey-1, 1024)
		d = d.AddMinMax()
		//d = d.AddUniform(MinKey, MaxKey, 1<<20)
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		datas = append(datas, d.Keys...)

		spList := plan()
		//for j, sp := range spList {
		//	mem := fakeMemTable{
		//		keys: rangeLimit(d.Keys, sp.low, sp.High),
		//		v:    uint64(i),
		//	}
		//	spList[j].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], int(manifest.NumLevels-1-sp.WriteTo))
		//}
		multilevelFlushConcurrent(db, d.Keys, uint64(i), spList, flushConcurrency)
		pmtinternal.PartIdx = newPartIdxFromSubParts(spList)
		println(fmt.Sprintf("done %d", i))
	}
	for isCompacting(db) {
		println("wait for compact")
		time.Sleep(1 * time.Second)
	}
	println(fmt.Sprintf("写入阶段耗时(ms): %d", time.Since(writeStart).Milliseconds()))

	metrics := stat(db)
	use(metrics)
	use(pmtinternal.PartIdx)
	println(metrics.Total().BytesIn)

	// ------------------------------

	//avg(db)

	// --- get every key ---
	//	db := MustDB(EnablePebble, func(options *Options) *Options {
	//		options.FS = vfs.Default
	//		pagesize := 4 << 10                       // 4KB
	//		options.CacheSize = int64(128 * pagesize) //
	//		options.DisableAutomaticCompactions = false
	//		return options
	//	})
	// disk 128pagescache nocompression 128round, 0.129ms

	// disk 512pagescache nocompression 128round, 0.112ms
	// disk 256pagescache nocompression 32round, 0.0297
	// disk 128pagescache nocompression 32round, 0.0613

	// pmt 256pagescache nocompression 64round, 0.037516

	// pmt 256pagescache nocompression 128round, 47.1us

	//rand.Shuffle(len(datas), func(i, j int) {
	//	datas[i], datas[j] = datas[j], datas[i]
	//})
	//println("start benchmark")
	//start := time.Now()
	//for i := 0; i < 1<<20; i++ {
	//	_, _, err := db.Get(BigEndian(datas[i]))
	//	if err != nil {
	//		println(err.Error())
	//	}
	//}
	//println(time.Now().Sub(start).Milliseconds())
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
		path := filepath.Join(dir, fmt.Sprintf("normal_round_%03d.bin", i))
		SaveDataFile(path, d)
	}
}

func multilevelFlushConcurrent(db *DB, keys []uint64, v uint64, spList []SubPart, concurrency int) {
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
	groups := make([][]SubPart, 0, concurrency)
	for g := 0; g < concurrency; g++ {
		start := g * size
		end := start + size
		if end > len(spList) {
			end = len(spList)
		}
		if start < end {
			groups = append(groups, spList[start:end])
		}
	}
	wg.Add(len(groups))
	for _, group := range groups {
		go func(list []SubPart) {
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
					int(manifest.NumLevels-1-sp.WriteTo),
				)
			}
		}(group)
	}
	wg.Wait()
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
	db := MustDB(EnablePebble, func(options *Options) *Options {
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
	db := MustDB()
	meta := makeSST(db, vfs.Default.PathJoin("tmp", "a0"), []uint64{1, 10})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a1"), []uint64{1, 11})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a2"), []uint64{10, 15})
	meta = makeSST(db, vfs.Default.PathJoin("tmp", "a3"), []uint64{10, 15})

	use(meta)
}

// 放进指定level，触发multilevel compact
func Test_MustIngestToLevel(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		panic("")
	}
	println(cwd)

	//db := PMT()
	db := MustDB()
	MustIngestToLevel(db, "tmp", "a0", 0)
	MustIngestToLevel(db, "tmp", "a1", 1)
	MustIngestToLevel(db, "tmp", "a2", 2)
	MustIngestToLevel(db, "tmp", "a3", 3)
	printKeys(db)
	m := stat(db)
	db.MyCompact(BigEndian(0), BigEndian(100), 1)
	m = stat(db)
	use(m)
}

// bypass plan
func Test_multilevelFlush(t *testing.T) {
	db := MustDB()

	spList := []SubPart{
		SubPart{
			High:    math.MaxUint64,
			low:     0,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys := []uint64{0, 20}
	for i, sp := range spList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    1,
		}
		spList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], int(manifest.NumLevels-1-sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFromSubParts(spList)
	println(db.LSMViewURL())

	spList = []SubPart{
		SubPart{
			High:    20,
			low:     0,
			WriteTo: 0,
			Stack:   []FileNum{4},
			Outputs: nil,
		},
		SubPart{
			High:    math.MaxUint64,
			low:     21,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys = []uint64{1, 2, 32}
	for i, sp := range spList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    2,
		}
		spList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], int(manifest.NumLevels-1-sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFromSubParts(spList)
	println(db.LSMViewURL())

	spList = []SubPart{
		SubPart{
			High:    20,
			low:     0,
			WriteTo: 1,
			Stack:   []FileNum{5},
			Outputs: nil,
		},
		SubPart{
			High:    32,
			low:     21,
			WriteTo: 1,
			Stack:   []FileNum{6},
			Outputs: nil,
		},
		SubPart{
			High:    math.MaxUint64,
			low:     33,
			WriteTo: 0,
			Stack:   nil,
			Outputs: nil,
		},
	}
	keys = []uint64{16, 32}
	for i, sp := range spList {
		mem := fakeMemTable{
			keys: rangeLimit(keys, sp.low, sp.High),
			v:    3,
		}
		spList[i].Outputs = multilevelFlushWithResult(db, mem, sp.Stack[sp.WriteTo:], int(manifest.NumLevels-1-sp.WriteTo))
	}
	pmtinternal.PartIdx = newPartIdxFromSubParts(spList)
	println(db.LSMViewURL())
	use(pmtinternal.PartIdx)
}

func MustIngestToLevel(db *DB, dir string, name string, level int) {
	tmp := vfs.Default.PathJoin(dir, name)
	_, err := db.ingest0(context.TODO(), []string{tmp}, nil /* shared */, KeyRange{}, nil, level)
	if err != nil {
		panic(err)
	}
}

func plan() []SubPart {
	ret := make([]SubPart, 0, 256)
	for _, p := range pmtinternal.PartIdx {
		sp := SubPart{
			High:    p.High,
			low:     p.Low,
			WriteTo: uint16(len(p.Stack)),
			Stack:   p.Stack,
			Outputs: nil,
		}
		if len(sp.Stack) > 6 {
			println("oh!")
			sp.WriteTo = 0
		}
		ret = append(ret, sp)
	}
	return ret
}

// 打印文件中的所有k
func printFileContent(db *DB, file *manifest.TableMetadata) {
	if file == nil {
		panic("File is nil")
	}

	ctx := context.Background()
	iters, err := db.newIters(ctx, file, nil, internalIterOpts{}, iterPointKeys|iterRangeDeletions|iterRangeKeys)
	if err != nil {
		panic(err.Error())
	}
	defer iters.CloseAll()

	iter := iters.Point()
	if iter == nil {
		panic("Point iterator is nil")
	}

	println(fmt.Sprintf("=== FileNum=%d, Size=%d bytes ===", file.FileNum, file.Size))
	smallestKey := binary.BigEndian.Uint64(file.Smallest.UserKey)
	largestKey := binary.BigEndian.Uint64(file.Largest.UserKey)
	println(fmt.Sprintf("Smallest=%d, Largest=%d, SeqNum=[%d,%d]",
		smallestKey, largestKey, file.Smallest.SeqNum(), file.Largest.SeqNum()))

	count := 0
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		count++
		println(fmt.Sprintf("%d#%s,%s", binary.BigEndian.Uint64(kv.K.UserKey), kv.K.SeqNum(), kv.K.Kind()))
	}
	println(fmt.Sprintf("Total keys: %d", count))
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

// 平均重叠数
func avg(db *DB) {
	ct := 0
	sum := 0
	it, err := db.NewIter(nil)
	if err != nil {
		panic(err)
	}

	for ok := it.First(); ok; ok = it.Next() {
		ct++
		_, tables := db.MustGet(it.Key())
		sum += tables
	}
	println(fmt.Sprintf("ct:%d, avg:%f", ct, float64(sum)/float64(ct)))
}

// 演示如何使用 TableFormatPMT0
func Test_PMT_Format(t *testing.T) {
	// ...
}
