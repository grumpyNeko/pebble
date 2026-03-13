package pebble

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/pmtinternal"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/samber/lo"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
)

var (
	compactionResults   = make(map[JobID][]manifest.TableInfo)
	compactionResultsMu sync.Mutex
)

const baseRange = math.MaxUint32

// --------------------------------------------------------
func BigEndian(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func assertEmptyMemtable(db *DB) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, m := range db.mu.mem.queue {
		it := m.newIter(nil)
		for kv := it.First(); kv != nil; kv = it.Next() {
			panic("not enmpty")
		}
	}
}

// 这是早期用来研究的, 现在pmt不应该调用这个
func batchWrite(db *DB, keys []uint64, v uint64) {
	//db.mu.Lock()
	//defer db.mu.Unlock()
	if len(db.mu.compact.manual) != 0 {
		panic(`len(db.mu.compact.manual) != 0`)
	}
	startJobId := db.mu.nextJobID
	batch := db.NewIndexedBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Set(BigEndian(key), BigEndian(v), nil); err != nil {
			panic(err)
		}
	}
	if err := batch.Commit(&WriteOptions{Sync: false}); err != nil {
		panic(err)
	}
	// if batch.memTableSize < db.largeBatchThreshold
	// 太大的batch会直接flush?
	err := db.Flush()
	if err != nil {
		panic(err)
	}
	//println(fmt.Sprintf("batch.memTableSize: %d", batch.memTableSize))
	//for _, e := range db.mu.mem.queue {
	//	println(fmt.Sprintf("e.flushable.inuseBytes(): %d", e.flushable.inuseBytes()))
	//	if e.flushable.inuseBytes() != 0 {
	//		it := e.newIter(nil)
	//		for rec := it.First(); rec != nil; rec = it.Next() {
	//			println(rec.K.Pretty(DefaultComparer.FormatKey))
	//		}
	//	}
	//}
	//time.Sleep(500 * time.Millisecond)
	if db.mu.nextJobID != startJobId+1 {
		println(`db.mu.nextJobID != startJobId+1`)
	}
}

func pmtOptions() *Options {
	pmtinternal.EnablePMT = true
	opts := &Options{
		DisableAutomaticCompactions: true,
		FlushSplitBytes:             1 << 21, // 2MB, 512 page
		DisableWAL:                  true,    // 不完全有效
		FS:                          vfs.NewMem(),
		MemTableStopWritesThreshold: 2, // must >= 2

		MemTableSize: 1 << 24, // 16MB, 足够大就关闭自动flush
		EventListener: &EventListener{
			CompactionEnd: CompactionEnd,
			BackgroundError: func(err error) {
				println(fmt.Sprintf("BackgroundError %v", err))
			},
			WriteStallBegin: func(WriteStallBegin WriteStallBeginInfo) {
				println(fmt.Sprintf("WriteStallBegin %v", WriteStallBegin))
			},
			WriteStallEnd: func() {
				println(fmt.Sprintf("WriteStallEnd"))
			},
		},
	}
	opts.Experimental.MultiLevelCompactionHeuristic = NoMultiLevel{}
	opts.Experimental.EnableColumnarBlocks = func() bool {
		return false
	}
	opts.Experimental.EnableValueBlocks = func() bool {
		return false
	}
	opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool {
		return false
	}
	/*
			 写入空数据库
			 L0CompactionThreshold = 2就会触发compact，而且不是move
			 L0CompactionThreshold = 3不会触发任何compact
			 因为Score = (2*L0Sublevel数量) / L0CompactionThreshold
			 如果Score >= 1，就尝试compact
			 为何不move?
			 决定compact是否move的逻辑在newCompaction里
			 c.kind == compactionKindDefault
			 c.outputLevel.files.Empty()
			 !c.hasExtraLevelData()
			 c.startLevel.files.Len() == 1
		   要求L0只有一个文件, 有点蠢
	*/
	opts.L0CompactionThreshold = 4
	opts.L0StopWritesThreshold = 5
	opts.MaxConcurrentCompactions = func() int { return 8 }
	const pagesize = 4096
	opts.CacheSize = 256 * pagesize
	opts.Levels = make([]LevelOptions, manifest.NumLevels)
	for i := range opts.Levels {
		opts.Levels[i].BlockRestartInterval = 1
		opts.Levels[i].Compression = func() Compression {
			return NoCompression
		}
		opts.Levels[i].TargetFileSize = 2 << 20 // 2MB, 512 * 4KB
	}
	return opts
}

func EnablePebble(opts *Options) *Options {
	pmtinternal.EnablePMT = false
	pmtinternal.EnablePMTTableFormat = false

	opts.DisableAutomaticCompactions = false
	opts.FileFormat = sstable.TableFormatLevelDB
	opts.Experimental.MultiLevelCompactionHeuristic = NoMultiLevel{}

	opts.L0StopWritesThreshold = 5
	return opts
}

func EnableCompression(opts *Options) *Options {
	for i, _ := range opts.Levels {
		opts.Levels[i].BlockRestartInterval = 16 // TODO:
		opts.Levels[i].Compression = func() Compression {
			return SnappyCompression
		}
	}
	return opts
}

func CompactionEnd(info CompactionInfo) {
	if info.Reason == "move" {
		return
	}
	println(fmt.Sprintf("CompactionEnd %v", info))
	if !pmtinternal.EnablePMT {
		return
	}
	inputs := []manifest.TableInfo{}
	for _, l := range info.Input {
		for _, sst := range l.Tables {
			inputs = append(inputs, sst)
		}
	}
	outputs := []manifest.TableInfo{}
	for _, e := range info.Output.Tables {
		outputs = append(outputs, e)
	}

	for _, e := range outputs {
		pmtinternal.AddToMap(uint64(e.FileNum), pmtinternal.SstInfo{
			Size:     e.Size,
			Smallest: binary.BigEndian.Uint64(e.Smallest.UserKey),
			Largest:  binary.BigEndian.Uint64(e.Largest.UserKey),
		})
	}
	// Store results in the global map indexed by jobID
	compactionResultsMu.Lock()
	compactionResults[JobID(info.JobID)] = outputs
	compactionResultsMu.Unlock()
}

func newPartIdx(inputs []manifest.TableInfo, outputs []manifest.TableInfo) (partList []pmtinternal.Part) {
	// todo: if outputs.len == 0
	idx := pmtinternal.GetPartContain(binary.BigEndian.Uint64(outputs[0].Smallest.UserKey))
	p := pmtinternal.PartIdx[idx]
	// assert all inputs are on the same part
	for _, sst := range inputs {
		if idx0 := pmtinternal.GetPartContain(binary.BigEndian.Uint64(sst.Smallest.UserKey)); idx0 != idx {
			panic("inputs must be on same part")
		}
		if idx1 := pmtinternal.GetPartContain(binary.BigEndian.Uint64(sst.Largest.UserKey)); idx1 != idx {
			panic("inputs must be on same part")
		}
	}
	// assert all outputs are in this part
	for _, sst := range outputs {
		if idx0 := pmtinternal.GetPartContain(binary.BigEndian.Uint64(sst.Smallest.UserKey)); idx0 != idx {
			panic("inputs must be on same part")
		}
		if idx1 := pmtinternal.GetPartContain(binary.BigEndian.Uint64(sst.Largest.UserKey)); idx1 != idx {
			panic("inputs must be on same part")
		}
	}

	var minIdx = len(p.Stack)
	for _, sst := range inputs {
		curIdx := MustFind(p.Stack, uint64(sst.FileNum))
		if curIdx < minIdx {
			minIdx = curIdx
		}
	}
	p.Stack = p.Stack[:minIdx]
	// handle output, 以下几乎与flush的逻辑一样
	for _, e := range outputs {
		idx := pmtinternal.GetPartContain(binary.BigEndian.Uint64(e.Smallest.UserKey))
		if idx0 := pmtinternal.GetPartContain(binary.BigEndian.Uint64(e.Largest.UserKey)); idx0 != idx {
			panic("should on same part")
		}
		pmtinternal.PartIdx[idx].Tmp = append(pmtinternal.PartIdx[idx].Tmp, e.FileNum)
	}
	// update PartIdx
	for _, e := range pmtinternal.PartIdx {
		if len(e.Tmp) == 0 {
			partList = append(partList, e)
			continue
		}
		if len(e.Stack) == 0 { // 这个区间可被分裂
			currentLow := e.Low
			for _, filenum := range e.Tmp {
				f := pmtinternal.SstMap[uint64(filenum)]
				partList = append(partList, pmtinternal.Part{
					Low:   currentLow,
					High:  f.Largest,
					Stack: []base.FileNum{base.FileNum(filenum)},
					Tmp:   nil,
				})
				currentLow = f.Largest + 1
			}
			continue
		}
		partList = append(partList, pmtinternal.Part{
			Low:   e.Low,
			High:  e.High,
			Stack: append(e.Stack, e.Tmp...),
			Tmp:   nil,
		})
	}
	n := len(partList)
	if n == 0 {
		panic("why len(newPartIdx) == 0")
	}
	if partList[n-1].High != math.MaxUint64 { // 哨兵
		partList = append(partList, pmtinternal.Part{
			Low:  partList[n-1].High + 1, // TODO: ...
			High: math.MaxUint64,
		})
	}
	return
}

func MustFind(list []base.FileNum, target uint64) int {
	for i, e := range list {
		if uint64(e) == target {
			return i
		}
	}
	panic("not found")
}

type OptionPatch func(options *Options) *Options

func MustDB(path string, clear bool, list ...OptionPatch) *DB {
	opts := pmtOptions()
	for _, e := range list {
		opts = e(opts)
	}
	if opts.FS != vfs.Default && clear {
		println("===============删除数据库并以内存启动=================")
	}
	if opts.FS == vfs.Default && !clear {
		println("===============非空数据库=================")
		if !opts.ReadOnly {
			panic("非空数据库必须只读")
		}
	}
	// 删除所有数据
	if clear {
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}
	db, err := Open(path, opts)
	if err != nil {
		panic(err)
	}
	db.largeBatchThreshold = 1 // TODO: 这个竟然是没法配置的...

	if pmtinternal.EnablePMT {
		if !pmtinternal.EnablePMTTableFormat {
			println("======================EnablePMT but not EnablePMTTableFormat=================================")
		}
	}
	return db
}

// like getInternal
// panic if not found
// return filesAccessed
func (d *DB) MustGet(key []byte) (val []byte, filesAccessed int) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	var seqNum base.SeqNum
	seqNum = d.mu.versions.visibleSeqNum.Load()

	buf := getIterAllocPool.Get().(*getIterAlloc)

	get := &buf.get
	*get = getIter{
		comparer: d.opts.Comparer,
		newIters: d.newIters,
		snapshot: seqNum,
		iterOpts: IterOptions{
			// TODO(sumeer): replace with a parameter provided by the caller.
			Category:                      categoryGet,
			logger:                        d.opts.Logger,
			snapshotForHideObsoletePoints: seqNum,
		},
		key: key,
		// Compute the key prefix for bloom filtering.
		prefix:  key[:d.opts.Comparer.Split(key)],
		batch:   nil,
		mem:     readState.memtables,
		l0:      readState.current.L0SublevelFiles,
		version: readState.current,
	}

	// Strip off memtables which cannot possibly contain the seqNum being read
	// at.
	for len(get.mem) > 0 {
		n := len(get.mem)
		if logSeqNum := get.mem[n-1].logSeqNum; logSeqNum < seqNum {
			break
		}
		get.mem = get.mem[:n-1]
	}

	i := &buf.dbi
	pointIter := get
	*i = Iterator{
		ctx:          context.Background(),
		getIterAlloc: buf,
		iter:         pointIter,
		pointIter:    pointIter,
		merge:        d.merge,
		comparer:     *d.opts.Comparer,
		readState:    readState,
		keyBuf:       buf.keyBuf,
	}

	if !i.First() {
		err := i.Close()
		if err != nil {
			panic(err)
		}
		if len(key) != 8 {
			panic(fmt.Sprintf("not found key(len=%d,hex=%x)", len(key), key))
		}
		return BigEndian(999), filesAccessed // TODO: 暂时的妥协
	}
	val = append([]byte(nil), i.Value()...)
	filesAccessed = get.filesAccessed
	if err := i.Close(); err != nil {
		panic(err)
	}
	return val, filesAccessed
}

// PMTGet performs a PMT-specific lookup path:
// PartIdx -> Stack (newest to oldest) -> candidate tables.
// It returns (value, found, tableCt), where tableCt is the number of tables probed.
func (d *DB) PMTGet(k uint64) (v uint64, found bool, tableCt int) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if collectorEnabled() {
		if cv, ok := collectorGet(k); ok {
			return cv, true, 0
		}
	}
	part, ok := pmtPartForKey(k)
	if !ok || len(part.Stack) == 0 {
		return 0, false, 0
	}
	// Copy stack to avoid races with concurrent PartIdx updates.
	stack := append([]base.FileNum(nil), part.Stack...)

	readState := d.loadReadState()
	defer readState.unref()

	// Build a fileNum -> tableMetadata map for files in this partition stack.
	wanted := make(map[base.FileNum]struct{}, len(stack))
	for _, fn := range stack {
		wanted[fn] = struct{}{}
	}
	metaByFile := make(map[base.FileNum]*tableMetadata, len(wanted))
	for level := 0; level < numLevels && len(metaByFile) < len(wanted); level++ {
		iter := readState.current.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if _, ok := wanted[f.FileNum]; ok {
				metaByFile[f.FileNum] = f
			}
		}
	}

	// Newest files are appended to stack tail; probe tail -> head.
	for i := len(stack) - 1; i >= 0; i-- {
		fn := stack[i]
		info, ok := pmtinternal.SstMap[uint64(fn)]
		if !ok {
			continue
		}
		if k < info.Smallest || k > info.Largest {
			continue
		}
		meta, ok := metaByFile[fn]
		if !ok {
			continue
		}
		tableCt++
		if val, ok := d.pmtGetFromFile(meta, k); ok {
			return val, true, tableCt
		}
	}
	return 0, false, tableCt
}

func pmtPartForKey(k uint64) (pmtinternal.Part, bool) {
	parts := pmtinternal.PartIdx
	if len(parts) == 0 {
		return pmtinternal.Part{}, false
	}
	low, high := 0, len(parts)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if parts[mid].High < k {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low >= len(parts) {
		return pmtinternal.Part{}, false
	}
	p := parts[low]
	if p.Low <= k && k <= p.High {
		return p, true
	}
	return pmtinternal.Part{}, false
}

func pmtCollectorBoundsForFlush(keys []uint64, files []base.FileNum) (low uint64, high uint64, ok bool) {
	if len(keys) > 0 {
		part, ok := pmtPartForKey(keys[0])
		if !ok {
			return 0, 0, false
		}
		return part.Low, part.High, true
	}
	if len(files) > 0 {
		info, ok := pmtinternal.SstMap[uint64(files[0])]
		if !ok {
			panic(fmt.Sprintf("collector flush: file %d not found in SstMap", files[0]))
		}
		part, ok := pmtPartForKey(info.Smallest)
		if !ok {
			return 0, 0, false
		}
		if info.Largest > part.High {
			panic("collector flush: file cross part")
		}
		return part.Low, part.High, true
	}
	return 0, 0, false
}

type pmtPartPersist struct {
	Low   uint64   `json:"low"`
	High  uint64   `json:"high"`
	Stack []uint64 `json:"stack"`
}

func resolvePMTPartIdxPath(path string) string {
	if path == "" {
		panic("resolvePMTPartIdxPath: empty path")
	}
	if filepath.Ext(path) != "" {
		return filepath.Join(filepath.Dir(path), pmtinternal.PMTPartIdxFilename)
	}
	return filepath.Join(path, pmtinternal.PMTPartIdxFilename)
}

// SavePMTPartIdx saves current PMT PartIdx to a JSON file.
func SavePMTPartIdx(path string) {
	path = resolvePMTPartIdxPath(path)
	parts := make([]pmtPartPersist, 0, len(pmtinternal.PartIdx))
	for _, p := range pmtinternal.PartIdx {
		stack := make([]uint64, 0, len(p.Stack))
		for _, fn := range p.Stack {
			stack = append(stack, uint64(fn))
		}
		parts = append(parts, pmtPartPersist{
			Low:   p.Low,
			High:  p.High,
			Stack: stack,
		})
	}
	data, err := json.MarshalIndent(parts, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		panic(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		// Windows may reject rename-over-existing-file; remove destination then retry.
		if rmErr := os.Remove(path); rmErr == nil || os.IsNotExist(rmErr) {
			if retryErr := os.Rename(tmp, path); retryErr == nil {
				return
			}
		}
		// Fallback to non-atomic overwrite to keep benchmark flow.
		if writeErr := os.WriteFile(path, data, 0644); writeErr == nil {
			_ = os.Remove(tmp)
			return
		}
		panic(err)
	}
}

// LoadPMTPartIdx loads PartIdx from a JSON file.
func LoadPMTPartIdx(path string) {
	path = resolvePMTPartIdxPath(path)
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var partsOnDisk []pmtPartPersist
	if err := json.Unmarshal(data, &partsOnDisk); err != nil {
		panic(err)
	}
	if len(partsOnDisk) == 0 {
		panic("LoadPMTPartIdx: empty partidx file")
	}
	parts := make([]pmtinternal.Part, 0, len(partsOnDisk))
	for _, p := range partsOnDisk {
		stack := make([]base.FileNum, 0, len(p.Stack))
		for _, fn := range p.Stack {
			stack = append(stack, base.FileNum(fn))
		}
		parts = append(parts, pmtinternal.Part{
			Low:   p.Low,
			High:  p.High,
			Stack: stack,
			Tmp:   nil,
		})
	}
	pmtinternal.PartIdx = parts
}

// RecoverPMTSstMapFromCurrentVersion rebuilds SstMap from current live SSTs.
func (d *DB) RecoverPMTSstMapFromCurrentVersion() {
	vers := d.DebugCurrentVersion()
	newMap := make(map[uint64]pmtinternal.SstInfo, 1024)
	for level := 0; level < numLevels; level++ {
		iter := vers.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if !f.HasPointKeys {
				continue
			}
			if len(f.SmallestPointKey.UserKey) != 8 || len(f.LargestPointKey.UserKey) != 8 {
				panic(fmt.Sprintf("RecoverPMTSstMapFromCurrentVersion: file %s has non-uint64 keys", f.FileNum))
			}
			smallest := binary.BigEndian.Uint64(f.SmallestPointKey.UserKey)
			largest := binary.BigEndian.Uint64(f.LargestPointKey.UserKey)
			if largest < smallest {
				panic(fmt.Sprintf("RecoverPMTSstMapFromCurrentVersion: invalid key bounds for file %s", f.FileNum))
			}
			newMap[uint64(f.FileNum)] = pmtinternal.SstInfo{
				Size:     uint64(f.Size),
				Smallest: smallest,
				Largest:  largest,
			}
		}
	}
	for _, p := range pmtinternal.PartIdx {
		for _, fn := range p.Stack {
			if _, ok := newMap[uint64(fn)]; !ok {
				panic(fmt.Sprintf("RecoverPMTSstMapFromCurrentVersion: stack file %d not found in live SSTs", fn))
			}
		}
	}
	pmtinternal.SstMap = newMap
}

// LoadPMTPartIdxAndRecoverMap loads PartIdx from file, then scans DB to recover SstMap.
func (d *DB) LoadPMTPartIdxAndRecoverMap(path string) {
	LoadPMTPartIdx(path)
	d.RecoverPMTSstMapFromCurrentVersion()
}

// PMTMissProbeCountForKey estimates PMT lookup probes on a miss for key k.
// It counts files in the matched partition stack whose [Smallest, Largest] contains k.
func (d *DB) PMTMissProbeCountForKey(k uint64) int {
	_ = d
	part, ok := pmtPartForKey(k)
	if !ok {
		panic("why")
	}
	probeCount := 0
	for i := len(part.Stack) - 1; i >= 0; i-- {
		fn := part.Stack[i]
		info, ok := pmtinternal.SstMap[uint64(fn)]
		if !ok {
			panic(fmt.Sprintf("file %d not found in SstMap", fn))
		}
		if info.Smallest <= k && k <= info.Largest {
			probeCount++
		}
	}
	return probeCount
}

// PMTAverageMissProbeCount returns average PMT miss probe count across keys.
func (d *DB) PMTAverageMissProbeCount(keys []uint64) float64 {
	if len(keys) == 0 {
		return 0
	}
	var sum uint64
	for _, k := range keys {
		sum += uint64(d.PMTMissProbeCountForKey(k))
	}
	return float64(sum) / float64(len(keys))
}

func (d *DB) pmtGetFromFile(meta *tableMetadata, k uint64) (uint64, bool) {
	if meta == nil {
		return 0, false
	}
	f, err := d.objProvider.OpenForReading(
		context.Background(),
		base.FileTypeTable,
		meta.FileBacking.DiskFileNum,
		objstorage.OpenOptions{MustExist: true},
	)
	if err != nil {
		panic(err)
	}

	size := f.Size()
	if size <= 0 {
		if err := f.Close(); err != nil {
			panic(err)
		}
		return 0, false
	}

	seqNum := meta.LargestSeqNum
	if seqNum < meta.SmallestSeqNum {
		seqNum = meta.SmallestSeqNum
	}
	iter, err := pmtformat.NewIter(f, size, seqNum)
	if err != nil {
		_ = f.Close()
		panic(fmt.Sprintf("PMTGet open PMT iter failed for file %d: %v", meta.FileNum, err))
	}
	defer func() {
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	iter.SetContext(context.Background())

	var keyBuf [8]byte
	binary.BigEndian.PutUint64(keyBuf[:], k)
	kv := iter.SeekGE(keyBuf[:], base.SeekGEFlagsNone)
	if err := iter.Error(); err != nil {
		panic(err)
	}
	if kv == nil {
		return 0, false
	}
	gotKey := binary.BigEndian.Uint64(kv.K.UserKey)
	if gotKey != k {
		return 0, false
	}
	v, _, err := kv.Value(nil)
	if err != nil {
		panic(err)
	}
	if len(v) != 8 {
		panic(fmt.Sprintf("PMTGet invalid value length %d in file %d", len(v), meta.FileNum))
	}
	return binary.BigEndian.Uint64(v), true
}

type myAlwaysMultiLevel struct{}

func (d myAlwaysMultiLevel) pick(
	pcOrig *pickedCompaction, opts *Options, diskAvailBytes uint64,
) *pickedCompaction {
	pcMulti := setupMultiLevelCandidate0(pcOrig, opts, diskAvailBytes)
	pcMulti = setupMultiLevelCandidate0(pcOrig, opts, diskAvailBytes)
	return pcMulti
}

func (d myAlwaysMultiLevel) allowL0() bool  { return false }
func (d myAlwaysMultiLevel) String() string { return "always" }

func stat(db *DB) *Metrics { // todo: 改为计算WA? 但是需要知道单个文件大小
	//println(db.LSMViewURL())
	m := db.Metrics()
	println(fmt.Sprintf("%v", m))
	return m
}

func use(arg ...any) {}

// from manualCompact
// 手动compact，现在没什么用了
func (d *DB) MyCompact(start, end []byte, inputLevel int) {
	// TODO: 限制在start和end内
	d.mu.Lock()
	curr := d.mu.versions.currentVersion()
	files := curr.Overlaps(inputLevel, base.UserKeyBoundsInclusive(start, end))
	if files.Empty() {
		d.mu.Unlock()
		return
	}

	c := &manualCompaction{
		level: inputLevel,
		done:  make(chan error, 1),
		start: start,
		end:   end,
	}
	d.mu.compact.manualID++
	c.id = d.mu.compact.manualID

	d.mu.compact.manual = append(d.mu.compact.manual, c)
	d.mu.compact.manualLen.Store(int32(len(d.mu.compact.manual)))
	d.maybeScheduleCompaction()
	d.mu.Unlock()

	if err := <-c.done; err != nil {
		panic(err)
	}
}

// ----------------------------------------------------------------------------
func makeSST(db *DB, path string, keys []uint64) *sstable.WriterMetadata {
	b := db.NewBatch()
	for _, k := range keys {
		err := b.Set(BigEndian(k), nil, nil)
		if err != nil {
			panic(err)
		}
	}

	f, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)
	writable := objstorageprovider.NewFileWritable(f)
	meta, err := _writeSSTForIngestion(
		db,
		iter, rangeDelIter, rangeKeyIter,
		false, /* uniquePrefixes */
		nil,   /* syntheticSuffix */
		nil,   /* syntheticPrefix */
		writable,
		db.FormatMajorVersion(),
	)
	if err != nil {
		panic(err)
	}
	return meta
}

// clone from metamorphic/build.go
func _writeSSTForIngestion(
	db *DB,
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
	uniquePrefixes bool,
	syntheticSuffix sstable.SyntheticSuffix,
	syntheticPrefix sstable.SyntheticPrefix,
	writable objstorage.Writable,
	targetFMV FormatMajorVersion,
) (*sstable.WriterMetadata, error) {
	opts := db.opts
	writerOpts := opts.MakeWriterOptions(0, targetFMV.MaxTableFormat())
	writerOpts.DisableValueBlocks = true

	w := sstable.NewWriter(writable, writerOpts)
	pointIterCloser := base.CloseHelper(pointIter)
	defer func() {
		_ = pointIterCloser.Close()
		if rangeDelIter != nil {
			rangeDelIter.Close()
		}
		if rangeKeyIter != nil {
			rangeKeyIter.Close()
		}
	}()

	outputKey := func(key []byte, syntheticSuffix sstable.SyntheticSuffix) []byte {
		if !syntheticPrefix.IsSet() && !syntheticSuffix.IsSet() {
			return slices.Clone(key)
		}
		if syntheticPrefix.IsSet() {
			key = syntheticPrefix.Apply(key)
		}
		if syntheticSuffix.IsSet() {
			n := opts.Comparer.Split(key)
			key = append(key[:n:n], syntheticSuffix...)
		}
		return key
	}

	var lastUserKey []byte
	for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
		// Ignore duplicate keys.
		if lastUserKey != nil {
			last := lastUserKey
			this := kv.K.UserKey
			if uniquePrefixes {
				last = last[:opts.Comparer.Split(last)]
				this = this[:opts.Comparer.Split(this)]
			}
			if opts.Comparer.Equal(last, this) {
				continue
			}
		}
		lastUserKey = append(lastUserKey[:0], kv.K.UserKey...)

		k := *kv
		k.K.SetSeqNum(base.SeqNumZero)
		k.K.UserKey = outputKey(k.K.UserKey, syntheticSuffix)
		value := kv.LazyValue()
		// It's possible that we wrote the key on a batch from a db that supported
		// DeleteSized, but will be ingesting into a db that does not. Detect this
		// case and translate the key to an InternalKeyKindDelete.
		if targetFMV < FormatDeleteSizedAndObsolete && kv.Kind() == InternalKeyKindDeleteSized {
			value = LazyValue{}
			k.K.SetKind(InternalKeyKindDelete)
		}
		valBytes, _, err := value.Value(nil)
		if err != nil {
			return nil, err
		}
		opts.Comparer.ValidateKey.MustValidate(k.K.UserKey)
		if err := w.Raw().Add(k.K, valBytes, false); err != nil {
			return nil, err
		}
	}
	if err := pointIterCloser.Close(); err != nil {
		return nil, err
	}

	if rangeDelIter != nil {
		span, err := rangeDelIter.First()
		for ; span != nil; span, err = rangeDelIter.Next() {
			if syntheticSuffix.IsSet() {
				panic("synthetic suffix with RangeDel")
			}
			start := outputKey(span.Start, nil)
			end := outputKey(span.End, nil)
			opts.Comparer.ValidateKey.MustValidate(start)
			opts.Comparer.ValidateKey.MustValidate(end)
			if err := w.DeleteRange(start, end); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rangeDelIter.Close()
		rangeDelIter = nil
	}

	if rangeKeyIter != nil {
		span, err := rangeKeyIter.First()
		for ; span != nil; span, err = rangeKeyIter.Next() {
			// Coalesce the keys of this span and then zero the sequence
			// numbers. This is necessary in order to make the range keys within
			// the ingested sstable internally consistent at the sequence number
			// it's ingested at. The individual keys within a batch are
			// committed at unique sequence numbers, whereas all the keys of an
			// ingested sstable are given the same sequence number. A span
			// containing keys that both set and unset the same suffix at the
			// same sequence number is nonsensical, so we "coalesce" or collapse
			// the keys.
			collapsed := keyspan.Span{
				Start: outputKey(span.Start, nil),
				End:   outputKey(span.End, nil),
				Keys:  make([]keyspan.Key, 0, len(span.Keys)),
			}
			opts.Comparer.ValidateKey.MustValidate(collapsed.Start)
			opts.Comparer.ValidateKey.MustValidate(collapsed.End)
			keys := span.Keys
			if syntheticSuffix.IsSet() {
				keys = slices.Clone(span.Keys)
				for i := range keys {
					if keys[i].Kind() == base.InternalKeyKindRangeKeyUnset {
						panic("RangeKeyUnset with synthetic suffix")
					}
					if len(keys[i].Suffix) > 0 {
						keys[i].Suffix = syntheticSuffix
					}
				}
			}
			rangekey.Coalesce(opts.Comparer.CompareRangeSuffixes, keys, &collapsed.Keys)
			for i := range collapsed.Keys {
				collapsed.Keys[i].Trailer = base.MakeTrailer(0, collapsed.Keys[i].Kind())
			}
			keyspan.SortKeysByTrailer(collapsed.Keys)
			if err := w.Raw().EncodeSpan(collapsed); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rangeKeyIter.Close()
		rangeKeyIter = nil
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	sstMeta, err := w.Raw().Metadata()
	if err != nil {
		return nil, err
	}
	return sstMeta, nil
}

// ---------pmt features start----------------
func isCompacting(d *DB) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ret := d.mu.compact.compactingCount > 0 ||
		d.mu.compact.downloadingCount > 0 ||
		d.mu.compact.flushing ||
		len(d.mu.compact.inProgress) > 0
	return ret
}

// _newPickedFilesCompaction creates a pickedCompaction from a list of file numbers.
// no checking for overlaps.
func _newPickedFilesCompaction(
	vers *version,
	opts *Options,
	fileNums []base.FileNum,
	outputLevel int,
	baseLevel int,
	kind compactionKind,
) *pickedCompaction {
	type fileWithLevel struct {
		file  *manifest.TableMetadata
		level int
	}
	// filesMap, allFiles <: fileNums
	filesMap := make(map[int][]*manifest.TableMetadata) // level -> files
	var allFiles []fileWithLevel
	// 目前还不能直接从pmtinternal.SstMap找
	for level := 0; level < numLevels; level++ {
		levelFiles := vers.Levels[level]
		iter := levelFiles.Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			for _, targetNum := range fileNums {
				if f.FileNum == targetNum {
					filesMap[level] = append(filesMap[level], f)
					allFiles = append(allFiles, fileWithLevel{file: f, level: level})
					break
				}
			}
		}
	}
	if len(allFiles) == 0 {
		panic("no matching files found for fileNums in current version")
	}

	// assert
	minLevel := numLevels
	for level := range filesMap {
		if level < minLevel {
			minLevel = level
		}
	}

	startLevel := minLevel
	adjusted := adjustedOutputLevel(outputLevel, baseLevel)
	if outputLevel > 0 && adjusted < 1 {
		adjusted = 1
	}
	pc := &pickedCompaction{
		cmp:                    opts.Comparer.Compare,
		version:                vers,
		baseLevel:              baseLevel,
		kind:                   kind,
		maxOutputFileSize:      uint64(opts.Level(adjusted).TargetFileSize),
		maxOverlapBytes:        maxGrandparentOverlapBytes(opts, adjusted),
		maxReadCompactionBytes: maxReadCompactionBytes(opts, adjusted),
	}

	// inputs[0] = startLevel
	// inputs[1..n-1] = extra levels (if any)
	// inputs[n] = outputLevel
	var inputs []compactionLevel

	actualLevels := make([]int, 0, len(filesMap))
	for level := range filesMap {
		actualLevels = append(actualLevels, level)
	}
	sort.Ints(actualLevels)
	filesStartLevel, ok := filesMap[startLevel]
	if !ok {
		panic(fmt.Sprintf("why? startLevel %d not found in filesMap", startLevel))
	}
	inputs = append(inputs, compactionLevel{
		level: startLevel,
		files: manifest.NewLevelSliceKeySorted(opts.Comparer.Compare, filesStartLevel),
	})
	for _, level := range actualLevels[1:] {
		if level == outputLevel {
			continue
		}
		inputs = append(inputs, compactionLevel{
			level: level,
			files: manifest.NewLevelSliceKeySorted(opts.Comparer.Compare, filesMap[level]),
		})
	}

	var outputLevelFiles []*manifest.TableMetadata
	if files, ok := filesMap[outputLevel]; ok {
		outputLevelFiles = files
	}
	// If the target output level is already represented by an input level,
	// avoid duplicating the same files in the trailing output-level slot.
	if outputLevel == startLevel {
		outputLevelFiles = nil
	}
	inputs = append(inputs, compactionLevel{
		level: outputLevel,
		files: manifest.NewLevelSliceKeySorted(opts.Comparer.Compare, outputLevelFiles),
	})

	pc.inputs = inputs
	pc.startLevel = &pc.inputs[0]
	pc.outputLevel = &pc.inputs[len(pc.inputs)-1]
	// Set l0SublevelInfo if startLevel is L0
	if startLevel == 0 && !pc.startLevel.files.Empty() {
		pc.startLevel.l0SublevelInfo = generateSublevelInfo(pc.cmp, pc.startLevel.files)
	}
	// Set extraLevels
	if len(pc.inputs) > 2 {
		pc.extraLevels = make([]*compactionLevel, len(pc.inputs)-2)
		for i := 1; i < len(pc.inputs)-1; i++ {
			pc.extraLevels[i-1] = &pc.inputs[i]
		}
	}
	// 还需在multiflush中增加内存里的新数据
	pc.smallest, pc.largest = keyrange(lo.Map(allFiles, func(item fileWithLevel, index int) *manifest.TableMetadata {
		return item.file
	}), opts)

	return pc
}

func keyrange(files []*manifest.TableMetadata, opts *Options) (smallest InternalKey, largest InternalKey) {
	firstKey := true
	for _, f := range files {
		if firstKey {
			smallest = f.Smallest
			largest = f.Largest
			firstKey = false
		} else {
			if base.InternalCompare(opts.Comparer.Compare, f.Smallest, smallest) < 0 {
				smallest = f.Smallest
			}
			if base.InternalCompare(opts.Comparer.Compare, f.Largest, largest) > 0 {
				largest = f.Largest
			}
		}
	}
	return smallest, largest
}

// rangeLimit returns a slice of keys that fall within [low, high] (inclusive)
// keys already is sorted in ascending order
func rangeLimit(keys []uint64, low uint64, high uint64) []uint64 {
	if len(keys) == 0 {
		return nil
	}

	// Find the first index where keys[i] >= low
	start := sort.Search(len(keys), func(i int) bool {
		return keys[i] >= low
	})

	// If no key >= low, return empty slice
	if start >= len(keys) {
		return nil
	}

	// Find the first index where keys[i] > high
	end := sort.Search(len(keys), func(i int) bool {
		return keys[i] > high
	})

	// Return the slice [start, end)
	return keys[start:end]
}

// a single flush + multi-level compaction
// Assume batch does not overlap with other files.
// wait for compaction to finish.
func multilevelFlush(db *DB, mem fakeMemTable, files []base.FileNum, outputLevel int) JobID {
	keys := mem.keys
	v := mem.v
	if len(keys) == 0 && len(files) == 0 {
		// println("multilevelFlush nothing to do")
		return -1
	}
	// 用于研究concurrent multi flush
	if isCompacting(db) {
		db.mu.Lock()
		compacting := db.mu.compact.compactingCount
		downloading := db.mu.compact.downloadingCount
		flushing := db.mu.compact.flushing
		inProgress := len(db.mu.compact.inProgress)
		db.mu.Unlock()
		println(fmt.Sprintf(
			"multilevelFlush: another compaction in progress (compacting=%d downloading=%d flushing=%v inProgress=%d)",
			compacting, downloading, flushing, inProgress,
		))
	}
	db.mu.Lock()
	// Create an iterator for mem
	seqNum := db.mu.versions.logSeqNum.Add(1) - 1
	memIter := newSimpleMemIter(keys, v, seqNum)

	baseLevel := db.mu.versions.picker.getBaseLevel()
	kind := compactionKindFlushMultilevel
	opts := db.opts
	vers := db.mu.versions.currentVersion()
	var pc *pickedCompaction
	if len(files) == 0 { // 说明只flush
		// Pebble原流程涉及adjustedOutputLevel, 此处暂时去掉
		pc = &pickedCompaction{
			cmp:                    opts.Comparer.Compare,
			version:                vers,
			baseLevel:              baseLevel,
			kind:                   kind,
			maxOutputFileSize:      uint64(opts.Level(outputLevel).TargetFileSize),
			maxOverlapBytes:        maxGrandparentOverlapBytes(opts, outputLevel),
			maxReadCompactionBytes: maxReadCompactionBytes(opts, outputLevel),
			inputs:                 []compactionLevel{{level: -1}, {level: outputLevel}},
		}
		pc.startLevel = &pc.inputs[0]
		pc.outputLevel = &pc.inputs[len(pc.inputs)-1]
	} else {
		pc = _newPickedFilesCompaction(vers, opts, files, outputLevel, baseLevel, kind)
	}
	// Update pc.smallest and pc.largest to include memory iterator
	if memIter != nil && len(memIter.keys) > 0 {
		memSmallest := memIter.keys[0]
		memLargest := memIter.keys[len(memIter.keys)-1]
		// Initialize or widen bounds safely when only-flush path leaves them zero.
		if pc.smallest.UserKey == nil || base.InternalCompare(opts.Comparer.Compare, memSmallest, pc.smallest) < 0 {
			pc.smallest = memSmallest
		}
		if pc.largest.UserKey == nil || base.InternalCompare(opts.Comparer.Compare, memLargest, pc.largest) > 0 {
			pc.largest = memLargest
		}
	}

	low, high := pmtinternal.ExtraParam.FlushExtraParams.GetAndDel()
	//pc.smallest = base.MakeInternalKey(BigEndian(low), seqNum, base.InternalKeyKindSet)
	//pc.largest = base.MakeInternalKey(BigEndian(high), seqNum, base.InternalKeyKindSet) // 注意high不需要加一
	if collectorEnabled() {
		if minK, maxK, has := collectorMinMaxInRange(low, high); has {
			collectorSmallest := base.MakeInternalKey(BigEndian(minK), seqNum, base.InternalKeyKindSet)
			collectorLargest := base.MakeInternalKey(BigEndian(maxK), seqNum, base.InternalKeyKindSet)
			if base.InternalCompare(opts.Comparer.Compare, collectorSmallest, pc.smallest) < 0 {
				pc.smallest = collectorSmallest
			}
			if base.InternalCompare(opts.Comparer.Compare, collectorLargest, pc.largest) > 0 {
				pc.largest = collectorLargest
			}
		}
	}

	comp := newCompaction(pc, opts, db.timeNow(), db.objProvider, noopGrantHandle{})
	if comp == nil {
		panic(`why comp == nil?`)
	}
	// Add memory iterator as a flushable
	if memIter != nil && len(memIter.keys) > 0 {
		comp.flushing = make(flushableList, 1)
		flushableMem := &flushableMemIter{iter: memIter}
		seqNum := memIter.keys[0].SeqNum()
		comp.flushing[0] = db.newFlushableEntry(flushableMem, base.DiskFileNum(0), seqNum)
	}

	// Add collector iterator, 这是补丁, 就该这么写
	if collectorEnabled() {
		collectorValidateRange(low, high)
		flushing := make(flushableList, 0, 2)
		if len(comp.flushing) == 1 {
			flushing = append(flushing, comp.flushing[0])
		}
		if len(comp.flushing) > 1 {
			panic("why")
		}

		flushableCollector := &flushableCollectorIter{
			lower: BigEndian(low),
			upper: BigEndian(high),
		}
		flushing = append(flushing, db.newFlushableEntry(flushableCollector, base.DiskFileNum(0), seqNum))
		comp.flushing = flushing
	}

	// TODO: 如果不执行以下几行会怎样
	c := &manualCompaction{
		done: make(chan error, 1),
	}
	db.mu.compact.manualID++
	db.mu.compact.compactingCount++
	db.addInProgressCompaction(comp)
	db.mu.Unlock()

	db.compact(comp, c.done)
	if err := <-c.done; err != nil {
		if strings.Contains(err.Error(), "output table has no keys") {
			return -1
		}
		panic(err)
	}

	// not only logSeqNum, but also visibleSeqNum
	curLog := db.mu.versions.logSeqNum.Load()
	if db.mu.versions.visibleSeqNum.Load() < curLog {
		db.mu.versions.visibleSeqNum.Store(curLog)
	}

	// Retrieve jobID after compaction completes (it's set by compact1)
	jobID := comp.jobID
	return jobID
}

type fakeMemTable struct {
	keys []uint64
	v    uint64 // common value
}

// multilevelFlushWithResult runs multilevelFlush and returns the created output file numbers.
func multilevelFlushWithResult(db *DB, mem fakeMemTable, files []base.FileNum, outputLevel int) []uint64 {
	if outputLevel < 0 {
		panic(`outputLevel < 0`)
	}
	jobID := multilevelFlush(db, mem, files, outputLevel)
	if jobID == -1 {
		return nil
	}
	return collectCompactionOutputs(jobID)
}

func collectCompactionOutputs(jobID JobID) []uint64 {
	// Retrieve results from the map using jobID
	compactionResultsMu.Lock()
	outputs, ok := compactionResults[jobID]
	if !ok {
		panic(fmt.Sprintf("compactionResults[%d] not found", jobID))
	}
	// Delete map entry after retrieval
	delete(compactionResults, jobID)
	compactionResultsMu.Unlock()

	var nums []uint64
	for _, t := range outputs {
		nums = append(nums, uint64(t.FileNum))
	}
	return nums
}

// ----------------------------------
