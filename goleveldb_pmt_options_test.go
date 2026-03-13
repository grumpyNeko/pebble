package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	goleveldb "github.com/syndtr/goleveldb/leveldb"
	goleveldbjournal "github.com/syndtr/goleveldb/leveldb/journal"
	goleveldbmemdb "github.com/syndtr/goleveldb/leveldb/memdb"
	golevelopt "github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbstorage "github.com/syndtr/goleveldb/leveldb/storage"
)

func Test_goleveldb_wa(t *testing.T) {
	path := "ww/goleveldb"
	db := mustOpenGoLevelDB(path, true)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close goleveldb failed: %v", err)
		}
	}()

	const times = 128
	datas := make([]uint64, 0, times<<20)
	for i := 0; i < times; i++ {
		path := filepath.Join(datasetPath, fmt.Sprintf("normal_plus_round_%03d.bin", i))
		d := LoadDataFile(path)
		datas = append(datas, d.Keys...)
	}

	writeStart := time.Now()
	for i := 0; i < times; i++ {
		keys := datas[i<<20 : (i+1)<<20]
		batchWriteGoLevelDB(db, keys, uint64(i))
		println(fmt.Sprintf("goleveldb write done round%d", i))
	}
	writeTime := time.Since(writeStart).Milliseconds()

	compactWaitStart := time.Now()
	waitForGoLevelDBCompaction(db, 30*time.Minute)
	compactTime := time.Since(compactWaitStart).Milliseconds()

	totalWrite, totalTableCount := totalWriteGoLevelDB(db)
	println(fmt.Sprintf("w=%dMB \t tables=%d \t time=%dms", totalWrite, totalTableCount, writeTime+compactTime))

	statsText, err := db.GetProperty("leveldb.stats")
	if err != nil {
		t.Fatalf("GetProperty(leveldb.stats) failed: %v", err)
	}
	println(statsText)

	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 1)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 4)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 8)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 12)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 16)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 24)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 32)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 48)
	benchmarkRandomReadMultiThreadGoLevelDB(datas, db, 64)
}

// Best-effort mapping of Test_pebble_wa / pmtOptions onto goleveldb.
// goleveldb has no public equivalent for DisableWAL, DisableAutomaticCompactions,
// MemTableStopWritesThreshold, FlushSplitBytes, or MaxConcurrentCompactions.
func goLevelDBWAOptions() *golevelopt.Options {
	const pageSize = 4096

	return &golevelopt.Options{
		NoSync:                       true,
		WriteBuffer:                  1 << 24,
		CompactionTableSize:          2 << 20,
		CompactionL0Trigger:          3,
		WriteL0PauseTrigger:          1024,
		WriteL0SlowdownTrigger:       1024,
		BlockRestartInterval:         1,
		BlockCacheCapacity:           1024 * pageSize,
		Compression:                  golevelopt.NoCompression,
		DisableLargeBatchTransaction: true,
	}
}

func goLevelDBInMemory() bool {
	return os.Getenv("GOLEVELDB_IN_MEMORY") == "1"
}

func mustOpenGoLevelDB(path string, clear bool) *goleveldb.DB {
	if goLevelDBInMemory() {
		db, err := goleveldb.Open(goleveldbstorage.NewMemStorage(), goLevelDBWAOptions())
		if err != nil {
			panic(err)
		}
		return db
	}
	if clear {
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
	}
	db, err := goleveldb.OpenFile(path, goLevelDBWAOptions())
	if err != nil {
		panic(err)
	}
	return db
}

func batchWriteGoLevelDB(db *goleveldb.DB, keys []uint64, v uint64) {
	batch := new(goleveldb.Batch)
	value := BigEndian(v)
	for _, key := range keys {
		batch.Put(BigEndian(key), value)
	}
	if err := db.Write(batch, &golevelopt.WriteOptions{Sync: false}); err != nil {
		panic(err)
	}
}

func mustGoLevelDBStats(db *goleveldb.DB) goleveldb.DBStats {
	var stats goleveldb.DBStats
	if err := db.Stats(&stats); err != nil {
		panic(err)
	}
	return stats
}

func totalWriteGoLevelDB(db *goleveldb.DB) (ioWriteMB int, totalTableCount int) {
	stats := mustGoLevelDBStats(db)
	// Use SST output bytes only, so WAL/manifest writes do not inflate WA.
	for _, writeBytes := range stats.LevelWrite {
		ioWriteMB += int(writeBytes >> 20)
	}
	for _, count := range stats.LevelTablesCounts {
		totalTableCount += count
	}
	return
}

type goleveldbInternalKey []byte

type goleveldbMemDBMirror struct {
	db  unsafe.Pointer
	DB  *goleveldbmemdb.DB
	ref int32
}

type goleveldbTFileMirror struct {
	fd       goleveldbstorage.FileDesc
	seekLeft int32
	size     int64
	imin     goleveldbInternalKey
	imax     goleveldbInternalKey
}

type goleveldbTFiles []*goleveldbTFileMirror

type goleveldbTOpsMirror struct{}

type goleveldbIComparerMirror struct{}

type goleveldbSessionMirror struct {
	stNextFileNum    int64
	stJournalNum     int64
	stPrevJournalNum int64
	stTempFileNum    int64
	stSeqNum         uint64
	stor             unsafe.Pointer
	storLock         goleveldbstorage.Locker
	o                unsafe.Pointer
	icmp             *goleveldbIComparerMirror
	tops             *goleveldbTOpsMirror
	manifest         *goleveldbjournal.Writer
	manifestWriter   goleveldbstorage.Writer
	manifestFd       goleveldbstorage.FileDesc
	stCompPtrs       []goleveldbInternalKey
	stVersion        *goleveldbVersionMirror
}

type goleveldbVersionMirror struct {
	id     int64
	s      *goleveldbSessionMirror
	levels []goleveldbTFiles
}

type goleveldbDBMirror struct {
	seq           uint64
	cWriteDelay   int64
	cWriteDelayN  int32
	inWritePaused int32
	aliveSnaps    int32
	aliveIters    int32
	s             *goleveldbSessionMirror
	memMu         sync.RWMutex
	memPool       chan *goleveldbmemdb.DB
	mem           *goleveldbMemDBMirror
	frozenMem     *goleveldbMemDBMirror
}

const (
	goleveldbKeyTypeDel  = uint8(0)
	goleveldbKeyTypeVal  = uint8(1)
	goleveldbKeyTypeSeek = goleveldbKeyTypeVal
	goleveldbKeyMaxSeq   = (uint64(1) << 56) - 1
)

func makeGoLevelDBInternalKey(ukey []byte, seq uint64, kt uint8) goleveldbInternalKey {
	buf := make([]byte, len(ukey)+8)
	copy(buf, ukey)
	binary.LittleEndian.PutUint64(buf[len(ukey):], (seq<<8)|uint64(kt))
	return goleveldbInternalKey(buf)
}

func parseGoLevelDBInternalKey(ik []byte) (ukey []byte, seq uint64, kt uint8) {
	if len(ik) < 8 {
		panic("invalid internal key")
	}
	num := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	return ik[:len(ik)-8], uint64(num >> 8), uint8(num & 0xff)
}

func compareGoLevelDBInternalKey(a, b []byte) int {
	ua, anumU, _ := parseGoLevelDBInternalKey(a)
	ub, bnumU, _ := parseGoLevelDBInternalKey(b)
	if x := bytes.Compare(ua, ub); x != 0 {
		return x
	}
	if anumU > bnumU {
		return -1
	}
	if anumU < bnumU {
		return 1
	}
	return 0
}

func (ik goleveldbInternalKey) ukey() []byte {
	if len(ik) < 8 {
		panic("invalid internal key")
	}
	return ik[:len(ik)-8]
}

func (t *goleveldbTFileMirror) overlaps(ukey []byte) bool {
	return bytes.Compare(ukey, t.imin.ukey()) >= 0 && bytes.Compare(ukey, t.imax.ukey()) <= 0
}

func (tf goleveldbTFiles) searchMax(ikey []byte) int {
	return sort.Search(len(tf), func(i int) bool {
		return compareGoLevelDBInternalKey(tf[i].imax, ikey) >= 0
	})
}

//go:linkname goleveldbTableNeedCompaction github.com/syndtr/goleveldb/leveldb.(*DB).tableNeedCompaction
func goleveldbTableNeedCompaction(db *goleveldb.DB) bool

//go:linkname goleveldbHasFrozenMem github.com/syndtr/goleveldb/leveldb.(*DB).hasFrozenMem
func goleveldbHasFrozenMem(db *goleveldb.DB) bool

//go:linkname goleveldbTableFind github.com/syndtr/goleveldb/leveldb.(*tOps).find
func goleveldbTableFind(tops *goleveldbTOpsMirror, f *goleveldbTFileMirror, key []byte, ro *golevelopt.ReadOptions) (rkey, rvalue []byte, err error)

func goleveldbMemGet(m *goleveldbMemDBMirror, ikey []byte) (ok bool, value []byte, err error) {
	if m == nil || m.DB == nil {
		return false, nil, nil
	}
	mk, mv, err := m.DB.Find(ikey)
	if err == nil {
		ukey, _, kt := parseGoLevelDBInternalKey(mk)
		if bytes.Equal(ukey, goleveldbInternalKey(ikey).ukey()) {
			if kt == goleveldbKeyTypeDel {
				return true, nil, goleveldb.ErrNotFound
			}
			return true, mv, nil
		}
		return false, nil, nil
	}
	if err == goleveldbmemdb.ErrNotFound {
		return false, nil, nil
	}
	return true, nil, err
}

func goleveldbGetWithFilesAccessed(db *goleveldb.DB, key []byte) (value []byte, filesAccessed int, err error) {
	mirror := (*goleveldbDBMirror)(unsafe.Pointer(db))
	seq := atomic.LoadUint64(&mirror.seq)
	ikey := makeGoLevelDBInternalKey(key, seq, goleveldbKeyTypeSeek)

	for _, mem := range [...]*goleveldbMemDBMirror{mirror.mem, mirror.frozenMem} {
		if ok, mv, me := goleveldbMemGet(mem, ikey); ok {
			if mv != nil {
				return append([]byte{}, mv...), 0, me
			}
			return nil, 0, me
		}
	}

	v := mirror.s.stVersion
	ukey := key
	var (
		zfound bool
		zseq   uint64
		zkt    uint8
		zval   []byte
	)

	for level, tables := range v.levels {
		if len(tables) == 0 {
			continue
		}

		if level == 0 {
			for _, t := range tables {
				if !t.overlaps(ukey) {
					continue
				}
				filesAccessed++
				fikey, fval, ferr := goleveldbTableFind(mirror.s.tops, t, ikey, nil)
				if ferr == goleveldb.ErrNotFound {
					continue
				}
				if ferr != nil {
					return nil, filesAccessed, ferr
				}
				fukey, fseq, fkt := parseGoLevelDBInternalKey(fikey)
				if !bytes.Equal(ukey, fukey) {
					continue
				}
				if fseq >= zseq {
					zfound = true
					zseq = fseq
					zkt = fkt
					zval = fval
				}
			}
			if zfound {
				if zkt == goleveldbKeyTypeDel {
					return nil, filesAccessed, goleveldb.ErrNotFound
				}
				return zval, filesAccessed, nil
			}
			continue
		}

		i := tables.searchMax(ikey)
		if i >= len(tables) {
			continue
		}
		t := tables[i]
		if bytes.Compare(ukey, t.imin.ukey()) < 0 {
			continue
		}

		filesAccessed++
		fikey, fval, ferr := goleveldbTableFind(mirror.s.tops, t, ikey, nil)
		if ferr == goleveldb.ErrNotFound {
			continue
		}
		if ferr != nil {
			return nil, filesAccessed, ferr
		}
		fukey, _, fkt := parseGoLevelDBInternalKey(fikey)
		if !bytes.Equal(ukey, fukey) {
			continue
		}
		if fkt == goleveldbKeyTypeDel {
			return nil, filesAccessed, goleveldb.ErrNotFound
		}
		return fval, filesAccessed, nil
	}

	return nil, filesAccessed, goleveldb.ErrNotFound
}

func waitForGoLevelDBCompaction(db *goleveldb.DB, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		if !goleveldbHasFrozenMem(db) && !goleveldbTableNeedCompaction(db) {
			return
		}
		if time.Now().After(deadline) {
			statsText, err := db.GetProperty("leveldb.stats")
			if err != nil {
				panic(err)
			}
			panic(fmt.Sprintf("waitForGoLevelDBCompaction timeout\n%s", statsText))
		}
		println("wait for compact")
		time.Sleep(500 * time.Millisecond)
	}
}

func benchmarkRandomReadMultiThreadGoLevelDB(data []uint64, db *goleveldb.DB, concurrency int) {
	const readN = 1 << 20
	if concurrency < 1 {
		panic("concurrency < 1")
	}
	if concurrency > readN {
		panic("concurrency > readN")
	}
	if readN > len(data) {
		panic("readN > len(data)")
	}

	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	startStats := mustGoLevelDBStats(db)
	var wg sync.WaitGroup
	var totalFilesAccessed atomic.Int64
	var totalReads atomic.Int64
	chunk := (readN + concurrency - 1) / concurrency
	start := time.Now()
	for g := 0; g < concurrency; g++ {
		begin := g * chunk
		end := begin + chunk
		if end > readN {
			end = readN
		}
		keys := data[begin:end]
		wg.Add(1)
		go func(keys []uint64) {
			defer wg.Done()
			var localFilesAccessed int64
			for _, k := range keys {
				_, filesAccessed, err := goleveldbGetWithFilesAccessed(db, BigEndian(k))
				if err != nil {
					panic(err)
				}
				localFilesAccessed += int64(filesAccessed)
			}
			totalFilesAccessed.Add(localFilesAccessed)
			totalReads.Add(int64(len(keys)))
		}(keys)
	}
	wg.Wait()
	cost := time.Since(start).Milliseconds()
	endStats := mustGoLevelDBStats(db)
	avgFilesAccessed := float64(totalFilesAccessed.Load()) / float64(totalReads.Load())
	println(fmt.Sprintf(
		"randomread%d=%dms, reads=%d, avgFilesAccessed=%.4f, blockCache=%d->%d, openedTables=%d->%d",
		concurrency, cost, totalReads.Load(),
		avgFilesAccessed,
		startStats.BlockCacheSize, endStats.BlockCacheSize,
		startStats.OpenedTablesCount, endStats.OpenedTablesCount,
	))
}
