package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
	"github.com/cockroachdb/pebble/vfs"
)

func Test_TableFormat(t *testing.T) {
	const (
		queryCount = 1 << 20
	)

	maxEntries := pmtformat.MaxEntriesPerTable()
	rng := rand.New(rand.NewSource(20260306))
	keySet := make(map[uint64]struct{}, maxEntries)
	keys := make([]uint64, 0, maxEntries)
	for len(keys) < maxEntries {
		k := uint64(rng.Uint32())
		if _, ok := keySet[k]; ok {
			continue
		}
		keySet[k] = struct{}{}
		keys = append(keys, k)
	}
	slices.Sort(keys)

	vals := make([]uint64, len(keys))
	for i := range vals {
		vals[i] = rng.Uint64()
	}

	queries := make([]uint64, queryCount)
	for i := range queries {
		queries[i] = keys[rng.Intn(len(keys))]
	}
	var warmupN = 1 << 14
	if warmupN > len(queries) {
		warmupN = len(queries)
	}

	levelElapsed := measureRandomPointLookupLevelDB(
		t, keys, vals, queries, warmupN, SnappyCompression, 16,
	)
	pmtElapsed := measureRandomPointLookupPMT(t, keys, vals, queries, warmupN)

	levelNSPerOp := float64(levelElapsed.Nanoseconds()) / float64(queryCount)
	pmtNSPerOp := float64(pmtElapsed.Nanoseconds()) / float64(queryCount)
	t.Logf(
		"entries=%d, queries=%d, leveldb=%s(%.1f ns/op), pmt=%s(%.1f ns/op), pmt/leveldb=%.3f",
		len(keys), queryCount,
		levelElapsed, levelNSPerOp,
		pmtElapsed, pmtNSPerOp,
		float64(pmtElapsed.Nanoseconds())/float64(levelElapsed.Nanoseconds()),
	)
}

func runRandomPointLookups(iter base.InternalIterator, queries []uint64) time.Duration {
	var keyBuf [8]byte
	start := time.Now()
	for _, k := range queries {
		binary.BigEndian.PutUint64(keyBuf[:], k)
		kv := iter.SeekGE(keyBuf[:], base.SeekGEFlagsNone)
		if kv == nil {
			panic(fmt.Sprintf("query key not found: %d", k))
		}
	}
	if err := iter.Error(); err != nil {
		panic(err)
	}
	return time.Since(start)
}

func measureRandomPointLookupLevelDB(
	t *testing.T,
	keys []uint64,
	vals []uint64,
	queries []uint64,
	warmupN int,
	compression Compression,
	blockRestartInterval int,
) time.Duration {
	t.Helper()

	mem := vfs.NewMem()
	const filename = "point_lookup_compare_leveldb.sst"
	f, err := mem.Create(filename, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("create sstable failed: %v", err)
	}

	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat:          sstable.TableFormatLevelDB,
		Compression:          compression,
		BlockRestartInterval: blockRestartInterval,
	})
	for i, k := range keys {
		if err := w.Add(base.MakeInternalKey(BigEndian(k), 0, base.InternalKeyKindSet), BigEndian(vals[i]), false /* forceObsolete */); err != nil {
			t.Fatalf("writer.Add failed (leveldb key=%d): %v", k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close failed (leveldb): %v", err)
	}
	levelInfo, err := mem.Stat(filename)
	if err != nil {
		t.Fatalf("stat leveldb sstable failed: %v", err)
	}
	t.Logf("leveldb file size=%d bytes", levelInfo.Size())

	rf, err := mem.Open(filename)
	if err != nil {
		t.Fatalf("open sstable failed: %v", err)
	}
	readable, err := sstable.NewSimpleReadable(rf)
	if err != nil {
		t.Fatalf("new simple readable failed: %v", err)
	}
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("new sstable reader failed: %v", err)
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil)
	if err != nil {
		t.Fatalf("new sstable iterator failed: %v", err)
	}
	if warmupN > 0 {
		_ = runRandomPointLookups(iter, queries[:warmupN])
	}
	elapsed := runRandomPointLookups(iter, queries)
	if err := iter.Close(); err != nil {
		t.Fatalf("close sstable iter failed: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close sstable reader failed: %v", err)
	}
	return elapsed
}

func measureRandomPointLookupPMT(
	t *testing.T, keys []uint64, vals []uint64, queries []uint64, warmupN int,
) time.Duration {
	t.Helper()

	mem := vfs.NewMem()
	const filename = "point_lookup_compare_pmt.sst"
	f, err := mem.Create(filename, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("create pmt sstable failed: %v", err)
	}

	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat: sstable.TableFormatPMT0,
	})
	for i, k := range keys {
		if err := w.Add(base.MakeInternalKey(BigEndian(k), 1, base.InternalKeyKindSet), BigEndian(vals[i]), false /* forceObsolete */); err != nil {
			t.Fatalf("writer.Add failed (pmt key=%d): %v", k, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close failed (pmt): %v", err)
	}
	pmtInfo, err := mem.Stat(filename)
	if err != nil {
		t.Fatalf("stat pmt sstable failed: %v", err)
	}
	t.Logf("pmt file size=%d bytes", pmtInfo.Size())

	rf, err := mem.Open(filename)
	if err != nil {
		t.Fatalf("open pmt sstable failed: %v", err)
	}
	readable, err := sstable.NewSimpleReadable(rf)
	if err != nil {
		t.Fatalf("new simple readable failed: %v", err)
	}

	iter, err := pmtformat.NewIter(readable, readable.Size(), base.SeqNum(1))
	if err != nil {
		t.Fatalf("new PMT iter failed: %v", err)
	}
	if warmupN > 0 {
		_ = runRandomPointLookups(iter, queries[:warmupN])
	}
	elapsed := runRandomPointLookups(iter, queries)
	if err := iter.Close(); err != nil {
		t.Fatalf("close PMT iter failed: %v", err)
	}
	return elapsed
}
