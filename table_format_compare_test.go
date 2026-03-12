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
	"github.com/cockroachdb/pebble/objstorage"
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
	// generate keys and vals
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

	levelMem := vfs.NewMem()
	const levelFilename = "point_lookup_compare_leveldb.sst"
	err := buildSST(levelMem, levelFilename, keys, vals, 0, sstable.WriterOptions{
		TableFormat:          sstable.TableFormatLevelDB,
		Compression:          SnappyCompression,
		BlockRestartInterval: 16,
	})
	if err != nil {
		t.Fatalf("build leveldb sstable failed: %v", err)
	}
	logSSTSize(t, levelMem, levelFilename, "leveldb")

	pmtMem := vfs.NewMem()
	const pmtFilename = "point_lookup_compare_pmt.sst"
	err = buildSST(pmtMem, pmtFilename, keys, vals, 1, sstable.WriterOptions{
		TableFormat: sstable.TableFormatPMT0,
	})
	if err != nil {
		t.Fatalf("build pmt sstable failed: %v", err)
	}
	logSSTSize(t, pmtMem, pmtFilename, "pmt")

	levelIter, levelClose, err := openLevelDBIter(levelMem, levelFilename)
	if err != nil {
		t.Fatalf("open leveldb iter failed: %v", err)
	}
	levelElapsed := benchmarkRandomPointLookup(levelIter, queries, warmupN)
	if err := levelClose(); err != nil {
		t.Fatalf("close leveldb iter failed: %v", err)
	}

	pmtIter, pmtClose, err := openPMTIter(pmtMem, pmtFilename, base.SeqNum(1))
	if err != nil {
		t.Fatalf("open pmt iter failed: %v", err)
	}
	pmtElapsed := benchmarkRandomPointLookup(pmtIter, queries, warmupN)
	if err := pmtClose(); err != nil {
		t.Fatalf("close pmt iter failed: %v", err)
	}

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

func benchmarkRandomPointLookup(
	iter base.InternalIterator, queries []uint64, warmupN int,
) time.Duration {
	if warmupN > 0 {
		_ = runRandomPointLookups(iter, queries[:warmupN])
	}
	return runRandomPointLookups(iter, queries)
}

func buildSST(
	fs vfs.FS, filename string, keys []uint64, vals []uint64, seqNum base.SeqNum, writerOpts sstable.WriterOptions,
) error {
	f, err := fs.Create(filename, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	for i, k := range keys {
		err := w.Add(
			base.MakeInternalKey(BigEndian(k), seqNum, base.InternalKeyKindSet),
			BigEndian(vals[i]),
			false, /* forceObsolete */
		)
		if err != nil {
			_ = w.Close()
			return err
		}
	}
	return w.Close()
}

func logSSTSize(t *testing.T, fs vfs.FS, filename string, label string) {
	t.Helper()
	info, err := fs.Stat(filename)
	if err != nil {
		t.Fatalf("stat %s sstable failed: %v", label, err)
	}
	t.Logf("%s file size=%d bytes", label, info.Size())
}

func openReadable(fs vfs.FS, filename string) (objstorage.Readable, error) {
	f, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	return sstable.NewSimpleReadable(f)
}

func openLevelDBIter(
	fs vfs.FS, filename string,
) (base.InternalIterator, func() error, error) {
	readable, err := openReadable(fs, filename)
	if err != nil {
		return nil, nil, err
	}
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{})
	if err != nil {
		_ = readable.Close()
		return nil, nil, err
	}
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil)
	if err != nil {
		_ = reader.Close()
		return nil, nil, err
	}
	closeFn := func() error {
		if err := iter.Close(); err != nil {
			return err
		}
		return reader.Close()
	}
	return iter, closeFn, nil
}

func openPMTIter(
	fs vfs.FS, filename string, seqNum base.SeqNum,
) (base.InternalIterator, func() error, error) {
	readable, err := openReadable(fs, filename)
	if err != nil {
		return nil, nil, err
	}
	iter, err := pmtformat.NewIter(readable, readable.Size(), seqNum)
	if err != nil {
		_ = readable.Close()
		return nil, nil, err
	}
	return iter, iter.Close, nil
}
