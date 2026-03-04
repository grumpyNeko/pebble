package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
)

var _ internalIterator = (*pmtformat.Iter)(nil)
var _ pmtformat.Readable = (*pmtCachedReadable)(nil)

const pmtPageSize = 4096

type pmtCachedReadable struct {
	fileNum     base.DiskFileNum
	readable    objstorage.Readable
	readHandle  objstorage.ReadHandle
	cacheHandle *cache.Handle
	cacheReads  bool
}

func (r *pmtCachedReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
	if len(p) != pmtPageSize || off < 0 || off%pmtPageSize != 0 {
		panic(fmt.Sprintf("pebble: PMT reader only supports 4KB aligned reads: len=%d off=%d", len(p), off))
	}

	if !r.cacheReads || r.cacheHandle == nil {
		if err := r.readFromSource(ctx, p, off); err != nil {
			panic(fmt.Sprintf("pebble: PMT reader read failed at off=%d: %v", off, err))
		}
		return nil
	}

	cv, rh, _, _, err := r.cacheHandle.GetWithReadHandle(ctx, r.fileNum, uint64(off))
	if err != nil {
		panic(fmt.Sprintf("pebble: PMT cache get failed at off=%d: %v", off, err))
	}
	if cv != nil {
		copy(p, cv.RawBuffer())
		cv.Release()
		return nil
	}

	v := cache.Alloc(len(p))
	buf := v.RawBuffer()
	if err := r.readFromSource(ctx, buf, off); err != nil {
		rh.SetReadError(err)
		v.Release()
		panic(fmt.Sprintf("pebble: PMT reader read failed at off=%d: %v", off, err))
	}
	rh.SetReadValue(v)
	copy(p, buf)
	// SetReadValue takes ownership for the cache; release local reference.
	v.Release()
	return nil
}

func (r *pmtCachedReadable) readFromSource(ctx context.Context, p []byte, off int64) error {
	if r.readHandle != nil {
		return r.readHandle.ReadAt(ctx, p, off)
	}
	return r.readable.ReadAt(ctx, p, off)
}

func (r *pmtCachedReadable) Close() error {
	var err error
	if r.readHandle != nil {
		if closeErr := r.readHandle.Close(); err == nil {
			err = closeErr
		}
		r.readHandle = nil
	}
	if r.readable != nil {
		if closeErr := r.readable.Close(); err == nil {
			err = closeErr
		}
		r.readable = nil
	}
	return err
}

func (h *fileCacheHandle) newPMTIters(
	ctx context.Context,
	file *manifest.TableMetadata,
	opts *IterOptions,
	internalOpts internalIterOpts,
	kinds iterKinds,
) (iterSet, error) {
	var iters iterSet
	if !kinds.Point() {
		return iters, nil
	}
	if file == nil {
		return iterSet{}, errors.New("pebble: PMT reader: nil table metadata")
	}
	if !file.HasPointKeys || file.HasRangeKeys {
		return iterSet{}, errors.Errorf("pebble: PMT reader: invalid key kinds for table %s", file.FileNum)
	}

	f, err := h.objProvider.OpenForReading(
		ctx,
		base.FileTypeTable,
		file.FileBacking.DiskFileNum,
		objstorage.OpenOptions{MustExist: true},
	)
	if err != nil {
		return iterSet{}, err
	}
	readHandle := f.NewReadHandle(objstorage.NoReadBefore)
	if internalOpts.compaction && readHandle != nil {
		readHandle.SetupForCompaction()
	}
	cachedReadable := &pmtCachedReadable{
		fileNum:     file.FileBacking.DiskFileNum,
		readable:    f,
		readHandle:  readHandle,
		cacheHandle: h.blockCacheHandle,
		cacheReads:  !internalOpts.compaction,
	}

	seqNum := file.LargestSeqNum
	if seqNum < file.SmallestSeqNum {
		seqNum = file.SmallestSeqNum
	}

	iter, err := pmtformat.NewIter(cachedReadable, f.Size(), seqNum)
	if err != nil {
		_ = cachedReadable.Close()
		return iterSet{}, err
	}
	iter.SetContext(ctx)
	if !internalOpts.compaction && opts != nil {
		iter.SetBounds(opts.GetLowerBound(), opts.GetUpperBound())
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return iterSet{}, err
		}
	}
	iters.point = iter
	return iters, nil
}
