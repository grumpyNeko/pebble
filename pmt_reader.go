package pebble

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
)

var _ internalIterator = (*pmtformat.Iter)(nil)

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

	seqNum := file.LargestSeqNum
	if seqNum < file.SmallestSeqNum {
		seqNum = file.SmallestSeqNum
	}

	iter, err := pmtformat.NewIter(f, f.Size(), seqNum)
	if err != nil {
		_ = f.Close()
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
