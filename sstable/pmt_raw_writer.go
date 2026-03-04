package sstable

import (
	"context"
	"encoding/binary"
	"github.com/cockroachdb/pebble/sstable/block"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
)

// writableWriter adapts objstorage.Writable to io.Writer and counts bytes written.
type writableWriter struct {
	w      objstorage.Writable
	n      uint64
	closed bool
}

func (ww *writableWriter) Write(p []byte) (int, error) {
	if ww.closed {
		return 0, errors.New("PMT0: write after close")
	}
	if err := ww.w.Write(p); err != nil {
		return 0, err
	}
	ww.n += uint64(len(p))
	return len(p), nil
}

// pmtRawWriter implements RawWriter for TableFormatPMT0 by delegating to pmtformat.Writer.
type pmtRawWriter struct {
	ww          *writableWriter
	pw          *pmtformat.Writer
	err         error
	o           WriterOptions
	prevUserKey []byte
	meta        WriterMetadata
}

func newPMTRawWriter(writable objstorage.Writable, o WriterOptions) RawWriter {
	ww := &writableWriter{w: writable}
	return &pmtRawWriter{
		ww: ww,
		pw: pmtformat.NewWriter(io.Writer(ww)),
		o:  o.ensureDefaults(),
	}
}

// Error returns the current accumulated error if any.
func (w *pmtRawWriter) Error() error { return w.err }

// Add adds a key-value pair to the sstable.
//
// PMT0 minimal support:
// - Only InternalKeyKindSet is allowed.
// - UserKey must be exactly 8 bytes (big-endian uint64).
// - Value must be exactly 8 bytes (big-endian uint64).
func (w *pmtRawWriter) Add(key InternalKey, value []byte, _ bool) error {
	if w.err != nil {
		return w.err
	}
	if key.Kind() != InternalKeyKindSet {
		w.err = errors.New("PMT0: only SET point keys are supported")
		return w.err
	}
	if len(key.UserKey) != 8 {
		w.err = errors.New("PMT0: user key must be 8 bytes (big-endian uint64)")
		return w.err
	}
	if len(value) != 8 {
		w.err = errors.New("PMT0: value must be 8 bytes (big-endian uint64)")
		return w.err
	}
	userKey := binary.BigEndian.Uint64(key.UserKey)
	val := binary.BigEndian.Uint64(value)
	if err := w.pw.Add(userKey, val); err != nil {
		w.err = err
		return err
	}
	// Update smallest/largest immediately for metadata.
	if !w.meta.HasPointKeys {
		w.meta.SetSmallestPointKey(key.Clone())
		w.meta.SetLargestPointKey(key.Clone())
		w.meta.updateSeqNum(key.SeqNum())
	} else {
		// Compare user keys to update extremes.
		if w.o.Comparer.Compare(key.UserKey, w.meta.SmallestPoint.UserKey) < 0 ||
			(w.o.Comparer.Compare(key.UserKey, w.meta.SmallestPoint.UserKey) == 0 && key.Trailer < w.meta.SmallestPoint.Trailer) {
			w.meta.SetSmallestPointKey(key.Clone())
		}
		if w.o.Comparer.Compare(key.UserKey, w.meta.LargestPoint.UserKey) > 0 ||
			(w.o.Comparer.Compare(key.UserKey, w.meta.LargestPoint.UserKey) == 0 && key.Trailer > w.meta.LargestPoint.Trailer) {
			w.meta.SetLargestPointKey(key.Clone())
		}
		w.meta.updateSeqNum(key.SeqNum())
	}
	// Track prev user key for ComparePrev.
	if cap(w.prevUserKey) < len(key.UserKey) {
		w.prevUserKey = make([]byte, len(key.UserKey))
	} else {
		w.prevUserKey = w.prevUserKey[:len(key.UserKey)]
	}
	copy(w.prevUserKey, key.UserKey)
	return nil
}

// AddWithBlobHandle is not supported by PMT0.
func (w *pmtRawWriter) AddWithBlobHandle(_ InternalKey, _ blob.InlineHandle, _ base.ShortAttribute, _ bool) error {
	w.err = errors.New("PMT0: blob values are not supported")
	return w.err
}

// EncodeSpan is not supported by PMT0 (no range keys).
func (w *pmtRawWriter) EncodeSpan(_ keyspan.Span) error {
	w.err = errors.New("PMT0: range keys are not supported")
	return w.err
}

// EstimatedSize returns a rough estimate based on pmtformat constants:
// pages = ceil(entries/256), size ≈ pages*4096 + 4096.
func (w *pmtRawWriter) EstimatedSize() uint64 {
	// We don't have entry count without adding state to pmtformat.Writer.
	// Give a conservative lower bound using bytes written so far + index page.
	const indexPage = 4096
	return w.ww.n + indexPage
}

// ComparePrev compares provided user key to the last point key's user key.
func (w *pmtRawWriter) ComparePrev(k []byte) int {
	if len(w.prevUserKey) == 0 {
		return +1
	}
	return w.o.Comparer.Compare(k, w.prevUserKey)
}

// SetSnapshotPinnedProperties is a no-op for PMT0.
func (w *pmtRawWriter) SetSnapshotPinnedProperties(_, _, _ uint64) {}

// Close finalizes the PMT writer and finishes the underlying object.
func (w *pmtRawWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if err := w.pw.Close(); err != nil {
		w.err = err
		return err
	}
	w.ww.closed = true
	if err := w.ww.w.Finish(); err != nil {
		w.err = err
		return err
	}
	w.meta.Size = w.ww.n
	return nil
}

// Metadata returns minimal metadata for the finished table.
func (w *pmtRawWriter) Metadata() (*WriterMetadata, error) {
	return &w.meta, nil
}

// The following low-level copy/transform APIs are not supported for PMT0.

func (w *pmtRawWriter) rewriteSuffixes(_ *Reader, _ []byte, _ WriterOptions, _, _ []byte, _ int) error {
	w.err = errors.New("PMT0: rewriteSuffixes not supported")
	return w.err
}

func (w *pmtRawWriter) copyDataBlocks(_ context.Context, _ []indexEntry, _ objstorage.ReadHandle) error {
	w.err = errors.New("PMT0: copyDataBlocks not supported")
	return w.err
}

func (w *pmtRawWriter) addDataBlock(_, _ []byte, _ block.HandleWithProperties) error {
	w.err = errors.New("PMT0: addDataBlock not supported")
	return w.err
}

func (w *pmtRawWriter) copyFilter(_ []byte, _ string) error {
	w.err = errors.New("PMT0: copyFilter not supported")
	return w.err
}

func (w *pmtRawWriter) copyProperties(_ Properties) {
	// No-op. PMT0 does not map properties.
}
