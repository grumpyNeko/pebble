package pmtformat

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// Readable is the minimal reader contract required by Iter.
type Readable interface {
	ReadAt(ctx context.Context, p []byte, off int64) error
	Close() error
}

// Iter iterates PMT point keys lazily by loading one DataPage at a time.
type Iter struct {
	r            Readable
	numDataPages int
	index        []uint64

	ctx context.Context

	pageBuf       []byte
	loadedPage    int
	loadedEntries int

	lower, upper       []byte
	hasLower, hasUpper bool
	lowerKey, upperKey uint64

	seqNum base.SeqNum

	pageIdx  int
	entryIdx int
	state    iterState

	keyBuf [8]byte
	valBuf [8]byte
	kv     base.InternalKV

	err    error
	closed bool
}

type iterState uint8

const (
	iterStateUnpositioned iterState = iota
	iterStateBOF
	iterStatePositioned
	iterStateEOF
)

var _ base.InternalIterator = (*Iter)(nil)

// NewIter creates an iterator over a PMT table.
func NewIter(r Readable, size int64, seqNum base.SeqNum) (*Iter, error) {
	if r == nil {
		return nil, errors.New("pmtformat: nil readable")
	}
	if size < int64(indexPageSize) {
		return nil, errors.Newf("pmtformat: file too small: %d bytes", size)
	}
	if size%int64(dataPageSize) != 0 {
		return nil, errors.Newf("pmtformat: invalid file size: %d (must be multiple of %d)", size, dataPageSize)
	}

	totalPages := int(size / int64(dataPageSize))
	if totalPages < 1 {
		return nil, errors.Newf("pmtformat: invalid page count: %d", totalPages)
	}
	numDataPages := totalPages - 1
	if numDataPages > maxDataPages {
		return nil, errors.Newf("pmtformat: too many data pages: %d > %d", numDataPages, maxDataPages)
	}

	iter := &Iter{
		r:             r,
		numDataPages:  numDataPages,
		index:         make([]uint64, numDataPages),
		ctx:           context.Background(),
		pageBuf:       make([]byte, dataPageSize),
		loadedPage:    -1,
		loadedEntries: 0,
		seqNum:        seqNum,
		state:         iterStateUnpositioned,
		pageIdx:       -1,
		entryIdx:      -1,
	}

	indexPage := make([]byte, indexPageSize)
	indexPageOffset := int64(numDataPages * dataPageSize)
	if err := iter.r.ReadAt(iter.ctx, indexPage, indexPageOffset); err != nil {
		return nil, err
	}
	for i := 0; i < numDataPages; i++ {
		offset := i * indexEntrySize
		iter.index[i] = binary.BigEndian.Uint64(indexPage[offset : offset+indexEntrySize])
	}

	return iter, nil
}

func (i *Iter) String() string {
	return "pmtformat-iter"
}

func (i *Iter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}

func (i *Iter) SeekGE(key []byte, _ base.SeekGEFlags) *base.InternalKV {
	if i.err != nil {
		return nil
	}
	k, err := decodeUserKey(key)
	if err != nil {
		i.err = err
		return nil
	}
	if i.hasLower && k < i.lowerKey {
		k = i.lowerKey
	}
	if i.hasUpper && k >= i.upperKey {
		i.setEOF()
		return nil
	}
	return i.seekGE(k)
}

func (i *Iter) SeekPrefixGE(_ []byte, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.SeekGE(key, flags)
}

func (i *Iter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	if i.err != nil {
		return nil
	}
	k, err := decodeUserKey(key)
	if err != nil {
		i.err = err
		return nil
	}
	if i.hasUpper && k > i.upperKey {
		k = i.upperKey
	}
	if i.hasLower && k <= i.lowerKey {
		i.setBOF()
		return nil
	}
	return i.seekLT(k)
}

func (i *Iter) First() *base.InternalKV {
	if i.err != nil {
		return nil
	}
	if i.hasLower {
		return i.seekGE(i.lowerKey)
	}
	if i.numDataPages == 0 {
		i.setEOF()
		return nil
	}
	if err := i.loadPage(0); err != nil {
		i.err = err
		return nil
	}
	return i.positionAt(0, 0)
}

func (i *Iter) Last() *base.InternalKV {
	if i.err != nil {
		return nil
	}
	if i.hasUpper {
		return i.seekLT(i.upperKey)
	}
	if i.numDataPages == 0 {
		i.setBOF()
		return nil
	}
	page := i.numDataPages - 1
	if err := i.loadPage(page); err != nil {
		i.err = err
		return nil
	}
	return i.positionAt(page, i.loadedEntries-1)
}

func (i *Iter) Next() *base.InternalKV {
	if i.err != nil {
		return nil
	}
	switch i.state {
	case iterStateBOF:
		return i.First()
	case iterStatePositioned:
		return i.stepForward()
	default:
		return nil
	}
}

func (i *Iter) NextPrefix(succKey []byte) *base.InternalKV {
	return i.SeekGE(succKey, base.SeekGEFlagsNone)
}

func (i *Iter) Prev() *base.InternalKV {
	if i.err != nil {
		return nil
	}
	switch i.state {
	case iterStateEOF:
		return i.Last()
	case iterStatePositioned:
		return i.stepBackward()
	default:
		return nil
	}
}

func (i *Iter) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
	i.hasLower = false
	i.hasUpper = false
	if lower != nil {
		k, err := decodeUserKey(lower)
		if err != nil {
			i.err = err
			return
		}
		i.lowerKey = k
		i.hasLower = true
	}
	if upper != nil {
		k, err := decodeUserKey(upper)
		if err != nil {
			i.err = err
			return
		}
		i.upperKey = k
		i.hasUpper = true
	}
	i.state = iterStateUnpositioned
	i.pageIdx = -1
	i.entryIdx = -1
}

func (i *Iter) SetContext(ctx context.Context) {
	if ctx == nil {
		i.ctx = context.Background()
		return
	}
	i.ctx = ctx
}

func (i *Iter) Error() error {
	return i.err
}

func (i *Iter) Close() error {
	if i.closed {
		return i.err
	}
	i.closed = true
	if err := i.r.Close(); err != nil && i.err == nil {
		i.err = err
	}
	return i.err
}

func (i *Iter) seekGE(k uint64) *base.InternalKV {
	if i.numDataPages == 0 {
		i.setEOF()
		return nil
	}
	page := sort.Search(len(i.index), func(j int) bool {
		return i.index[j] >= k
	})
	if page >= i.numDataPages {
		i.setEOF()
		return nil
	}
	if err := i.loadPage(page); err != nil {
		i.err = err
		return nil
	}
	entry := sort.Search(i.loadedEntries, func(j int) bool {
		return i.loadedKey(j) >= k
	})
	for page < i.numDataPages {
		if entry < i.loadedEntries {
			return i.positionAt(page, entry)
		}
		page++
		if page >= i.numDataPages {
			break
		}
		if err := i.loadPage(page); err != nil {
			i.err = err
			return nil
		}
		entry = 0
	}
	i.setEOF()
	return nil
}

func (i *Iter) seekLT(k uint64) *base.InternalKV {
	if i.numDataPages == 0 {
		i.setBOF()
		return nil
	}
	page := sort.Search(len(i.index), func(j int) bool {
		return i.index[j] >= k
	})
	if page >= i.numDataPages {
		page = i.numDataPages - 1
		if err := i.loadPage(page); err != nil {
			i.err = err
			return nil
		}
		return i.positionAt(page, i.loadedEntries-1)
	}
	if err := i.loadPage(page); err != nil {
		i.err = err
		return nil
	}
	entry := sort.Search(i.loadedEntries, func(j int) bool {
		return i.loadedKey(j) >= k
	}) - 1
	if entry >= 0 {
		return i.positionAt(page, entry)
	}
	for page--; page >= 0; page-- {
		if err := i.loadPage(page); err != nil {
			i.err = err
			return nil
		}
		if i.loadedEntries > 0 {
			return i.positionAt(page, i.loadedEntries-1)
		}
	}
	i.setBOF()
	return nil
}

func (i *Iter) stepForward() *base.InternalKV {
	page := i.pageIdx
	entry := i.entryIdx + 1
	for page < i.numDataPages {
		if err := i.loadPage(page); err != nil {
			i.err = err
			return nil
		}
		if entry < i.loadedEntries {
			return i.positionAt(page, entry)
		}
		page++
		entry = 0
	}
	i.setEOF()
	return nil
}

func (i *Iter) stepBackward() *base.InternalKV {
	page := i.pageIdx
	entry := i.entryIdx - 1
	for page >= 0 {
		if err := i.loadPage(page); err != nil {
			i.err = err
			return nil
		}
		if entry >= 0 {
			return i.positionAt(page, entry)
		}
		page--
		if page >= 0 {
			if err := i.loadPage(page); err != nil {
				i.err = err
				return nil
			}
			entry = i.loadedEntries - 1
		}
	}
	i.setBOF()
	return nil
}

func (i *Iter) positionAt(page, entry int) *base.InternalKV {
	if err := i.loadPage(page); err != nil {
		i.err = err
		return nil
	}
	if entry < 0 || entry >= i.loadedEntries {
		i.err = errors.Newf("pmtformat: invalid entry index page=%d entry=%d", page, entry)
		return nil
	}
	key := i.loadedKey(entry)
	if i.hasLower && key < i.lowerKey {
		i.setBOF()
		return nil
	}
	if i.hasUpper && key >= i.upperKey {
		i.setEOF()
		return nil
	}
	val := i.loadedValue(entry)
	binary.BigEndian.PutUint64(i.keyBuf[:], key)
	binary.BigEndian.PutUint64(i.valBuf[:], val)
	i.kv = base.MakeInternalKV(
		base.MakeInternalKey(i.keyBuf[:], i.seqNum, base.InternalKeyKindSet),
		i.valBuf[:],
	)
	i.pageIdx = page
	i.entryIdx = entry
	i.state = iterStatePositioned
	return &i.kv
}

func (i *Iter) loadPage(page int) error {
	if page < 0 || page >= i.numDataPages {
		return errors.Newf("pmtformat: invalid page index %d", page)
	}
	if i.loadedPage == page {
		return nil
	}
	offset := int64(page * dataPageSize)
	if err := i.r.ReadAt(i.ctx, i.pageBuf, offset); err != nil {
		return err
	}
	i.loadedPage = page
	i.loadedEntries = parsePageEntries(i.pageBuf)
	if i.loadedEntries == 0 {
		return errors.Newf("pmtformat: empty data page %d", page)
	}
	return nil
}

func (i *Iter) loadedKey(entry int) uint64 {
	offset := entry * dataEntrySize
	return binary.BigEndian.Uint64(i.pageBuf[offset : offset+8])
}

func (i *Iter) loadedValue(entry int) uint64 {
	offset := entry*dataEntrySize + 8
	return binary.BigEndian.Uint64(i.pageBuf[offset : offset+8])
}

func (i *Iter) setEOF() {
	i.state = iterStateEOF
	i.pageIdx = i.numDataPages
	i.entryIdx = 0
}

func (i *Iter) setBOF() {
	i.state = iterStateBOF
	i.pageIdx = -1
	i.entryIdx = -1
}

func parsePageEntries(page []byte) int {
	for e := 0; e < maxEntriesPerPage; e++ {
		offset := e * dataEntrySize
		key := binary.BigEndian.Uint64(page[offset : offset+8])
		if key == 0 && e > 0 {
			return e
		}
	}
	return maxEntriesPerPage
}

func decodeUserKey(key []byte) (uint64, error) {
	if len(key) != 8 {
		return 0, errors.Newf("pmtformat: expected 8-byte user key, got %d", len(key))
	}
	return binary.BigEndian.Uint64(key), nil
}
