package pmtformat

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
)

// PMT0 格式设计:
// TableFormatPMT0 由多个 DataPage 和一个 IndexPage 组成
// - 每个 DataPage: 固定 4KB 大小，包含多个 {userkey, val} 对
// - IndexPage: 固定 4KB，每个 entry 对应一个 DataPage 的最大 userkey
//
// 文件布局:
// [DataPage 0: 4KB]
// [DataPage 1: 4KB]
// ...
// [DataPage N: 4KB]
// [IndexPage: 4KB]
//
// DataPage 结构 (4KB):
// - Entries: 每个 entry 16 bytes (userkey 8 + val 8)
// - 剩余空间用零填充, userkey=0判断数据结束（key递增）
//
// IndexPage 结构 (4KB):
// - MaxUserKey: 8 bytes (uint64) - 对应 DataPage 中的最大 userkey

const (
	dataPageSize   = 4096 // 4KB per data page
	indexPageSize  = 4096 // fixed 4KB index page
	dataEntrySize  = 16   // userkey(8) + val(8)
	indexEntrySize = 8    // maxUserKey(8)

	maxEntriesPerPage  = dataPageSize / dataEntrySize   // 每页最多256个entries
	maxDataPages       = indexPageSize / indexEntrySize // 固定4KB索引页最多索引512个DataPage
	maxEntriesPerTable = maxEntriesPerPage * maxDataPages
)

// Entry represents a key-value pair in PMT format
type Entry struct {
	UserKey uint64
	Value   uint64
}

// Writer writes data in PMT0 format
type Writer struct {
	w   io.Writer
	err error
	// 流式写入所需状态
	pageBuf          []byte   // 当前DataPage缓冲，固定4096字节
	numEntriesInPage int      // 当前页已写入的entry数量
	currentMaxKey    uint64   // 当前页最大userkey
	numDataPages     uint64   // 已写入的DataPage数量
	indexKeys        []uint64 // 每页最大key列表（用于IndexPage）
	lastUserKey      uint64   // 上一次写入的userkey
	hasPrevKey       bool     // 是否已有前一个key（用于单调性校验）
}

// NewWriter creates a new PMT writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:                w,
		pageBuf:          make([]byte, dataPageSize),
		numEntriesInPage: 0,
		currentMaxKey:    0,
		numDataPages:     0,
		indexKeys:        make([]uint64, 0, maxDataPages),
	}
}

// 其他文件格式的Writer还要传入seqnum和kind
// Add adds an entry to the writer
func (w *Writer) Add(userKey uint64, value uint64) error {
	if w.err != nil {
		return w.err
	}

	// 要求按非递减顺序追加key，便于构造有序的DataPages和IndexPage
	if w.hasPrevKey && userKey < w.lastUserKey {
		w.err = errors.Newf("keys must be added in non-decreasing order: prev=%d, got=%d", w.lastUserKey, userKey)
		return w.err
	}
	w.lastUserKey = userKey
	w.hasPrevKey = true

	// 如果当前页已满，先写出当前页
	if w.numEntriesInPage == maxEntriesPerPage {
		if err := w.flushCurrentPage(); err != nil {
			w.err = err
			return err
		}
	}
	// 当前页为空且已用满索引页可表示的数据页数量时，不能再写入新entry。
	if w.numEntriesInPage == 0 && w.numDataPages == maxDataPages {
		w.err = errors.Newf("pmtformat: table is full: max %d entries", maxEntriesPerTable)
		return w.err
	}

	// 将entry写入当前页缓冲
	entryOffset := w.numEntriesInPage * dataEntrySize
	binary.BigEndian.PutUint64(w.pageBuf[entryOffset:entryOffset+8], userKey)
	binary.BigEndian.PutUint64(w.pageBuf[entryOffset+8:entryOffset+16], value)
	if w.numEntriesInPage == 0 || userKey > w.currentMaxKey {
		w.currentMaxKey = userKey
	}
	w.numEntriesInPage++

	return nil
}

// Close finalizes the PMT file
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	// 写出最后一页（如有未满的页）。
	if w.numEntriesInPage > 0 {
		if err := w.flushCurrentPage(); err != nil {
			w.err = err
			return err
		}
	}

	// 写入固定4KB IndexPage。
	indexPage := make([]byte, indexPageSize)
	for i, key := range w.indexKeys {
		entryOffset := i * indexEntrySize
		binary.BigEndian.PutUint64(indexPage[entryOffset:entryOffset+indexEntrySize], key)
	}
	if _, err := w.w.Write(indexPage); err != nil {
		w.err = err
		return err
	}

	return nil
}

// 将当前页写出到底层writer，并记录索引key
func (w *Writer) flushCurrentPage() error {
	// 禁止写入空的 DataPage
	if w.numEntriesInPage == 0 {
		return errors.New("pmtformat: empty DataPage is not allowed")
	}
	if w.numDataPages >= maxDataPages {
		return errors.Newf("pmtformat: too many DataPages: max %d", maxDataPages)
	}
	// 写入整个4KB页面（包含padding）
	if _, err := w.w.Write(w.pageBuf); err != nil {
		return err
	}
	// 记录该页的最大key
	w.indexKeys = append(w.indexKeys, w.currentMaxKey)
	w.numDataPages++
	// 重置当前页状态
	w.pageBuf = make([]byte, dataPageSize)
	w.numEntriesInPage = 0
	w.currentMaxKey = 0
	return nil
}

// MaxEntriesPerTable returns the PMT0 hard limit under fixed-size pages.
func MaxEntriesPerTable() int {
	return maxEntriesPerTable
}
