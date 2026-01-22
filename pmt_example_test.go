package pebble

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/pmtformat"
)

// Test_PMT_Format_Basic 演示 PMT 格式的基本用法
func Test_PMT_Format_Basic(t *testing.T) {
	var buf bytes.Buffer
	w := pmtformat.NewWriter(&buf)

	// 添加数据
	w.Add(100, 1)
	w.Add(200, 2)
	w.Add(100, 3) // 更新 key 100
	w.Add(300, 4)

	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// 读取
	data := buf.Bytes()
	r, err := pmtformat.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	// 验证
	if len(r.Entries()) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(r.Entries()))
	}
	if len(r.Index()) != 3 {
		t.Errorf("Expected 3 index keys, got %d", len(r.Index()))
	}

	// Get 返回最新值
	if val, ok := r.Get(100); !ok || val != 1 {
		t.Errorf("Expected Get(100) = 1, got %d, %v", val, ok)
	}
}

// Test_PMT_TableFormat 验证 TableFormatPMT0 常量
func Test_PMT_TableFormat(t *testing.T) {
	if sstable.TableFormatPMT0.String() != "(PMT,v0)" {
		t.Errorf("Expected (PMT,v0), got %s", sstable.TableFormatPMT0.String())
	}

	magic, version := sstable.TableFormatPMT0.AsTuple()
	if version != 7 {
		t.Errorf("Expected version 7, got %d", version)
	}
	fmt.Printf("TableFormatPMT0: magic=%x, version=%d\n", []byte(magic), version)
}
