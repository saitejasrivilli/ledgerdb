// Package regression accumulates tests from every tagged version — this
// directory only grows, per the versioned build plan's regression rule.
package regression

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/storage"
)

func TestV02_SequentialWriteRead(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.Open(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	var offsets []int
	for i := 0; i < 100; i++ {
		payload := []byte(fmt.Sprintf("record-%d", i))
		off, err := log.Append(payload)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		offsets = append(offsets, off)
	}

	for i, off := range offsets {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("read offset %d: %v", off, err)
		}
		want := []byte(fmt.Sprintf("record-%d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("offset %d: got %q want %q", off, got, want)
		}
	}
}

func TestV02_SegmentRoll(t *testing.T) {
	dir := t.TempDir()
	// tiny max segment size forces frequent rolls
	log, err := storage.Open(dir, 64)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	for i := 0; i < 50; i++ {
		if _, err := log.Append([]byte(fmt.Sprintf("payload-%03d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if log.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments, got %d", log.SegmentCount())
	}

	for i := 0; i < 50; i++ {
		got, err := log.Read(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		want := []byte(fmt.Sprintf("payload-%03d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("offset %d: got %q want %q", i, got, want)
		}
	}
}

func TestV02_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.Open(dir, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := log.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// simulate crash: close without any special flush/finalize step
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := storage.Open(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if reopened.NextOffset() != 10 {
		t.Fatalf("expected nextOffset 10 after recovery, got %d", reopened.NextOffset())
	}
	for i := 0; i < 10; i++ {
		got, err := reopened.Read(i)
		if err != nil {
			t.Fatalf("read after recovery %d: %v", i, err)
		}
		want := []byte(fmt.Sprintf("rec-%d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("offset %d: got %q want %q", i, got, want)
		}
	}

	// append more after recovery, confirm continuity
	off, err := reopened.Append([]byte("rec-10"))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if off != 10 {
		t.Fatalf("expected next offset 10, got %d", off)
	}
}

func TestV02_Compaction(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.Open(dir, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	for i := 0; i < 60; i++ {
		if _, err := log.Append([]byte(fmt.Sprintf("data-%03d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	totalSegments := log.SegmentCount()
	if totalSegments < 3 {
		t.Fatalf("test needs several segments to be meaningful, got %d", totalSegments)
	}

	if err := log.Compact(2); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if log.SegmentCount() != 2 {
		t.Fatalf("expected 2 segments after compaction, got %d", log.SegmentCount())
	}

	oldest := log.OldestOffset()
	if oldest == 0 {
		t.Fatalf("expected oldest offset to advance past 0 after compaction")
	}

	// data still in retained segments must read correctly
	next := log.NextOffset()
	for off := oldest; off < next; off++ {
		if _, err := log.Read(off); err != nil {
			t.Fatalf("read retained offset %d after compaction: %v", off, err)
		}
	}

	// dropped offsets must error, not silently return garbage
	if _, err := log.Read(0); err == nil {
		t.Fatalf("expected error reading compacted-away offset 0")
	}
}
