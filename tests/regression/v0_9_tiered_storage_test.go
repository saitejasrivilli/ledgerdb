package regression

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/storage"
)

func TestV09_ReadFromColdTierAfterMigration(t *testing.T) {
	dataDir := t.TempDir()
	coldDir := t.TempDir()

	log, err := storage.Open(dataDir, 32) // tiny segments, forces rolls
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	cold, err := storage.NewLocalDirColdStore(coldDir)
	if err != nil {
		t.Fatalf("new cold store: %v", err)
	}
	log.EnableTiering(cold)

	var want [][]byte
	for i := 0; i < 40; i++ {
		payload := []byte(fmt.Sprintf("record-%03d", i))
		if _, err := log.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		want = append(want, payload)
	}
	if log.SegmentCount() < 2 {
		t.Fatalf("test needs multiple segments to be meaningful, got %d", log.SegmentCount())
	}

	tiered, err := log.TierSegments()
	if err != nil {
		t.Fatalf("tier segments: %v", err)
	}
	if tiered == 0 {
		t.Fatalf("expected at least one segment to tier out")
	}

	// every offset, including ones now served from cold storage, must
	// still read back byte-identical
	for off := 0; off < len(want); off++ {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("read offset %d after tiering: %v", off, err)
		}
		if !bytes.Equal(got, want[off]) {
			t.Fatalf("offset %d: got %q want %q", off, got, want[off])
		}
	}
}

func TestV09_NoDataLossOnFailedUpload(t *testing.T) {
	dataDir := t.TempDir()
	log, err := storage.Open(dataDir, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	failing := &storage.FailingColdStore{Err: errors.New("simulated upload failure")}
	log.EnableTiering(failing)

	var want [][]byte
	for i := 0; i < 20; i++ {
		payload := []byte(fmt.Sprintf("rec-%03d", i))
		if _, err := log.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		want = append(want, payload)
	}
	if log.SegmentCount() < 2 {
		t.Fatalf("test needs multiple segments, got %d", log.SegmentCount())
	}

	// tiering should fail (upload errors), and must not have deleted any
	// local segment data as a side effect
	if _, err := log.TierSegments(); err == nil {
		t.Fatalf("expected TierSegments to fail with a failing cold store")
	}

	for off := 0; off < len(want); off++ {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("read offset %d after failed tiering attempt: %v — local data must survive a failed upload", off, err)
		}
		if !bytes.Equal(got, want[off]) {
			t.Fatalf("offset %d: got %q want %q", off, got, want[off])
		}
	}
}

func TestV09_ActiveSegmentNeverTiers(t *testing.T) {
	dataDir := t.TempDir()
	coldDir := t.TempDir()
	log, err := storage.Open(dataDir, 0) // large max size: stays one segment
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	cold, err := storage.NewLocalDirColdStore(coldDir)
	if err != nil {
		t.Fatalf("new cold store: %v", err)
	}
	log.EnableTiering(cold)

	log.Append([]byte("only record"))

	tiered, err := log.TierSegments()
	if err != nil {
		t.Fatalf("tier segments: %v", err)
	}
	if tiered != 0 {
		t.Fatalf("expected the sole (active) segment to never tier, got %d tiered", tiered)
	}

	got, err := log.Read(0)
	if err != nil {
		t.Fatalf("read offset 0: %v", err)
	}
	if string(got) != "only record" {
		t.Fatalf("got %q want %q", got, "only record")
	}
}
