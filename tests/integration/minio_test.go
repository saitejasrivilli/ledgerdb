// Package integration holds tests that need a real external system
// running (MinIO, here) — deliberately separate from tests/regression,
// which must pass with nothing but `go test ./...` and no external
// dependencies. These tests skip themselves locally when no MinIO
// endpoint is configured, and run for real in CI (see
// .github/workflows/minio-integration.yml, which starts an actual MinIO
// service container) — the thing the v0.9 design doc's "never run
// against an actual MinIO/S3 endpoint" gap asked for.
package integration

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/storage"
)

func minioEnv(t *testing.T) (endpoint, accessKey, secretKey string) {
	t.Helper()
	endpoint = os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_ENDPOINT not set — skipping real MinIO integration test locally; runs in CI (.github/workflows/minio-integration.yml)")
	}
	accessKey = os.Getenv("MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey = os.Getenv("MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	return endpoint, accessKey, secretKey
}

// TestRealMinIO_ReadFromColdTierAfterMigration is the real-external-system
// version of TestV09_ReadFromColdTierAfterMigration: same tiering logic
// (storage.Log.TierSegments, unmodified), but ColdStore is a real MinIO
// bucket instead of the local-directory stand-in — this is what actually
// converts the v0.9 "interface-tested" claim into "integrated with
// MinIO" for real.
func TestRealMinIO_ReadFromColdTierAfterMigration(t *testing.T) {
	endpoint, accessKey, secretKey := minioEnv(t)

	dataDir := t.TempDir()
	log, err := storage.Open(dataDir, 32) // tiny segments, forces rolls
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer log.Close()

	bucket := "ledgerdb-integration-test"
	cold, err := storage.NewMinioColdStore(endpoint, accessKey, secretKey, bucket, false)
	if err != nil {
		t.Fatalf("new minio cold store: %v", err)
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
		t.Fatalf("tier segments to real MinIO: %v", err)
	}
	if tiered == 0 {
		t.Fatalf("expected at least one segment to tier out to MinIO")
	}

	for off := 0; off < len(want); off++ {
		got, err := log.Read(off)
		if err != nil {
			t.Fatalf("read offset %d after tiering to real MinIO: %v", off, err)
		}
		if !bytes.Equal(got, want[off]) {
			t.Fatalf("offset %d: got %q want %q", off, got, want[off])
		}
	}
}
