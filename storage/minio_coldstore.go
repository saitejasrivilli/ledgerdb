package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioColdStore is a ColdStore backed by a real MinIO (S3-compatible)
// bucket — the real external system LocalDirColdStore (v0.9) stands in
// for in the unit/regression test suite. See
// tests/integration/minio_test.go and .github/workflows/minio-integration.yml
// for where this is actually exercised against a running MinIO instance;
// it requires a reachable MinIO endpoint, which the regular local test
// suite (`go test ./...`) does not provide.
type MinioColdStore struct {
	client *minio.Client
	bucket string
}

// NewMinioColdStore connects to a MinIO (or any S3-compatible) endpoint
// and ensures bucket exists, creating it if necessary.
func NewMinioColdStore(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinioColdStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket: %w", err)
		}
	}

	return &MinioColdStore{client: client, bucket: bucket}, nil
}

func (m *MinioColdStore) Put(key string, logBytes, indexBytes []byte) error {
	ctx := context.Background()
	if _, err := m.client.PutObject(ctx, m.bucket, key+".log", bytes.NewReader(logBytes), int64(len(logBytes)), minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("minio put log: %w", err)
	}
	if _, err := m.client.PutObject(ctx, m.bucket, key+".index", bytes.NewReader(indexBytes), int64(len(indexBytes)), minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("minio put index: %w", err)
	}
	return nil
}

func (m *MinioColdStore) Get(key string) (logBytes, indexBytes []byte, err error) {
	ctx := context.Background()

	logObj, err := m.client.GetObject(ctx, m.bucket, key+".log", minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("minio get log: %w", err)
	}
	defer logObj.Close()
	logBytes, err = io.ReadAll(logObj)
	if err != nil {
		return nil, nil, fmt.Errorf("minio read log: %w", err)
	}

	indexObj, err := m.client.GetObject(ctx, m.bucket, key+".index", minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("minio get index: %w", err)
	}
	defer indexObj.Close()
	indexBytes, err = io.ReadAll(indexObj)
	if err != nil {
		return nil, nil, fmt.Errorf("minio read index: %w", err)
	}

	return logBytes, indexBytes, nil
}

func (m *MinioColdStore) Delete(key string) error {
	ctx := context.Background()
	if err := m.client.RemoveObject(ctx, m.bucket, key+".log", minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("minio delete log: %w", err)
	}
	if err := m.client.RemoveObject(ctx, m.bucket, key+".index", minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("minio delete index: %w", err)
	}
	return nil
}
