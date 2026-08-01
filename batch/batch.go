// Package batch implements producer-side batching + gzip compression
// described in docs/design_batching_compression.md.
package batch

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// magic identifies a batch-encoded, gzip-compressed blob so the apply
// side can tell it apart from a plain, unbatched single-message payload
// written directly via producer.Write (used by earlier versions' tests
// and unaffected by this package).
var magic = [4]byte{'L', 'D', 'B', 'B'}

// Encode packs msgs into [count][len+payload]... and gzip-compresses the
// whole thing, prefixed with the magic marker.
func Encode(msgs [][]byte) ([]byte, error) {
	var body bytes.Buffer
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(msgs)))
	body.Write(countBuf)
	for _, m := range msgs {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(m)))
		body.Write(lenBuf)
		body.Write(m)
	}

	var out bytes.Buffer
	out.Write(magic[:])
	gz := gzip.NewWriter(&out)
	if _, err := gz.Write(body.Bytes()); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// TryDecode unpacks a blob previously produced by Encode. ok is false
// (with a nil error) if blob doesn't start with the batch magic marker —
// callers should treat it as a plain, unbatched single message instead.
func TryDecode(blob []byte) (msgs [][]byte, ok bool, err error) {
	if len(blob) < len(magic) || !bytes.Equal(blob[:len(magic)], magic[:]) {
		return nil, false, nil
	}

	gz, err := gzip.NewReader(bytes.NewReader(blob[len(magic):]))
	if err != nil {
		return nil, true, err
	}
	defer gz.Close()
	body, err := io.ReadAll(gz)
	if err != nil {
		return nil, true, err
	}

	if len(body) < 4 {
		return nil, true, errors.New("batch: truncated body")
	}
	count := binary.BigEndian.Uint32(body[0:4])
	pos := 4
	result := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(body) {
			return nil, true, errors.New("batch: truncated length prefix")
		}
		msgLen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+msgLen > len(body) {
			return nil, true, errors.New("batch: truncated payload")
		}
		result = append(result, body[pos:pos+msgLen])
		pos += msgLen
	}
	return result, true, nil
}

// Batcher accumulates messages and flushes them as one Encode-d blob via
// writeFn, whichever fires first: maxBatchSize messages accumulated, or
// maxLinger elapsed since the first message in the current batch.
type Batcher struct {
	maxBatchSize int
	maxLinger    time.Duration
	writeFn      func([]byte) error

	mu         sync.Mutex
	pending    [][]byte
	firstAdded time.Time

	stopCh chan struct{}
	doneCh chan struct{}
}

func NewBatcher(maxBatchSize int, maxLinger time.Duration, writeFn func([]byte) error) *Batcher {
	b := &Batcher{
		maxBatchSize: maxBatchSize,
		maxLinger:    maxLinger,
		writeFn:      writeFn,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	go b.lingerLoop()
	return b
}

// Add appends msg to the current batch, flushing immediately (in this
// goroutine, synchronously) if that fills the batch to maxBatchSize.
func (b *Batcher) Add(msg []byte) error {
	cp := append([]byte(nil), msg...)

	b.mu.Lock()
	if len(b.pending) == 0 {
		b.firstAdded = time.Now()
	}
	b.pending = append(b.pending, cp)
	var toFlush [][]byte
	if len(b.pending) >= b.maxBatchSize {
		toFlush = b.pending
		b.pending = nil
	}
	b.mu.Unlock()

	if toFlush != nil {
		return b.flush(toFlush)
	}
	return nil
}

func (b *Batcher) lingerLoop() {
	defer close(b.doneCh)
	tick := b.maxLinger / 4
	if tick <= 0 {
		tick = time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			var toFlush [][]byte
			if len(b.pending) > 0 && time.Since(b.firstAdded) >= b.maxLinger {
				toFlush = b.pending
				b.pending = nil
			}
			b.mu.Unlock()
			if toFlush != nil {
				b.flush(toFlush)
			}
		}
	}
}

func (b *Batcher) flush(msgs [][]byte) error {
	blob, err := Encode(msgs)
	if err != nil {
		return err
	}
	return b.writeFn(blob)
}

// Flush forces any pending messages out immediately, regardless of
// linger/size triggers — used for clean shutdown or tests that need a
// deterministic flush point.
func (b *Batcher) Flush() error {
	b.mu.Lock()
	toFlush := b.pending
	b.pending = nil
	b.mu.Unlock()
	if toFlush == nil {
		return nil
	}
	return b.flush(toFlush)
}

func (b *Batcher) Close() {
	close(b.stopCh)
	<-b.doneCh
}
