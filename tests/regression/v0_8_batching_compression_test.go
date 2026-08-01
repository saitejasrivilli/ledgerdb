package regression

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/batch"
)

func TestV08_CompressedDataRoundTrips(t *testing.T) {
	msgs := [][]byte{
		[]byte("first message"),
		[]byte("second message, a bit longer than the first"),
		[]byte(""), // empty message must survive too
		[]byte("fourth"),
	}
	blob, err := batch.Encode(msgs)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, ok, err := batch.TryDecode(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true for a batch-encoded blob")
	}
	if len(decoded) != len(msgs) {
		t.Fatalf("decoded %d messages, want %d", len(decoded), len(msgs))
	}
	for i := range msgs {
		if !bytes.Equal(decoded[i], msgs[i]) {
			t.Fatalf("message %d: got %q want %q", i, decoded[i], msgs[i])
		}
	}
}

func TestV08_NonBatchPayloadDecodesAsRaw(t *testing.T) {
	raw := []byte("plain unbatched payload, exactly what v0.4-v0.7 wrote")
	_, ok, err := batch.TryDecode(raw)
	if err != nil {
		t.Fatalf("expected no error for a non-batch payload, got: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for a payload that isn't batch-encoded")
	}
}

func TestV08_BatchFlushTriggersOnCount(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	b := batch.NewBatcher(3, time.Hour, func(blob []byte) error {
		mu.Lock()
		flushed = append(flushed, blob)
		mu.Unlock()
		return nil
	})
	defer b.Close()

	for i := 0; i < 3; i++ {
		if err := b.Add([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	mu.Lock()
	n := len(flushed)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("expected exactly 1 flush triggered by count, got %d", n)
	}
}

func TestV08_BatchFlushTriggersOnLinger(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	b := batch.NewBatcher(100, 100*time.Millisecond, func(blob []byte) error {
		mu.Lock()
		flushed = append(flushed, blob)
		mu.Unlock()
		return nil
	})
	defer b.Close()

	if err := b.Add([]byte("only message, well under count threshold")); err != nil {
		t.Fatalf("add: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(flushed)
		mu.Unlock()
		if n == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected linger-triggered flush within timeout, none happened")
}

// TestV08_BatchedWriteAppliesAsIndividualMessages confirms the full path:
// a batcher's flush goes through producer.Write as one Raft entry, and the
// apply loop (replication package) unpacks it back into individual
// messages at individual storage offsets.
func TestV08_BatchedWriteAppliesAsIndividualMessages(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findLeader(t, net, nodes)

	b := batch.NewBatcher(3, time.Hour, func(blob []byte) error {
		idx, _, isLeader := nodes[leader].Propose(blob)
		if !isLeader {
			t.Fatalf("leader %d rejected batch proposal", leader)
		}
		if !nodes[leader].WaitApplied(idx, 2*time.Second) {
			t.Fatalf("batch entry never applied")
		}
		return nil
	})
	defer b.Close()

	want := [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")}
	for _, m := range want {
		b.Add(m)
	}

	for off, w := range want {
		got, err := nodes[leader].ReadLocal(off)
		if err != nil {
			t.Fatalf("read offset %d: %v", off, err)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("offset %d: got %q want %q", off, got, w)
		}
	}

	// unbatched write right after must land at the next sequential offset
	idx, _, isLeader := nodes[leader].Propose([]byte("unbatched"))
	if !isLeader {
		t.Fatalf("expected leader to accept unbatched write")
	}
	if !nodes[leader].WaitApplied(idx, 2*time.Second) {
		t.Fatalf("unbatched entry never applied")
	}
	got, err := nodes[leader].ReadLocal(3)
	if err != nil {
		t.Fatalf("read offset 3: %v", err)
	}
	if string(got) != "unbatched" {
		t.Fatalf("offset 3: got %q want %q", got, "unbatched")
	}
}
