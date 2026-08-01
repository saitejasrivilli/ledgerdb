package regression

import (
	"testing"
	"time"

	"github.com/saitejasrivilli/ledgerdb/group"
)

func assertBalanced(t *testing.T, assignment map[string][]int, numPartitions int) {
	t.Helper()
	n := len(assignment)
	if n == 0 {
		return
	}
	min := numPartitions / n
	max := (numPartitions + n - 1) / n
	total := 0
	for id, parts := range assignment {
		if len(parts) < min || len(parts) > max {
			t.Fatalf("consumer %q has %d partitions, want between %d and %d", id, len(parts), min, max)
		}
		total += len(parts)
	}
	if total != numPartitions {
		t.Fatalf("total assigned partitions = %d, want %d", total, numPartitions)
	}
}

func TestV05_RebalanceOnJoin(t *testing.T) {
	c := group.NewCoordinator(6, 200*time.Millisecond)
	defer c.Close()

	c.Join("consumer-a")
	assertBalanced(t, c.FullAssignment(), 6)
	if len(c.Assignment("consumer-a")) != 6 {
		t.Fatalf("single consumer should own all 6 partitions, got %d", len(c.Assignment("consumer-a")))
	}

	c.Join("consumer-b")
	assertBalanced(t, c.FullAssignment(), 6)

	c.Join("consumer-c")
	assertBalanced(t, c.FullAssignment(), 6)

	full := c.FullAssignment()
	if len(full) != 3 {
		t.Fatalf("expected 3 members, got %d", len(full))
	}
}

func TestV05_RebalanceOnCrash(t *testing.T) {
	c := group.NewCoordinator(4, 200*time.Millisecond)
	defer c.Close()

	c.Join("consumer-a")
	c.Join("consumer-b")
	assertBalanced(t, c.FullAssignment(), 4)

	// consumer-a stops heartbeating (simulated crash) — never call
	// Heartbeat for it again, let the expiry loop mark it dead
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.Heartbeat("consumer-b")
		if len(c.FullAssignment()) == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	full := c.FullAssignment()
	if len(full) != 1 {
		t.Fatalf("expected consumer-a to be evicted, members: %v", full)
	}
	if len(full["consumer-b"]) != 4 {
		t.Fatalf("consumer-b should own all 4 partitions after consumer-a's crash, got %d", len(full["consumer-b"]))
	}
}

func TestV05_RebalanceOnLeave(t *testing.T) {
	c := group.NewCoordinator(3, 500*time.Millisecond)
	defer c.Close()

	c.Join("consumer-a")
	c.Join("consumer-b")
	assertBalanced(t, c.FullAssignment(), 3)

	c.Leave("consumer-a")
	full := c.FullAssignment()
	if len(full) != 1 {
		t.Fatalf("expected 1 member after leave, got %d", len(full))
	}
	if len(full["consumer-b"]) != 3 {
		t.Fatalf("consumer-b should own all 3 partitions after consumer-a leaves, got %d", len(full["consumer-b"]))
	}
}

func TestV05_NoPartitionStarvation(t *testing.T) {
	cases := []struct{ partitions, consumers int }{
		{12, 3}, {12, 5}, {7, 3}, {1, 4}, {10, 1},
	}
	for _, tc := range cases {
		c := group.NewCoordinator(tc.partitions, time.Second)
		for i := 0; i < tc.consumers; i++ {
			c.Join(string(rune('a' + i)))
		}
		assertBalanced(t, c.FullAssignment(), tc.partitions)
		c.Close()
	}
}
