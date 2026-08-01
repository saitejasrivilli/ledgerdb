package regression

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/storage"
)

func TestV03_PartitionIsolation(t *testing.T) {
	dir := t.TempDir()
	pl, err := storage.OpenPartitioned(dir, 4, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pl.Close()

	// write distinct keys, confirm each key's writes always land in the
	// same partition and never bleed into another partition's offset space
	keyPartitions := map[string]int{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%d", i%8)
		partition, _, err := pl.Append(key, []byte(fmt.Sprintf("val-%d", i)))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if prev, ok := keyPartitions[key]; ok && prev != partition {
			t.Fatalf("key %q routed to different partitions: %d then %d", key, prev, partition)
		}
		keyPartitions[key] = partition
	}
}

func TestV03_IndependentOffsetSpaces(t *testing.T) {
	dir := t.TempDir()
	pl, err := storage.OpenPartitioned(dir, 3, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pl.Close()

	// force writes into a specific partition by finding keys that hash there
	target := 1
	written := 0
	for i := 0; written < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		if pl.PartitionFor(key) != target {
			continue
		}
		if _, _, err := pl.Append(key, []byte(fmt.Sprintf("p1-%d", written))); err != nil {
			t.Fatalf("append: %v", err)
		}
		written++
	}
	if pl.NextOffset(target) != 5 {
		t.Fatalf("partition %d expected nextOffset 5, got %d", target, pl.NextOffset(target))
	}

	// untouched partitions must still start at offset 0
	for p := 0; p < 3; p++ {
		if p == target {
			continue
		}
		if pl.NextOffset(p) != 0 {
			t.Fatalf("untouched partition %d expected nextOffset 0, got %d", p, pl.NextOffset(p))
		}
	}
}

func TestV03_ReadBackByPartitionAndOffset(t *testing.T) {
	dir := t.TempDir()
	pl, err := storage.OpenPartitioned(dir, 4, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pl.Close()

	type loc struct {
		partition, offset int
		want              []byte
	}
	var locs []loc
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key-%d", i)
		payload := []byte(fmt.Sprintf("payload-%d", i))
		p, off, err := pl.Append(key, payload)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		locs = append(locs, loc{p, off, payload})
	}

	for _, l := range locs {
		got, err := pl.Read(l.partition, l.offset)
		if err != nil {
			t.Fatalf("read partition %d offset %d: %v", l.partition, l.offset, err)
		}
		if !bytes.Equal(got, l.want) {
			t.Fatalf("partition %d offset %d: got %q want %q", l.partition, l.offset, got, l.want)
		}
	}
}
