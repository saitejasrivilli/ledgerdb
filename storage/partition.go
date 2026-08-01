package storage

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
)

// PartitionedLog routes writes/reads across N independent Log instances,
// each with its own offset space and segment directory (see
// docs/design_partitioning.md). Not safe for concurrent use across
// goroutines without external locking, same as Log itself.
type PartitionedLog struct {
	dir             string
	numPartitions   int
	maxSegmentBytes int64
	partitions      []*Log
}

// OpenPartitioned opens (or creates) numPartitions independent logs under
// dir, one subdirectory per partition.
func OpenPartitioned(dir string, numPartitions int, maxSegmentBytes int64) (*PartitionedLog, error) {
	if numPartitions < 1 {
		return nil, fmt.Errorf("numPartitions must be >= 1, got %d", numPartitions)
	}
	pl := &PartitionedLog{
		dir:             dir,
		numPartitions:   numPartitions,
		maxSegmentBytes: maxSegmentBytes,
	}
	for i := 0; i < numPartitions; i++ {
		partDir := filepath.Join(dir, fmt.Sprintf("partition-%d", i))
		log, err := Open(partDir, maxSegmentBytes)
		if err != nil {
			return nil, fmt.Errorf("open partition %d: %w", i, err)
		}
		pl.partitions = append(pl.partitions, log)
	}
	return pl, nil
}

// PartitionFor returns the partition index a key routes to.
func (pl *PartitionedLog) PartitionFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % pl.numPartitions
}

// Append writes payload to the partition the key routes to, returning the
// partition index and the offset it was assigned within that partition.
func (pl *PartitionedLog) Append(key string, payload []byte) (partition int, offset int, err error) {
	partition = pl.PartitionFor(key)
	offset, err = pl.partitions[partition].Append(payload)
	return partition, offset, err
}

// Read returns the payload at a specific (partition, offset) pair.
func (pl *PartitionedLog) Read(partition int, offset int) ([]byte, error) {
	if partition < 0 || partition >= pl.numPartitions {
		return nil, fmt.Errorf("partition %d out of range [0,%d)", partition, pl.numPartitions)
	}
	return pl.partitions[partition].Read(offset)
}

func (pl *PartitionedLog) NumPartitions() int {
	return pl.numPartitions
}

func (pl *PartitionedLog) NextOffset(partition int) int {
	return pl.partitions[partition].NextOffset()
}

func (pl *PartitionedLog) Close() error {
	for i, p := range pl.partitions {
		if err := p.Close(); err != nil {
			return fmt.Errorf("close partition %d: %w", i, err)
		}
	}
	return nil
}
