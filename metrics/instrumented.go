package metrics

import (
	"fmt"
	"time"

	"github.com/saitejasrivilli/ledgerdb/producer"
	"github.com/saitejasrivilli/ledgerdb/replication"
)

func ackLevelName(ack producer.AckLevel) string {
	switch ack {
	case producer.AckNone:
		return "ack=0"
	case producer.AckLeader:
		return "ack=1"
	case producer.AckAll:
		return "ack=all"
	default:
		return "unknown"
	}
}

// InstrumentedWrite wraps producer.Write, recording write count (by
// result) and latency without touching producer.Write itself.
func InstrumentedWrite(node *replication.ReplicatedPartition, payload []byte, ack producer.AckLevel) (int, error) {
	ackName := ackLevelName(ack)
	start := time.Now()
	idx, err := producer.Write(node, payload, ack)
	WriteLatencySeconds.WithLabelValues(ackName).Observe(time.Since(start).Seconds())

	result := "success"
	if err != nil {
		result = "failure"
	}
	WritesTotal.WithLabelValues(ackName, result).Inc()
	return idx, err
}

// SampleLeaderStatus updates the ledgerdb_raft_is_leader gauge for node,
// identified by nodeName.
func SampleLeaderStatus(nodeName string, node *replication.ReplicatedPartition) {
	_, isLeader := node.GetState()
	value := 0.0
	if isLeader {
		value = 1.0
	}
	RaftIsLeader.WithLabelValues(nodeName).Set(value)
}

// RecordConsumerLag updates the ledgerdb_consumer_lag gauge given the
// partition's latest offset and the consumer's last-read offset.
func RecordConsumerLag(group, consumer string, latestOffset, lastReadOffset int) {
	lag := latestOffset - lastReadOffset
	if lag < 0 {
		lag = 0
	}
	ConsumerLag.WithLabelValues(group, consumer).Set(float64(lag))
}

// NodeName is a small helper for consistent label values in tests/demos.
func NodeName(i int) string {
	return fmt.Sprintf("node-%d", i)
}
