// Package metrics implements the Prometheus instrumentation described in
// docs/design_observability.md — wraps existing, already-tested types
// (producer.Write, ReplicatedPartition.GetState) rather than modifying
// them.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	WritesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerdb_writes_total",
		Help: "Total producer writes, labeled by ack level and result.",
	}, []string{"ack_level", "result"})

	WriteLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledgerdb_write_latency_seconds",
		Help:    "Producer write latency in seconds, labeled by ack level.",
		Buckets: prometheus.DefBuckets,
	}, []string{"ack_level"})

	RaftIsLeader = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ledgerdb_raft_is_leader",
		Help: "1 if this node currently believes it is the Raft leader, 0 otherwise.",
	}, []string{"node"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ledgerdb_consumer_lag",
		Help: "Difference between a partition's latest offset and a consumer's last-read offset.",
	}, []string{"group", "consumer"})
)

// Handler returns the standard Prometheus scrape handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
