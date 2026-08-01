package regression

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/metrics"
	"github.com/saitejasrivilli/ledgerdb/producer"
)

func TestV11_MetricsEndpointReturnsExpectedFields(t *testing.T) {
	net, nodes := makeReplicatedCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	leader := findLeader(t, net, nodes)

	// drive real writes/reads through the instrumented wrappers so the
	// scraped values aren't just zero-initialized placeholders
	if _, err := metrics.InstrumentedWrite(nodes[leader], []byte("hello"), producer.AckAll); err != nil {
		t.Fatalf("instrumented write: %v", err)
	}
	for i, n := range nodes {
		metrics.SampleLeaderStatus(metrics.NodeName(i), n)
	}
	metrics.RecordConsumerLag("group-a", "consumer-1", 10, 7)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	text := string(body)

	for _, name := range []string{
		"ledgerdb_writes_total",
		"ledgerdb_write_latency_seconds",
		"ledgerdb_raft_is_leader",
		"ledgerdb_consumer_lag",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("expected metric %q in scrape output, not found", name)
		}
	}

	if !strings.Contains(text, `ack_level="ack=all"`) {
		t.Fatalf("expected ack_level label for ack=all in scrape output")
	}
	if !strings.Contains(text, `group="group-a"`) {
		t.Fatalf("expected consumer lag label group=\"group-a\" in scrape output")
	}
}
