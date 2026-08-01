package regression

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/saitejasrivilli/ledgerdb/dashboard"
)

// TestDashboard_StatusEndpointServesRealClusterState drives /status via a
// real HTTP request against a real 3-node cluster and confirms the JSON
// matches what GetState() reports directly on each node — the endpoint
// reports real state, not placeholder zeros. See
// docs/design_dashboard_ui.md.
func TestDashboard_StatusEndpointServesRealClusterState(t *testing.T) {
	net, nodes := makeDocStoreCluster(t, 3, "")
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	findDocStoreLeader(t, net, nodes)

	statersArg := make([]dashboard.NodeStater, len(nodes))
	for i, n := range nodes {
		statersArg[i] = n
	}
	srv := httptest.NewServer(dashboard.New(statersArg...))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want 200", resp.StatusCode)
	}

	var got dashboard.ClusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Fatalf("expected 3 nodes in status, got %d", len(got.Nodes))
	}

	leaderCount := 0
	for i, n := range got.Nodes {
		wantTerm, wantIsLeader := nodes[i].GetState()
		if n.Term != wantTerm || n.IsLeader != wantIsLeader {
			t.Fatalf("node %d: status={term:%d,leader:%v} want={term:%d,leader:%v}", i, n.Term, n.IsLeader, wantTerm, wantIsLeader)
		}
		if n.IsLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader reported in status, got %d", leaderCount)
	}

	// the static page itself must be served too
	resp2, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp2.StatusCode)
	}
}
