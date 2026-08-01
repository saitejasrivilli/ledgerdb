// Command dashboard_demo starts a 3-node replicated docstore cluster and
// serves the dashboard UI at :8080 — for manual verification, per
// docs/design_dashboard_ui.md's stated boundary that the static page
// itself isn't covered by an automated (headless-browser) test.
package main

import (
	"fmt"
	"os"

	"github.com/saitejasrivilli/ledgerdb/dashboard"
	"github.com/saitejasrivilli/ledgerdb/raft"
	"net/http"

	"github.com/saitejasrivilli/ledgerdb/docstore"
)

func main() {
	net := raft.MakeNetwork()
	peers := []int{0, 1, 2}
	nodes := make([]*docstore.ReplicatedDocStore, 3)
	staters := make([]dashboard.NodeStater, 3)
	for i := 0; i < 3; i++ {
		dir, err := os.MkdirTemp("", "dashboard-demo-*")
		if err != nil {
			panic(err)
		}
		ds, err := docstore.NewReplicatedDocStore(net, i, peers, dir, "")
		if err != nil {
			panic(err)
		}
		nodes[i] = ds
		staters[i] = ds
	}

	fmt.Println("serving dashboard at http://localhost:8080")
	if err := http.ListenAndServe(":8080", dashboard.New(staters...)); err != nil {
		panic(err)
	}
}
