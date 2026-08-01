// Command transport_benchmark produces
// benchmarks/results/v0.14_transport.json — the same kill-leader chaos
// measurement as v0.7's cmd/benchmark, re-run over a real TCPTransport
// instead of the in-process simulated network. See
// docs/design_real_transport.md for why this is reported as its own
// number, not a replacement for v0.7_baseline.json's.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/saitejasrivilli/ledgerdb/benchmarks"
)

func main() {
	fmt.Println("running chaos benchmark over a real TCP transport (kill leader via bidirectional connection block) ...")

	const runs = 5
	results := make([]benchmarks.ChaosResult, runs)
	for i := 0; i < runs; i++ {
		results[i] = benchmarks.RunRealTransportChaos()
	}

	out, err := json.MarshalIndent(struct {
		GeneratedAt string                   `json:"generated_at"`
		Transport   string                   `json:"transport"`
		Runs        []benchmarks.ChaosResult `json:"runs"`
	}{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Transport:   "real TCP (net/rpc over net.Listen/net.Dial), kill simulated via bidirectional TCPTransport.BlockPeer",
		Runs:        results,
	}, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}

	path := "benchmarks/results/v0.14_transport.json"
	if err := os.WriteFile(path, out, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
	fmt.Println(string(out))
}
