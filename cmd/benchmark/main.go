// Command benchmark runs the v0.7 baseline suite and writes
// benchmarks/results/v0.7_baseline.json. Every number in that file comes
// from this actual run — see docs/design_benchmarking.md.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/saitejasrivilli/ledgerdb/benchmarks"
	"github.com/saitejasrivilli/ledgerdb/producer"
)

func main() {
	burst := 3 * time.Second

	fmt.Println("running throughput benchmark: ack=0 ...")
	ackNone := benchmarks.RunThroughput(producer.AckNone, "ack=0", burst)
	fmt.Println("running throughput benchmark: ack=1 ...")
	ackLeader := benchmarks.RunThroughput(producer.AckLeader, "ack=1", burst)
	fmt.Println("running throughput benchmark: ack=all ...")
	ackAll := benchmarks.RunThroughput(producer.AckAll, "ack=all", burst)

	fmt.Println("running chaos benchmark (kill leader, measure recovery) ...")
	chaos := benchmarks.RunChaos()

	results := benchmarks.BaselineResults{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Throughput:  []benchmarks.ThroughputResult{ackNone, ackLeader, ackAll},
		Chaos:       chaos,
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal results:", err)
		os.Exit(1)
	}

	path := "benchmarks/results/v0.7_baseline.json"
	if err := os.WriteFile(path, out, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write results:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
	fmt.Println(string(out))
}
