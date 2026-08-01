// Command mvcc_benchmark produces benchmarks/results/v1.0_mvcc.json —
// real measured concurrent-read throughput, lock-based store vs. MVCC
// store, under identical load. See docs/design_mvcc.md.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/saitejasrivilli/ledgerdb/benchmarks"
)

func main() {
	fmt.Println("running MVCC vs. lock-based comparison (this takes a few seconds) ...")
	result := benchmarks.RunMVCCComparison(8, 2*time.Second)

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}
	path := "benchmarks/results/v1.0_mvcc.json"
	if err := os.WriteFile(path, out, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
	fmt.Println(string(out))
}
