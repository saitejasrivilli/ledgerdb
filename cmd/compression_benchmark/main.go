// Command compression_benchmark produces
// benchmarks/results/v0.8_compression.json — a real measured gzip ratio
// on a realistic batch of log-line-style messages, per
// docs/design_batching_compression.md.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/saitejasrivilli/ledgerdb/benchmarks"
)

func main() {
	var messages [][]byte
	for i := 0; i < 1000; i++ {
		line := fmt.Sprintf(`{"level":"info","service":"ledgerdb","event":"write","offset":%d,"user":"user-%d","status":"ok"}`, i, i%50)
		messages = append(messages, []byte(line))
	}

	result := benchmarks.RunCompressionComparison(messages)

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}
	path := "benchmarks/results/v0.8_compression.json"
	if err := os.WriteFile(path, out, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
	fmt.Println(string(out))
}
