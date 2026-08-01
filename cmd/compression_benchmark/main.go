// Command compression_benchmark produces
// benchmarks/results/v0.8_compression.json — a real measured gzip ratio
// on a batch of log-line-style messages, per
// docs/design_batching_compression.md.
//
// Corpus note: earlier versions of this benchmark used a low-entropy,
// near-identical template (two varying integers per line) and measured
// ~28.9x — an inflated number that didn't reflect real log diversity.
// This corpus adds per-line variation realistic services actually
// produce (varying paths, latencies, UUIDs, occasional free-text error
// messages) so the ratio means something comparable to a real workload.
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/saitejasrivilli/ledgerdb/benchmarks"
)

var paths = []string{"/api/v1/objects", "/api/v1/objects/{id}", "/health", "/metrics", "/api/v1/buckets", "/api/v1/buckets/{name}/list"}
var statuses = []string{"ok", "ok", "ok", "ok", "timeout", "not_found", "internal_error"}
var errorMessages = []string{
	"connection reset by peer while reading chunk 4 of 9",
	"quorum not reached: 1 of 3 replicas acknowledged within deadline",
	"segment file corrupt at offset 88213, index rebuild required",
	"leader stepped down mid-request, retry against new leader",
}

func main() {
	r := rand.New(rand.NewSource(42)) // fixed seed: reproducible, not cherry-picked per run

	var messages [][]byte
	for i := 0; i < 1000; i++ {
		path := paths[r.Intn(len(paths))]
		status := statuses[r.Intn(len(statuses))]
		latencyMs := 0.5 + r.Float64()*250
		userID := r.Int63n(50000)
		requestID := randomHexID(r, 16)

		line := fmt.Sprintf(
			`{"ts":%d,"level":"info","service":"ledgerdb","path":"%s","status":"%s","latency_ms":%.3f,"user_id":%d,"request_id":"%s"`,
			1700000000+i, path, status, latencyMs, userID, requestID,
		)
		if status != "ok" {
			line += fmt.Sprintf(`,"error":"%s"`, errorMessages[r.Intn(len(errorMessages))])
		}
		line += "}"

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

func randomHexID(r *rand.Rand, n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hex[r.Intn(len(hex))]
	}
	return string(b)
}
