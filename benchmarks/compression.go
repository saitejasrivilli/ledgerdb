package benchmarks

import (
	"github.com/saitejasrivilli/ledgerdb/batch"
)

// CompressionResult compares batched+compressed vs. naive uncompressed
// concatenation of the same messages — the "disk footprint" number the
// v0.8 design doc's benchmark requirement asks for.
type CompressionResult struct {
	MessageCount      int     `json:"message_count"`
	UncompressedBytes int     `json:"uncompressed_bytes"`
	CompressedBytes   int     `json:"compressed_bytes"`
	CompressionRatio  float64 `json:"compression_ratio"`
}

// RunCompressionComparison encodes a realistic batch of repetitive
// log-line-style messages (the shape most append-only logs actually
// carry) and measures the real gzip ratio achieved — not estimated.
func RunCompressionComparison(messages [][]byte) CompressionResult {
	uncompressed := 0
	for _, m := range messages {
		uncompressed += len(m)
	}

	blob, err := batch.Encode(messages)
	if err != nil {
		panic(err)
	}

	ratio := 0.0
	if len(blob) > 0 {
		ratio = float64(uncompressed) / float64(len(blob))
	}

	return CompressionResult{
		MessageCount:      len(messages),
		UncompressedBytes: uncompressed,
		CompressedBytes:   len(blob),
		CompressionRatio:  ratio,
	}
}
