package benchmarks

import "os"

func makeTempDir() (string, error) {
	return os.MkdirTemp("", "ledgerdb-bench-*")
}
