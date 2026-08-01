package storage

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

const defaultMaxSegmentBytes = 4 * 1024 * 1024 // 4MB, matches chunk size default elsewhere in project

// Log is a single append-only log backed by segment files on disk. Not
// safe for concurrent use from multiple goroutines without external
// locking — that's the caller's job (Raft's log integration in v0.4 will
// hold its own mutex around calls into this package).
type Log struct {
	dir             string
	maxSegmentBytes int64
	segments        []*segment
	active          *segment
}

// Open opens (or creates) a log rooted at dir, recovering any existing
// segments found there.
func Open(dir string, maxSegmentBytes int64) (*Log, error) {
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	l := &Log{dir: dir, maxSegmentBytes: maxSegmentBytes}

	baseOffsets, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(baseOffsets) == 0 {
		baseOffsets = []int{0}
	}
	for _, base := range baseOffsets {
		s, err := openSegment(dir, base)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, s)
	}
	l.active = l.segments[len(l.segments)-1]
	return l, nil
}

func discoverSegments(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base := strings.TrimSuffix(name, ".log")
		n, err := strconv.Atoi(base)
		if err != nil {
			continue
		}
		seen[n] = true
	}
	var bases []int
	for n := range seen {
		bases = append(bases, n)
	}
	sort.Ints(bases)
	return bases, nil
}

// Append writes payload to the active segment, rolling to a new segment
// first if it would exceed maxSegmentBytes.
func (l *Log) Append(payload []byte) (int, error) {
	needed := int64(lenPrefixSize + len(payload))
	if l.active.size+needed > l.maxSegmentBytes && l.active.size > 0 {
		if err := l.roll(); err != nil {
			return 0, err
		}
	}
	return l.active.append(payload)
}

func (l *Log) roll() error {
	newBase := l.active.nextOffset
	s, err := openSegment(l.dir, newBase)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.active = s
	return nil
}

// Read returns the payload at the given offset.
func (l *Log) Read(offset int) ([]byte, error) {
	seg := l.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found", offset)
	}
	return seg.read(offset)
}

// findSegment binary-searches for the segment covering offset.
func (l *Log) findSegment(offset int) *segment {
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1
	if idx < 0 || idx >= len(l.segments) {
		return nil
	}
	seg := l.segments[idx]
	if offset < seg.baseOffset || offset >= seg.nextOffset {
		return nil
	}
	return seg
}

// NextOffset returns the offset the next Append call will assign.
func (l *Log) NextOffset() int {
	return l.active.nextOffset
}

// OldestOffset returns the lowest offset still present in the log (i.e.
// not yet dropped by compaction).
func (l *Log) OldestOffset() int {
	return l.segments[0].baseOffset
}

// Compact deletes whole segments older than keepSegments most-recent ones.
// Never removes the active segment. Retention-based deletion only — see
// design doc for why this isn't dedup/merge compaction.
func (l *Log) Compact(keepSegments int) error {
	if keepSegments < 1 {
		keepSegments = 1
	}
	toRemove := len(l.segments) - keepSegments
	if toRemove <= 0 {
		return nil
	}
	for i := 0; i < toRemove; i++ {
		if err := l.segments[i].remove(); err != nil {
			return err
		}
	}
	l.segments = l.segments[toRemove:]
	return nil
}

func (l *Log) SegmentCount() int {
	return len(l.segments)
}

func (l *Log) Close() error {
	for _, s := range l.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	return nil
}
