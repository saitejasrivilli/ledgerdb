package storage

import "fmt"

// EnableTiering wires a ColdStore into this Log so TierEligible segments
// can be migrated. Tiering is opt-in — a Log with no ColdStore configured
// behaves exactly as it did before v0.9.
func (l *Log) EnableTiering(coldStore ColdStore) {
	l.coldStore = coldStore
}

// TierSegments migrates every closed (non-active) segment to cold storage
// via the configured ColdStore. Never tiers the active segment. Returns
// the number of segments successfully tiered. A failed upload leaves that
// segment's local files untouched — the invariant this version's test
// suite checks directly.
func (l *Log) TierSegments() (int, error) {
	if l.coldStore == nil {
		return 0, fmt.Errorf("tiering not enabled: call EnableTiering first")
	}

	tiered := 0
	for _, seg := range l.segments {
		if seg == l.active || seg.tiered {
			continue
		}
		key := fmt.Sprintf(segmentNameFmt, seg.baseOffset)
		if err := seg.tierOut(l.coldStore, key); err != nil {
			return tiered, fmt.Errorf("tier segment base %d: %w", seg.baseOffset, err)
		}
		tiered++
	}
	return tiered, nil
}
