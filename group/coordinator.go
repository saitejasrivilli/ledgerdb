// Package group implements consumer-group coordination described in
// docs/design_consumer_groups.md: a single coordinator assigns partitions
// to live consumers, round-robin, rebalancing on join/leave/heartbeat
// timeout.
package group

import (
	"sort"
	"sync"
	"time"
)

const defaultHeartbeatTimeout = 500 * time.Millisecond

// Coordinator assigns numPartitions partitions round-robin across the
// currently-live members of one consumer group.
type Coordinator struct {
	mu               sync.Mutex
	numPartitions    int
	heartbeatTimeout time.Duration
	lastHeartbeat    map[string]time.Time
	assignment       map[string][]int

	stopCh chan struct{}
	closed bool
}

// NewCoordinator starts a coordinator for a group reading numPartitions
// partitions. heartbeatTimeout of 0 uses the default.
func NewCoordinator(numPartitions int, heartbeatTimeout time.Duration) *Coordinator {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = defaultHeartbeatTimeout
	}
	c := &Coordinator{
		numPartitions:    numPartitions,
		heartbeatTimeout: heartbeatTimeout,
		lastHeartbeat:    make(map[string]time.Time),
		assignment:       make(map[string][]int),
		stopCh:           make(chan struct{}),
	}
	go c.expiryLoop()
	return c
}

// Join registers a consumer and triggers an immediate rebalance.
func (c *Coordinator) Join(consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastHeartbeat[consumerID] = time.Now()
	c.rebalanceLocked()
}

// Heartbeat records liveness for consumerID. Does not itself trigger a
// rebalance — membership only changes on Join/Leave/expiry, a bare
// heartbeat from an already-known consumer changes nothing to assign.
func (c *Coordinator) Heartbeat(consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.lastHeartbeat[consumerID]; ok {
		c.lastHeartbeat[consumerID] = time.Now()
	}
}

// Leave removes a consumer immediately (clean shutdown path) and
// rebalances the remaining members.
func (c *Coordinator) Leave(consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.lastHeartbeat, consumerID)
	c.rebalanceLocked()
}

// Assignment returns the partitions currently assigned to consumerID.
func (c *Coordinator) Assignment(consumerID string) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := c.assignment[consumerID]
	out := make([]int, len(parts))
	copy(out, parts)
	return out
}

// FullAssignment returns a copy of the entire consumer -> partitions map,
// for tests/inspection.
func (c *Coordinator) FullAssignment() map[string][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]int, len(c.assignment))
	for k, v := range c.assignment {
		cp := make([]int, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// rebalanceLocked recomputes assignment round-robin over sorted live
// consumer IDs and sorted partition IDs — deterministic given the same
// membership set (see design doc).
func (c *Coordinator) rebalanceLocked() {
	var live []string
	for id := range c.lastHeartbeat {
		live = append(live, id)
	}
	sort.Strings(live)

	newAssignment := make(map[string][]int, len(live))
	for _, id := range live {
		newAssignment[id] = nil
	}
	for p := 0; p < c.numPartitions; p++ {
		if len(live) == 0 {
			break
		}
		owner := live[p%len(live)]
		newAssignment[owner] = append(newAssignment[owner], p)
	}
	c.assignment = newAssignment
}

// expiryLoop periodically marks consumers dead if their heartbeat is
// older than heartbeatTimeout, rebalancing when membership changes.
func (c *Coordinator) expiryLoop() {
	ticker := time.NewTicker(c.heartbeatTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.mu.Lock()
			changed := false
			now := time.Now()
			for id, last := range c.lastHeartbeat {
				if now.Sub(last) > c.heartbeatTimeout {
					delete(c.lastHeartbeat, id)
					changed = true
				}
			}
			if changed {
				c.rebalanceLocked()
			}
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.stopCh)
}
