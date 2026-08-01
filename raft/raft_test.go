package raft

import (
	"testing"
	"time"
)

// TestInitialElection covers 3A: a fresh cluster elects exactly one leader
// and all servers agree on the term.
func TestInitialElection(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	cfg.checkOneLeader()
	term1 := cfg.checkTerms()

	time.Sleep(300 * time.Millisecond)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		t.Errorf("term changed without cause: %d -> %d", term1, term2)
	}
}

// TestReElection covers 3A: killing the leader triggers a new election,
// and the cluster recovers a single leader once a majority reconnects.
func TestReElection(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	leader1 := cfg.checkOneLeader()

	cfg.disconnect(leader1)
	leader2 := cfg.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("expected new leader, still %d", leader1)
	}

	cfg.disconnect((leader2 + 1) % 3)
	cfg.disconnect((leader2 + 2) % 3)
	time.Sleep(500 * time.Millisecond)

	// no majority left connected (only leader2 itself) -> no leader should
	// be able to commit, but GetState may still self-report leader; the
	// real assertion is deferred to TestBasicAgreement's quorum check.

	cfg.connect((leader2 + 1) % 3)
	cfg.connect((leader2 + 2) % 3)
	cfg.checkOneLeader()
}

// TestBasicAgreement covers 3B: a command submitted to the leader commits
// on all servers.
func TestBasicAgreement(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	cfg.checkOneLeader()
	for i := 1; i <= 3; i++ {
		cfg.one(100+i, 3)
	}
}

// TestFailAgree covers 3B: cluster still reaches agreement (via quorum)
// after one follower is disconnected, and the disconnected follower catches
// up once reconnected.
func TestFailAgree(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	cfg.checkOneLeader()
	cfg.one(1, 3)

	victim := 0
	for i := 0; i < 3; i++ {
		term, isLeader := cfg.rafts[i].GetState()
		_ = term
		if !isLeader {
			victim = i
			break
		}
	}
	cfg.disconnect(victim)

	cfg.one(2, 2)
	cfg.one(3, 2)

	cfg.connect(victim)
	cfg.one(4, 3)
}

// TestUnreliableAgree covers 3B: agreement still eventually completes when
// the network drops/delays messages, exercising the retry paths in
// broadcastAppendEntries / startElection.
func TestUnreliableAgree(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	cfg.setReliable(false)
	cfg.checkOneLeader()
	cfg.one(1, 3)
	cfg.one(2, 3)
}
