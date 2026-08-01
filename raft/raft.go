// Package raft implements the Raft consensus protocol (see
// docs/design_raft.md for the design rationale). Scope for v0.1: leader
// election + log replication for a single Raft group, no snapshotting.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

type state int

const (
	follower state = iota
	candidate
	leader
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

// LogEntry is one entry in the replicated log.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// ApplyMsg is sent on the apply channel once an entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft is the per-server consensus state.
type Raft struct {
	mu      sync.Mutex
	net     *Network
	me      int
	peers   []int
	applyCh chan ApplyMsg

	// persistent state (Figure 2)
	currentTerm int
	votedFor    int // -1 if none
	log         []LogEntry

	// volatile state
	state       state
	commitIndex int
	lastApplied int

	// volatile leader state, reinitialized on election
	nextIndex  map[int]int
	matchIndex map[int]int

	electionResetAt time.Time
	killed          bool
}

func Make(net *Network, me int, peers []int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		net:         net,
		me:          me,
		peers:       peers,
		applyCh:     applyCh,
		votedFor:    -1,
		state:       follower,
		log:         []LogEntry{{Term: 0, Index: 0}}, // index 0 sentinel
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.resetElectionTimer()
	net.AddServer(me, rf)
	go rf.electionTicker()
	go rf.applyTicker()
	return rf
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.killed = true
}

func (rf *Raft) isKilled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.killed
}

// GetState returns currentTerm and whether this server believes it's leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

func randElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (rf *Raft) resetElectionTimer() {
	rf.electionResetAt = time.Now()
}

// electionTicker fires elections when no heartbeat/vote reset the timer
// within a randomized timeout, per Figure 2's election timeout mechanism.
func (rf *Raft) electionTicker() {
	for !rf.isKilled() {
		timeout := randElectionTimeout()
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		elapsed := time.Since(rf.electionResetAt)
		isLeader := rf.state == leader
		rf.mu.Unlock()

		if !isLeader && elapsed >= timeout {
			rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	rf.mu.Unlock()

	votes := 1
	var votesMu sync.Mutex
	majority := len(rf.peers)/2 + 1

	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.net.Call(rf.me, peer, func(target *Raft) bool {
				return target.RequestVote(args, reply)
			})
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				count := votes
				votesMu.Unlock()
				if count >= majority && rf.state == candidate {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeFollower must be called with rf.mu held.
func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimer()
}

// becomeLeader must be called with rf.mu held.
func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	lastIndex := rf.log[len(rf.log)-1].Index
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = lastIndex + 1
		rf.matchIndex[peer] = 0
	}

	// §5.4.2: a leader can only advance commitIndex by counting replicas
	// of entries from its OWN term — it must never commit by counting
	// replicas of a prior-term entry directly, or a future leader could
	// disagree on whether that entry was really committed. Appending a
	// no-op immediately on election gives the leader something in its own
	// term to commit, which (once a majority replicates it) also commits
	// everything before it. Without this, a newly elected leader can sit
	// forever with committed-but-uncounted entries in its log.
	noopIndex := lastIndex + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: noopIndex})
	rf.matchIndex[rf.me] = noopIndex

	go rf.leaderHeartbeatLoop(rf.currentTerm)
}

func (rf *Raft) leaderHeartbeatLoop(term int) {
	for !rf.isKilled() {
		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.broadcastAppendEntries(term)
		time.Sleep(heartbeatInterval)
	}
}

// lastLogInfo must be called with rf.mu held.
func (rf *Raft) lastLogInfo() (int, int) {
	last := rf.log[len(rf.log)-1]
	return last.Index, last.Term
}

// applyTicker pushes newly committed entries to applyCh in order.
func (rf *Raft) applyTicker() {
	for !rf.isKilled() {
		rf.mu.Lock()
		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			if entry.Command == nil {
				// no-op entry appended on election (see becomeLeader) —
				// advances lastApplied/commitIndex but has nothing for
				// the state machine to apply.
				continue
			}
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			})
		}
		rf.mu.Unlock()

		for _, m := range msgs {
			rf.applyCh <- m
		}
		time.Sleep(10 * time.Millisecond)
	}
}
