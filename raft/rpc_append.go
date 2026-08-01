package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// ConflictIndex/ConflictTerm let the leader skip nextIndex back a whole
	// term at a time on mismatch, instead of one entry per RPC (§5.3
	// optimization) — matters once logs diverge by more than a few entries.
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries handles both heartbeats (empty Entries) and log replication.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return true
	}

	// valid leader for this term: reset election timer, step down if we
	// were mid-election ourselves (candidate seeing a current leader).
	rf.state = follower
	rf.resetElectionTimer()

	lastIndex := rf.log[len(rf.log)-1].Index
	if args.PrevLogIndex > lastIndex {
		reply.Success = false
		reply.ConflictIndex = lastIndex + 1
		reply.ConflictTerm = -1
		return true
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == conflictTerm {
			idx--
		}
		reply.Success = false
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = idx
		return true
	}

	// find first index where our log diverges from the leader's entries,
	// truncate from there, then append the rest.
	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx <= rf.log[len(rf.log)-1].Index && rf.log[idx].Term != entry.Term {
			rf.log = rf.log[:idx]
			break
		}
	}
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx > rf.log[len(rf.log)-1].Index {
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	return true
}

// broadcastAppendEntries sends AppendEntries (heartbeat or log entries) to
// every peer for the given term. Safe to call from the heartbeat loop or
// right after Start() to push a new entry out immediately.
func (rf *Raft) broadcastAppendEntries(term int) {
	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendAppendEntriesTo(peer, term)
	}
}

func (rf *Raft) sendAppendEntriesTo(peer int, term int) {
	rf.mu.Lock()
	if rf.state != leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]
	prevLogIndex := next - 1
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	prevLogTerm := rf.log[prevLogIndex].Term
	var entries []LogEntry
	if next <= rf.log[len(rf.log)-1].Index {
		entries = append(entries, rf.log[next:]...)
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, ok := rf.transport.SendAppendEntries(peer, args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = newMatch + 1
		rf.advanceCommitIndex()
		return
	}

	// backtrack nextIndex using the follower's conflict hint.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
		return
	}
	lastIdxOfTerm := -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			lastIdxOfTerm = i
			break
		}
	}
	if lastIdxOfTerm >= 0 {
		rf.nextIndex[peer] = lastIdxOfTerm + 1
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
}

// advanceCommitIndex must be called with rf.mu held. A leader commits an
// entry once it's replicated on a majority AND it was created in the
// leader's current term (§5.4.2 — never commit by counting replicas of an
// entry from a prior term, that's the classic Raft correctness pitfall).
func (rf *Raft) advanceCommitIndex() {
	majority := len(rf.peers)/2 + 1
	for n := rf.log[len(rf.log)-1].Index; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = n
			break
		}
	}
}

// Start appends a command to the leader's log and kicks off replication.
// Returns the index the command will occupy if committed, the current
// term, and whether this server believes it's the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}
	index := rf.log[len(rf.log)-1].Index + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	rf.broadcastAppendEntries(term)
	return index, term, true
}
