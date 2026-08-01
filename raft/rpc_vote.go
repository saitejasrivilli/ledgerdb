package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote handles a vote request. Grants vote only if candidate's log
// is at least as up-to-date as the voter's own (§5.4.1 of the Raft paper) —
// this is what prevents a candidate with a stale log from ever winning and
// overwriting already-committed entries.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return true
	}

	lastIndex, lastTerm := rf.lastLogInfo()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if canVote && logOK {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return true
}
