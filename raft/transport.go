package raft

// Transport is how a Raft peer sends RequestVote/AppendEntries RPCs to
// another peer, by peer ID. Introduced in v0.14 so the consensus logic
// (raft.go, rpc_vote.go, rpc_append.go — all unmodified in substance)
// doesn't care whether peers are reached in-process (simTransport, what
// every test since v0.1 has used) or over a real socket (TCPTransport,
// added in v0.14 — see docs/design_real_transport.md).
type Transport interface {
	SendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool)
	SendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool)
}

// simTransport adapts the in-process simulated Network (v0.1) into the
// Transport interface — this is what keeps every test from v0.1 through
// v1.0 passing completely unmodified: Make() still builds one of these
// internally, so nothing about the existing constructor changed.
type simTransport struct {
	net *Network
	me  int
}

func (t *simTransport) SendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	ok := t.net.Call(t.me, peer, func(target *Raft) bool {
		return target.RequestVote(args, reply)
	})
	return reply, ok
}

func (t *simTransport) SendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	ok := t.net.Call(t.me, peer, func(target *Raft) bool {
		return target.AppendEntries(args, reply)
	})
	return reply, ok
}
