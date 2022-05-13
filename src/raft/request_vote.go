package raft

import "sync"

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once) {
	DPrintf(dVote, "向s%d请求选票", rf.me, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < args.Term {
		DPrintf(dTerm, "s%d的term已经过期", rf.me, serverId)
		return
	}

	if reply.Term > args.Term {
		DPrintf(dTerm, "s%d在新term:%d, 更新当前term", serverId, reply.Term)
		rf.setNewTerm(reply.Term)
		// TODO: persisit
		return
	}

	if !reply.VoteGranted {
		// 在这个term中，serverId没有投票给args.CandiateId
		DPrintf(dTerm, "s%d在Term:%d没有投票给%d", rf.me, serverId, reply.Term, rf.me)
		return
	}

	*voteCounter++
	// TODO: 获得大多数选票，转变为leader
	if *voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		becomeLeader.Do(func() {
			rf.state = Leader
			DPrintf(dLeader, "在Term:%d获得大多数选票，State变换为Leader", rf.me, args.Term)
			rf.appendEntries(true)
		})
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		// TODO: persist
	}

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	// BUG: myLastLog = rf.log.lastLog() // candidate’s log is at least as up-to-date as receiver’s log,
	if rf.voteFor == -1 || rf.voteFor == args.CandiateId {
		rf.voteFor = args.CandiateId
		DPrintf(dVote, "向s%d 投票", rf.me, args.CandiateId)

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
}
