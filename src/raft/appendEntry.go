package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) appendEntries(heartbeat bool) {
	// TODO:
	for peer := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		// TODO: 目前仅实现心跳RPC
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}

		go rf.leaderSendEntries(peer, &args)
	}
}

func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {
	DPrintf(dLog, "向s%d发送AppendEntries", rf.me, server)
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	// TODO: reply为success时的处理
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dInfo, "在Term:%d收到s%d的AppendEntriesRPC", rf.me, rf.currentTerm, args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	rf.resetElectionTimer()
	if rf.state == Candidate {
		rf.state = Follower
		rf.voteFor = -1
	}

	reply.Success = true
}
