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
	// The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will send to that follower.
	//  When a leader first comes to power,
	// it initializes all nextIndex values to the index just after the last one in its log (11 in Figure 7).
	lastLog := rf.log.lastLog()
	for peer := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]

			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}

			if nextIndex <= 0 {
				nextIndex = 1
			}

			preLog := rf.log.at(nextIndex - 1)

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm:  preLog.Term,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}

			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(peer, &args)
		}
	}
}

func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {
	DPrintf(dLog, "向s%d发送AppendEntries", rf.me, server)
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		DPrintf(dError, "s%d没有正确回应", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		DPrintf(dTerm, "s%d的Term比当前term更新, Leader更新Term转为Follower", rf.me, server)
		rf.setNewTerm(reply.Term)
		return
	}

	// TODO: reply为success时的处理
	if args.Term == rf.currentTerm {
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
			DPrintf(dLeader, "S%d追加日志成功", rf.me, server)
		} else {
			// If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency check will fail in the next AppendEntries RPC.
			// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
			rf.nextIndex[server]--
		}

		rf.leaderCommitRule()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dInfo, "在Term:%d收到s%d的AppendEntriesRPC, preLogIndex: %d, preLogTerm:%d, LogEntry:%v, 当前日志%v", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.log)

	reply.Term = rf.currentTerm
	reply.Success = false
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// Rules for servers:
	// All servers:
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	rf.resetElectionTimer()
	// Rules for servers:
	// Candiates (5.2):
	// If AppendEntries RPC received from new leader: convert to follower
	if rf.state == Candidate {
		rf.state = Follower
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.log.len() || rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		return
	}

	for idx, entry := range args.Entries {
		// 3. If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
			rf.persist()
		}

		// 4. Append any new entries not already in the log
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			rf.persist()
			DPrintf(dLog, "追加日志Entry:%v, 当前日志:%v", rf.me, args.Entries[idx:], rf.log)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}

	reply.Success = true
}

func (rf *Raft) leaderCommitRule() {
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf(dCommit, "leader尝试提交 index %v, 当前日志%v", rf.me, rf.commitIndex, rf.log)
				rf.apply()
				break
			}
		}
	}
}
