package raft

import (
	"math/rand"
	"sync"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandiateId  int
	LastLogIndx int
	LastLogTerm int
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	// randomized election timeouts to ensure that split votes are rare and that they are resolved quickly.
	electionTimeOut := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeOut)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		DPrintf(dTerm, "根据RPC可知当前Term:%d已经过期,更新为:%d", rf.me, rf.currentTerm, term)
		rf.state = Follower
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
	}
}

func (rf *Raft) LeaderElection() {
	var voteCounter int
	// a follower increments its current term and transitions to candidate state.
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me

	rf.persist()

	voteCounter++
	rf.resetElectionTimer()

	DPrintf(dInfo, "在Term：%d发起选举", rf.me, rf.currentTerm)
	// TODO: persist
	term := rf.currentTerm
	// It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
	lastlog := rf.log.lastLog()
	args := RequestVoteArgs{
		Term:        term,
		CandiateId:  rf.me,
		LastLogIndx: lastlog.Index,
		LastLogTerm: lastlog.Term,
	}

	var becomeLeader sync.Once
	for i := range rf.peers {
		if i != rf.me {
			go rf.candidateRequestVote(i, &args, &voteCounter, &becomeLeader)
		}
	}
}
