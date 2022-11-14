package raft

type AppendEntriesArgs struct {
	Term         int //leaders term
	LeaderId     int
	PrevLogIndex int     //最新日志的前一条日志索引
	PrevLogTerm  int     //最新日志的前一条日志term
	Entries      []Entry //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int     //leader已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term     int  //当前term
	Success  bool //如果follower所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了,则为 true
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) appendEntries(heartbeat bool) {
	lastLogIndex := rf.log.lastLogIndex()
	//给所有服务器发送心跳
	for i := range rf.peers {
		if i == rf.me {
			rf.setElectionTime()
		}
		if lastLogIndex >= rf.nextIndex[i] || heartbeat {
			nextIndex := rf.nextIndex[i]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLogIndex+1 < nextIndex {
				nextIndex = lastLogIndex
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, lastLogIndex-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(i, &args)
		}
	}
}

//leaderSendEntries leader发送心跳
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {

}
