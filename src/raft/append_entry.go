package raft

type AppendEntriesArgs struct {
	Term         int
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
	//lastLogIndex := rf.log.lastLogIndex()
	//for i:=range rf.peers{
	//	if peer,_:=range r {
	//
	//	}
	//}
}
