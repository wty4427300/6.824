package raft

type AppendEntriesArgs struct {
	Term         int //leaders term
	LeaderId     int
	PrevLogIndex int     //最新日志的前一条日志索引
	PrevLogTerm  int     //最新日志的前一条日志term
	Entries      []Entry //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空;为了提高效率可能一次性发送多个）
	LeaderCommit int     //leader已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term     int  //当前term
	Success  bool //如果follower所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了,则为 true
	Conflict bool
	XTerm    int //冲突 entry 的任期，如果存在的话
	XIndex   int //XTerm 的第一条 entry 的 index
	XLen     int //缺失的 log 长度，case 3 中 S1 的 XLen 为 1
}

func (rf *Raft) appendEntries(heartbeat bool) {
	//leader的最新日志
	lastLogIndex := rf.log.lastLogIndex()
	//给所有服务器发送心跳
	for i := range rf.peers {
		if i == rf.me {
			//如果是leader,只重置选举超时时间
			rf.setElectionTime()
			continue
		}
		//rules for leader 3
		nextIndex := rf.nextIndex[i]
		if lastLogIndex >= nextIndex || heartbeat {
			if nextIndex <= 0 {
				//日志的index 0为空日志,所以从1开始
				nextIndex = 1
			}
			if lastLogIndex+1 < nextIndex {
				nextIndex = lastLogIndex
			}
			prevLog := rf.log.at(nextIndex - 1)
			//如果follower落后需要补充日志
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

// leaderSendEntries leader发送心跳
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		return
	}
	// rules for leader 3.1
	if args.Term == rf.currentTerm {
		if reply.Success {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			nextIndex := matchIndex + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], nextIndex)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], matchIndex)
			DPrintf("节点[%v]: 节点[%v] append success next[%v] match[%v]", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			//日志冲突
			DPrintf("节点[%v]: Conflict from [%v] [%#v]", rf.me, serverId, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[serverId] = reply.XLen
			} else {
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("节点[%v]: lastLogInXTerm[%v]", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}

			DPrintf("节点[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLogIndex(); i > 0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.lastLogIndex(); n++ {
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
				DPrintf("节点[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("节点[%d]: term[%d] follower 收到 leader[%v] AppendEntries[%d], prevIndex[%v], prevTerm[%v]", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// rules for servers
	// all servers 2
	reply.Success = false
	reply.Term = rf.currentTerm
	// append entries rpc 1
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
		return
	}
	rf.setElectionTime()

	// candidate rule 3
	if rf.state == Candidate {
		rf.state = Follower
	}
	// append entries rpc 2
	if rf.log.lastLogIndex() < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log.log)
		DPrintf("[%v]: Conflict XTerm[%v], XIndex[%v], XLen[%v]", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	//快速冲突处理
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = len(rf.log.log)
		DPrintf("节点[%v]: Conflict XTerm[%v], XIndex[%v], XLen[%v]", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc 3
		if entry.Index <= rf.log.lastLogIndex() && rf.log.at(entry.Index).Term != entry.Term {
			//index相同,term不同,删除之后的所有日志
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		if entry.Index > rf.log.lastLogIndex() {
			rf.log.appends(args.Entries[idx:]...)
			DPrintf("节点[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLogIndex())
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
