package raft

import (
	"6.824/labrpc"
	"fmt"
	rand2 "math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
// 创建一个新的raft服务
// 开始协议在一个新的日志条目上
// 获取一个raft服务当前任期并检查他是不是leader
// 每次一个新的命令写入日志，每个raft节点应该个发送一个applymsg给同一个服务。
// rf = Make(...)
// create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
// start agreement on a new log entry
// rf.GetState() (term, isLeader)
// ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester)
// in the same server.
//

// ApplyMsg 每个raft节点日志提交成就应该发送applymsg给服务，通过传递给make的CommandValid设置为true表示applymsg包含新的提交日志。
// 在2d中需要发送其他消息使CommandValid设置为false。
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
// 我们要做的就是补全数据结构
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	state        raftState // raft的state由term和isleader构成
	electionTime time.Time // 选举时间
	heartBeat    time.Duration

	//persistent state
	currentTerm int // 当前的任期
	votedFor    int // 当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	log         Log // 日志条目(第一个索引为1)

	//volatile state
	commitIndex int // 已提交的最大的日志条目索引(从零开始)
	lastApplied int // 已经被应用到状态机的最大的日志条目索引(从零开始)

	//leader sate
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	//Snapshot state
	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	waitingSnapshot []byte
	waitingIndex    int //lastIncludedIndex
	waitingTerm     int //lastIncludedTerm
}

type raftState uint64

const (
	Follower raftState = iota
	Candidate
	Leader
)

// GetState 获取raft的state
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// 这里应该是做持久化的地方
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// 投票参数的结构体，字段开头需要大写
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的Id
	LastLogIndex int //	候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// 投票回复的结构体，字段开头必须大写。
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

func (rf *Raft) RequestVotesL() {
	//初始化投票的参数，这里暂时还有点问题还需要修改
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		//当前节点的最后的日志索引
		rf.log.lastLogIndex(),
		//最后的term
		rf.log.lastLog().Term,
	}
	votes := 1
	//遍历所有的节点向除了本节点以外的所有节点发送投票
	for i := range rf.peers {
		//其他节点发送投票prc
		var reply = RequestVoteReply{}
		if i != rf.me {
			go rf.candidateRequestVote(&args, &reply, &votes, i)
		}
	}
}

func (rf *Raft) candidateRequestVote(args *RequestVoteArgs, reply *RequestVoteReply, votes *int, serverId int) {
	ok := rf.sendRequestVote(serverId, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//term落后
	if reply.Term > args.Term {
		rf.newTermL(reply.Term)
		return
	}
	if reply.Term < args.Term {
		return
	}
	if !reply.VoteGranted {
		return
	}
	//获取选票
	*votes++
	//获取一半以上的投票
	if *votes > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		rf.becomeLeaderL()
	}
}

// RequestVote 投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//给其他节点发送投票
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	//投票失败
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//加强选举,term最新,日志最长
	lastLog := rf.log.lastLog()
	powerPeer := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && powerPeer {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		//同意投票重置超时时间
		rf.setElectionTime()
		DPrintf("节点[%d]: 节点[%d]投票给节点[%d]\n", rf.me, rf.currentTerm, rf.me)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// 成为leader后需要修改的一些状态
func (rf *Raft) becomeLeaderL() {
	DPrintf("节点[%v]: 成为Leader term[%v]", rf.me, rf.currentTerm)
	rf.state = Leader
	lastLogIndex := rf.log.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.appendEntries(true)
}

func (rf *Raft) newTermL(term int) {
	rf.currentTerm = term
	//因为在新的任期中还没有投票所以设置为-1
	rf.votedFor = -1
	rf.state = Follower
	DPrintf("节点[%v]: newTerm[%v] follower\n", rf.me, term)
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//获取rpc的client并发送
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if rf.state != Leader {
		return -1, term, false
	}

	index := rf.log.lastLogIndex() + 1
	log := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.log.append(log)
	DPrintf("节点[%v]: term[%v] addLog [%d]", rf.me, term, log)
	rf.persist()
	rf.appendEntries(false)
	return index, term, true
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 设置选举时间,为了减少选举冲突,这里每次选举的时间随机(150-300ms)
func (rf *Raft) setElectionTime() {
	t := time.Now()
	rf.electionTime = t.Add(time.Duration(150+rand2.Intn(150)) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.heartBeat)
		rf.tick()
	}
}

// 检查心跳
// 如果当前节点是leader就发送心跳
// 如果当前节点不是leader(那就是follow)并且当前选举超时时间没有收到心跳，则重置心跳超时时间重新选举
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("节点[%v]: tick state %v\n", rf.me, rf.state)
	if rf.state == Leader {
		//leader需要发送心跳
		rf.appendEntries(true)
	}
	//如果当前时间大于超时时间说明心跳断开了
	if time.Now().After(rf.electionTime) {
		//角色变为候选人,重新开始选举
		rf.startElectionL()
	}
}

// 起选举，因为该方法是在tick里面调用的，方法外部已经获取了锁，所以不用加锁
func (rf *Raft) startElectionL() {
	//发起投票当前任期加1
	rf.currentTerm++
	//先将自己变成Candidate
	rf.state = Candidate
	//先给自己投一票
	rf.votedFor = rf.me
	//发起一轮选举的时候重置超时时间
	rf.setElectionTime()
	rf.persist()
	DPrintf("节点[%v]: 发起选举 for term[%v]\n", rf.me, rf.currentTerm)
	//给其他节点发送rpc
	rf.RequestVotesL()
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("节点[%v]: rf.applyCond.Broadcast()", rf.me)
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 这里是用来初始化一个raft对象
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	//刚开始所有的节点都是follower,term从0开始
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	//心跳时间
	rf.heartBeat = 100 * time.Millisecond
	//设置选举的时间
	rf.setElectionTime()
	//初始化日志,first index is 1
	rf.log = mkLogEntry()
	//已提交的最大日志索引
	rf.commitIndex = 0
	//已应用到状态机的最新日志索引
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applier()
	return rf
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// all server rule 1
		if rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}

func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log.at(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}
