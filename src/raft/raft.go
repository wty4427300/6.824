package raft

import (
	"6.824/labrpc"
	rand2 "math/rand"
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

// 每个raft节点日志提交成就应该发送applymsg给服务，通过传递给make的CommandValid设置为true表示applymsg包含新的提交日志。
// 在2d中需要发送其他消息使CommandValid设置为false。
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

// A Go object implementing a single Raft peer.
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
	electionTime time.Time

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
	StatePreCandidate
	numStates
)

// 下面附带一些会用到的一些角色转换的函数。
func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
}

//获取raft的state
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//投票参数的结构体，字段开头需要大写
type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的Id
	LastLogIndex int //	候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
//投票回复的结构体，字段开头必须大写。
type RequestVoteReply struct {
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
	// Your data here (2A).
}

func (rf *Raft) RequestVotesL() {
	//初始化投票的参数，这里暂时还有点问题还需要修改
	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		//当前节点的最后的日志索引
		len(rf.log.log) - 1,
		//最后的term
		rf.log.log[len(rf.log.log)-1].Term,
	}
	var reply = RequestVoteReply{}
	//每个任期只能投1票
	votes := 1
	//遍历所有的节点向除了本节点以外的所有节点发送投票
	for i, _ := range rf.peers {
		//其他节点发送投票prc
		if i != rf.me {
			go rf.RequestVote(votes, i, args, &reply)
		}
	}
}

// 选举rpc
func (rf *Raft) RequestVote(votes int, peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	//给其他节点发送投票
	vote := rf.sendRequestVote(peer, args, reply)
	if vote {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//如果投票的任期大于节点现在任期，那么就同意这次投票
		if args.Term > rf.currentTerm {
			rf.newTermL(args.Term)
			reply.VoteGranted = true
		}
		//投票成功
		if reply.VoteGranted {
			votes += 1
			//获得一半以上的选票
			if votes > len(rf.peers)/2 {
				if rf.currentTerm == args.Term {
					rf.becomeLeaderL()
				}
			}
		}
	}
}

//成为leader后需要修改的一些状态
func (rf *Raft) becomeLeaderL() {
	DPrintf("becomeLeader")
	rf.state = Leader
	for i := range rf.nextIndex {
		println(i)
		//这里需要重新设置一下应该发送的日志，但是我的日志结构还没设计好暂时先这样吧
		//rf.nextIndex[i]=rf
	}
}

func (rf *Raft) newTermL(term int) {
	DPrintf("%v newTerm %v follower\n", rf.me, term)
	rf.currentTerm = term
	//因为在新的任期中还没有投票所以设置为-1
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//获取rpc的client并发送
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const electionTime = 1 * time.Second

//设置选举时间
func (rf *Raft) SetElectionTime() {
	//现在的时间
	t := time.Now()
	//超时的时间
	t = t.Add(electionTime)
	//随机的毫秒数
	ms := rand2.Int63() % 300
	//随机后的超时时间
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//每50毫米执行一此ticker
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

//检查心跳
//如果当前节点是leader就重置超时时间
//如果当前节点不是leader(那就是follow)并且当前选举超时时间没有收到心跳，则重置心跳超时时间重新选举
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v: tick state %v\n", rf.me, rf.state)
	//随机超时时间
	if rf.state == Leader {
		//设置超时时间
		rf.SetElectionTime()
	}
	//如果当前时间大于超时时间说明心跳断开了
	if time.Now().After(rf.electionTime) {
		//随机超时时间
		rf.SetElectionTime()
		//角色变为候选人，重新开始选举
		rf.startElectionL()
	}
}

// 发起选举，因为该方法是在tick里面调用的，方法外部已经获取了锁，所以不用加锁
func (rf *Raft) startElectionL() {
	//发起投票当前任期加1
	rf.currentTerm += 1
	//先将自己变成Candidate
	rf.state = Candidate
	//先给自己投一票
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("%v:发起选举 for term %v\n", rf.me, rf.currentTerm)
	//给其他节点发送rpc
	rf.RequestVotesL()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//这里是用来初始化一个raft对象
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	//因为现在是在做一些初始化的处理所以term和state都应该初始化状态
	//初始状态下大家都是follow
	rf.currentTerm = 0
	rf.state = Follower
	rf.SetElectionTime()

	rf.votedFor = -1
	rf.log = mkLogEntry()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	return rf
}
