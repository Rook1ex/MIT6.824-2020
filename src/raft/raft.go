package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
	"go.starlark.net/repl"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type ServerState int
const (
	Follower ServerState = iota
	Candidate
	Leader 
)

const NoneVotedFor int = -1  // 表示未投票

// 日志条目
type LogEntry struct {
	Term int  // 该日志条目被 leader 创建时的任期号
	Command interface{}  // 客户端提交的命令（Raft只负责复制，不关心内容）
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist state
	currentTerm int  // 服务器见过最大的任期号
	votedFor int  // 在currentTerm中投给了谁
	log []LogEntry  // 日志

	// volatile state
	commitIndex int  // 已经被多数确认的最大日志索引
	lastApplied int  // 应用到状态机的最大索引

	// volatile state in leaders
	nextIndex []int  // 对每个follower下一个要发送的日志索引
	matchIndex []int  // 每个follower已经复制到的最大日志索引

	state ServerState  // 当前服务器状态

	lastElectionReset time.Time   // 最近一次收到Leader心跳的时间点
}

// return log is-up-to-date

func (rf *Raft) isUpToDate(LastLogIndex int, LastLogTerm int) bool {
	currentlastlogindex := len(rf.log) - 1
	currentlastlogterm := rf.log[currentlastlogindex].Term

	if (currentlastlogterm < LastLogTerm) || (currentlastlogterm == LastLogTerm && currentlastlogindex <= LastLogIndex) {
		return true
	}

	return false
}

// 重置选举时间函数
func (rf *Raft) resetSelectTimeout() {
	rf.lastElectionReset = time.Now()
}

// 发起选举
func (rf *Raft) startElection() {
	if !rf.killed() {
		DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me)
		rf.state = Candidate
		rf.currentTerm ++
		rf.votedFor = rf.me

		voteCount := 0 // 记录所得票数

		// 构建请求投票RPC参数
		currentLastLogIndex := len(rf.log) - 1
		args := RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
			LastLogIndex: currentLastLogIndex,
			LastLogTerm: rf.log[currentLastLogIndex].Term,
		}

		// 并行发送投票请求
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(peer int) {
				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(peer, &args, &reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term < rf.currentTerm { // 拒绝比旧任期的回复
						return
					} else if reply.Term > rf.currentTerm { // 收到比当前任期大的回复，其他节点选举成功
						rf.state = Follower
						rf.currentTerm, rf.votedFor = reply.Term, NoneVotedFor
					} else if reply.Term == rf.currentTerm && rf.state == Candidate && reply.VoteGranted {
						voteCount ++
						
						if voteCount > len(rf.peers) / 2 {
							rf.state = Leader
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							// TODO 这里需要发送心跳告知其他节点
						}
					}
				}
			} (peer)
		}
	}
}

// 定时器检测是否超时
func (rf *Raft) electionTicker()  {
	for !rf.killed() {
		// 设定随机选举超时时间
		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		// 每10ms 醒来一次检查是否超过随机选举时间
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.lastElectionReset) > timeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求投票的候选人 ID
	LastLogIndex int // 候选人最后一条日志的索引
	LastLogTerm  int // 候选人最后一条日志的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前服务器的任期号，用于候选人更新自己
	VoteGranted bool // 是否同意给该候选人投票
}


// AppendEntriesArgs 是 Raft 协议中 Leader 向 Follower 发送的 AppendEntries RPC 请求参数结构
// 主要用于日志复制和心跳检测（心跳时 Entries 为空）
type AppendEntriesArgs struct {
	Term         int         // Leader 的当前任期号，用于 Follower 检测 Leader 是否过期
	LeaderId     int         // Leader 的节点 ID，Follower 可以通过这个 ID 重定向请求
	
	PrevLogIndex int         // 新日志条目被追加之前，需要匹配的最后一条日志的索引
	PrevLogTerm  int         // 新日志条目被追加之前，需要匹配的最后一条日志的任期号
	// （PrevLogIndex 和 PrevLogTerm 用于日志一致性检查：Follower 必须在该索引和任期上有匹配的日志，
	//  否则拒绝接收新日志，保证日志的连续性和一致性）

	Entries      []LogEntry  // 需要被复制到 Follower 的日志条目列表（心跳时为空）
	// 每个 LogEntry 通常包含：索引、任期、具体命令/数据

	LeaderCommit int         // Leader 已经提交的日志条目的最高索引，用于同步 Follower 的提交状态
}

// AppendEntriesReply 是 Follower 对 Leader 的 AppendEntries RPC 请求的响应结构
type AppendEntriesReply struct {
	Term    int    // Follower 的当前任期号，Leader 收到后会更新自己的任期（如果发现更大的任期）
	Success bool   // true 表示 Follower 成功匹配 PrevLogIndex 和 PrevLogTerm 并追加日志，false 表示匹配失败
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 检查这里逻辑是不是和发起选举重复
	rf.mu.Lock()
	defer rf.mu.Unlock() 
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return 
	}
	
	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.votedFor = NoneVotedFor
		rf.currentTerm = args.Term
	} 

	// 此时 args.Term == rf.currentTerm
	reply.Term = rf.currentTerm

	if (rf.votedFor == NoneVotedFor || rf.votedFor == args.CandidateId) && 
		rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		
		rf.resetSelectTimeout()
	} else {
		reply.VoteGranted = false
	}

	return 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { // 拒绝比当前任期低的请求
		reply.Success = false
		reply.Term = rf.currentTerm
		return 
	}

	if args.Term > rf.currentTerm { 
		// 收到比当前任期大的请求，说明选主成功， 转为Follower，更新任期，更新当前任期下未投票
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = NoneVotedFor
	}

	rf.resetSelectTimeout()

	currentLogLen := len(args.Entries)
	if currentLogLen == 0 { // heartbeat
		reply.Success = true
		reply.Term = args.Term
		return 
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
