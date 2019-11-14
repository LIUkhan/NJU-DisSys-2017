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

import "sync"
import "labrpc"
import "time"
 import "math/rand"
 import "sync/atomic"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
const (
	Follower uint = iota
	Candidate
	Leader
)

const broadcastTime  time.Duration = time.Duration(10) * time.Millisecond

type ApplyMsg struct {
	Index       int
	Command		interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    Command		interface{}
    Term    int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister//持续程序
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm	int
	votedFor 	int
	logs	[]LogEntry

	// Volatile state on all servers
	commitIndex int // 已知的提交了的日志条目的最高索引
	lastApplied int //应用于状态机中日志条目的最高索引（从0单调递增

	// Volatile state on leaders每次选举后重置
	nextIndex  []int // (lastLogIndex+1)指向下一个要发送到server的日志条目索引
	matchIndex []int // 已知的复制到server上的日志条目最高索引（从0单调递增

    // State
    state   uint//三个状态 follower、candidate、leader
	//applyMsg chan
	applyCh chan ApplyMsg
    // Timer
    electionTimeout *time.Timer
	broadcastTimeout *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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

}

//
// restore previously persisted state.
//make里面加过锁了，这里不需要再加，否则死锁
func (rf *Raft) readPersist(data []byte) {

}

// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct	{
	Term int
	LeaderID int
}

type AppendEntriesReply struct	{
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//接受到该RPC的server的处理
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if	rf.state == Leader	{	
		return
	}
	//任期检查，日志和当前任期关系不大,决定replyRPC的情况
	//当前server任期比candidate大 false
	if args.Term < rf.currentTerm	{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}	else if args.Term == rf.currentTerm	{
		//还没投票
		if rf.votedFor == -1  {
			rf.votedFor = args.CandidateID
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateID)
	}	else if args.Term > rf.currentTerm	{
		if rf.state != Follower	{
			rf.convertFollower(args.Term)
		}
		if rf.votedFor == -1  {
			rf.votedFor = args.CandidateID
		}
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateID)
	}
	// fmt.Println(rf.me,"receive request from",args.CandidateID,"Vote for ",rf.votedFor)
	if reply.VoteGranted {
			var electionTime time.Duration
			electionTime = RandElectionTimeout()
			rf.electionTimeout.Reset(electionTime)
	}
	return
}

//选举leader,默认进来的都是candidate
func (rf *Raft) leaderElection()	{
	//重新开始electionTimeout
	var electionTime time.Duration
	electionTime = RandElectionTimeout()
	rf.electionTimeout.Reset(electionTime)

	args :=	RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		len(rf.logs) - 1,
		rf.logs[len(rf.logs)-1].Term,
	}
	num := len(rf.peers)

	var votes_sum int32
	votes_sum = 0

	for i := 0; i < num; i++ {
		if i == rf.me {
			atomic.AddInt32(&votes_sum, 1)
			continue
		}
		go func(server int,args RequestVoteArgs,rf *Raft)	{
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server,args,&reply)	{
				//回复是有问题的情况
				if reply.Term > rf.currentTerm {
					rf.convertFollower(reply.Term)
				}
				//回复成功的情况
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&votes_sum, 1)
					if atomic.LoadInt32(&votes_sum) > int32(len(rf.peers)/2) {
						rf.convertLeader()
					}
				}
			}
		}(i,args,rf)
	}
}

//server收到AppendEntries信号时候的处理,要考虑到第一次的情况
//存在收到该消息的server是leader或者candidate的情况一般是follower
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//面对leader信号，一切让步
	if args.Term < rf.currentTerm	{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}	else	 { 
		 //但是leader的term不能小，重置超时
		rf.convertFollower(args.Term)
	}	
	reply.Term = rf.currentTerm
	reply.Success = true	
	return
}

//由leader发送信号,若是不需要发有日志的rpc会自动计算出来的
func (rf *Raft) LogReplication()	{
	for i := 0; i < len(rf.peers); i++	{
		if i == rf.me	{
			continue
		}
		go func(server int,rf *Raft)	{
			//由于采用时钟驱动的goroutine，因此传入的rf需要筛选出leader
			if rf.state != Leader {
				return
			}		
			args :=	AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
			}

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server,args,&reply)	{
					if(reply.Term > rf.currentTerm)	{
						rf.convertFollower(reply.Term)			
				}
			}
		}(i,rf)
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//不是leader返回false，是leader同意提交并快速返回，log进入集群的入口，只能从
//leader节点进入
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index :=  -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	//初始化随机数种子，用于随机生成electionTimeouts
	rand.Seed(time.Now().UnixNano())
	// Your initialization code here.
	rf.state =  Follower
	//persistent state
	rf.currentTerm = 0
	rf.votedFor = -1//nil无法用int表示，故采用-1
	//如果slice的第一个元素为nil会导致gob Encode/Decode为空,这里改为一个空的LogEntry便于编码
	rf.logs = make([]LogEntry,1)//创建数组
	temp := LogEntry{}
	rf.logs = append(rf.logs, temp)

	//volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	//volatile state on leaders初始化数组
	rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	//时钟相关
	var electionTime time.Duration
	electionTime = RandElectionTimeout()
	rf.electionTimeout =  time.NewTimer	(electionTime)
	rf.broadcastTimeout = time.NewTimer(broadcastTime)
	rf.broadcastTimeout.Stop()

	go func(rf *Raft)	{
		for	{
			select	{
				case <-rf.electionTimeout.C:	{				
					rf.convertCandidate()	
				}	
				case <-rf.broadcastTimeout.C:	{
					if rf.state == Leader	{
						rf.LogReplication()
						rf.broadcastTimeout.Reset(broadcastTime)
						// fmt.Println(rf.me,"broadcastTImeout ")
					}
				}
			}
		}
	}(rf)

	return rf
}

func RandElectionTimeout ()  time.Duration {
	// 获取随机数
	a := 300 + rand.Intn(200)  // [300,500)
	electionTimeout := time.Duration(a) * time.Millisecond
	return electionTimeout
}

func (rf *Raft) convertLeader ()	{
	rf.state = Leader
	//时钟相关
	rf.electionTimeout.Stop()
	//重新开始计时
	rf.broadcastTimeout.Reset(broadcastTime)
}

func (rf *Raft) convertCandidate ()	{
	rf.state = Candidate
	//rf.electionTimeout不能Stop，因为存在多个candidate冲突后重新选举的情况
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderElection()
}

func (rf *Raft) convertFollower (term int)	{
	//Follower宕机导致任期号十分不一样，也需要及时修改，不应该不改变，所以以下三句不可取，由于该函数集成了term的变化
	// if	rf.state == Follower	{
	// 	return
	// }
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	var electionTime time.Duration
	electionTime = RandElectionTimeout()
	rf.electionTimeout.Reset(electionTime)
	rf.broadcastTimeout.Stop()
}