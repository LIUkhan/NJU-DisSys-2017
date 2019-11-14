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
 import "bytes"
 import "math/rand"
 import "encoding/gob"
 import "sync/atomic"
//  import "fmt"

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
	//extra 自定义变量
	repliNum int//用于控制commitIndex是否需要更新
}

func StateString(s uint) string {
	if s == Follower	{
		return "Follower"
	}
	if s == Candidate	{
		return "Candidate"
	}		
	if s == Leader	{
		return "Leader"
	}	
	return ""
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
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//make里面加过锁了，这里不需要再加，否则死锁
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
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
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
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
	defer rf.persist()
	logcheck	:=	 true
    if len(rf.logs) > 0 {
		//日志安全性检查
		//最新的日志的任期号要小于候选者的，等于的情况下要长度要小，否则candidate886
        if (rf.logs[len(rf.logs)-1].Term > args.LastLogTerm) ||
            (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex) {
				logcheck = false
        }
	}
	//任期检查，日志和当前任期关系不大,决定replyRPC的情况
	//当前server任期比candidate大 false
	if args.Term < rf.currentTerm	{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}	else if args.Term == rf.currentTerm	{
		//还没投票
		if rf.votedFor == -1  && logcheck {
			rf.votedFor = args.CandidateID
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateID)
	}	else if args.Term > rf.currentTerm	{
		// rf.convertState(Follower)
		// rf.currentTerm = args.Term
		if rf.state != Follower	{
			rf.convertFollower(args.Term)
		}

		if rf.votedFor == -1 && logcheck {
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
					// rf.currentTerm = reply.Term
					// rf.convertState(Follower)
					rf.convertFollower(reply.Term)
					rf.persist()
				}
				//回复成功的情况
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&votes_sum, 1)
					if atomic.LoadInt32(&votes_sum) > int32(len(rf.peers)/2) {
						// rf.convertState(Leader)
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
	defer rf.persist()
	//面对leader信号，一切让步
	if args.Term < rf.currentTerm	{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}	else	 {  //但是leader的term不能小
		// rf.currentTerm = args.Term
		// rf.convertState(Follower)
		rf.convertFollower(args.Term)
	}	
	//接受到该RPC后重置选举超时,前面的情况不需要重置
	// var electionTime time.Duration
	// electionTime = RandElectionTimeout()
	// rf.electionTimeout.Reset(electionTime)

	// fmt.Println(rf.me,"receive log from ",args.LeaderID)
	
	//根据图7的情况，有可能少了很多份日志，也有可能多了
	lastLogIndex := len(rf.logs) - 1
	if lastLogIndex < args.PrevLogIndex	{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//排除第一次发送日志没有prevlogterm的情况，多了和不匹配的情况处理如下
	if	args.PrevLogTerm != 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term	{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	//非心跳
	if	len(args.Entries) >= 1 {
		rf.logs = append(rf.logs, args.Entries...)
		if args.LeaderCommit > rf.commitIndex	{
			newLogIndex := args.PrevLogIndex + len(args.Entries) 
			if newLogIndex > args.LeaderCommit	{
				rf.commitIndex = args.LeaderCommit
			}	else	{
				rf.commitIndex = newLogIndex
			}
		}	
		rf.applyLog()
	}	
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
			//heartbeat为空，可以发送多条提升效率
			var entry []LogEntry
			prevLogIndex := rf.nextIndex[server] - 1
			var prevLogTerm int
			if prevLogIndex == 0 {
				prevLogTerm = -1
			}	else{
				prevLogTerm = rf.logs[prevLogIndex].Term
			}

			entry = make([]LogEntry,0)
			entry = append(entry,rf.logs[rf.nextIndex[server]:]...)
			
			args :=	AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				prevLogIndex,
				prevLogTerm,
				entry,
				rf.commitIndex,
			}

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server,args,&reply)	{
				if reply.Success	{
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)//len(rf.logs) - 1
					rf.nextIndex[server] = rf.matchIndex[server]  + 1//len(rf.logs)
					threhold := len(rf.peers)/2 + 1
					//限制是为了防止图8的问题）旧的已经复制到大多数服务器的日志被覆盖
					rf.commitIndex = rf.updateCommitIndex(threhold)
					rf.applyLog()
				}	else	{
					if(reply.Term > rf.currentTerm)	{
						// rf.convertState(Follower)
						// rf.currentTerm = reply.Term		
						rf.convertFollower(reply.Term)			
					}
					if	reply.Term != args.PrevLogTerm	{
						//减少nextindex，由领导人去覆盖,不去做优化,论文中怀疑reply多出的信息的可用性不大
						if rf.nextIndex[server] > 0	{
							rf.nextIndex[server]--//这样可以做到已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 
						}							
					}
				}
				rf.persist()
			}
		}(i,rf)
	}
}


//外面需要加锁
func (rf *Raft) updateCommitIndex(threhold int) int{
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		support := 0
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= i {
				support++
			}
		}  
		//必须保证日志在大多数服务器并且是当前任期的日志
		if support > threhold {
			rf.repliNum = 0
			return	i
		}
	}
	return rf.commitIndex
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
	// var index,term int
	// var isLeader bool
	if isLeader {
			index =  len(rf.logs) 
			entry := LogEntry{
				Term:    term,
				Command: command,
			}
			rf.logs = append(rf.logs, entry)
			rf.matchIndex[rf.me] = index
			rf.nextIndex[rf.me] = index + 1
			rf.persist()
			rf.LogReplication()
	}
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
	//默认rf.lastApplied =0所以说，0号索引位置不能用，从1开始
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
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

	//extra
	rf.repliNum = 0
	rf.persist()

	go func(rf *Raft)	{
		for	{
			select	{
				case <-rf.electionTimeout.C:	{
					// if	rf.state == Follower	{
					// 	fmt.Println("Electimeout ",rf.me)
					// 	rf.convertState(Candidate)
					// }	else	{
					// 	//在有多个candidate冲突的情况下，candidate重新在timeout时候重新选举
					// 	//但是此时任期要增加,补充converState(Candidate)的工作
					// 	rf.currentTerm++
					// 	rf.votedFor = rf.me
					// 	rf.leaderElection()
					// }				
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


// 对于所有服务器都需要执行的,执行前外部需要解锁
func (rf *Raft) applyLog() {
	if(rf.commitIndex > rf.lastApplied)	{
		entriesToApply := append([]LogEntry{}, rf.logs[rf.lastApplied+1:rf.commitIndex+1]...)
		beginningIdx := rf.lastApplied+1
		go func(beginningIdx int,entries []LogEntry,rf *Raft)	{
			for idx,entry := range entries	{
				msg := ApplyMsg{
					Index:   beginningIdx+idx,
					Command: entry.Command,
				}
				rf.applyCh <- msg 
				if rf.lastApplied < msg.Index {
					rf.lastApplied = msg.Index
				}
			}
		}(beginningIdx,entriesToApply,rf)
	}	
}

//状态、时钟、candidate任期、投票状态、领导人初始化nextindex和matchindex
//外界上锁
func (rf *Raft) convertState (state uint)	{
	defer rf.persist()
	if state == rf.state {
		return
	}
	rf.state = state
	switch	state	{
		case Follower:	{
			// fmt.Println(rf.me,"convert2 Follower")
			rf.votedFor = -1
			var electionTime time.Duration
			electionTime = RandElectionTimeout()
			rf.electionTimeout.Reset(electionTime)
			rf.broadcastTimeout.Stop()
		}
		case Candidate:	{
			//rf.electionTimeout不能Stop，因为存在多个candidate冲突后重新选举的情况
			// fmt.Println(rf.me,"convert2 Candidate")
			rf.currentTerm++
			rf.votedFor = rf.me
			// fmt.Println(rf.me," ",rf.currentTerm)
			rf.leaderElection()
		}
		case Leader:	{	
			// fmt.Println(rf.me,"convert2 Leader")
			rf.nextIndex = make([]int,len(rf.peers))
			for i := 0 ; i < len(rf.peers) ;i++ {
				rf.nextIndex[i] = len(rf.logs)
			}
			rf.matchIndex = make([]int,len(rf.peers))
			//时钟相关
			rf.electionTimeout.Stop()
			//重新开始计时
			rf.broadcastTimeout.Reset(broadcastTime)
		}
	}
}

func RandElectionTimeout ()  time.Duration {
	// 获取随机数
	a := 300 + rand.Intn(200)  // [300,500)
	// fmt.Println("electimeout:",a)
	electionTimeout := time.Duration(a) * time.Millisecond
	return electionTimeout
}

func (rf *Raft) convertLeader ()	{
	defer rf.persist()
	rf.state = Leader
	// fmt.Println(rf.me,"convert2 Leader ",rf.currentTerm)
	rf.nextIndex = make([]int,len(rf.peers))
	for i := 0 ; i < len(rf.peers) ;i++ {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int,len(rf.peers))
	//时钟相关
	rf.electionTimeout.Stop()
	//重新开始计时
	rf.broadcastTimeout.Reset(broadcastTime)
}

func (rf *Raft) convertCandidate ()	{
	defer rf.persist()
	rf.state = Candidate
	//rf.electionTimeout不能Stop，因为存在多个candidate冲突后重新选举的情况
	// fmt.Println(rf.me,"convert2 Candidate ",rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
	// fmt.Println(rf.me," ",rf.currentTerm)
	rf.leaderElection()
}

func (rf *Raft) convertFollower (term int)	{
	defer rf.persist()
	//Follower宕机导致任期号十分不一样，也需要及时修改，不应该不改变，所以以下三句不可取，由于该函数集成了term的变化
	// if	rf.state == Follower	{
	// 	return
	// }
	rf.currentTerm = term
	rf.state = Follower
	// fmt.Println(rf.me,"convert2 Follower ",rf.currentTerm)
	rf.votedFor = -1
	var electionTime time.Duration
	electionTime = RandElectionTimeout()
	rf.electionTimeout.Reset(electionTime)
	rf.broadcastTimeout.Stop()
}