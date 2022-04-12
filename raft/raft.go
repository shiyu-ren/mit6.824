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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math"
	"math/rand"
)

// raft state
const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2

// timer
const ELECTION_LOW = 300
const ELECTION_HIGH = 500
const HEARTBEATSINTVAL = 50
const CHECKAPPENDINTVAL = 10
const CHECKCOMMITINTVAL = 10

// var rander = rand.New(rand.NewSource(time.Now().UnixNano()))
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
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

//
// A Go object implementing a single Raft peer.
//

type Entry struct {
	Term		int
	Command		interface{}
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

	// persistent state on all servers
	currentTerm int
	votedFor	int
	logs		[]Entry
	
	// volatile state on all servers
	commitIndex	int  
	lastApplied int 
	state 		int
	cond		*sync.Cond
	applyCh		chan ApplyMsg

	//volatile state on leaders
	nextIndex 	[]int
	matchIndex	[]int

	//timers
	timestamp	time.Time
	electionTimer 		*time.Timer
	heartsbeatsTimer 	*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LEADER {
		isLeader = true
	} else {
		isLeader = false
	}

	return term, isLeader
}

func (rf *Raft) ChangeToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) ChangeToFollower(newTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = newTerm
	rf.votedFor = -1
}	

func (rf *Raft) ChangeToLeader() {
	rf.state = LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) != nil {
		DPrintf("[server %v] readPersist error ", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.mu.Unlock()
	}
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int 
	VoteGranted		bool
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm 	int
	Entries			[]Entry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term			int
	Success			bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// 延迟执行，在函数返回前执行
	defer rf.mu.Unlock()
	// defer rf.persist()
	// defer DPrintf("{Node %v}'s state is {state %v, term %v, commitIndex %v, lastApplied %v, firstLog %v, lastLog %v} before processing")
	DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d]", rf.currentTerm, rf.me, args.CandidateId)
	// 若请求的节点的term小于当前任期，则跳过此请求（无leader权利）
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		// rf.ChangeState(StateFollower)
		// rf.currentTerm, rf.votedFor = args.term, -1
		rf.ChangeToFollower(args.Term)
		rf.persist()
	}

	reply.Term = rf.currentTerm
	// 候选者至少要比接受者的日志信息新
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.logs[len(rf.logs)-1].Term

		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.logs)-1) {
				
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				// rf.electionTimer.Reset(time.Millisecond*time.Duration(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW))
				rf.timestamp = time.Now()
				rf.persist()
				DPrintf("[term %d]: Raft [%d] vote for Raft [%d]", rf.currentTerm, rf.me, rf.votedFor)
				return
			}
	}
	reply.VoteGranted = false
	
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	// DPrintf()

	if(args.Term < rf.currentTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		// rf.ChangeState(StateFollower)
		// rf.currentTerm, rf.votedFor = args.term, -1
		rf.ChangeToFollower(args.Term)
		rf.persist()
	}

	rf.state = FOLLOWER
	reply.Term = rf.currentTerm

	// rf.electionTimer.Reset(time.Millisecond*time.Duration(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW))
	rf.timestamp = time.Now()

	// 如果当前rf日志在preLogIndex不包含term匹配的条目
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// 解决有冲突的条目并添加新的条目
	if(len(args.Entries) > 0) {
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	// i := 0
	// j := args.PrevLogIndex + 1

	// for i = 0; i < len(args.Entries); i++ {
	// 	// 没有冲突，结束循环
	// 	if j >= len(rf.logs) {
	// 		break
	// 	}
	// 	// 跳过相同的
	// 	if rf.logs[j].Term == args.Entries[i].Term {
	// 		j++
	// 	// 从冲突部分j后开始覆盖
	// 	} else {
	// 		rf.logs = append(rf.logs[:j], args.Entries[i:]...)
	// 		i = len(args.Entries)
	// 		j = len(rf.logs) - 1
	// 		break
	// 	}
	// }

	// // // 无冲突时候正常的append
	// if i < len(args.Entries) {
	// 	rf.logs = append(rf.logs, args.Entries[i:]...)
	// }
	// j = len(rf.logs) - 1
	// } else {
	// 	j--
	// }
	reply.Success = true
	lastLogIdx := int(math.Max(float64(len(rf.logs)-1), 0))
	// 判断是否要commit到状态机上
	// 首先要leader的commit大于当前rf节点commite
	if args.LeaderCommit > rf.commitIndex {

		oldCommitIndex := rf.commitIndex
		// 取LeaderCommit与自己logs最大idx的最小值
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastLogIdx)))

		if rf.commitIndex > oldCommitIndex {
			// 唤醒applycommit
			rf.cond.Broadcast()
		}
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

func (rf *Raft)	sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果不是leader或者已经kill，返回
	if rf.state != LEADER || rf.killed() {
		return index, term, false;
	}
	DPrintf("[term %d]: Raft [%d] start consensus", rf.currentTerm, rf.me)

	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
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



func (rf *Raft) commitChecker() {
	for {
		rf.mu.Lock()
		consensus := 1
		if rf.commitIndex < len(rf.logs) - 1 {
			N := rf.commitIndex + 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					consensus++
				}
			}
			if consensus*2 > len(rf.peers) && rf.logs[N].Term == rf.currentTerm {
				DPrintf("[term %d]:Raft [%d] [state %d] commit log entry %d successfully", rf.currentTerm, rf.me, rf.state, N)
				rf.commitIndex = N
				rf.cond.Broadcast()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(CHECKCOMMITINTVAL))
	}
}

func (rf *Raft) appendChecker(server int){
	for rf.killed() == false {

		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		rf.mu.Lock()
		lastLogIdx := len(rf.logs) - 1
		nextIdx := rf.nextIndex[server]
		// term := rf.currentTerm
		prevLogIndex := int(math.Max(float64(nextIdx - 1), 0))
		// prevLogTerm := rf.logs[prevLogIndex].Term
		// leaderCommit = rf.commitIndex
		// entries := rf.logs[nextIdx:]
		
		args := AppendEntriesArgs{
			Term:			rf.currentTerm,
			LeaderId:		rf.me,
			PrevLogIndex:	prevLogIndex,
			PrevLogTerm:	rf.logs[prevLogIndex].Term,
			Entries:		rf.logs[nextIdx:],
			LeaderCommit:	rf.commitIndex,
		}

		reply := AppendEntriesReply{}

		rf.mu.Unlock()
		if lastLogIdx >= nextIdx {
			rf.mu.Lock()
			DPrintf("[term %d]: Raft[%d] send real appendEntries to Raft[%d]", rf.currentTerm, rf.me, server)
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)

			rf.mu.Lock()

			if ok == false {
				rf.mu.Unlock()
				continue
			} else {
				// // 发送rpc期间term发生变化（应该不会发生） term confusion
				if args.Term != rf.currentTerm {
					rf.mu.Unlock()
					continue
				}

				if reply.Term > rf.currentTerm {
					rf.ChangeToFollower(reply.Term)
					rf.persist()
					rf.mu.Unlock()
					continue
				}

				if reply.Success {
					rf.nextIndex[server] = nextIdx + len(args.Entries)
					rf.matchIndex[server] = prevLogIndex + len(args.Entries)
				} else {
					// 如果因为日志不一致没有成功，则降低nextIndex并重试
					rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()
			}
		}
		time.Sleep(time.Millisecond * time.Duration(CHECKAPPENDINTVAL))
	}
}


func (rf *Raft) allocateAppendCheckers(){
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.appendChecker(peer)
	}
}

func (rf *Raft) DoBroadcastHeartbeat() {
	rf.mu.Lock()
	DPrintf("[term %d]:Raft [%d] [state %d] becomes leader !", rf.currentTerm, rf.me, rf.state)
	rf.mu.Unlock()
	for rf.killed() == false {

		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}
		
		rf.mu.Lock()
		// Term:			rf.currentTerm,
		// LeaderId:		rf.me,
		// PrevLogIndex:	prevLogIndex,
		// PrevLogTerm:	rf.logs[prevLogIndex].Term,
		// Entries:		rf.logs[nextIdx:],
		// LeaderCommit:	rf.commitIndex,
		// args := AppendEntriesArgs{
		// 	Term:			rf.currentTerm,
		// 	LeaderId:		rf.me,
		// 	PrevLogIndex:	len(rf.logs) - 1,
		// 	PrevLogTerm:	rf.logs[len(rf.logs)-1].Term,
		// 	Entries:		make([]Entry, 0),
		// 	LeaderCommit:	rf.commitIndex,
		// }

		
		// rf.electionTimer.Reset(time.Millisecond*time.Duration(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW))
		rf.timestamp = time.Now()
		rf.mu.Unlock()
	
		for peer, _ := range rf.peers {
			if peer == rf.me {
				continue
			}
	
			rf.mu.Lock()
			// nextIdx := rf.nextIndex[peer]
			// prevLogIndex := int(math.Max(float64(nextIdx - 1), 0))
			nextIdx := len(rf.logs)
			prevLogIndex := int(math.Max(float64(nextIdx - 1), 0))
			
			args := AppendEntriesArgs{
				Term:			rf.currentTerm,
				LeaderId:		rf.me,
				PrevLogIndex:	prevLogIndex,
				PrevLogTerm:	rf.logs[prevLogIndex].Term,
				// PrevLogIndex:	len(rf.logs) - 1,
				// PrevLogTerm:	rf.logs[len(rf.logs)-1].Term,
				Entries:		make([]Entry, 0),
				LeaderCommit:	rf.commitIndex,
			}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()
	
			go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				ok := rf.sendAppendEntries(server, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[term %d]:Raft [%d] [state %d] sends heartbeats to server[%d]", rf.currentTerm, rf.me, rf.state, server)
				if ok == false {
					return 
				} else {

					// // 发送rpc期间term发生变化（应该不会发生） 
					if args.Term != rf.currentTerm {
						return
					}

					if reply.Term > rf.currentTerm {

						rf.ChangeToFollower(reply.Term)
						rf.persist()
					}

				}
	
			}(peer, &args, &reply)
		}
		
		time.Sleep(time.Duration(HEARTBEATSINTVAL)*time.Millisecond)
	}
}

func (rf *Raft) DoElection() {

	// time.Sleep(time.Millisecond * time.Duration(ELECTION_LOW))

	rf.mu.Lock()

	rf.ChangeToCandidate()
	// rf.electionTimer.Reset(time.Millisecond*time.Duration(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW))
	rf.timestamp = time.Now()
	term := rf.currentTerm
	lastLogIdx := int(math.Max(float64(len(rf.logs) - 1), 0))
	lastLogTerm := rf.logs[lastLogIdx].Term
	args := RequestVoteArgs{
		Term:			term,
		CandidateId:	rf.me,
		LastLogIndex:	lastLogIdx,
		LastLogTerm:	lastLogTerm,
	}
	
	rf.persist()
	DPrintf("[term %d]:Raft [%d][state %d] starts an election", term, rf.me, rf.state)

	rf.mu.Unlock()


	grantedVotes := 1	// 用于记录我的得票数
	// electionFinished := false
	// rf.persist()
	// var voteMutex sync.Mutex
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// fmt.Printf("DoEletioning...\n")
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply){
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf()
				// 如果我是候选者并且任期号与其他持平
				if rf.currentTerm == reply.Term && rf.state == CANDIDATE {
					// 给了我票
					// voteMutex.Lock()
					if reply.VoteGranted {
						grantedVotes += 1
						// 超过半数
						if grantedVotes > len(rf.peers) / 2 {
							// DPrintf
							// rf.state = LEADER
							// electionFinished = true
							rf.ChangeToLeader()
							// 初始化nextindex与matchindex
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0
							}

							go rf.DoBroadcastHeartbeat()
							go rf.allocateAppendCheckers()
							go rf.commitChecker()
						}
						
					// 其他raft结点任期号大于我，我失去leader资格

					} else if reply.Term > rf.currentTerm {
						// Dprintf
						// rf.state = FOLLOWER
						// fmt.Printf("DoElection reply.Term > rf.currentTerm\n")
						rf.ChangeToFollower(reply.Term)
						// rf.currentTerm, rf.votedFor = reply.term, -1
						rf.persist()
					}
					// voteMutex.Unlock()
				}
			}
		}(peer)
	}
	// fmt.Printf("DoEletion end...\n")
}

func (rf *Raft) applyCommit() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service, ",
				rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		cmtIdx := rf.lastApplied
		command := rf.logs[cmtIdx].Command
		
		msg := ApplyMsg {
			CommandValid:	true,
			Command:		command,
			CommandIndex:	cmtIdx,
		}

		rf.applyCh <- msg
		DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service successfully", 
				rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		
		rf.mu.Unlock()
	}
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().Unix())
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// fmt.Printf("ticker...\n")
		// select {
		// case <- rf.electionTimer.C:
		// 	// rf.ChangeState()
		// 	// fmt.Printf("ticker to election\n")
		// 	// time.Sleep(time.Millisecond * time.Duration(ELECTION_LOW))
		// 	rf.mu.Lock()
		// 	// rf.state = CANDIDATE
		// 	// rf.ChangeToCandidate()
		// 	go rf.DoElection()
		// 	rf.mu.Unlock()
		// }
		timeout := int(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW)
		time.Sleep(time.Millisecond * time.Duration(timeout+1))
		rf.mu.Lock()
		// if timeout and the server is not a leader, start election
		if time.Since(rf.timestamp) > time.Duration(timeout)*time.Millisecond && rf.state != LEADER {
			// start a new go routine to do the election. This is important
			// so that if you are a candidate (i.e., you are currently running an election),
			// but the election timer fires, you should start another election.


			go rf.DoElection()
		}
		rf.mu.Unlock()

	}
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
	rf.dead = 0
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	// rf.logs = make([]Entry, 1)
	rf.logs = make([]Entry, 1)
	// rf.logs = append(rf.logs, Entry{Term : 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timestamp = time.Now()
	// rand.Seed(time.Now().Unix())
	// rf.electionTimer = time.NewTimer(time.Millisecond*time.Duration(rand.Float64()*(ELECTION_HIGH-ELECTION_LOW)+ELECTION_LOW))
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// rf.heartsbeatsTimer = time.NewTimer(time.Second*10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// 启动携程周期性的提交log entry到state machine
	go rf.applyCommit()

	return rf
}
