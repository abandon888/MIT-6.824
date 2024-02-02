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
// 好规范啊这文档

import (
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var logger = setupLogging()
var logPrefix = "raft: "

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
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

type electionStatus int

const (
	follower electionStatus = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	log            []interface{}  // each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	electionStatus electionStatus // 0: follower, 1: candidate, 2: leader
	voteGranted    int            // the number of votes that the candidate has received
	resetChan      chan struct{}  // channel to reset election timer
	electionTimer  *time.Timer    // election timer

	commitIndex int // index of highest log entry known to be committed，即已经被提交的最高日志条目的索引值
	lastApplied int // index of highest log entry applied to state machine，即最后被应用到状态机的日志条目索引值
	// just for leader
	nextIndex  []int // for each server, index of the next log entry to send to that server,即下一个要发送的日志条目的索引
	matchIndex []int // for each server, index of highest log entry known to be replicated on server,即已经复制给该服务器的日志的最高索引值
}

// AppendEntriesArgs is the struct for AppendEntries RPC arguments.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

// AppendEntriesReply is the struct for AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.electionStatus == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.SetPrefix("RequestVote: ")
	//log.Println("server ", rf.me, "in term", rf.currentTerm, "with status", rf.electionStatus)
	// If RPC request or response contains term T < currentTerm: return false
	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		//log.Println("After RV:server ", rf.me, "in term", rf.currentTerm, "with status", rf.electionStatus)
		rf.resetElectionTimer()
		return
	}

}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.electionStatus == leader {
		//PrintYellow("leader " + strconv.Itoa(rf.me) + "receive append entries from server(leader) " + strconv.Itoa(args.LeaderId) + "in term" + strconv.Itoa(args.Term))
		Debug(dInfo, "S%d receive append entries from server(leader) %d in term %d", rf.me, args.LeaderId, args.Term)
	}
	if args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term)
		reply.Success = true
	} else {
		reply.Success = false
	}
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//how to deal with reply?save it to rf?
	//log.SetPrefix("sendRequestVote: ")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return ok
	}

	// 确保仍然处于候选者状态且任期未变
	if rf.electionStatus == candidate && args.Term == rf.currentTerm {
		if reply.VoteGranted {
			rf.voteGranted++
		}
		Debug(dVote, "S%d send request vote to S%d, in term %d, get %v", rf.me, server, reply.Term, reply.VoteGranted)
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(dAppend, "S%d send append entries to S%d,in term %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//PrintRed("server " + strconv.Itoa(rf.me) + "send append entries to server " + strconv.Itoa(server) + "failed")
		Debug(dError, "S%d send append entries to S%d failed", rf.me, server)
	}
	//if reply term is higher,then convert to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	//log.Printf("after:S%d send append entries to S%d,in term %d,get %v", rf.me, server, reply.Term, reply.Success)
	Debug(dAppend, "S%d send append entries to S%d,in term %d,get %v", rf.me, server, reply.Term, reply.Success)
	return ok
}

// convert to follower
func (rf *Raft) convertToFollower(term int) {

	//rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionStatus = follower
	//log.Println("server ", rf.me, "convert to follower", "in term", rf.currentTerm, "with status", rf.electionStatus)
	Debug(dTerm, "S%d convert to follower in term %d with status %d", rf.me, rf.currentTerm, rf.electionStatus)
	rf.resetElectionTimer()
	//rf.mu.Unlock()
}

// convert to candidate
func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionStatus = candidate
	rf.voteGranted = 1
	rf.mu.Unlock()
	//log.Println("server ", rf.me, "convert to candidate", "in term", rf.currentTerm, "with status", rf.electionStatus)
	Debug(dVote, "S%d convert to candidate in term %d with status %d", rf.me, rf.currentTerm, rf.electionStatus)
	rf.resetElectionTimer()
}

// convert to leader
func (rf *Raft) convertToLeader() {
	//rf.mu.Lock()
	rf.votedFor = rf.me    //prevent vote for others
	rf.stopElectionTimer() //stop election timer//todo:why stop election timer here?
	rf.electionStatus = leader
	//rf.mu.Unlock()
	//log.Println("server ", rf.me, "stop election timer", "in term", rf.currentTerm, "with status", rf.electionStatus)
	Debug(dTimer, "S%d stop election timer in term %d with status %d", rf.me, rf.currentTerm, rf.electionStatus)
	rf.startHeartBeat()
}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

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
	//log.SetPrefix("Kill: ")
	//log.Println("\033[31m raft server killed\033[0m")
	//Debug(dInfo, "raft server killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// send heartbeat to all peers
func (rf *Raft) sendHeartBeat() {
	//use go routine to send heartbeat to all peers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(server, args, reply)
		}(i)
	}
}

// start heartbeat
func (rf *Raft) startHeartBeat() {
	// send initial empty AppendEntries RPCs (heartbeat) to each server
	// repeat during idle periods to prevent election timeouts (§5.2)
	go func() {
		for {
			rf.sendHeartBeat()
			timeout := time.Duration(150+rand.Intn(80)) * time.Millisecond
			time.Sleep(timeout)
		}
	}()
}

// send request vote to all peers
func (rf *Raft) sendRequestVoteToAll() {
	//It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
	var wg sync.WaitGroup
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		// Except self
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(server int) { //use goroutine to send request vote to all peers
			defer wg.Done()
			ok := rf.sendRequestVote(server, &RequestVoteArgs{currentTerm, rf.me, len(rf.log), currentTerm}, &RequestVoteReply{})
			if ok {
				rf.mu.Lock()
				if rf.voteGranted > len(rf.peers)/2 && rf.electionStatus == candidate {
					Debug(dLeader, "S%d become leader in term %d with %d votes", rf.me, currentTerm, rf.voteGranted)
					rf.convertToLeader()
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	wg.Wait() // Wait for all goroutines to finish
}

func (rf *Raft) startElection() {
	rf.convertToCandidate()
	rf.sendRequestVoteToAll()
}

// reset election timer, send a signal to election timer and call resetElectionTimeout
func (rf *Raft) resetElectionTimer() {
	rf.resetChan <- struct{}{}
}

// reset election timer to a random time
func (rf *Raft) resetElectionTimeout() {
	Debug(dTimer, "S%d reset election timer", rf.me)
	rf.electionTimer.Reset(time.Duration(520+rand.Intn(150)) * time.Millisecond)
}

// stop election timer
func (rf *Raft) stopElectionTimer() {
	PrintBlue("server " + strconv.Itoa(rf.me) + " stop election timer")
	rf.electionTimer.Stop()
}

// ticker is the ticker goroutine for Raft.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		rf.mu.Lock()
		status := rf.electionStatus
		rf.mu.Unlock()

		//if status == candidate {
		//	//PrintBlue("candidate " + strconv.Itoa(rf.me) + "in term" + strconv.Itoa(rf.currentTerm) + "with status" + strconv.Itoa(int(rf.electionStatus)))
		//	Debug(dLog, "S%d in term %d with status %d", rf.me, rf.currentTerm, rf.electionStatus)
		//}
		if status == leader {
			return
		} else {
			Debug(dTimer, "S%d in term %d with status %d", rf.me, rf.currentTerm, rf.electionStatus)
			// Check if a leader election should be started.
			select {
			case <-rf.electionTimer.C:
				rf.startElection()
			case <-rf.resetChan:
				// 确保在重置计时器之前先停止计时器并清空其通道
				if !rf.electionTimer.Stop() && len(rf.electionTimer.C) > 0 {
					<-rf.electionTimer.C // 清空通道
				}
				rf.resetElectionTimeout()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		currentTerm:    0,
		votedFor:       -1,
		log:            make([]interface{}, 0),
		electionStatus: follower,
		voteGranted:    0,
		resetChan:      make(chan struct{}, 1),
		electionTimer:  time.NewTimer(time.Duration(520+rand.Intn(150)) * time.Millisecond),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
