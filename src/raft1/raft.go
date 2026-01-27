package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type RaftSate int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.
	
	// Persistent State on all servers
	currentTerm int
	votedFor int
	log []LogEntry

	// Volatile State on all servers
	commitIndex int
	lastApplied int

	//Volatile State on leaders
	nextIndex map[int]int
	matchIndex map[int]int
	electionTimer time.Time

	State int

}

type LogEntry struct {
	Term int
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var Term int
	var isleader bool
	// Your code here (3A).
	
	Term = rf.currentTerm
	isleader = rf.State == LEADER
	return Term, isleader
}

// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftState := w.Bytes()
	// rf.persister.Save(raftState, nil)
}


// restore previously persisted State.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}
 type AppendEntriesReply struct {
	Term int
	Success bool
}

// LastLogTermAndIndex helps get last log Term and idnex 
func (rf *Raft) LastLogTermAndIndex() (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	index := len(rf.log)-1
	return rf.log[index].Term, index 

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm=args.Term
		rf.votedFor = -1 
		rf.State = FOLLOWER
	}

	LastLogTerm, LastLogIndex := rf.LastLogTermAndIndex()
	logUpToDate := args.LastLogTerm > LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex >= LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && logUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionTimer = time.Now()
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
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(Command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true

	// Your code here (3B).


	return index, Term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimeout() time.Duration {
	interval := rand.Intn(300-150) + 150
	return time.Duration(interval) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed(){

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		state := rf.State
		lastEelection := rf.electionTimer
		rf.mu.Unlock()

		if time.Since(lastEelection) >= rf.electionTimeout() && state != LEADER {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.State = CANDIDATE
	rf.currentTerm++
	rf.votedFor =  rf.me
	rf.electionTimer = time.Now()
	currentTerm := rf.currentTerm
	LastLogTerm, lastLogIdx := rf.LastLogTermAndIndex()
	rf.mu.Unlock()

	var votes int32 = 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func (peer int)  {
			args := &RequestVoteArgs{
				Term: currentTerm,
				CandidateID: rf.me,
				LastLogTerm: LastLogTerm,
				LastLogIndex: lastLogIdx,
			}

			reply := &RequestVoteReply{}

			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.State != CANDIDATE || rf.currentTerm != currentTerm {
				return
			}

			if reply.Term > currentTerm {
				// become FOLLOWER
				rf.State = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1 
				rf.electionTimer = time.Now()
				return
			}

			if reply.VoteGranted && reply.Term == currentTerm { 
				newVotes := atomic.AddInt32(&votes, 1)

				if int(newVotes)*2 > len(rf.peers) && rf.State == CANDIDATE {
					rf.leader()
				}
			}
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1 
		rf.currentTerm = args.Term
	}

	rf.State = FOLLOWER
	rf.electionTimer = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) leader(){
	rf.State = LEADER

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.sendHeartbeats()
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.State != LEADER {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex

		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(peer int) {
				rf.mu.Lock()

				if rf.State != LEADER {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := len(rf.log) - 1
				prevLogTerm := rf.log[prevLogIndex].Term

				args := &AppendEntriesArgs{
					Term: currentTerm,
					LeaderID: rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries: []LogEntry{},
					LeaderCommit: commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				if rf.sendAppendEntries(peer, args, reply) {
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.State = FOLLOWER
						rf.votedFor = -1 
						rf.electionTimer = time.Now()
						rf.currentTerm = reply.Term
					}
					rf.mu.Unlock()
				}
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.State = FOLLOWER
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.matchIndex = make(map[int]int)
	rf.nextIndex = make(map[int]int)
	rf.electionTimer = time.Now()


	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
