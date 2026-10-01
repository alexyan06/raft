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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
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

// [2A] one log entry. Command is opaque to Raft — the service layer
// decides what it means. Term is stamped at creation and never changes,
// which is what makes the Log Matching Property and the 5.4.2 commit
// rule possible.
type LogEntry struct {
	Command interface{}
	Term    int
}

// [2A] Figure 2's AppendEntries box. In 2A only Term/LeaderId were used
// (empty Entries = heartbeat). [2B] PrevLogIndex/PrevLogTerm drive the
// consistency check, Entries carries real commands, LeaderCommit tells
// followers how far is safe to apply.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// [2A] not in Figure 2's State box — the figure implies it by having
// separate rule sections per role, but never names a field.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// --- [2A] persistent state on all servers (Figure 2) ---
	currentTerm int        // latest term seen
	votedFor    int        // peer index voted for this term, -1 for none
	log         []LogEntry // index 0 is a dummy so slice index == log index

	// --- [2A] not in Figure 2, needed to implement the rules ---
	role             Role      // follower / candidate / leader
	electionDeadline time.Time // when to give up waiting and run

	// --- [2B] volatile state on all servers (Figure 2) ---
	commitIndex int // highest index known committed
	lastApplied int // highest index pushed to applyCh

	// --- [2B] volatile state on leaders, reinitialized after election ---
	nextIndex  []int // per follower: next index to send
	matchIndex []int // per follower: highest index confirmed replicated

	// --- [2B] the lab's interface back up to the service layer ---
	applyCh chan ApplyMsg
}

// [2A] return currentTerm and whether this server believes it is the
// leader. "Believes" is literal — a partitioned leader still says true.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// [2A] the leader's periodic job: send AppendEntries to every peer so
// their election timers keep getting reset.
// [2B] now also carries real entries — args differ per peer because each
// follower sits at a different point in the log.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			// [2B] everything this follower is missing, from nextIndex on.
			// Empty slice means it's caught up — a plain heartbeat.
			prevLogIndex := rf.nextIndex[i] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			// copy, don't slice: a later append to rf.log could reallocate
			// the backing array while the goroutine below still reads it
			copy(entries, rf.log[rf.nextIndex[i]:])

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			go func(server int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if !rf.sendAppendEntries(server, &args, &reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// [2A] we released the lock to make the call, so everything
				// we assumed when sending may now be false. Three checks:
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					return
				}
				if rf.currentTerm != args.Term || rf.role != Leader {
					return
				}

				// [2B] success means the follower's log now matches ours
				// through PrevLogIndex + len(Entries). Compute from args,
				// not current state — the log may have grown since we sent.
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex()
				} else if rf.nextIndex[server] > 1 {
					// consistency check failed — back up one and retry
					rf.nextIndex[server]--
				}
			}(i, args)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond) // tester caps heartbeats at 10/sec
	}
}

// [2A] Figure 2, Candidates: on conversion to candidate, start election.
// Caller must hold rf.mu.
func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.resetElectionDeadline()

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// [2B] real values now — these feed the 5.4.1 election restriction.
		// Dummy at index 0 means the last real index is len-1.
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	votes := 1 // voted for ourselves

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				// voter is in a newer term — an election happened without us
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}

			if rf.currentTerm != args.Term {
				// reply answers a question from a previous term
				return
			}

			if rf.role != Candidate {
				// already won or already stepped down
				return
			}

			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.role = Leader
					// [2B] Figure 2: reinitialize leader state after election.
					// nextIndex is optimistic — assume followers are caught up,
					// and let the consistency check correct us if not.
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					go rf.sendHeartbeats()
				}
			}
		}(i)
	}
}

// [2A] runs on every server. Polls the election deadline; if it passes
// without a heartbeat, run for office. Leaders skip the check — they're
// the ones sending heartbeats.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
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

// [2A] Figure 2's RequestVote box. LastLogIndex/LastLogTerm exist for the
// 5.4.1 election restriction — sent in 2A but not yet checked by the handler.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// [2A] you're the voter. Decide whether to grant your one vote this term.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// stale candidate — reject, and don't reset our deadline
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// higher term deposes us BEFORE we consider the vote, which makes us
	// an eligible voter in the new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	// votedFor holds WHO, not whether — a duplicated request from the same
	// candidate must get the same answer twice (Figure 2: "votedFor is null
	// or candidateId")
	//
	// [2B]: add the 5.4.1 check — refuse if our log is more up to date
	// than the candidate's (later last term wins; equal term, longer wins)

	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	// if our log is less up to date compared to candidate, then vote for the candidate cause they're more sigma than you
	candidateBetter := args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateBetter {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionDeadline()
		return
	}

	reply.VoteGranted = false
}

// [2A] you're receiving a heartbeat. Older term -> reject and keep our
// timer running. Otherwise accept, step down, and push the deadline out.
//
// TODO [2B]: add the consistency check (reject unless our log has an entry
// at PrevLogIndex with PrevLogTerm), delete any conflicting suffix, append
// the new entries, and advance commitIndex from LeaderCommit.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rule 1: stale leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// outside the term check on purpose: a candidate hearing from a leader
	// in its OWN term has lost and must step down
	rf.role = Follower
	rf.resetElectionDeadline()
	reply.Term = rf.currentTerm
	

	// rule 2: consistency check
	// do we have an entry at prevLogIndex with prevlogterm?
	// if not, refuse: the leader will back up nextIndex
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// rule 3 and 4, if an existing entry conflicts with a new one (same Index but different terms)
	// delete the existing entry and all that follow it
	// then append any jnew entries that's not already in the log

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// rule 5: adopt the leader's commit point, bounded by what we actually have
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, last)
	}

	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
// [2B] fire-and-forget: append locally and return the index the caller
// should watch on applyCh. Replication happens in the background.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	// stamping with currentTerm is what makes the 5.4.2 commit rule possible
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index := len(rf.log) - 1

	return index, rf.currentTerm, true
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

// [2A] fresh random draw every call — reusing one value would make two
// servers split the vote identically forever. Caller holds rf.mu.
func (rf *Raft) resetElectionDeadline() {
	rf.electionDeadline = time.Now().Add(time.Duration(300+rand.Intn(300)) * time.Millisecond)
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

	// [2A]
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}} // dummy — first real command lands at index 1
	rf.role = Follower
	rf.resetElectionDeadline()

	// [2B]
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}

// [2B]: updateCommitIndex() — called from sendHeartbeats
// Scan for the highest N where a majority of matchIndex >= N
func(rf *Raft) updateCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- { // want highest qualifying index
		if rf.log[n].Term != rf.currentTerm { // if this entry was created in earlier term, counting replicas proves nothing, 5.4.2
			continue
		}
		count := 1 // ourselves

		// count how many servers have index n
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		// majority = committed
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			return
		}
	}
}

// [2B] Figure 2, Rules for servers / all servers:
// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine

// runs on every server, not just leader, every replica has to execute the same commands in the same order
// apply to state machien = push an ApplyMsg to applyCh in index order, which is where
// the entry leaves raft and reaches the service above it

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		// commitIndex can jump several indices at once
		// when a batch crosses a majority, all must go out in log order
		
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			// send blocks until service reads it, holding rf.mu across that would free every RPC handler
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

// AND rf.log[N].Term == rf.currentTerm (the 5.4.2 rule).