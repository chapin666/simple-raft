package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"time"
)

type node struct {
	connect bool
	address string
}

// 新建节点
func newNode(address string) *node {
	node := &node{}
	node.address = address
	return node
}

// State def
type State int

// status of node
const (
	Follower State = iota + 1
	Candidate
	Leader
)

// Raft Node
type Raft struct {
	me int

	nodes map[int]*node

	state       State
	currentTerm int
	votedFor    int
	voteCount   int

	heartbeatC chan bool
	toLeaderC  chan bool
}

// RequestVote rpc method
func (rf *Raft) RequestVote(args VoteArgs, reply *VoteReply) error {

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	return nil
}

// Heartbeat rpc method
func (rf *Raft) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {

	// 如果 leader 节点小于当前节点 term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	reply.Term = rf.currentTerm
	rf.heartbeatC <- true

	return nil
}

func (rf *Raft) rpc(port string) {
	rpc.Register(rf)
	rpc.HandleHTTP()
	go func() {
		err := http.ListenAndServe(port, nil)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
	}()
}

func (rf *Raft) start() {
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	go func() {

		rand.Seed(time.Now().UnixNano())

		for {
			switch rf.state {
			case Follower:
				select {
				case <-rf.heartbeatC:
					log.Printf("follower-%d recived heartbeat\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					log.Printf("follower-%d timeout\n", rf.me)
					rf.state = Candidate
				}
			case Candidate:
				fmt.Printf("Node: %d, I'm candidate\n", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				go rf.broadcastRequestVote()

				select {
				case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
					rf.state = Follower
				case <-rf.toLeaderC:
					fmt.Printf("Node: %d, I'm leader\n", rf.me)
					rf.state = Leader
				}
			case Leader:
				rf.broadcastHeartbeat()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

type VoteArgs struct {
	Term        int
	CandidateID int
}

type VoteReply struct {
	//当前任期号， 以便候选人去更新自己的任期号
	Term int
	//候选人赢得此张选票时为真
	VoteGranted bool
}

func (rf *Raft) broadcastRequestVote() {
	var args = VoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := range rf.nodes {
		go func(i int) {
			var reply VoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

func (rf *Raft) sendRequestVote(serverID int, args VoteArgs, reply *VoteReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing: ", err)
	}

	defer client.Close()
	client.Call("Raft.RequestVote", args, reply)

	// 当前candidate节点无效
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.nodes)/2+1 {
			rf.toLeaderC <- true
		}
	}
}

type HeartbeatArgs struct {
	Term     int
	LeaderID int
}

type HeartbeatReply struct {
	Term int
}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.nodes {
		args := HeartbeatArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		go func(i int, args HeartbeatArgs) {
			var reply HeartbeatReply
			rf.sendHeartbeat(i, args, &reply)
		}(i, args)
	}
}

func (rf *Raft) sendHeartbeat(serverID int, args HeartbeatArgs, reply *HeartbeatReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()
	client.Call("Raft.Heartbeat", args, reply)

	// 如果 leader 节点落后于 follower 节点
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
	}
}
