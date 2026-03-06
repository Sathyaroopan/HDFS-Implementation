package main

import (
	"fmt"
	"sync"
	"time"
)

// ElectionState manages the Bully leader election algorithm state
type ElectionState struct {
	node               *Node
	mu                 sync.Mutex
	electionInProgress bool
	answerReceived     bool
}

// NewElectionState creates a new election state manager
func NewElectionState(node *Node) *ElectionState {
	return &ElectionState{
		node: node,
	}
}

// StartElection initiates the Bully leader election algorithm
func (n *Node) StartElection() {
	n.logger.Printf("Starting leader election")
	fmt.Printf("\nNode %d starting election\n", n.ID)

	// Find all nodes with higher IDs
	higherNodes := []int{}
	for _, nc := range n.Config.Nodes {
		if nc.ID > n.ID {
			higherNodes = append(higherNodes, nc.ID)
		}
	}

	// If no higher nodes, we are the leader
	if len(higherNodes) == 0 {
		n.declareLeader()
		return
	}

	// Send ELECTION message to all nodes with higher IDs
	answerReceived := false
	var answerMu sync.Mutex
	answerChan := make(chan bool, len(higherNodes))

	for _, peerID := range higherNodes {
		fmt.Printf("Node %d sends election message to Node %d\n", n.ID, peerID)
		n.logger.Printf("Sending ELECTION to Node %d", peerID)

		msg := &Message{
			Type:     MSG_ELECTION,
			SenderID: n.ID,
		}
		err := n.sendToNode(peerID, msg)
		if err != nil {
			n.logger.Printf("Failed to send ELECTION to Node %d: %v", peerID, err)
		}
	}

	// Wait for ANSWER messages with a timeout
	go func() {
		// Set a flag on the node to track answers
		n.mu.Lock()
		n.electionAnswerChan = answerChan
		n.mu.Unlock()

		// Wait for timeout
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()

		select {
		case <-answerChan:
			answerMu.Lock()
			answerReceived = true
			answerMu.Unlock()
			n.logger.Printf("Received ANSWER, waiting for COORDINATOR")
			fmt.Printf("Node %d received answer, higher node will take over\n", n.ID)
		case <-timer.C:
			answerMu.Lock()
			got := answerReceived
			answerMu.Unlock()
			if !got {
				// No answer received - we become the leader
				n.logger.Printf("No ANSWER received, declaring self as leader")
				n.declareLeader()
			}
		}

		n.mu.Lock()
		n.electionAnswerChan = nil
		n.mu.Unlock()
	}()
}

// handleElection processes an incoming ELECTION message
func (n *Node) handleElection(msg *Message) {
	senderID := msg.SenderID
	fmt.Printf("\nNode %d received election message from Node %d\n", n.ID, senderID)
	n.logger.Printf("Received ELECTION from Node %d", senderID)

	// If our ID is higher, send ANSWER and start our own election
	if n.ID > senderID {
		answer := &Message{
			Type:     MSG_ANSWER,
			SenderID: n.ID,
		}
		err := n.sendToNode(senderID, answer)
		if err != nil {
			n.logger.Printf("Failed to send ANSWER to Node %d: %v", senderID, err)
		} else {
			fmt.Printf("Node %d responds to Node %d\n", n.ID, senderID)
			n.logger.Printf("Sent ANSWER to Node %d", senderID)
		}

		// Start our own election
		go n.StartElection()
	}
}

// handleAnswer processes an incoming ANSWER message
func (n *Node) handleAnswer(msg *Message) {
	fmt.Printf("Node %d received answer from Node %d\n", n.ID, msg.SenderID)
	n.logger.Printf("Received ANSWER from Node %d", msg.SenderID)

	n.mu.Lock()
	ch := n.electionAnswerChan
	n.mu.Unlock()

	if ch != nil {
		select {
		case ch <- true:
		default:
		}
	}
}

// handleCoordinator processes an incoming COORDINATOR message
func (n *Node) handleCoordinator(msg *Message) {
	n.mu.Lock()
	n.leader = msg.SenderID
	n.mu.Unlock()

	fmt.Printf("\nLeader announced: Node %d\n", msg.SenderID)
	n.logger.Printf("New leader: Node %d", msg.SenderID)
}

// declareLeader declares this node as the leader and broadcasts COORDINATOR
func (n *Node) declareLeader() {
	n.mu.Lock()
	n.leader = n.ID
	n.mu.Unlock()

	fmt.Printf("\nNode %d becomes leader\n", n.ID)
	fmt.Printf("Leader announced: Node %d\n", n.ID)
	n.logger.Printf("Declared self as leader")

	// Broadcast COORDINATOR to all nodes
	msg := &Message{
		Type:     MSG_COORDINATOR,
		SenderID: n.ID,
	}
	n.broadcast(msg)
	n.logger.Printf("Broadcast COORDINATOR to all nodes")
}

// ShowLeader prints the current known leader
func (n *Node) ShowLeader() {
	n.mu.Lock()
	leader := n.leader
	n.mu.Unlock()

	if leader == -1 {
		fmt.Println("No leader elected yet")
	} else {
		fmt.Printf("Current leader: Node %d\n", leader)
	}
}
