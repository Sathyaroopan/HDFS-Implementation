package main

import (
	"fmt"
	"sync"
	"time"
)

// SnapshotState manages the Chandy-Lamport snapshot algorithm state
type SnapshotState struct {
	node *Node
	mu   sync.Mutex

	recording     bool             // whether we are currently recording
	snapshotID    int              // ID of the current snapshot
	localState    int              // recorded local state (counter value)
	markerCount   int              // how many markers we have received
	expectedCount int              // total markers expected (number of peers)
	channelState  map[int][]string // channel states: peerID -> list of in-transit messages
	markerRecv    map[int]bool     // tracks which peers we received markers from

	// For the initiator: collected states from all nodes
	isInitiator       bool
	collectedStates   map[int]int              // nodeID -> counter value
	collectedChannels map[int]map[int][]string // nodeID -> channel states
	stateResponses    int
}

// NewSnapshotState creates a new snapshot state manager
func NewSnapshotState(node *Node) *SnapshotState {
	return &SnapshotState{
		node:         node,
		channelState: make(map[int][]string),
		markerRecv:   make(map[int]bool),
	}
}

// InitiateSnapshot starts the Chandy-Lamport snapshot algorithm
func (s *SnapshotState) InitiateSnapshot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshotID++
	snapID := s.snapshotID

	// Record local state
	s.node.mu.Lock()
	s.localState = s.node.counter
	s.node.mu.Unlock()

	s.recording = true
	s.isInitiator = true
	s.markerCount = 0
	s.expectedCount = len(s.node.peerAddr)
	s.channelState = make(map[int][]string)
	s.markerRecv = make(map[int]bool)
	s.collectedStates = make(map[int]int)
	s.collectedChannels = make(map[int]map[int][]string)
	s.stateResponses = 0

	// Record own state
	s.collectedStates[s.node.ID] = s.localState
	s.collectedChannels[s.node.ID] = make(map[int][]string)

	// Initialize channel recording for all incoming channels
	for peerID := range s.node.peerAddr {
		s.channelState[peerID] = []string{}
	}

	fmt.Printf("\nNode %d initiated snapshot (ID: %d)\n", s.node.ID, snapID)
	fmt.Printf("Node %d recorded local state: counter=%d\n", s.node.ID, s.localState)
	s.node.logger.Printf("Snapshot %d initiated, local state: counter=%d", snapID, s.localState)

	// Send marker to all peers
	s.node.mu.Lock()
	peerIDs := make([]int, 0)
	for id := range s.node.peers {
		peerIDs = append(peerIDs, id)
	}
	s.node.mu.Unlock()

	for _, peerID := range peerIDs {
		marker := &Message{
			Type:       MSG_MARKER,
			SenderID:   s.node.ID,
			SnapshotID: snapID,
		}
		err := s.node.sendToNode(peerID, marker)
		if err != nil {
			s.node.logger.Printf("Failed to send marker to Node %d: %v", peerID, err)
		} else {
			fmt.Printf("Node %d sent marker to Node %d\n", s.node.ID, peerID)
			s.node.logger.Printf("Sent MARKER to Node %d for snapshot %d", peerID, snapID)
		}
	}
}

// handleMarker processes an incoming marker message
func (n *Node) handleMarker(msg *Message) {
	s := n.snapshot
	s.mu.Lock()
	defer s.mu.Unlock()

	senderID := msg.SenderID
	snapID := msg.SnapshotID

	fmt.Printf("\nNode %d received marker from Node %d (snapshot %d)\n", n.ID, senderID, snapID)
	n.logger.Printf("Received MARKER from Node %d for snapshot %d", senderID, snapID)

	if !s.recording {
		// First marker received - start recording
		s.snapshotID = snapID
		s.recording = true
		s.isInitiator = false
		s.markerCount = 0
		s.expectedCount = len(n.peerAddr)
		s.channelState = make(map[int][]string)
		s.markerRecv = make(map[int]bool)

		// Record local state
		n.mu.Lock()
		s.localState = n.counter
		n.mu.Unlock()

		fmt.Printf("Node %d recorded local state: counter=%d\n", n.ID, s.localState)
		n.logger.Printf("Recorded local state: counter=%d", s.localState)

		// Initialize channel recording for all peers
		for peerID := range n.peerAddr {
			s.channelState[peerID] = []string{}
		}

		// Mark this sender's channel as empty (marker arrived, no in-transit messages)
		s.markerRecv[senderID] = true
		s.markerCount++

		// Forward markers to all peers
		n.mu.Lock()
		peerIDs := make([]int, 0)
		for id := range n.peers {
			peerIDs = append(peerIDs, id)
		}
		n.mu.Unlock()

		for _, peerID := range peerIDs {
			marker := &Message{
				Type:       MSG_MARKER,
				SenderID:   n.ID,
				SnapshotID: snapID,
			}
			err := n.sendToNode(peerID, marker)
			if err != nil {
				n.logger.Printf("Failed to forward marker to Node %d: %v", peerID, err)
			} else {
				fmt.Printf("Node %d forwarding marker to Node %d\n", n.ID, peerID)
				n.logger.Printf("Forwarded MARKER to Node %d", peerID)
			}
		}
	} else {
		// Subsequent marker - stop recording on this channel
		s.markerRecv[senderID] = true
		s.markerCount++
		n.logger.Printf("Marker from Node %d, channel recording stopped (count: %d/%d)",
			senderID, s.markerCount, s.expectedCount)
	}

	// Check if snapshot is complete for this node
	if s.markerCount >= s.expectedCount {
		s.recording = false
		fmt.Printf("Node %d snapshot complete\n", n.ID)
		n.logger.Printf("Snapshot %d complete for this node", s.snapshotID)

		// Print local snapshot result
		s.printLocalSnapshot()

		// Send state response to the initiator if we are not the initiator
		if !s.isInitiator {
			n.sendSnapshotState(msg.SenderID, snapID)
		} else {
			// Initiator: add own channel states and check if all collected
			s.collectedChannels[n.ID] = s.channelState
			s.checkAndPrintGlobalSnapshot()
		}
	}
}

// recordMessage records a message on a channel during snapshot
func (s *SnapshotState) recordMessage(senderID int, msg *Message) {
	// Only record if we haven't received a marker from this sender yet
	if !s.markerRecv[senderID] {
		s.channelState[senderID] = append(s.channelState[senderID],
			fmt.Sprintf("[from Node %d: %s]", senderID, msg.Content))
	}
}

// sendSnapshotState sends this node's snapshot state to the initiator
func (n *Node) sendSnapshotState(initiatorHint int, snapID int) {
	// Build channel state as content string
	channelInfo := ""
	n.snapshot.mu.Lock()
	for peerID, msgs := range n.snapshot.channelState {
		if len(msgs) > 0 {
			for _, m := range msgs {
				channelInfo += fmt.Sprintf("%d->%d:%s;", peerID, n.ID, m)
			}
		}
	}
	localState := n.snapshot.localState
	n.snapshot.mu.Unlock()

	// Broadcast state to all peers (the initiator will pick it up)
	msg := &Message{
		Type:       MSG_STATE_RESP,
		SenderID:   n.ID,
		SnapshotID: snapID,
		Counter:    localState,
		Content:    channelInfo,
	}
	n.broadcast(msg)
	n.logger.Printf("Sent snapshot state response (counter=%d)", localState)
}

// handleStateRequest handles a request for snapshot state
func (n *Node) handleStateRequest(msg *Message) {
	n.logger.Printf("Received STATE_REQUEST from Node %d", msg.SenderID)
}

// handleStateResponse handles a snapshot state response from another node
func (n *Node) handleStateResponse(msg *Message) {
	s := n.snapshot
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isInitiator {
		return // only the initiator collects states
	}

	n.logger.Printf("Received STATE_RESPONSE from Node %d: counter=%d", msg.SenderID, msg.Counter)

	s.collectedStates[msg.SenderID] = msg.Counter
	if s.collectedChannels[msg.SenderID] == nil {
		s.collectedChannels[msg.SenderID] = make(map[int][]string)
	}
	// Parse channel info from content if present
	if msg.Content != "" {
		s.collectedChannels[msg.SenderID] = parseChannelInfo(msg.Content)
	}
	s.stateResponses++

	s.checkAndPrintGlobalSnapshot()
}

// parseChannelInfo parses channel state info from message content
func parseChannelInfo(content string) map[int][]string {
	// Simple parsing: format is "fromID->toID:message;"
	result := make(map[int][]string)
	if content == "" {
		return result
	}
	// For now, store raw channel info
	entries := splitEntries(content)
	for _, entry := range entries {
		if entry != "" {
			// Parse "fromID->toID:message"
			var fromID, toID int
			var message string
			n, _ := fmt.Sscanf(entry, "%d->%d:%s", &fromID, &toID, &message)
			if n >= 2 {
				result[fromID] = append(result[fromID], entry)
			}
		}
	}
	return result
}

// splitEntries splits semicolon-separated entries
func splitEntries(s string) []string {
	var result []string
	current := ""
	for _, c := range s {
		if c == ';' {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// checkAndPrintGlobalSnapshot checks if all states are collected and prints the global snapshot
func (s *SnapshotState) checkAndPrintGlobalSnapshot() {
	totalNodes := len(s.node.Config.Nodes)
	if len(s.collectedStates) >= totalNodes {
		s.printGlobalSnapshot()
	}
}

// printLocalSnapshot prints this node's snapshot state
func (s *SnapshotState) printLocalSnapshot() {
	fmt.Printf("\n--- Node %d Snapshot ---\n", s.node.ID)
	fmt.Printf("Local state: counter=%d\n", s.localState)

	hasInTransit := false
	for peerID, msgs := range s.channelState {
		if len(msgs) > 0 {
			hasInTransit = true
			fmt.Printf("Channel %d -> %d: %v\n", peerID, s.node.ID, msgs)
		}
	}
	if !hasInTransit {
		fmt.Printf("All incoming channels: empty\n")
	}
	fmt.Println()
}

// printGlobalSnapshot prints the complete global snapshot
func (s *SnapshotState) printGlobalSnapshot() {
	fmt.Println()
	fmt.Println("----- GLOBAL SNAPSHOT -----")
	fmt.Println()

	// Print node states
	for _, nc := range s.node.Config.Nodes {
		if counter, ok := s.collectedStates[nc.ID]; ok {
			fmt.Printf("Node %d state: counter=%d\n", nc.ID, counter)
		} else {
			fmt.Printf("Node %d state: (not received)\n", nc.ID)
		}
	}

	fmt.Println()
	fmt.Println("Channel states:")

	// Print channel states
	hasChannelData := false
	for nodeID, channels := range s.collectedChannels {
		for peerID, msgs := range channels {
			if len(msgs) > 0 {
				hasChannelData = true
				fmt.Printf("  %d -> %d : message in transit\n", peerID, nodeID)
			}
		}
	}

	// Print empty channels
	for _, nc1 := range s.node.Config.Nodes {
		for _, nc2 := range s.node.Config.Nodes {
			if nc1.ID != nc2.ID {
				// Check if there are messages
				found := false
				if channels, ok := s.collectedChannels[nc2.ID]; ok {
					if msgs, ok := channels[nc1.ID]; ok && len(msgs) > 0 {
						found = true
					}
				}
				if !found {
					fmt.Printf("  %d -> %d : empty\n", nc1.ID, nc2.ID)
				}
			}
		}
	}
	_ = hasChannelData

	fmt.Println()
	fmt.Println("---------------------------")
	fmt.Println()

	s.node.logger.Printf("Global snapshot complete")

	// Set up a collection timeout for late responses
	go func() {
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		s.isInitiator = false
		s.mu.Unlock()
	}()
}
