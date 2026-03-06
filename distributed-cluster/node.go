package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// NodeConfig holds the configuration for a single node
type NodeConfig struct {
	ID   int    `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// ClusterConfig holds the full cluster configuration
type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Node represents a single node in the distributed cluster
type Node struct {
	ID      int
	Address string
	Config  ClusterConfig

	mu       sync.Mutex
	peers    map[int]net.Conn // connections to other nodes
	peerAddr map[int]string   // address of each peer
	counter  int              // local message counter (local state)
	leader   int              // current known leader

	// Snapshot state
	snapshot *SnapshotState

	// Election state
	electionAnswerChan chan bool

	// HDFS state
	hdfs *HDFSState

	// Logger
	logger  *log.Logger
	logFile *os.File
}

// LoadConfig reads the cluster configuration from a JSON file
func LoadConfig(path string) (ClusterConfig, error) {
	var config ClusterConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return config, fmt.Errorf("failed to read config: %v", err)
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to parse config: %v", err)
	}
	return config, nil
}

// NewNode creates and initializes a new node
func NewNode(id int, config ClusterConfig) *Node {
	var address string
	peerAddr := make(map[int]string)

	for _, n := range config.Nodes {
		addr := fmt.Sprintf("%s:%d", n.IP, n.Port)
		if n.ID == id {
			address = addr
		} else {
			peerAddr[n.ID] = addr
		}
	}

	// Set up log file
	os.MkdirAll("logs", 0755)
	logPath := fmt.Sprintf("logs/node%d.log", id)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Warning: could not open log file: %v\n", err)
		logFile = nil
	}

	var logger *log.Logger
	if logFile != nil {
		logger = log.New(logFile, fmt.Sprintf("[Node %d] ", id), log.LstdFlags)
	} else {
		logger = log.New(os.Stderr, fmt.Sprintf("[Node %d] ", id), log.LstdFlags)
	}

	node := &Node{
		ID:       id,
		Address:  address,
		Config:   config,
		peers:    make(map[int]net.Conn),
		peerAddr: peerAddr,
		counter:  0,
		leader:   -1,
		logger:   logger,
		logFile:  logFile,
	}

	node.snapshot = NewSnapshotState(node)
	node.hdfs = NewHDFSState(node)

	return node
}

// Start begins the node's TCP server and connects to peers
func (n *Node) Start() {
	// Start TCP listener
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		fmt.Printf("Failed to start listener on %s: %v\n", n.Address, err)
		os.Exit(1)
	}

	fmt.Printf("Node %d started on %s\n", n.ID, n.Address)
	n.logger.Printf("Node started on %s", n.Address)

	// Accept incoming connections in background
	go n.acceptConnections(listener)

	// Connect to peers with lower IDs (they should already be listening)
	// Peers with higher IDs will connect to us
	time.Sleep(1 * time.Second)
	n.connectToPeers()

	// Wait a moment for all connections to establish
	time.Sleep(2 * time.Second)
	fmt.Println("Connected to cluster")

	// Start HDFS
	n.hdfs.Start()

	fmt.Println()
	n.printHelp()
}

// acceptConnections handles incoming TCP connections
func (n *Node) acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			n.logger.Printf("Accept error: %v", err)
			continue
		}
		go n.handleConnection(conn)
	}
}

// connectToPeers establishes connections to all peer nodes
func (n *Node) connectToPeers() {
	for peerID, addr := range n.peerAddr {
		go n.connectToPeer(peerID, addr)
	}
}

// connectToPeer connects to a single peer with retry logic
func (n *Node) connectToPeer(peerID int, addr string) {
	for attempt := 0; attempt < 10; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			n.logger.Printf("Connection attempt %d to Node %d (%s) failed: %v", attempt+1, peerID, addr, err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Send our ID so the peer knows who we are
		idMsg := &Message{
			Type:     MSG_DATA,
			SenderID: n.ID,
			Content:  "HELLO",
		}
		data, _ := idMsg.Encode()
		conn.Write(data)

		n.mu.Lock()
		n.peers[peerID] = conn
		n.mu.Unlock()

		n.logger.Printf("Connected to Node %d at %s", peerID, addr)
		go n.readFromPeer(peerID, conn)
		return
	}
	n.logger.Printf("Failed to connect to Node %d after retries", peerID)
}

// handleConnection handles a new incoming connection
func (n *Node) handleConnection(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	// Increase buffer size for large messages
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	// First message should identify the sender
	if scanner.Scan() {
		msg, err := DecodeMessage(scanner.Bytes())
		if err != nil {
			n.logger.Printf("Failed to read hello from incoming connection: %v", err)
			conn.Close()
			return
		}

		peerID := msg.SenderID
		n.mu.Lock()
		n.peers[peerID] = conn
		n.mu.Unlock()

		n.logger.Printf("Accepted connection from Node %d", peerID)

		// Continue reading messages
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			msg, err := DecodeMessage(line)
			if err != nil {
				n.logger.Printf("Failed to decode message: %v", err)
				continue
			}
			n.handleMessage(msg)
		}
	}

	conn.Close()
}

// readFromPeer reads messages from a connected peer
func (n *Node) readFromPeer(peerID int, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		msg, err := DecodeMessage(line)
		if err != nil {
			n.logger.Printf("Failed to decode message from Node %d: %v", peerID, err)
			continue
		}
		n.handleMessage(msg)
	}

	n.logger.Printf("Connection to Node %d closed", peerID)
}

// handleMessage routes incoming messages to the appropriate handler
func (n *Node) handleMessage(msg *Message) {
	n.logger.Printf("Received %s from Node %d", msg.Type, msg.SenderID)

	switch msg.Type {
	case MSG_DATA:
		n.handleDataMessage(msg)
	case MSG_MARKER:
		n.handleMarker(msg)
	case MSG_STATE_REQ:
		n.handleStateRequest(msg)
	case MSG_STATE_RESP:
		n.handleStateResponse(msg)
	case MSG_ELECTION:
		n.handleElection(msg)
	case MSG_ANSWER:
		n.handleAnswer(msg)
	case MSG_COORDINATOR:
		n.handleCoordinator(msg)

	// HDFS messages
	case MSG_HDFS_HEARTBEAT:
		n.hdfs.handleHeartbeat(msg)
	case MSG_HDFS_HEARTBEAT_ACK:
		// no-op on client side
	case MSG_HDFS_PUT_REQ:
		n.hdfs.handlePutRequest(msg)
	case MSG_HDFS_PUT_RESP:
		n.hdfs.handlePutResponse(msg)
	case MSG_HDFS_BLOCK_STORE:
		n.hdfs.handleBlockStore(msg)
	case MSG_HDFS_BLOCK_ACK:
		n.hdfs.handleBlockAck(msg)
	case MSG_HDFS_GET_REQ:
		n.hdfs.handleGetRequest(msg)
	case MSG_HDFS_GET_RESP:
		n.hdfs.handleGetResponse(msg)
	case MSG_HDFS_BLOCK_REQ:
		n.hdfs.handleBlockRequest(msg)
	case MSG_HDFS_BLOCK_DATA:
		n.hdfs.handleBlockData(msg)
	case MSG_HDFS_DELETE:
		n.hdfs.handleDeleteRequest(msg)
	case MSG_HDFS_DELETE_RESP:
		n.hdfs.handleDeleteResponse(msg)
	case MSG_HDFS_DELETE_BLOCK:
		n.hdfs.handleDeleteBlock(msg)
	case MSG_HDFS_LS_REQ:
		n.hdfs.handleLsRequest(msg)
	case MSG_HDFS_LS_RESP:
		n.hdfs.handleLsResponse(msg)
	case MSG_HDFS_STATUS_REQ:
		n.hdfs.handleStatusRequest(msg)
	case MSG_HDFS_STATUS_RESP:
		n.hdfs.handleStatusResponse(msg)
	case MSG_HDFS_PUT_DONE:
		n.logger.Printf("PUT done from Node %d", msg.SenderID)
	case MSG_HDFS_REPLICATE:
		n.hdfs.handleReplicate(msg)

	default:
		n.logger.Printf("Unknown message type: %s", msg.Type)
	}
}

// handleDataMessage processes a regular data message
func (n *Node) handleDataMessage(msg *Message) {
	if msg.Content == "HELLO" {
		return // ignore hello messages used for handshake
	}

	n.mu.Lock()
	n.counter++

	// If snapshot is in progress, record message on channel
	if n.snapshot != nil && n.snapshot.recording {
		n.snapshot.recordMessage(msg.SenderID, msg)
	}
	n.mu.Unlock()

	fmt.Printf("Received message from Node %d: %s\n", msg.SenderID, msg.Content)
	n.logger.Printf("DATA from Node %d: %s (counter now: %d)", msg.SenderID, msg.Content, n.counter)
}

// SendMessage sends a data message to a specific peer
func (n *Node) SendMessage(peerID int, content string) {
	n.mu.Lock()
	conn, ok := n.peers[peerID]
	n.counter++
	n.mu.Unlock()

	if !ok {
		fmt.Printf("Node %d is not connected\n", peerID)
		return
	}

	msg := &Message{
		Type:     MSG_DATA,
		SenderID: n.ID,
		Content:  content,
	}

	data, err := msg.Encode()
	if err != nil {
		fmt.Printf("Failed to encode message: %v\n", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		fmt.Printf("Failed to send message to Node %d: %v\n", peerID, err)
		return
	}

	fmt.Printf("Sent message to Node %d: %s\n", peerID, content)
	n.logger.Printf("Sent DATA to Node %d: %s", peerID, content)
}

// sendToNode sends a raw message to a specific peer
func (n *Node) sendToNode(peerID int, msg *Message) error {
	n.mu.Lock()
	conn, ok := n.peers[peerID]
	n.mu.Unlock()

	if !ok {
		return fmt.Errorf("node %d not connected", peerID)
	}

	data, err := msg.Encode()
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}

// broadcast sends a message to all connected peers
func (n *Node) broadcast(msg *Message) {
	n.mu.Lock()
	peerIDs := make([]int, 0, len(n.peers))
	for id := range n.peers {
		peerIDs = append(peerIDs, id)
	}
	n.mu.Unlock()

	for _, id := range peerIDs {
		err := n.sendToNode(id, msg)
		if err != nil {
			n.logger.Printf("Failed to send to Node %d: %v", id, err)
		}
	}
}

// printHelp displays available commands
func (n *Node) printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  send <nodeID> <message>  - Send a message to a node")
	fmt.Println("  snapshot                 - Initiate global snapshot")
	fmt.Println("  election                 - Start leader election")
	fmt.Println("  leader                   - Show current leader")
	fmt.Println("  put <filepath>           - Upload file to HDFS")
	fmt.Println("  get <hdfsname> <output>  - Download file from HDFS")
	fmt.Println("  ls                       - List HDFS files")
	fmt.Println("  rm <hdfsname>            - Delete file from HDFS")
	fmt.Println("  hdfs-status              - Show HDFS cluster status")
	fmt.Println("  exit                     - Exit the program")
	fmt.Println()
}

// Shutdown cleanly shuts down the node
func (n *Node) Shutdown() {
	n.mu.Lock()
	for id, conn := range n.peers {
		conn.Close()
		n.logger.Printf("Closed connection to Node %d", id)
	}
	n.mu.Unlock()

	if n.logFile != nil {
		n.logFile.Close()
	}
	fmt.Printf("Node %d shut down\n", n.ID)
}
