package main

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	BlockSize         = 256 // bytes per block (small for demo)
	ReplicationFactor = 3   // copies of each block
	HeartbeatInterval = 5   // seconds between heartbeats
	HeartbeatTimeout  = 18  // seconds before marking node dead
	NameNodeID        = 1   // Node 1 is always the NameNode
)

// Rack assignments: Node 1,2 -> Rack 1; Node 3,4 -> Rack 2
var RackAssignment = map[int]int{
	1: 1,
	2: 1,
	3: 2,
	4: 2,
}

// ----- File and Block metadata -----

// FileInfo stores metadata about a file in HDFS
type FileInfo struct {
	FileName   string
	FileSize   int
	BlockIDs   []string
	NumBlocks  int
	ReplFactor int
	CreatedAt  time.Time
}

// BlockInfo stores metadata about a block
type BlockInfo struct {
	BlockID    string
	FileName   string
	BlockIndex int
	Size       int
	NodeIDs    []int // nodes that have this block
}

// DataNodeStatus tracks a DataNode's health
type DataNodeStatus struct {
	NodeID        int
	RackID        int
	Alive         bool
	LastHeartbeat time.Time
	BlockCount    int
}

// ----- HDFS State -----

// HDFSState manages the HDFS layer for a node
type HDFSState struct {
	node *Node
	mu   sync.Mutex

	// NameNode state (only used on Node 1)
	isNameNode bool
	files      map[string]*FileInfo    // fileName -> metadata
	blocks     map[string]*BlockInfo   // blockID -> metadata
	dataNodes  map[int]*DataNodeStatus // nodeID -> status

	// DataNode state (all nodes)
	blockStore map[string][]byte // blockID -> data

	// Response channels for synchronous operations
	putRespChan    chan *Message
	getRespChan    chan *Message
	blockDataChan  chan *Message
	lsRespChan     chan *Message
	statusRespChan chan *Message
	deleteRespChan chan *Message
	blockAckCount  int
	blockAckChan   chan bool
}

// NewHDFSState creates a new HDFS state manager
func NewHDFSState(node *Node) *HDFSState {
	h := &HDFSState{
		node:       node,
		isNameNode: node.ID == NameNodeID,
		files:      make(map[string]*FileInfo),
		blocks:     make(map[string]*BlockInfo),
		dataNodes:  make(map[int]*DataNodeStatus),
		blockStore: make(map[string][]byte),
	}

	// Initialize DataNode status on NameNode
	if h.isNameNode {
		for _, nc := range node.Config.Nodes {
			h.dataNodes[nc.ID] = &DataNodeStatus{
				NodeID:        nc.ID,
				RackID:        RackAssignment[nc.ID],
				Alive:         true,
				LastHeartbeat: time.Now(),
				BlockCount:    0,
			}
		}
	}

	return h
}

// Start begins HDFS background tasks
func (h *HDFSState) Start() {
	if h.isNameNode {
		go h.heartbeatChecker()
		fmt.Println("HDFS NameNode active on this node")
	}
	go h.heartbeatSender()
	fmt.Println("HDFS DataNode active on this node")
	h.node.logger.Printf("HDFS state initialized (NameNode: %v)", h.isNameNode)
}

// =============================================
// HEARTBEAT SYSTEM
// =============================================

// heartbeatSender sends periodic heartbeats to the NameNode
func (h *HDFSState) heartbeatSender() {
	for {
		time.Sleep(time.Duration(HeartbeatInterval) * time.Second)

		if h.node.ID == NameNodeID {
			// NameNode updates its own heartbeat
			h.mu.Lock()
			if ds, ok := h.dataNodes[h.node.ID]; ok {
				ds.LastHeartbeat = time.Now()
				ds.Alive = true
				ds.BlockCount = len(h.blockStore)
			}
			h.mu.Unlock()
			continue
		}

		h.mu.Lock()
		blockCount := len(h.blockStore)
		h.mu.Unlock()

		msg := &Message{
			Type:     MSG_HDFS_HEARTBEAT,
			SenderID: h.node.ID,
			Counter:  blockCount,
		}
		err := h.node.sendToNode(NameNodeID, msg)
		if err != nil {
			h.node.logger.Printf("Failed to send heartbeat to NameNode: %v", err)
		}
	}
}

// heartbeatChecker runs on the NameNode and monitors DataNode health
func (h *HDFSState) heartbeatChecker() {
	for {
		time.Sleep(time.Duration(HeartbeatInterval) * time.Second)

		h.mu.Lock()
		now := time.Now()
		for nodeID, ds := range h.dataNodes {
			if nodeID == NameNodeID {
				continue
			}
			if ds.Alive && now.Sub(ds.LastHeartbeat) > time.Duration(HeartbeatTimeout)*time.Second {
				ds.Alive = false
				fmt.Printf("\nHDFS: DataNode %d marked as DEAD (missed heartbeats)\n", nodeID)
				h.node.logger.Printf("DataNode %d marked as dead", nodeID)
				// Trigger re-replication
				go h.reReplicateBlocks(nodeID)
			}
		}
		h.mu.Unlock()
	}
}

// handleHeartbeat processes a heartbeat from a DataNode (NameNode only)
func (h *HDFSState) handleHeartbeat(msg *Message) {
	if !h.isNameNode {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ds, ok := h.dataNodes[msg.SenderID]
	if !ok {
		ds = &DataNodeStatus{
			NodeID: msg.SenderID,
			RackID: RackAssignment[msg.SenderID],
		}
		h.dataNodes[msg.SenderID] = ds
	}

	wasAlive := ds.Alive
	ds.Alive = true
	ds.LastHeartbeat = time.Now()
	ds.BlockCount = msg.Counter

	if !wasAlive {
		fmt.Printf("\nHDFS: DataNode %d is back ALIVE\n", msg.SenderID)
		h.node.logger.Printf("DataNode %d is back alive", msg.SenderID)
	}

	// Send ACK
	ack := &Message{
		Type:     MSG_HDFS_HEARTBEAT_ACK,
		SenderID: h.node.ID,
	}
	h.node.sendToNode(msg.SenderID, ack)
}

// =============================================
// PUT FILE (upload)
// =============================================

// PutFile reads a local file, splits it into blocks, and stores them in HDFS
func (h *HDFSState) PutFile(localPath string) {
	// Read the local file
	data, err := os.ReadFile(localPath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	// Extract filename from path
	fileName := localPath
	if idx := strings.LastIndexAny(localPath, "/\\"); idx >= 0 {
		fileName = localPath[idx+1:]
	}

	fileSize := len(data)
	numBlocks := (fileSize + BlockSize - 1) / BlockSize

	fmt.Printf("\nHDFS PUT: %s (%d bytes, %d blocks)\n", fileName, fileSize, numBlocks)
	h.node.logger.Printf("PUT file: %s, size: %d, blocks: %d", fileName, fileSize, numBlocks)

	if h.node.ID == NameNodeID {
		// We ARE the NameNode, handle directly
		h.processPutRequest(h.node.ID, fileName, fileSize, data)
	} else {
		// Send put request to NameNode
		h.putRespChan = make(chan *Message, 1)

		msg := &Message{
			Type:      MSG_HDFS_PUT_REQ,
			SenderID:  h.node.ID,
			FileName:  fileName,
			FileSize:  fileSize,
			BlockData: base64.StdEncoding.EncodeToString(data),
		}
		err := h.node.sendToNode(NameNodeID, msg)
		if err != nil {
			fmt.Printf("Error contacting NameNode: %v\n", err)
			return
		}

		// Wait for placement plan from NameNode
		select {
		case resp := <-h.putRespChan:
			if !resp.Success {
				fmt.Printf("NameNode rejected PUT: %s\n", resp.Content)
				return
			}
			h.storeBlocks(data, resp.BlockList)
		case <-time.After(10 * time.Second):
			fmt.Println("Timeout waiting for NameNode response")
		}
	}
}

// processPutRequest handles a PUT request (NameNode only)
func (h *HDFSState) processPutRequest(clientID int, fileName string, fileSize int, data []byte) {
	h.mu.Lock()

	// Check if file already exists
	if _, exists := h.files[fileName]; exists {
		h.mu.Unlock()
		fmt.Printf("HDFS: File '%s' already exists\n", fileName)
		if clientID != h.node.ID {
			resp := &Message{
				Type:     MSG_HDFS_PUT_RESP,
				SenderID: h.node.ID,
				FileName: fileName,
				Success:  false,
				Content:  "file already exists",
			}
			h.node.sendToNode(clientID, resp)
		}
		return
	}

	numBlocks := (fileSize + BlockSize - 1) / BlockSize

	// Create file metadata
	fileInfo := &FileInfo{
		FileName:   fileName,
		FileSize:   fileSize,
		BlockIDs:   make([]string, numBlocks),
		NumBlocks:  numBlocks,
		ReplFactor: ReplicationFactor,
		CreatedAt:  time.Now(),
	}

	// Create block placement plan
	placements := make([]BlockPlacement, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockID := fmt.Sprintf("%s_block_%d", fileName, i)
		fileInfo.BlockIDs[i] = blockID

		nodeIDs := h.selectDataNodes(i)
		rackIDs := make([]int, len(nodeIDs))
		for j, nid := range nodeIDs {
			rackIDs[j] = RackAssignment[nid]
		}

		h.blocks[blockID] = &BlockInfo{
			BlockID:    blockID,
			FileName:   fileName,
			BlockIndex: i,
			NodeIDs:    nodeIDs,
		}

		placements[i] = BlockPlacement{
			BlockID:    blockID,
			BlockIndex: i,
			NodeIDs:    nodeIDs,
			RackIDs:    rackIDs,
		}

		fmt.Printf("  Block %d (%s) -> Nodes %v (Racks %v)\n", i, blockID, nodeIDs, rackIDs)
		h.node.logger.Printf("Block %s -> Nodes %v, Racks %v", blockID, nodeIDs, rackIDs)
	}

	h.files[fileName] = fileInfo
	h.mu.Unlock()

	if clientID == h.node.ID {
		// NameNode is the client, store blocks directly
		h.storeBlocks(data, placements)
	} else {
		// Send placement plan back to client
		resp := &Message{
			Type:      MSG_HDFS_PUT_RESP,
			SenderID:  h.node.ID,
			FileName:  fileName,
			Success:   true,
			BlockList: placements,
		}
		h.node.sendToNode(clientID, resp)
	}
}

// selectDataNodes picks nodes for block placement with rack awareness
func (h *HDFSState) selectDataNodes(blockIndex int) []int {
	// Get list of alive nodes
	aliveNodes := []int{}
	for nodeID, ds := range h.dataNodes {
		if ds.Alive {
			aliveNodes = append(aliveNodes, nodeID)
		}
	}
	sort.Ints(aliveNodes)

	if len(aliveNodes) == 0 {
		return nil
	}

	replFactor := ReplicationFactor
	if replFactor > len(aliveNodes) {
		replFactor = len(aliveNodes)
	}

	// Rack-aware placement:
	// 1. First replica: pick a node (rotate based on block index)
	// 2. Second replica: pick a node on a DIFFERENT rack
	// 3. Third replica: pick another node on the second rack
	selected := make([]int, 0, replFactor)
	usedNodes := make(map[int]bool)

	// First replica
	firstNode := aliveNodes[blockIndex%len(aliveNodes)]
	selected = append(selected, firstNode)
	usedNodes[firstNode] = true
	firstRack := RackAssignment[firstNode]

	// Second replica: different rack
	for _, nid := range aliveNodes {
		if !usedNodes[nid] && RackAssignment[nid] != firstRack {
			selected = append(selected, nid)
			usedNodes[nid] = true
			break
		}
	}

	// Third replica and beyond: prefer the second rack, then any available
	for len(selected) < replFactor {
		for _, nid := range aliveNodes {
			if !usedNodes[nid] && RackAssignment[nid] != firstRack {
				selected = append(selected, nid)
				usedNodes[nid] = true
				break
			}
		}
		// If no more nodes on other rack, pick from any rack
		if len(selected) < replFactor {
			for _, nid := range aliveNodes {
				if !usedNodes[nid] {
					selected = append(selected, nid)
					usedNodes[nid] = true
					break
				}
			}
		}
		// Safety: avoid infinite loop
		if len(selected) < replFactor {
			break
		}
	}

	return selected
}

// storeBlocks splits data into blocks and sends them to assigned DataNodes
func (h *HDFSState) storeBlocks(data []byte, placements []BlockPlacement) {
	totalAcks := 0
	expectedAcks := 0

	for _, p := range placements {
		expectedAcks += len(p.NodeIDs)
	}

	h.mu.Lock()
	h.blockAckCount = 0
	h.blockAckChan = make(chan bool, expectedAcks)
	h.mu.Unlock()

	for _, placement := range placements {
		// Extract block data
		start := placement.BlockIndex * BlockSize
		end := start + BlockSize
		if end > len(data) {
			end = len(data)
		}
		blockData := data[start:end]
		encodedData := base64.StdEncoding.EncodeToString(blockData)

		blockSize := len(blockData)

		for _, nodeID := range placement.NodeIDs {
			if nodeID == h.node.ID {
				// Store locally
				h.mu.Lock()
				h.blockStore[placement.BlockID] = make([]byte, len(blockData))
				copy(h.blockStore[placement.BlockID], blockData)
				h.mu.Unlock()

				fmt.Printf("  Stored block %s locally (%d bytes)\n", placement.BlockID, blockSize)
				h.node.logger.Printf("Stored block %s locally, %d bytes", placement.BlockID, blockSize)
				totalAcks++

				// Update NameNode block count if we are NameNode
				if h.isNameNode {
					h.mu.Lock()
					if ds, ok := h.dataNodes[h.node.ID]; ok {
						ds.BlockCount = len(h.blockStore)
					}
					h.mu.Unlock()
				}
			} else {
				// Send to remote DataNode
				msg := &Message{
					Type:       MSG_HDFS_BLOCK_STORE,
					SenderID:   h.node.ID,
					BlockID:    placement.BlockID,
					BlockData:  encodedData,
					BlockIndex: placement.BlockIndex,
					FileName:   "", // not needed for storage
				}
				err := h.node.sendToNode(nodeID, msg)
				if err != nil {
					fmt.Printf("  Failed to send block %s to Node %d: %v\n", placement.BlockID, nodeID, err)
					h.node.logger.Printf("Failed to send block %s to Node %d: %v", placement.BlockID, nodeID, err)
				} else {
					fmt.Printf("  Sent block %s to Node %d (%d bytes)\n", placement.BlockID, nodeID, blockSize)
					h.node.logger.Printf("Sent block %s to Node %d", placement.BlockID, nodeID)
				}
			}
		}
	}

	// Wait for ACKs from remote nodes
	ackTimeout := time.After(10 * time.Second)
	for totalAcks < expectedAcks {
		select {
		case <-h.blockAckChan:
			totalAcks++
		case <-ackTimeout:
			fmt.Printf("  Warning: Only received %d/%d block ACKs\n", totalAcks, expectedAcks)
			goto done
		}
	}
done:

	// Notify NameNode that put is complete
	if h.node.ID != NameNodeID {
		doneMsg := &Message{
			Type:     MSG_HDFS_PUT_DONE,
			SenderID: h.node.ID,
		}
		h.node.sendToNode(NameNodeID, doneMsg)
	}

	fmt.Printf("HDFS PUT complete: %d/%d block replicas stored\n", totalAcks, expectedAcks)
	h.node.logger.Printf("PUT complete: %d/%d ACKs", totalAcks, expectedAcks)
}

// handleBlockStore processes a block store request (DataNode)
func (h *HDFSState) handleBlockStore(msg *Message) {
	blockData, err := base64.StdEncoding.DecodeString(msg.BlockData)
	if err != nil {
		h.node.logger.Printf("Failed to decode block data for %s: %v", msg.BlockID, err)
		return
	}

	h.mu.Lock()
	h.blockStore[msg.BlockID] = blockData
	h.mu.Unlock()

	fmt.Printf("Stored block %s (%d bytes) from Node %d\n", msg.BlockID, len(blockData), msg.SenderID)
	h.node.logger.Printf("Stored block %s, %d bytes from Node %d", msg.BlockID, len(blockData), msg.SenderID)

	// Send ACK back
	ack := &Message{
		Type:     MSG_HDFS_BLOCK_ACK,
		SenderID: h.node.ID,
		BlockID:  msg.BlockID,
		Success:  true,
	}
	h.node.sendToNode(msg.SenderID, ack)
}

// handleBlockAck processes a block storage acknowledgment
func (h *HDFSState) handleBlockAck(msg *Message) {
	h.node.logger.Printf("Received ACK for block %s from Node %d", msg.BlockID, msg.SenderID)

	h.mu.Lock()
	if h.blockAckChan != nil {
		select {
		case h.blockAckChan <- true:
		default:
		}
	}
	h.mu.Unlock()
}

// handlePutRequest handles a PUT request from a client (NameNode only)
func (h *HDFSState) handlePutRequest(msg *Message) {
	if !h.isNameNode {
		return
	}

	fmt.Printf("\nHDFS NameNode: PUT request for '%s' from Node %d (%d bytes)\n",
		msg.FileName, msg.SenderID, msg.FileSize)
	h.node.logger.Printf("PUT request: %s, %d bytes from Node %d", msg.FileName, msg.FileSize, msg.SenderID)

	data, err := base64.StdEncoding.DecodeString(msg.BlockData)
	if err != nil {
		h.node.logger.Printf("Failed to decode file data: %v", err)
		return
	}

	h.processPutRequest(msg.SenderID, msg.FileName, msg.FileSize, data)
}

// handlePutResponse handles a PUT response from NameNode (client)
func (h *HDFSState) handlePutResponse(msg *Message) {
	h.node.logger.Printf("Received PUT response for %s, success: %v", msg.FileName, msg.Success)
	if h.putRespChan != nil {
		select {
		case h.putRespChan <- msg:
		default:
		}
	}
}

// =============================================
// GET FILE (download)
// =============================================

// GetFile downloads a file from HDFS and writes it locally
func (h *HDFSState) GetFile(hdfsName string, localPath string) {
	fmt.Printf("\nHDFS GET: %s -> %s\n", hdfsName, localPath)
	h.node.logger.Printf("GET file: %s -> %s", hdfsName, localPath)

	if h.node.ID == NameNodeID {
		// We are the NameNode, look up directly
		h.mu.Lock()
		fi, exists := h.files[hdfsName]
		if !exists {
			h.mu.Unlock()
			fmt.Printf("File '%s' not found in HDFS\n", hdfsName)
			return
		}

		blockPlacements := make([]BlockPlacement, fi.NumBlocks)
		for i, blockID := range fi.BlockIDs {
			bi := h.blocks[blockID]
			blockPlacements[i] = BlockPlacement{
				BlockID:    blockID,
				BlockIndex: i,
				NodeIDs:    bi.NodeIDs,
			}
		}
		fileSize := fi.FileSize
		h.mu.Unlock()

		h.fetchBlocks(blockPlacements, fileSize, localPath)
	} else {
		// Send GET request to NameNode
		h.getRespChan = make(chan *Message, 1)

		msg := &Message{
			Type:     MSG_HDFS_GET_REQ,
			SenderID: h.node.ID,
			FileName: hdfsName,
		}
		err := h.node.sendToNode(NameNodeID, msg)
		if err != nil {
			fmt.Printf("Error contacting NameNode: %v\n", err)
			return
		}

		select {
		case resp := <-h.getRespChan:
			if !resp.Success {
				fmt.Printf("File not found: %s\n", hdfsName)
				return
			}
			h.fetchBlocks(resp.BlockList, resp.FileSize, localPath)
		case <-time.After(10 * time.Second):
			fmt.Println("Timeout waiting for NameNode response")
		}
	}
}

// handleGetRequest handles a GET request (NameNode only)
func (h *HDFSState) handleGetRequest(msg *Message) {
	if !h.isNameNode {
		return
	}

	h.mu.Lock()
	fi, exists := h.files[msg.FileName]
	if !exists {
		h.mu.Unlock()
		resp := &Message{
			Type:     MSG_HDFS_GET_RESP,
			SenderID: h.node.ID,
			FileName: msg.FileName,
			Success:  false,
		}
		h.node.sendToNode(msg.SenderID, resp)
		return
	}

	placements := make([]BlockPlacement, fi.NumBlocks)
	for i, blockID := range fi.BlockIDs {
		bi := h.blocks[blockID]
		placements[i] = BlockPlacement{
			BlockID:    blockID,
			BlockIndex: i,
			NodeIDs:    bi.NodeIDs,
		}
	}
	fileSize := fi.FileSize
	h.mu.Unlock()

	resp := &Message{
		Type:      MSG_HDFS_GET_RESP,
		SenderID:  h.node.ID,
		FileName:  msg.FileName,
		FileSize:  fileSize,
		Success:   true,
		BlockList: placements,
	}
	h.node.sendToNode(msg.SenderID, resp)
	h.node.logger.Printf("Sent GET response for %s to Node %d", msg.FileName, msg.SenderID)
}

// handleGetResponse handles GET response from NameNode
func (h *HDFSState) handleGetResponse(msg *Message) {
	if h.getRespChan != nil {
		select {
		case h.getRespChan <- msg:
		default:
		}
	}
}

// fetchBlocks retrieves all blocks and reassembles the file
func (h *HDFSState) fetchBlocks(placements []BlockPlacement, fileSize int, localPath string) {
	fileData := make([]byte, 0, fileSize)
	sortedPlacements := make([]BlockPlacement, len(placements))
	copy(sortedPlacements, placements)
	sort.Slice(sortedPlacements, func(i, j int) bool {
		return sortedPlacements[i].BlockIndex < sortedPlacements[j].BlockIndex
	})

	for _, p := range sortedPlacements {
		blockData := h.fetchSingleBlock(p)
		if blockData == nil {
			fmt.Printf("Failed to retrieve block %s\n", p.BlockID)
			return
		}
		fileData = append(fileData, blockData...)
	}

	// Trim to actual file size
	if len(fileData) > fileSize {
		fileData = fileData[:fileSize]
	}

	err := os.WriteFile(localPath, fileData, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		return
	}

	fmt.Printf("HDFS GET complete: %s (%d bytes)\n", localPath, len(fileData))
	h.node.logger.Printf("GET complete: %s, %d bytes", localPath, len(fileData))
}

// fetchSingleBlock retrieves a single block from available DataNodes
func (h *HDFSState) fetchSingleBlock(p BlockPlacement) []byte {
	// Try local first
	h.mu.Lock()
	if data, ok := h.blockStore[p.BlockID]; ok {
		h.mu.Unlock()
		fmt.Printf("  Block %s read locally\n", p.BlockID)
		return data
	}
	h.mu.Unlock()

	// Try each remote node
	for _, nodeID := range p.NodeIDs {
		if nodeID == h.node.ID {
			continue
		}

		h.blockDataChan = make(chan *Message, 1)

		req := &Message{
			Type:     MSG_HDFS_BLOCK_REQ,
			SenderID: h.node.ID,
			BlockID:  p.BlockID,
		}
		err := h.node.sendToNode(nodeID, req)
		if err != nil {
			h.node.logger.Printf("Failed to request block %s from Node %d: %v", p.BlockID, nodeID, err)
			continue
		}

		select {
		case resp := <-h.blockDataChan:
			if resp.Success {
				data, err := base64.StdEncoding.DecodeString(resp.BlockData)
				if err == nil {
					fmt.Printf("  Block %s fetched from Node %d\n", p.BlockID, nodeID)
					return data
				}
			}
		case <-time.After(5 * time.Second):
			h.node.logger.Printf("Timeout fetching block %s from Node %d", p.BlockID, nodeID)
		}
	}

	return nil
}

// handleBlockRequest handles a block data request (DataNode)
func (h *HDFSState) handleBlockRequest(msg *Message) {
	h.mu.Lock()
	data, ok := h.blockStore[msg.BlockID]
	h.mu.Unlock()

	resp := &Message{
		Type:     MSG_HDFS_BLOCK_DATA,
		SenderID: h.node.ID,
		BlockID:  msg.BlockID,
		Success:  ok,
	}

	if ok {
		resp.BlockData = base64.StdEncoding.EncodeToString(data)
	}

	h.node.sendToNode(msg.SenderID, resp)
	h.node.logger.Printf("Sent block %s data to Node %d (found: %v)", msg.BlockID, msg.SenderID, ok)
}

// handleBlockData handles received block data
func (h *HDFSState) handleBlockData(msg *Message) {
	if h.blockDataChan != nil {
		select {
		case h.blockDataChan <- msg:
		default:
		}
	}
}

// =============================================
// LIST FILES
// =============================================

// ListFiles lists all files in HDFS
func (h *HDFSState) ListFiles() {
	if h.node.ID == NameNodeID {
		h.printFileList()
	} else {
		h.lsRespChan = make(chan *Message, 1)

		msg := &Message{
			Type:     MSG_HDFS_LS_REQ,
			SenderID: h.node.ID,
		}
		h.node.sendToNode(NameNodeID, msg)

		select {
		case resp := <-h.lsRespChan:
			fmt.Print(resp.Content)
		case <-time.After(5 * time.Second):
			fmt.Println("Timeout waiting for file list")
		}
	}
}

// handleLsRequest handles an LS request (NameNode only)
func (h *HDFSState) handleLsRequest(msg *Message) {
	if !h.isNameNode {
		return
	}

	h.mu.Lock()
	listing := h.buildFileList()
	h.mu.Unlock()

	resp := &Message{
		Type:     MSG_HDFS_LS_RESP,
		SenderID: h.node.ID,
		Content:  listing,
	}
	h.node.sendToNode(msg.SenderID, resp)
}

// handleLsResponse handles LS response from NameNode
func (h *HDFSState) handleLsResponse(msg *Message) {
	if h.lsRespChan != nil {
		select {
		case h.lsRespChan <- msg:
		default:
		}
	}
}

// printFileList prints files stored in HDFS
func (h *HDFSState) printFileList() {
	h.mu.Lock()
	listing := h.buildFileList()
	h.mu.Unlock()
	fmt.Print(listing)
}

func (h *HDFSState) buildFileList() string {
	if len(h.files) == 0 {
		return "\nHDFS: No files stored\n\n"
	}

	var sb strings.Builder
	sb.WriteString("\n----- HDFS FILE LISTING -----\n\n")

	for _, fi := range h.files {
		sb.WriteString(fmt.Sprintf("  %s  (%d bytes, %d blocks, replication: %d)\n",
			fi.FileName, fi.FileSize, fi.NumBlocks, fi.ReplFactor))

		for _, blockID := range fi.BlockIDs {
			if bi, ok := h.blocks[blockID]; ok {
				rackIDs := make([]int, len(bi.NodeIDs))
				for j, nid := range bi.NodeIDs {
					rackIDs[j] = RackAssignment[nid]
				}
				sb.WriteString(fmt.Sprintf("    %s -> Nodes %v (Racks %v)\n", blockID, bi.NodeIDs, rackIDs))
			}
		}
	}

	sb.WriteString("\n-----------------------------\n\n")
	return sb.String()
}

// =============================================
// DELETE FILE
// =============================================

// DeleteFile removes a file from HDFS
func (h *HDFSState) DeleteFile(fileName string) {
	fmt.Printf("\nHDFS DELETE: %s\n", fileName)

	if h.node.ID == NameNodeID {
		h.processDelete(fileName, h.node.ID)
	} else {
		h.deleteRespChan = make(chan *Message, 1)

		msg := &Message{
			Type:     MSG_HDFS_DELETE,
			SenderID: h.node.ID,
			FileName: fileName,
		}
		h.node.sendToNode(NameNodeID, msg)

		select {
		case resp := <-h.deleteRespChan:
			if resp.Success {
				fmt.Printf("HDFS DELETE complete: %s\n", fileName)
			} else {
				fmt.Printf("HDFS DELETE failed: %s\n", resp.Content)
			}
		case <-time.After(5 * time.Second):
			fmt.Println("Timeout waiting for delete response")
		}
	}
}

// processDelete removes file metadata and blocks (NameNode)
func (h *HDFSState) processDelete(fileName string, clientID int) {
	h.mu.Lock()
	fi, exists := h.files[fileName]
	if !exists {
		h.mu.Unlock()
		fmt.Printf("File '%s' not found\n", fileName)
		if clientID != h.node.ID {
			resp := &Message{
				Type:     MSG_HDFS_DELETE_RESP,
				SenderID: h.node.ID,
				FileName: fileName,
				Success:  false,
				Content:  "file not found",
			}
			h.node.sendToNode(clientID, resp)
		}
		return
	}

	// Collect block info and delete metadata
	blockIDs := fi.BlockIDs
	blockNodes := make(map[string][]int)
	for _, blockID := range blockIDs {
		if bi, ok := h.blocks[blockID]; ok {
			blockNodes[blockID] = bi.NodeIDs
			delete(h.blocks, blockID)
		}
	}
	delete(h.files, fileName)
	h.mu.Unlock()

	fmt.Printf("HDFS NameNode: Deleting %d blocks for '%s'\n", len(blockIDs), fileName)
	h.node.logger.Printf("Deleting file %s, %d blocks", fileName, len(blockIDs))

	// Tell DataNodes to remove blocks
	for blockID, nodeIDs := range blockNodes {
		for _, nodeID := range nodeIDs {
			if nodeID == h.node.ID {
				h.mu.Lock()
				delete(h.blockStore, blockID)
				h.mu.Unlock()
				fmt.Printf("  Deleted block %s locally\n", blockID)
			} else {
				delMsg := &Message{
					Type:     MSG_HDFS_DELETE_BLOCK,
					SenderID: h.node.ID,
					BlockID:  blockID,
				}
				h.node.sendToNode(nodeID, delMsg)
				fmt.Printf("  Sent delete block %s to Node %d\n", blockID, nodeID)
			}
		}
	}

	if clientID != h.node.ID {
		resp := &Message{
			Type:     MSG_HDFS_DELETE_RESP,
			SenderID: h.node.ID,
			FileName: fileName,
			Success:  true,
		}
		h.node.sendToNode(clientID, resp)
	} else {
		fmt.Printf("HDFS DELETE complete: %s\n", fileName)
	}
}

// handleDeleteRequest handles delete request (NameNode)
func (h *HDFSState) handleDeleteRequest(msg *Message) {
	if !h.isNameNode {
		return
	}
	h.processDelete(msg.FileName, msg.SenderID)
}

// handleDeleteResponse handles delete response from NameNode
func (h *HDFSState) handleDeleteResponse(msg *Message) {
	if h.deleteRespChan != nil {
		select {
		case h.deleteRespChan <- msg:
		default:
		}
	}
}

// handleDeleteBlock removes a block from local store (DataNode)
func (h *HDFSState) handleDeleteBlock(msg *Message) {
	h.mu.Lock()
	delete(h.blockStore, msg.BlockID)
	h.mu.Unlock()

	fmt.Printf("Deleted block %s (instructed by NameNode)\n", msg.BlockID)
	h.node.logger.Printf("Deleted block %s", msg.BlockID)
}

// =============================================
// HDFS STATUS
// =============================================

// ShowStatus shows HDFS cluster status
func (h *HDFSState) ShowStatus() {
	if h.node.ID == NameNodeID {
		h.printStatus()
	} else {
		h.statusRespChan = make(chan *Message, 1)

		msg := &Message{
			Type:     MSG_HDFS_STATUS_REQ,
			SenderID: h.node.ID,
		}
		h.node.sendToNode(NameNodeID, msg)

		select {
		case resp := <-h.statusRespChan:
			fmt.Print(resp.Content)
		case <-time.After(5 * time.Second):
			fmt.Println("Timeout waiting for status")
		}
	}
}

// handleStatusRequest handles status request (NameNode)
func (h *HDFSState) handleStatusRequest(msg *Message) {
	if !h.isNameNode {
		return
	}

	h.mu.Lock()
	status := h.buildStatus()
	h.mu.Unlock()

	resp := &Message{
		Type:     MSG_HDFS_STATUS_RESP,
		SenderID: h.node.ID,
		Content:  status,
	}
	h.node.sendToNode(msg.SenderID, resp)
}

// handleStatusResponse handles status response from NameNode
func (h *HDFSState) handleStatusResponse(msg *Message) {
	if h.statusRespChan != nil {
		select {
		case h.statusRespChan <- msg:
		default:
		}
	}
}

func (h *HDFSState) printStatus() {
	h.mu.Lock()
	status := h.buildStatus()
	h.mu.Unlock()
	fmt.Print(status)
}

func (h *HDFSState) buildStatus() string {
	var sb strings.Builder
	sb.WriteString("\n----- HDFS CLUSTER STATUS -----\n\n")
	sb.WriteString(fmt.Sprintf("  Block size:         %d bytes\n", BlockSize))
	sb.WriteString(fmt.Sprintf("  Replication factor: %d\n", ReplicationFactor))
	sb.WriteString(fmt.Sprintf("  Total files:        %d\n", len(h.files)))
	sb.WriteString(fmt.Sprintf("  Total blocks:       %d\n", len(h.blocks)))
	sb.WriteString("\n  DataNode Status:\n")

	// Sort by node ID
	nodeIDs := make([]int, 0, len(h.dataNodes))
	for nid := range h.dataNodes {
		nodeIDs = append(nodeIDs, nid)
	}
	sort.Ints(nodeIDs)

	for _, nid := range nodeIDs {
		ds := h.dataNodes[nid]
		status := "ALIVE"
		if !ds.Alive {
			status = "DEAD"
		}
		sb.WriteString(fmt.Sprintf("    Node %d (Rack %d): %s, %d blocks\n",
			ds.NodeID, ds.RackID, status, ds.BlockCount))
	}

	sb.WriteString("\n-------------------------------\n\n")
	return sb.String()
}

// =============================================
// RE-REPLICATION (fault tolerance)
// =============================================

// reReplicateBlocks handles re-replication when a DataNode fails
func (h *HDFSState) reReplicateBlocks(failedNodeID int) {
	h.mu.Lock()

	// Find all blocks that were on the failed node
	affectedBlocks := []*BlockInfo{}
	for _, bi := range h.blocks {
		for _, nid := range bi.NodeIDs {
			if nid == failedNodeID {
				affectedBlocks = append(affectedBlocks, bi)
				break
			}
		}
	}

	if len(affectedBlocks) == 0 {
		h.mu.Unlock()
		return
	}

	fmt.Printf("\nHDFS: Re-replicating %d blocks from failed Node %d\n", len(affectedBlocks), failedNodeID)
	h.node.logger.Printf("Re-replicating %d blocks from failed Node %d", len(affectedBlocks), failedNodeID)

	for _, bi := range affectedBlocks {
		// Remove failed node from block's node list
		newNodeIDs := []int{}
		for _, nid := range bi.NodeIDs {
			if nid != failedNodeID {
				newNodeIDs = append(newNodeIDs, nid)
			}
		}
		bi.NodeIDs = newNodeIDs

		// Find a new node to replicate to
		if len(bi.NodeIDs) < ReplicationFactor {
			newNode := h.findReplicationTarget(bi)
			if newNode > 0 {
				bi.NodeIDs = append(bi.NodeIDs, newNode)

				// Find a source node that has this block
				sourceNode := -1
				for _, nid := range newNodeIDs {
					if ds, ok := h.dataNodes[nid]; ok && ds.Alive {
						sourceNode = nid
						break
					}
				}

				if sourceNode > 0 {
					fmt.Printf("  Block %s: replicating from Node %d to Node %d\n",
						bi.BlockID, sourceNode, newNode)
					h.node.logger.Printf("Replicating block %s from Node %d to Node %d",
						bi.BlockID, sourceNode, newNode)

					// Request the block from source and send to target
					go h.doReplication(bi.BlockID, sourceNode, newNode)
				}
			} else {
				fmt.Printf("  Block %s: no available target for re-replication\n", bi.BlockID)
			}
		}
	}
	h.mu.Unlock()
}

// findReplicationTarget finds a suitable node for a new replica
func (h *HDFSState) findReplicationTarget(bi *BlockInfo) int {
	usedNodes := make(map[int]bool)
	for _, nid := range bi.NodeIDs {
		usedNodes[nid] = true
	}

	// Get racks used by existing replicas
	usedRacks := make(map[int]bool)
	for _, nid := range bi.NodeIDs {
		usedRacks[RackAssignment[nid]] = true
	}

	// Prefer nodes on different racks
	candidates := []int{}
	for nodeID, ds := range h.dataNodes {
		if ds.Alive && !usedNodes[nodeID] {
			if !usedRacks[RackAssignment[nodeID]] {
				return nodeID // Different rack, best choice
			}
			candidates = append(candidates, nodeID)
		}
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))]
	}
	return -1
}

// doReplication fetches a block from source and sends it to target
func (h *HDFSState) doReplication(blockID string, sourceNode int, targetNode int) {
	// Check if we have it locally
	h.mu.Lock()
	localData, hasLocal := h.blockStore[blockID]
	h.mu.Unlock()

	if hasLocal {
		// Send directly to target
		msg := &Message{
			Type:      MSG_HDFS_BLOCK_STORE,
			SenderID:  h.node.ID,
			BlockID:   blockID,
			BlockData: base64.StdEncoding.EncodeToString(localData),
		}
		h.node.sendToNode(targetNode, msg)
		fmt.Printf("  Sent block %s to Node %d for re-replication\n", blockID, targetNode)
		return
	}

	// Request from source
	msg := &Message{
		Type:     MSG_HDFS_REPLICATE,
		SenderID: h.node.ID,
		BlockID:  blockID,
		Counter:  targetNode, // reuse Counter field for target node
	}
	h.node.sendToNode(sourceNode, msg)
}

// handleReplicate handles a replication request (DataNode)
func (h *HDFSState) handleReplicate(msg *Message) {
	h.mu.Lock()
	data, ok := h.blockStore[msg.BlockID]
	h.mu.Unlock()

	if !ok {
		h.node.logger.Printf("Cannot replicate block %s: not found locally", msg.BlockID)
		return
	}

	targetNode := msg.Counter // target node ID passed in Counter field

	storeMsg := &Message{
		Type:      MSG_HDFS_BLOCK_STORE,
		SenderID:  h.node.ID,
		BlockID:   msg.BlockID,
		BlockData: base64.StdEncoding.EncodeToString(data),
	}
	err := h.node.sendToNode(targetNode, storeMsg)
	if err != nil {
		h.node.logger.Printf("Failed to replicate block %s to Node %d: %v", msg.BlockID, targetNode, err)
	} else {
		fmt.Printf("Replicated block %s to Node %d\n", msg.BlockID, targetNode)
		h.node.logger.Printf("Replicated block %s to Node %d", msg.BlockID, targetNode)
	}
}
