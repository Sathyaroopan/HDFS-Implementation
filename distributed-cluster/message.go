package main

import (
	"encoding/json"
	"fmt"
)

// Message types
const (
	MSG_DATA        = "DATA"
	MSG_MARKER      = "MARKER"
	MSG_ELECTION    = "ELECTION"
	MSG_ANSWER      = "ANSWER"
	MSG_COORDINATOR = "COORDINATOR"
	MSG_LEADER_Q    = "LEADER_QUERY"
	MSG_LEADER_R    = "LEADER_RESPONSE"
	MSG_STATE_REQ   = "STATE_REQUEST"
	MSG_STATE_RESP  = "STATE_RESPONSE"

	// HDFS message types
	MSG_HDFS_PUT_REQ       = "HDFS_PUT_REQ"       // client -> NameNode: request to store a file
	MSG_HDFS_PUT_RESP      = "HDFS_PUT_RESP"      // NameNode -> client: block placement plan
	MSG_HDFS_BLOCK_STORE   = "HDFS_BLOCK_STORE"   // client -> DataNode: store a block
	MSG_HDFS_BLOCK_ACK     = "HDFS_BLOCK_ACK"     // DataNode -> client: block stored
	MSG_HDFS_GET_REQ       = "HDFS_GET_REQ"       // client -> NameNode: request file metadata
	MSG_HDFS_GET_RESP      = "HDFS_GET_RESP"      // NameNode -> client: block locations
	MSG_HDFS_BLOCK_REQ     = "HDFS_BLOCK_REQ"     // client -> DataNode: request block data
	MSG_HDFS_BLOCK_DATA    = "HDFS_BLOCK_DATA"    // DataNode -> client: block content
	MSG_HDFS_DELETE        = "HDFS_DELETE"        // client -> NameNode: delete file
	MSG_HDFS_DELETE_RESP   = "HDFS_DELETE_RESP"   // NameNode -> client: delete confirmation
	MSG_HDFS_DELETE_BLOCK  = "HDFS_DELETE_BLOCK"  // NameNode -> DataNode: remove block
	MSG_HDFS_HEARTBEAT     = "HDFS_HEARTBEAT"     // DataNode -> NameNode: alive signal
	MSG_HDFS_HEARTBEAT_ACK = "HDFS_HEARTBEAT_ACK" // NameNode -> DataNode: heartbeat ack
	MSG_HDFS_REPLICATE     = "HDFS_REPLICATE"     // NameNode -> DataNode: replicate block
	MSG_HDFS_LS_REQ        = "HDFS_LS_REQ"        // client -> NameNode: list files
	MSG_HDFS_LS_RESP       = "HDFS_LS_RESP"       // NameNode -> client: file listing
	MSG_HDFS_STATUS_REQ    = "HDFS_STATUS_REQ"    // client -> NameNode: cluster status
	MSG_HDFS_STATUS_RESP   = "HDFS_STATUS_RESP"   // NameNode -> client: cluster status info
	MSG_HDFS_PUT_DONE      = "HDFS_PUT_DONE"      // client -> NameNode: all blocks stored
)

// Message represents a message sent between nodes
type Message struct {
	Type       string `json:"type"`
	SenderID   int    `json:"sender_id"`
	Content    string `json:"content,omitempty"`
	SnapshotID int    `json:"snapshot_id,omitempty"`
	Counter    int    `json:"counter,omitempty"`

	// HDFS fields
	FileName    string           `json:"file_name,omitempty"`
	FileSize    int              `json:"file_size,omitempty"`
	BlockID     string           `json:"block_id,omitempty"`
	BlockData   string           `json:"block_data,omitempty"` // base64-encoded block content
	BlockIndex  int              `json:"block_index,omitempty"`
	TotalBlocks int              `json:"total_blocks,omitempty"`
	BlockList   []BlockPlacement `json:"block_list,omitempty"` // block placement plan
	Success     bool             `json:"success,omitempty"`
}

// BlockPlacement describes where a block should be stored
type BlockPlacement struct {
	BlockID    string `json:"block_id"`
	BlockIndex int    `json:"block_index"`
	NodeIDs    []int  `json:"node_ids"` // DataNodes to store this block
	RackIDs    []int  `json:"rack_ids"` // corresponding rack IDs
}

// Encode serializes a message to JSON bytes with a newline delimiter
func (m *Message) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to encode message: %v", err)
	}
	data = append(data, '\n')
	return data, nil
}

// DecodeMessage deserializes JSON bytes into a Message
func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message: %v", err)
	}
	return &msg, nil
}
