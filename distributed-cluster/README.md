# Distributed Cluster - Case Study

A distributed systems demonstration project that simulates a cluster of 4 nodes, showcasing:

- **Chandy-Lamport Snapshot Algorithm** - Captures consistent global state
- **Bully Leader Election Algorithm** - Elects leader by highest node ID
- **Simplified HDFS File System** - Block splitting, rack-aware replication, fault tolerance

## Prerequisites

- [Go 1.21+](https://go.dev/dl/)
- 4 laptops on the same LAN (or run locally for testing)

## Setup

### 1. Edit Configuration

Update `config/nodes.json` with the actual IP addresses of each laptop:

```json
{
  "nodes": [
    {"id": 1, "ip": "192.168.1.10", "port": 5001},
    {"id": 2, "ip": "192.168.1.11", "port": 5002},
    {"id": 3, "ip": "192.168.1.12", "port": 5003},
    {"id": 4, "ip": "192.168.1.13", "port": 5004}
  ]
}
```

For **local testing**, use `127.0.0.1` for all IPs with different ports.

### 2. Copy the Project

Copy the `distributed-cluster` folder to all 4 laptops.

### 3. Start Nodes

On each laptop, open a terminal and run:

```bash
cd distributed-cluster
go run . --id=1   # on laptop 1
go run . --id=2   # on laptop 2
go run . --id=3   # on laptop 3
go run . --id=4   # on laptop 4
```

## Commands

| Command | Description |
|---|---|
| `send <nodeID> <message>` | Send a message to a specific node |
| `snapshot` | Initiate Chandy-Lamport global snapshot |
| `election` | Start Bully leader election |
| `leader` | Show current known leader |
| `put <filepath>` | Upload a file to HDFS |
| `get <hdfsname> <output>` | Download a file from HDFS |
| `ls` | List all files in HDFS |
| `rm <hdfsname>` | Delete a file from HDFS |
| `hdfs-status` | Show HDFS cluster status |
| `exit` | Shut down the node |

## HDFS Architecture

```
Node 1 (NameNode + DataNode, Rack 1)   -- manages file/block metadata
Node 2 (DataNode, Rack 1)
Node 3 (DataNode, Rack 2)
Node 4 (DataNode, Rack 2)
```

- **Block size**: 256 bytes (small for demo visibility)
- **Replication factor**: 3 (each block stored on 3 nodes)
- **Rack-aware placement**: replicas spread across Rack 1 and Rack 2
- **Heartbeat**: DataNodes send heartbeats to NameNode every 5s
- **Fault tolerance**: if a DataNode misses 3 heartbeats, NameNode marks it dead and re-replicates its blocks

## Demo Walkthrough

### Step 1 - Start All Nodes
Start all 4 nodes. Wait for "Connected to cluster" on each.

### Step 2 - Send Messages
```
> send 2 hello
> send 3 world
```

### Step 3 - Upload File to HDFS
Create a test file on any node, then:
```
> put myfile.txt
```
Output shows block splitting and placement across racks.

### Step 4 - List HDFS Files
```
> ls
```
Shows files with block locations and rack assignments.

### Step 5 - Download File from HDFS
On a different node:
```
> get myfile.txt downloaded.txt
```
Retrieves blocks from DataNodes and reassembles the file.

### Step 6 - Check HDFS Status
```
> hdfs-status
```
Shows DataNode health, block counts, and rack assignments.

### Step 7 - Take Snapshot
```
> snapshot
```
Captures and displays global state of all nodes.

### Step 8 - Leader Election
```
> election
```
Runs the Bully algorithm, elects highest-ID node as leader.

### Step 9 - Shutdown
```
> exit
```

## File Structure

```
distributed-cluster/
├── main.go          # Entry point, CLI parsing, command loop
├── node.go          # Node process, TCP server, peer management
├── snapshot.go      # Chandy-Lamport snapshot algorithm
├── election.go      # Bully leader election algorithm
├── hdfs.go          # HDFS: NameNode, DataNode, block placement, fault tolerance
├── message.go       # Message types and encoding
├── config/
│   └── nodes.json   # Cluster configuration
├── logs/            # Per-node log files (created at runtime)
└── README.md
```

## Log Files

Detailed logs are written to `logs/node<ID>.log` during execution. These include all message send/receive events, timestamps, and debug info.
