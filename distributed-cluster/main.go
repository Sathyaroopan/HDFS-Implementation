package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	// Parse command line flags
	nodeID := flag.Int("id", 0, "Node ID (required)")
	configPath := flag.String("config", "config/nodes.json", "Path to cluster config file")
	flag.Parse()

	if *nodeID == 0 {
		fmt.Println("Usage: go run . --id=<nodeID> [--config=<path>]")
		fmt.Println("  --id     Node ID (1, 2, 3, or 4)")
		fmt.Println("  --config Path to config file (default: config/nodes.json)")
		os.Exit(1)
	}

	// Load configuration
	config, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Validate node ID
	found := false
	for _, n := range config.Nodes {
		if n.ID == *nodeID {
			found = true
			break
		}
	}
	if !found {
		fmt.Printf("Node ID %d not found in config\n", *nodeID)
		os.Exit(1)
	}

	// Create and start the node
	node := NewNode(*nodeID, config)
	node.Start()

	// Interactive command loop
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		command := strings.ToLower(parts[0])

		switch command {
		case "send":
			if len(parts) < 3 {
				fmt.Println("Usage: send <nodeID> <message>")
			} else {
				targetID, err := strconv.Atoi(parts[1])
				if err != nil {
					fmt.Println("Invalid node ID")
				} else {
					node.SendMessage(targetID, parts[2])
				}
			}

		case "snapshot":
			node.snapshot.InitiateSnapshot()

		case "election":
			node.StartElection()

		case "leader":
			node.ShowLeader()

		case "exit":
			fmt.Println("Shutting down...")
			node.Shutdown()
			os.Exit(0)

		case "put":
			if len(parts) < 2 {
				fmt.Println("Usage: put <filepath>")
			} else {
				node.hdfs.PutFile(parts[1])
			}

		case "get":
			if len(parts) < 3 {
				fmt.Println("Usage: get <hdfsname> <outputpath>")
			} else {
				node.hdfs.GetFile(parts[1], parts[2])
			}

		case "ls":
			node.hdfs.ListFiles()

		case "rm":
			if len(parts) < 2 {
				fmt.Println("Usage: rm <hdfsname>")
			} else {
				node.hdfs.DeleteFile(parts[1])
			}

		case "hdfs-status":
			node.hdfs.ShowStatus()

		default:
			fmt.Println("Unknown command. Available commands:")
			fmt.Println("  send <nodeID> <message>")
			fmt.Println("  snapshot")
			fmt.Println("  election")
			fmt.Println("  leader")
			fmt.Println("  put <filepath>")
			fmt.Println("  get <hdfsname> <outputpath>")
			fmt.Println("  ls")
			fmt.Println("  rm <hdfsname>")
			fmt.Println("  hdfs-status")
			fmt.Println("  exit")
		}

		fmt.Print("> ")
	}
}
