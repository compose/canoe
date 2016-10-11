package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/compose/canoe"
	"os"
	"os/signal"
	"strings"
	"time"
)

var id = flag.Uint64("id", 0, "ID for canoe Node")
var clusterID = flag.Uint64("cluster-id", 0x100, "ClusterID for canoe cluster")

var raftPort = flag.Int("raft-port", 1234, "Port for raft messages to be sent")
var configPort = flag.Int("config-port", 1235, "Port for cluster configuration")

var peers = flag.String("peers", "", "Peers to connect to")
var bootstrap = flag.Bool("bootstrap", false, "Is this node a bootstrap node")
var dataDir = flag.String("data-dir", "./lifeboat_data", "Directory to store all data for arbiter")

func main() {
	flag.Parse()

	if *peers == "" && !*bootstrap {
		log.Fatal("Peers and/or bootstrap must be set as options")
		return
	}

	config := &canoe.NodeConfig{
		ID:                *id,
		ClusterID:         *clusterID,
		RaftPort:          *raftPort,
		ConfigurationPort: *configPort,
		BootstrapPeers:    strings.Split(*peers, ","),
		BootstrapNode:     *bootstrap,
		DataDir:           *dataDir,
		// This FSM essentially does nothing. It exists to appease canoe
		FSM: &arbiterFSM{},

		// No snapshotting since any snapshots from this peer would be invalid
	}

	node, err := canoe.NewNode(config)
	if err != nil {
		log.Fatalf("Error creating canoe node, %+v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Error starting canoe node, %+v", err)
	}

	log.Info("Lifeboat is running")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(node *canoe.Node) {
		for _ = range c {
			log.Info("Shutting down lifeboat")
			if err := node.Stop(); err != nil {
				log.Errorf("Error shutting down lifeboat, %+v", err)
				os.Exit(1)
			}

			log.Info("Clean Shutdown Finished")
		}
	}(node)

	for node.IsRunning() {
		time.Sleep(500 * time.Millisecond)
	}
}

type arbiterFSM struct {
}

func (*arbiterFSM) Apply(log canoe.LogData) error {
	return nil
}

func (*arbiterFSM) Snapshot() (canoe.SnapshotData, error) {
	return []byte{}, nil
}

func (*arbiterFSM) Restore(snapData canoe.SnapshotData) error {
	return nil
}
