package raftwrapper

import (
	"encoding/bytes"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/stats"
	"github.com/coreos/etcd/version"
	"golang.org/x/net/context"
	"net/http"
	"net/url"
	"sleep"
)

type RaftNode struct {
	node        raft.Node
	raftStorage *raft.MemoryStorage
	transport   *rafthttp.Transport
	peers       []string
	id          uint64
	port        int

	proposeC chan string
	fsm      *FSM
}

// note: peers is only for asking to join the cluster.
// It will not be able to connect if the peers don't respond to cluster node add request
// This is because each node defines it's own uuid at startup. We must be told this UUID
// by another node.
// TODO: Look into which config options we want others to specify. For now hardcoded
// NOTE: Peers are used EXCLUSIVELY to round-robin to other nodes and attempt to add
//		ourselves to an existing cluster or bootstrap node
func NewRaftNode(fsm *FSM, bindPort string, peers []string, bootstrapNode bool) (*raftNode, error) {
	rn := nonInitRNode(fsm, bindPort, peers, bootstrapNode)

	if err := rn.attachTransport(); err != nil {
		return nil, err
	}
	if err := rn.joinPeers(); err != nil {
		return nil, err
	}

	go rn.run()

	// blocking call with timeout
	// NOTE: We don't want to add ourself as our own peer yet.
	// We want some node to commit our addition to the cluster
	// so it will be written to us whenever a leader is elected
	rn.joinPeers()
	return rn, nil
}

func nonInitRNode(fsm *FSM, bindPort string, peers []string, boostrapNode bool) RaftNode {
	rn := &RaftNode{
		proposeC:    make(chan string),
		cluster:     0x1000,
		raftStorage: raft.NewMemoryStorage(),
		peers:       peers,
		id:          Uint64UUID(),
		port:        bindPort,
		fsm:         fsm,
	}

	c := &raft.Config{
		ID:              rn.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if bootstrapNode {
		rn.node = raft.StartNode(c, raft.Peer{ID: rn.id})
	} else {
		rn.node = raft.StartNode(c, nil)
	}
}

func (rn *raftNode) attachTransport() error {
	ss := &stats.ServerStats{}
	ss.Initialize()

	ls := &stats.LeaderStats{}

	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}

	rc.Transport.Start()
	return nil
}

func (rn *raftNode) joinPeers() error {
	resp, err := rn.askForAddition()
	if err != nil {
		return err
	}
}

func (rn *raftNode) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rd.CommittedEntries); !ok {
				return
			}

			rc.node.Advance()
		}
	}
}

func (rn *raftNode) Process(ctx context.Context, m raftpb.Message) {
	return rn.node.Step(ctx, m)
}
