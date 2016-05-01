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
	"sync"
)

type LogData []byte

type RaftNode struct {
	node        raft.Node
	raftStorage *raft.MemoryStorage
	transport   *rafthttp.Transport
	peers       []string
	id          uint64
	port        int

	initialized bool
	proposeC    chan string
	fsm         *FSM

	observers     map[uint64]*Observer
	observersLock sync.RWMutex
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

	go rn.run()

	if err := rn.joinPeers(); err != nil {
		return nil, err
	}

	// final step to mark node as initialized
	rn.initialized = true

	return rn, nil
}

func nonInitRNode(fsm *FSM, bindPort string, peers []string, boostrapNode bool) RaftNode {
	if bootstrapNode {
		peers = nil
	}
	rn := &RaftNode{
		proposeC:    make(chan string),
		cluster:     0x1000,
		raftStorage: raft.NewMemoryStorage(),
		peers:       peers,
		id:          Uint64UUID(),
		port:        bindPort,
		fsm:         fsm,
		initialized: false,
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

func (rn *raftNode) proposePeerAddition(addReq raftpb.ConfChange, async bool) error {
	addReq.Type = raftpb.ConfChangeAddNode

	if !async {
		// TODO: Setup listener for confchange commit
	}

	if err := rn.node.ProposeConfChange(context.TODO(), addReq); err != nil {
		return err
	}

	if async {
		return nil
	}

	observChan := make(chan Observation)
	filterFn := func(o *Observation) bool {
		switch o.(type) {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				// wait until we get a matching node id
				return addReq.NodeID == cc.NodeID
			default:
				return false
			}
		default:
			return false
		}
	}

	observer := NewObserver(observChan, filterFn)
	rn.RegisterObserver(observer)
	defer rn.UnregisterObserver(observer)

	// TODO: Do a retry here on failure for x retries
	switch {
	case <-observChan:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("Timed out waiting for add log to commit")

	}
}

func (rn *raftNode) canAddPeer() bool {
	return rn.isHealthy() && rn.initialized
}

// TODO: Define healthy
func (rn *raftNode) isHealthy() bool {
	return true
}

func (rn *raftNode) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)
			if ok := rn.publishEntries(rd.CommittedEntries); !ok {
				return
			}

			rc.node.Advance()
		}
	}
}

func (rn *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for _, entry := range ents {
		rn.observe(entry)
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				break
			}
			// Yes, this is probably a blocking call
			// An FSM should be responsible for being efficient
			// for high-load situations
			rn.fsm.Apply(entry.Data.(LogData))

			rn.observe(entry)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			rn.node.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I have been removed!")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}

			rn.observe(entry)
		}
		// TODO: Add support for replay commits
	}
	return true
}

func (rn *raftNode) Process(ctx context.Context, m raftpb.Message) {
	return rn.node.Step(ctx, m)
}
