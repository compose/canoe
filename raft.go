package raftwrapper

import (
	"fmt"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"golang.org/x/net/context"
	"strconv"
	"sync"
	"time"
)

type LogData []byte

type RaftNode struct {
	node        raft.Node
	raftStorage *raft.MemoryStorage
	transport   *rafthttp.Transport
	peers       []string
	id          uint64
	port        int
	cluster     int

	initialized bool
	proposeC    chan string
	fsm         FSM

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
func NewRaftNode(fsm FSM, bindPort int, peers []string, bootstrapNode bool) (*RaftNode, error) {
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

func nonInitRNode(fsm FSM, bindPort int, peers []string, bootstrapNode bool) *RaftNode {
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
		rn.node = raft.StartNode(c, []raft.Peer{raft.Peer{ID: rn.id}})
	} else {
		rn.node = raft.StartNode(c, nil)
	}

	return rn
}

func (rn *RaftNode) attachTransport() error {
	ss := &stats.ServerStats{}
	ss.Initialize()

	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(rn.id, 10)),
		ErrorC:      make(chan error),
	}

	rn.transport.Start()
	return nil
}

func (rn *RaftNode) joinPeers() error {
	err := rn.requestSelfAddition()
	if err != nil {
		return err
	}
	return nil
}

func (rn *RaftNode) proposePeerAddition(addReq *raftpb.ConfChange, async bool) error {
	addReq.Type = raftpb.ConfChangeAddNode

	observChan := make(chan Observation)
	// setup listener for node addition
	// before asking for node addition
	if !async {
		filterFn := func(o Observation) bool {
			switch o.(type) {
			case raftpb.Entry:
				entry := o.(raftpb.Entry)
				switch entry.Type {
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
			default:
				return false
			}
		}

		observer := NewObserver(observChan, filterFn)
		rn.RegisterObserver(observer)
		defer rn.UnregisterObserver(observer)
	}

	if err := rn.node.ProposeConfChange(context.TODO(), *addReq); err != nil {
		return err
	}

	if async {
		return nil
	}

	// TODO: Do a retry here on failure for x retries
	select {
	case <-observChan:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("Timed out waiting for add log to commit")

	}
}

func (rn *RaftNode) canAddPeer() bool {
	return rn.isHealthy() && rn.initialized
}

// TODO: Define healthy
func (rn *RaftNode) isHealthy() bool {
	return true
}

func (rn *RaftNode) run() {
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

			rn.node.Advance()
		}
	}
}

func (rn *RaftNode) publishEntries(ents []raftpb.Entry) bool {
	for _, entry := range ents {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				break
			}
			// Yes, this is probably a blocking call
			// An FSM should be responsible for being efficient
			// for high-load situations
			rn.fsm.Apply(LogData(entry.Data))

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
					fmt.Println("I have been removed!")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}

		}
		rn.observe(entry)
		// TODO: Add support for replay commits
		// After replaying old commits/snapshots then mark
		// this node operational
	}
	return true
}

func (rn *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}
func (rn *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (rn *RaftNode) ReportUnreachable(id uint64)                          {}
func (rn *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
