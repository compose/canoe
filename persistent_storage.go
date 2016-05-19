package raftwrapper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

type walMetadata struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
}

// Correct order of ops
// 1: Apply any persisted snapshot to FSM
// 2: Apply any persisted WAL data to FSM
// 3: Restore Metadata from WAL
// 4: Apply any Snapshot to raft storage
// 5: Apply any hardstate to raft storage
// 6: Apply and WAL Entries to raft storage
// 7: If old wal then restart Node. Otherwise just start
// TODO: Add old peers to Transport
func (rn *Node) restoreRaft() error {
	oldWAL := wal.Exist(rn.walDir())

	raftSnap, err := rn.initSnap()
	if err != nil {
		return err
	}

	// NOTE: Step 1
	if err := rn.restoreFSMFromSnapshot(raftSnap); err != nil {
		return err
	}

	var walSnap walpb.Snapshot

	walSnap.Index, walSnap.Term = raftSnap.Metadata.Index, raftSnap.Metadata.Term

	if err := rn.initWAL(walSnap); err != nil {
		return err
	}

	if rn.wal != nil {
		wMetadata, hState, ents, err := rn.wal.ReadAll()
		if err != nil {
			return err
		}

		// NOTE: Step 2
		if err := rn.restoreFSMFromWAL(ents); err != nil {
			return err
		}

		// NOTE: Step 3
		if err := rn.restoreMetadata(wMetadata); err != nil {
			return err
		}

		// NOTE: Steps 4, 5, 6
		if err := rn.restoreMemoryStorage(raftSnap, hState, ents); err != nil {
			return err
		}
	}

	// NOTE: Step 7
	if oldWAL {
		rn.node = raft.RestartNode(rn.raftConfig)
	} else {
		if rn.bootstrapNode {
			rn.node = raft.StartNode(rn.raftConfig, []raft.Peer{raft.Peer{ID: rn.id}})
		} else {
			rn.node = raft.StartNode(rn.raftConfig, nil)
		}
	}
	return nil
}

func (rn *Node) initSnap() (raftpb.Snapshot, error) {
	if rn.snapDir() == "" {
		return raftpb.Snapshot{}, nil
	}

	if err := os.MkdirAll(rn.snapDir(), 0750); err != nil && !os.IsExist(err) {
		return raftpb.Snapshot{}, err
	}

	rn.ss = snap.New(rn.snapDir())

	raftSnap, err := rn.ss.Load()
	if err != nil {
		if err == snap.ErrNoSnapshot || err == snap.ErrEmptySnapshot {
			return raftpb.Snapshot{}, nil
		} else {
			return raftpb.Snapshot{}, err
		}
	}

	return *raftSnap, nil
}

func (rn *Node) persistSnapshot(raftSnap raftpb.Snapshot) error {

	if rn.ss != nil {
		if err := rn.ss.SaveSnap(raftSnap); err != nil {
			return err
		}
	}

	if rn.wal != nil {
		var walSnap walpb.Snapshot
		walSnap.Index, walSnap.Term = raftSnap.Metadata.Index, raftSnap.Metadata.Term

		if err := rn.wal.SaveSnapshot(walSnap); err != nil {
			return err
		}
	}
	return nil
}

func (rn *Node) initWAL(walSnap walpb.Snapshot) error {
	if rn.walDir() == "" {
		return nil
	}

	if err := rn.openWAL(walSnap); err != nil {
		return err
	}

	return nil
}

func (rn *Node) openWAL(walSnap walpb.Snapshot) error {
	if !wal.Exist(rn.walDir()) {
		if err := os.MkdirAll(rn.walDir(), 0750); err != nil && !os.IsExist(err) {
			return err
		}

		metaStruct := &walMetadata{
			NodeID:    rn.id,
			ClusterID: rn.cid,
		}

		metaData, err := json.Marshal(metaStruct)
		if err != nil {
			return err
		}

		w, err := wal.Create(rn.walDir(), metaData)
		if err != nil {
			return err
		}
		if err = w.Close(); err != nil {
			return err
		}
	}

	fmt.Println("WAL Repair.  ", wal.Repair(rn.walDir()))

	w, err := wal.Open(rn.walDir(), walSnap)
	if err != nil {
		return err
	}
	rn.wal = w
	return nil
}

func (rn *Node) restoreMetadata(wMetadata []byte) error {
	var metaData walMetadata
	if err := json.Unmarshal(wMetadata, &metaData); err != nil {
		return err
	}

	rn.id, rn.cid = metaData.NodeID, metaData.ClusterID
	rn.raftConfig.ID = metaData.NodeID
	return nil
}

// restores FSM AND it sets the NodeID and ClusterID if present in Metadata
func (rn *Node) restoreFSMFromWAL(ents []raftpb.Entry) error {
	if rn.wal == nil {
		return nil
	}

	if ok := rn.publishEntries(ents); !ok {
		return errors.New("Could not restore entries from WAL")
	}

	return nil
}

func (rn *Node) restoreMemoryStorage(raftSnap raftpb.Snapshot, hState raftpb.HardState, ents []raftpb.Entry) error {
	if !raft.IsEmptySnap(raftSnap) {
		if err := rn.raftStorage.ApplySnapshot(raftSnap); err != nil {
			return err
		}
	}

	if rn.wal != nil {
		if err := rn.raftStorage.SetHardState(hState); err != nil {
			return err
		}

		if err := rn.raftStorage.Append(ents); err != nil {
			return err
		}
	}

	return nil
}

func (rn *Node) deletePersistentData() error {
	if rn.snapDir() != "" {
		if err := os.RemoveAll(rn.snapDir()); err != nil {
			return err
		}
	}
	if rn.walDir() != "" {
		if err := os.RemoveAll(rn.snapDir()); err != nil {
			return err
		}
	}
	return nil
}

func (rn *Node) walDir() string {
	if rn.dataDir == "" {
		return ""
	}
	return fmt.Sprintf("%s%s", rn.dataDir, walDirExtension)
}

func (rn *Node) snapDir() string {
	if rn.dataDir == "" {
		return ""
	}
	return fmt.Sprintf("%s%s", rn.dataDir, snapDirExtension)
}
