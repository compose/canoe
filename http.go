package raftwrapper

import (
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"
	"net/http"
)

var peerAddEndpoint = "/peerAddition"
var FSMAPIEndpoint = "/api"

func (rn *RaftNode) peerAPI() *http.Handler {
	r := mux.NewRouter()

	rn.fsm.RegisterAPI(r.PathPrefix(FSMAPIEndpoint).Subrouter())
	r.HandleFunc(peerAddEndpoint, rn.peerAddHandlerFunc())

	return r
}

// wrapper to allow rn state to persist through handler func
func (rn *RaftNode) peerAddHandlerFunc() {
	return func(w http.ResponseWriter, req *http.Request) {
		rn.handlePeerAddRequest(w, req)
	}
}

// if bootstrap node or in a cluster then accept these attempts,
// and wait for the message to be committed(err or retry after timeout?)
//
// Otherwise respond with an error that this node isn't in a state to add
// members
func (rn *RaftNode) handlePeerAddRequest(w http.ResponseWriter, req *http.Request) {
	if rn.canAddPeer() {
		var addReq peerAdditionRequest

		if err := json.NewDecoder(bytes.NewReader(req.Body)).Decode(&addReq); err != nil {
			writeError(w, http.StatusBadRequest, err)
		}

		url := fmt.Sprintf("%s:%s", req.RemoteAddr.Split(":")[0], addReq.Port)

		confChange := &raftpb.ConfChange{
			NodeID:  addReq.ID,
			Context: url,
		}

		if err := rn.proposePeerAddition(confChange, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}
		writeSuccess(w)
	} else {
		writeInvalidNode(w)
	}
}

// Host address should be able to be scraped from the Request on the server-end
type peerAdditionRequest struct {
	ID   string `json:"id"`
	Port string `json:"port"`
}

// TODO: Implement
func writeSuccess(w http.ResponseWriter) {
}

// TODO: Implement
func writeError(w http.ResponseWriter, code int, err error) {
}

// TODO: Implement
func writeInvalidNode(w http.ResponseWriter) {
}
