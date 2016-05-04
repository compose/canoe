package raftwrapper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"
	"net/http"
	"strings"
)

var peerAddEndpoint = "/peerAddition"
var FSMAPIEndpoint = "/api"

func (rn *Node) peerAPI() *mux.Router {
	r := mux.NewRouter()

	rn.fsm.RegisterAPI(r.PathPrefix(FSMAPIEndpoint).Subrouter())
	r.HandleFunc(peerAddEndpoint, rn.peerAddHandlerFunc())

	return r
}

func (rn *Node) serveHTTP() {
	router := rn.peerAPI()
	http.ListenAndServe(fmt.Sprintf(":%d", rn.apiPort), router)
}

func (rn *Node) serveRaft() {
	http.ListenAndServe(fmt.Sprintf(":%d", rn.raftPort), rn.transport.Handler())
}

// wrapper to allow rn state to persist through handler func
func (rn *Node) peerAddHandlerFunc() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		rn.handlePeerAddRequest(w, req)
	}
}

// if bootstrap node or in a cluster then accept these attempts,
// and wait for the message to be committed(err or retry after timeout?)
//
// Otherwise respond with an error that this node isn't in a state to add
// members
func (rn *Node) handlePeerAddRequest(w http.ResponseWriter, req *http.Request) {
	if rn.canAddPeer() {
		var addReq peerAdditionRequest

		if err := json.NewDecoder(req.Body).Decode(&addReq); err != nil {
			writeError(w, http.StatusBadRequest, err)
		}

		url := fmt.Sprintf("%s:%s", strings.Split(req.RemoteAddr, ":")[0], addReq.Port)

		confChange := &raftpb.ConfChange{
			NodeID:  addReq.ID,
			Context: []byte(url),
		}

		if err := rn.proposePeerAddition(confChange, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}
		writeSuccess(w)
	} else {
		writeNodeNotReady(w)
	}
}

func (rn *Node) requestSelfAddition() error {
	reqData := peerAdditionRequest{
		ID:   rn.id,
		Port: rn.raftPort,
	}
	for _, peer := range rn.peers {
		mar, err := json.Marshal(reqData)
		if err != nil {
			return err
		}

		reader := bytes.NewReader(mar)

		_, err = http.Post(peer, "application/json", reader)
		if err != nil {
			return err
		}

		//TODO: Determine how errors are returned over http
		//var respData PeerAdditionResponse

	}
	return nil
}

var PeerAdditionStatusSuccess = "success"
var PeerAdditionStatusError = "error"

type PeerAdditionResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

var PeerAdditionNodeNotReady = "Invalid Node"

// Host address should be able to be scraped from the Request on the server-end
type peerAdditionRequest struct {
	ID   uint64 `json:"id"`
	Port int    `json:"port"`
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusSuccess})
}
func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusError, Message: err.Error()})
}

func writeNodeNotReady(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusError, Message: PeerAdditionNodeNotReady})
}
