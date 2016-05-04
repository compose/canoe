package raftwrapper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var peerAddEndpoint = "/peerAddition"
var FSMAPIEndpoint = "/api"

func (rn *Node) peerAPI() *mux.Router {
	r := mux.NewRouter()

	rn.fsm.RegisterAPI(r.PathPrefix(FSMAPIEndpoint).Subrouter())
	r.HandleFunc(peerAddEndpoint, rn.peerAddHandlerFunc()).Methods("POST")

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

		url := fmt.Sprintf("http://%s:%d", strings.Split(req.RemoteAddr, ":")[0], addReq.Port)

		confChange := &raftpb.ConfChange{
			NodeID:  addReq.ID,
			Context: []byte(url),
		}

		if err := rn.proposePeerAddition(confChange, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}

		writeSuccess(w, rn)
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
		peerAPIURL := fmt.Sprintf("%s%s", peer, peerAddEndpoint)

		resp, err := http.Post(peerAPIURL, "application/json", reader)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		var respData PeerAdditionResponse
		if err = json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			return err
		}

		if respData.Status == PeerAdditionStatusError {
			return fmt.Errorf("Error %d - %s", resp.StatusCode, respData.Message)
		}

		// this ought to work since it should be added to cluster now
		var portID PeerAdditionAddMe
		if err := json.Unmarshal(respData.Data, &portID); err != nil {
			return err
		}

		peerURL, err := url.Parse(peer)
		if err != nil {
			return err
		}

		addURL := fmt.Sprintf("http://%s:%s", strings.Split(peerURL.Host, ":")[0], strconv.Itoa(portID.Port))

		rn.transport.AddPeer(types.ID(portID.ID), []string{addURL})

		//TODO: Determine how errors are returned over http
		//var respData PeerAdditionResponse

	}
	return nil
}

var PeerAdditionStatusSuccess = "success"
var PeerAdditionStatusError = "error"

type PeerAdditionAddMe struct {
	Port int    `json:"port"`
	ID   uint64 `json:"id"`
}

type PeerAdditionResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Data    []byte `json:"data,omitempty"`
}

var PeerAdditionNodeNotReady = "Invalid Node"

// Host address should be able to be scraped from the Request on the server-end
type peerAdditionRequest struct {
	ID   uint64 `json:"id"`
	Port int    `json:"port"`
}

func writeSuccess(w http.ResponseWriter, rn *Node) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	addMe := &PeerAdditionAddMe{
		Port: rn.raftPort,
		ID:   rn.id,
	}

	addMeData, err := json.Marshal(addMe)
	if err != nil {
		panic(err)
	}

	json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusSuccess, Data: addMeData})
	//json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusSuccess})
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
