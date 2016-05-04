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
		var peerData PeerAdditionResponseData
		if err := json.Unmarshal(respData.Data, &peerData); err != nil {
			return err
		}

		peerURL, err := url.Parse(peer)
		if err != nil {
			return err
		}

		addURL := fmt.Sprintf("http://%s:%s", strings.Split(peerURL.Host, ":")[0], strconv.Itoa(peerData.Port))

		rn.transport.AddPeer(types.ID(peerData.ID), []string{addURL})
		rn.peerMap[peerData.ID] = addURL

		for id, addURL := range peerData.RemotePeers {
			if id != rn.id {
				rn.transport.AddPeer(types.ID(id), []string{addURL})
			}
		}

		//TODO: Determine how errors are returned over http
		//var respData PeerAdditionResponse

	}
	return nil
}

var PeerAdditionStatusSuccess = "success"
var PeerAdditionStatusError = "error"

// PeerAdditionAddMe has self-identifying port and id
// With a list of all Peers in the cluster currently
//TODO: Get rid of []byte field below. Unnescessary
type PeerAdditionResponseData struct {
	Port        int               `json:"port"`
	ID          uint64            `json:"id"`
	RemotePeers map[uint64]string `json:"peers"`
}

func (p *PeerAdditionResponseData) MarshalJSON() ([]byte, error) {
	tmpStruct := &struct {
		Port        int               `json:"port"`
		ID          uint64            `json:"id"`
		RemotePeers map[string]string `json:"peers"`
	}{
		Port:        p.Port,
		ID:          p.ID,
		RemotePeers: make(map[string]string),
	}

	for key, val := range p.RemotePeers {
		tmpStruct.RemotePeers[strconv.FormatUint(key, 10)] = val
	}

	return json.Marshal(tmpStruct)
}

func (p *PeerAdditionResponseData) UnmarshalJSON(data []byte) error {
	tmpStruct := &struct {
		Port        int               `json:"port"`
		ID          uint64            `json:"id"`
		RemotePeers map[string]string `json:"peers"`
	}{}

	if err := json.Unmarshal(data, tmpStruct); err != nil {
		return err
	}

	p.Port = tmpStruct.Port
	p.ID = tmpStruct.ID
	p.RemotePeers = make(map[uint64]string)

	for key, val := range tmpStruct.RemotePeers {
		convKey, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return err
		}
		p.RemotePeers[convKey] = val
	}

	return nil
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

// TODO: Add all peers in the cluster with this response lest we break things
func writeSuccess(w http.ResponseWriter, rn *Node) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	addMe := &PeerAdditionResponseData{
		Port:        rn.raftPort,
		ID:          rn.id,
		RemotePeers: rn.peerMap,
	}

	respData, err := json.Marshal(addMe)
	if err != nil {
		panic(err)
	}

	err = json.NewEncoder(w).Encode(PeerAdditionResponse{Status: PeerAdditionStatusSuccess, Data: respData})
	if err != nil {
		fmt.Println("ERROROROR", err.Error())
	}
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
