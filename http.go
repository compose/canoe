package raftwrapper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var peerEndpoint = "/peers"
var FSMAPIEndpoint = "/api"

func (rn *Node) peerAPI() *mux.Router {
	r := mux.NewRouter()

	rn.fsm.RegisterAPI(r.PathPrefix(FSMAPIEndpoint).Subrouter())
	r.HandleFunc(peerEndpoint, rn.peerAddHandlerFunc()).Methods("POST")
	r.HandleFunc(peerEndpoint, rn.peerDeleteHandlerFunc()).Methods("DELETE")
	r.HandleFunc(peerEndpoint, rn.peerMembersHandlerFunc()).Methods("GET")

	return r
}

func (rn *Node) serveHTTP() error {
	router := rn.peerAPI()

	ln, err := newStoppableListener(fmt.Sprintf(":%d", rn.apiPort), rn.stopc)
	if err != nil {
		panic(err)
	}

	err = (&http.Server{Handler: router}).Serve(ln)
	select {
	case <-rn.stopc:
		return nil
	default:
		return err
	}
}

func (rn *Node) serveRaft() error {
	ln, err := newStoppableListener(fmt.Sprintf(":%d", rn.raftPort), rn.stopc)
	if err != nil {
		return err
	}

	err = (&http.Server{Handler: rn.transport.Handler()}).Serve(ln)

	select {
	case <-rn.stopc:
		return nil
	default:
		return err
	}
}

func (rn *Node) peerMembersHandlerFunc() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		rn.handlePeerMembersRequest(w, req)
	}
}

func (rn *Node) handlePeerMembersRequest(w http.ResponseWriter, req *http.Request) {
	if !rn.initialized {
		writeNodeNotReady(w)
	} else {
		membersResp := &PeerMembershipResponseData{
			HTTPPeerData{
				RaftPort:    rn.raftPort,
				APIPort:     rn.apiPort,
				ID:          rn.id,
				RemotePeers: rn.peerMap,
			},
		}

		writeSuccess(w, membersResp)
	}
}

func (rn *Node) peerDeleteHandlerFunc() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		rn.handlePeerDeleteRequest(w, req)
	}
}

func (rn *Node) handlePeerDeleteRequest(w http.ResponseWriter, req *http.Request) {
	if rn.canAlterPeer() {
		var delReq peerDeletionRequest

		if err := json.NewDecoder(req.Body).Decode(&delReq); err != nil {
			writeError(w, http.StatusBadRequest, err)
		}

		confChange := &raftpb.ConfChange{
			NodeID: delReq.ID,
		}

		if err := rn.proposePeerDeletion(confChange, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}

		writeSuccess(w, nil)
	} else {
		writeNodeNotReady(w)
	}
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
	if rn.canAlterPeer() {
		var addReq peerAdditionRequest

		if err := json.NewDecoder(req.Body).Decode(&addReq); err != nil {
			writeError(w, http.StatusBadRequest, err)
		}

		confContext := confChangeNodeContext{
			IP:       strings.Split(req.RemoteAddr, ":")[0],
			RaftPort: addReq.RaftPort,
			APIPort:  addReq.APIPort,
		}

		confContextData, err := json.Marshal(confContext)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}

		confChange := &raftpb.ConfChange{
			NodeID:  addReq.ID,
			Context: confContextData,
		}

		if err := rn.proposePeerAddition(confChange, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
		}

		addResp := &PeerAdditionResponseData{
			HTTPPeerData{
				RaftPort:    rn.raftPort,
				APIPort:     rn.apiPort,
				ID:          rn.id,
				RemotePeers: rn.peerMap,
			},
		}

		writeSuccess(w, addResp)
	} else {
		writeNodeNotReady(w)
	}
}

// TODO: Figure out how to handle these errs rather than just continue...
// thought of having a slice of accumulated errors?
// Or log.Warn on all failed attempts and if unsuccessful return a general failure
// error
// TODO: Pull out the node addition logic so we aren't repeating with add self
func (rn *Node) requestRejoinCluster() error {
	var resp *http.Response
	var respData PeerServiceResponse

	if len(rn.bootstrapPeers) == 0 {
		return nil
	}

	for _, peer := range rn.bootstrapPeers {
		peerAPIURL := fmt.Sprintf("%s%s", peer, peerEndpoint)

		resp, err := http.Get(peerAPIURL)
		if err != nil {
			continue
			return err
		}

		defer resp.Body.Close()

		if err = json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			continue
			return err
		}

		if respData.Status == PeerServiceStatusError {
			continue
		} else if respData.Status == PeerServiceStatusSuccess {

			var peerData PeerMembershipResponseData
			if err := json.Unmarshal(respData.Data, &peerData); err != nil {
				return err
			}

			return rn.addPeersFromRemote(peer, &peerData.HTTPPeerData)
		}
	}
	if respData.Status == PeerServiceStatusError {
		return fmt.Errorf("Error %d - %s", resp.StatusCode, respData.Message)
	}
	// TODO: Should return the general error from here
	return errors.New("Couldn't connect to thingy")
}

func (rn *Node) addPeersFromRemote(remotePeer string, remoteMemberResponse *HTTPPeerData) error {
	peerURL, err := url.Parse(remotePeer)
	if err != nil {
		return err
	}
	addURL := fmt.Sprintf("http://%s:%s",
		strings.Split(peerURL.Host, ":")[0],
		strconv.Itoa(remoteMemberResponse.RaftPort))

	rn.transport.AddPeer(types.ID(remoteMemberResponse.ID), []string{addURL})
	fmt.Printf("Peers add http: %x\n", remoteMemberResponse.ID)
	rn.peerMap[remoteMemberResponse.ID] = confChangeNodeContext{
		IP:       strings.Split(peerURL.Host, ":")[0],
		RaftPort: remoteMemberResponse.RaftPort,
		APIPort:  remoteMemberResponse.APIPort,
	}
	fmt.Printf("Cur Peer Map: %v", rn.peerMap)

	for id, context := range remoteMemberResponse.RemotePeers {
		if id != rn.id {
			addURL := fmt.Sprintf("http://%s:%s", context.IP, strconv.Itoa(context.RaftPort))
			rn.transport.AddPeer(types.ID(id), []string{addURL})
			fmt.Printf("Peers add http: %x\n", id)
		}
		rn.peerMap[id] = context
		fmt.Printf("Cur Peer Map: %v", rn.peerMap)
	}
	return nil
}

func (rn *Node) requestSelfAddition() error {
	var resp *http.Response
	var respData PeerServiceResponse

	reqData := peerAdditionRequest{
		ID:       rn.id,
		RaftPort: rn.raftPort,
		APIPort:  rn.apiPort,
	}

	for _, peer := range rn.bootstrapPeers {
		mar, err := json.Marshal(reqData)
		if err != nil {
			continue
			return err
		}

		reader := bytes.NewReader(mar)
		peerAPIURL := fmt.Sprintf("%s%s", peer, peerEndpoint)

		resp, err = http.Post(peerAPIURL, "application/json", reader)
		if err != nil {
			continue
			return err
		}

		defer resp.Body.Close()

		if err = json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			continue
			return err
		}

		if respData.Status == PeerServiceStatusError {
			continue
		} else if respData.Status == PeerServiceStatusSuccess {

			// this ought to work since it should be added to cluster now
			var peerData PeerAdditionResponseData
			if err := json.Unmarshal(respData.Data, &peerData); err != nil {
				return err
			}

			return rn.addPeersFromRemote(peer, &peerData.HTTPPeerData)
		}
	}
	if respData.Status == PeerServiceStatusError {
		return fmt.Errorf("Error %d - %s", resp.StatusCode, respData.Message)
	}
	return errors.New("No available nodey thingy")
}

func (rn *Node) requestSelfDeletion() error {
	var resp *http.Response
	var respData PeerServiceResponse
	reqData := peerDeletionRequest{
		ID: rn.id,
	}
	for id, peerData := range rn.peerMap {
		if id == rn.id {
			continue
		}
		mar, err := json.Marshal(reqData)
		if err != nil {
			return err
		}

		reader := bytes.NewReader(mar)
		peerAPIURL := fmt.Sprintf("http://%s:%d%s", peerData.IP, peerData.APIPort, peerEndpoint)

		req, err := http.NewRequest("DELETE", peerAPIURL, reader)
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err = (&http.Client{}).Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		if err = json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			return err
		}

		if respData.Status == PeerServiceStatusSuccess {
			return nil
		}

	}
	if respData.Status == PeerServiceStatusError {
		return fmt.Errorf("Error %d - %s", resp.StatusCode, respData.Message)
	}
	return nil
}

var PeerServiceStatusSuccess = "success"
var PeerServiceStatusError = "error"

// PeerAdditionAddMe has self-identifying port and id
// With a list of all Peers in the cluster currently
type PeerAdditionResponseData struct {
	HTTPPeerData
}

type PeerMembershipResponseData struct {
	HTTPPeerData
}

// This needs to be a different struct because it is important to seperate
// The API/Raft/ID of the node we're pinging from other remote nodes
type HTTPPeerData struct {
	RaftPort    int                              `json:"raft_port"`
	APIPort     int                              `json:"api_port"`
	ID          uint64                           `json:"id"`
	RemotePeers map[uint64]confChangeNodeContext `json:"peers"`
}

func (p *HTTPPeerData) MarshalJSON() ([]byte, error) {
	tmpStruct := &struct {
		RaftPort    int                              `json:"raft_port"`
		APIPort     int                              `json:"api_port"`
		ID          uint64                           `json:"id"`
		RemotePeers map[string]confChangeNodeContext `json:"peers"`
	}{
		RaftPort:    p.RaftPort,
		APIPort:     p.APIPort,
		ID:          p.ID,
		RemotePeers: make(map[string]confChangeNodeContext),
	}

	for key, val := range p.RemotePeers {
		tmpStruct.RemotePeers[strconv.FormatUint(key, 10)] = val
	}

	return json.Marshal(tmpStruct)
}

func (p *HTTPPeerData) UnmarshalJSON(data []byte) error {
	tmpStruct := &struct {
		RaftPort    int                              `json:"raft_port"`
		APIPort     int                              `json:"api_port"`
		ID          uint64                           `json:"id"`
		RemotePeers map[string]confChangeNodeContext `json:"peers"`
	}{}

	if err := json.Unmarshal(data, tmpStruct); err != nil {
		return err
	}

	p.APIPort = tmpStruct.APIPort
	p.RaftPort = tmpStruct.RaftPort
	p.ID = tmpStruct.ID
	p.RemotePeers = make(map[uint64]confChangeNodeContext)

	for key, val := range tmpStruct.RemotePeers {
		convKey, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return err
		}
		p.RemotePeers[convKey] = val
	}

	return nil
}

type PeerServiceResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Data    []byte `json:"data,omitempty"`
}

var PeerServiceNodeNotReady = "Invalid Node"

// Host address should be able to be scraped from the Request on the server-end
type peerAdditionRequest struct {
	ID       uint64 `json:"id"`
	RaftPort int    `json:"raft_port"`
	APIPort  int    `json:"api_port"`
}

type peerDeletionRequest struct {
	ID uint64 `json:"id"`
}

func writeSuccess(w http.ResponseWriter, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	var respData []byte
	var err error
	if body != nil {
		respData, err = json.Marshal(body)
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	if err = json.NewEncoder(w).Encode(PeerServiceResponse{Status: PeerServiceStatusSuccess, Data: respData}); err != nil {
		fmt.Println(err.Error())
	}
}
func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(PeerServiceResponse{Status: PeerServiceStatusError, Message: err.Error()})
}

func writeNodeNotReady(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(PeerServiceResponse{Status: PeerServiceStatusError, Message: PeerServiceNodeNotReady})
}
