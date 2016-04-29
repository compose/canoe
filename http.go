package raftwrapper

import (
	"github.com/gorilla/mux"
	"net/http"
)

var peerAddEndpoint = "/peerAddition"

func (rn *RaftNode) peerAPI() *http.Handler {
	r := mux.NewRouter()
	rn.fsm.RegisterAPI(r.PathPrefix("/api").Subrouter())

	r.HandleFunc(peerAddEndpoint, rn.peerAddHandlerFunc())
}

func (rn *RaftNode) peerAddHandlerFunc() {

}

// Host address should be able to be scraped from the Request on the server-end
type nodeAdditionRequest struct {
	ID   string `json:"id"`
	Port string `json:"port"`
}

func (rn *raftNode) joinCluster() error {

	addReq := nodeAdditionRequest{
		ID:   rn.id,
		Host: strings.Split(rn.address.Host, ":")[0],
	}

	for _, peer := range rn.peers {
		url := fmt.Sprintf("%s/%s", peer, raftAddEndpoint)
		req, err := http.NewRequest("POST", url, nil)
	}
}
