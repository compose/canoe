package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/compose/canoe/types"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

var (
	fSet           = flag.NewFlagSet("", flag.ExitOnError)
	clusterMembers = fSet.String("cluster-members", "", "Members of the cluster. Request made round robin until a good response returned")
	nodeID         = fSet.Uint64("id", 0x0, "ID of node to add/delete")
	nodeAPIPort    = fSet.Int("api-port", 1244, "API Port of node to add")
	nodeRaftPort   = fSet.Int("raft-port", 1234, "Raft port of node to add")
)

func main() {
	fSet.Parse(os.Args[2:])
	cmd := os.Args[1]

	switch cmd {
	case "delete-node":
		if *clusterMembers == "" || *nodeID == 0 {
			log.Fatal("Must specify --cluster-members and --id")
		}
		if errs := deleteNode(strings.Split(*clusterMembers, ","), *nodeID); len(errs) > 0 {
			log.Fatalf("Error deleting node: %+v", errs)
		}
	case "add-node":
		if *clusterMembers == "" || *nodeID == 0 || *nodeAPIPort == 0 || *nodeRaftPort == 0 {
			log.Fatal("Must specify --cluster-members, --id, --api-port, and --raft-port")
		}
		if errs := addNode(strings.Split(*clusterMembers, ","), *nodeID, *nodeAPIPort, *nodeRaftPort); len(errs) > 0 {
			log.Fatalf("Error adding node: %+v", errs)
		}
	case "list-members":
		if *clusterMembers == "" {
			log.Fatal("Must specify --cluster-members")
		}
		if errs := listMembers(strings.Split(*clusterMembers, ",")); len(errs) > 0 {
			log.Fatalf("Error getting members listing: %+v", errs)
		}
	}
}

func addNode(members []string, id uint64, raftPort, apiPort int) []error {
	requestData := struct {
		ID       uint64 `json:"id"`
		RaftPort int    `json:"raft_port"`
		APIPort  int    `json:"api_port"`
	}{
		ID:       id,
		RaftPort: raftPort,
		APIPort:  apiPort,
	}

	resp, errs := sendRequest(requestData, members, "POST")
	if len(errs) > 0 {
		return errs
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []error{err}
	}

	log.Println(string(respBytes))

	return []error{}
}

func listMembers(members []string) []error {
	resp, errs := sendRequest(nil, members, "GET")
	if len(errs) > 0 {
		return errs
	}
	var configResp types.ConfigServiceResponse
	if err := json.NewDecoder(resp.Body).Decode(&configResp); err != nil {
		return []error{err}
	}

	log.Println(string(configResp.Data))

	return []error{}
}

func deleteNode(members []string, id uint64) []error {
	requestData := struct {
		ID uint64 `json:"id"`
	}{ID: id}

	_, errs := sendRequest(requestData, members, "DELETE")
	if len(errs) > 0 {
		return errs
	}
	log.Println("Successfully deleted node: %x", id)

	return []error{}
}

func sendRequest(reqStruct interface{}, members []string, reqType string) (*http.Response, []error) {
	var errors []error
	for _, member := range members {
		peerURL, err := url.Parse(member)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		peerURL.Path = "peers"
		var req *http.Request

		if reqStruct != nil {
			data, err := json.Marshal(reqStruct)
			reader := bytes.NewReader(data)

			req, err = http.NewRequest(reqType, peerURL.String(), reader)
			if err != nil {
				errors = append(errors, err)
				continue
			}
		} else {
			var err error
			req, err = http.NewRequest(reqType, peerURL.String(), nil)
			if err != nil {
				errors = append(errors, err)
				continue
			}

		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			errors = append(errors, err)
			continue
		} else {
			return resp, []error{}
		}
	}
	return nil, errors
}
