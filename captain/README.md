# Captain

Captain is an admin tool for canoe clusters.

## Usage

### Options

* `--cluster-members` - comma separated list of members in a cluster to send requests. These are sent round robin until a non-error response is given or the end of the list is reached
* `--id` - ID is the member ID of the node you want to add/delete from a cluster
* `--api-port` - The API port of a node you want to add to a cluster
* `--raft-port` - The Raft port of a node you want to add to a cluster
* `--node-host` - The host address of where the node you want to add is bound to. Valid examples would be `localhost` or `10.0.0.1`

### list-members
`./captain list-members --cluster-members="<member/s>"`

### add-node

Note: add-node currently does not work with Canoe. The API was developed to automate cluster node additions.
Having additions manually manipulated isn't working in the Canoe Configuration API currently

`./captain add-node --cluster-members="<member/s>" --id=<node_id> --api-port=<port_num> --raft-port=<port_num> --node-host="<remote_host_addr>"`


### delete-node
`./captain delete-node --cluster-members="<member\s>" --id=<node_id>`
