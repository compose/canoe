# Lifeboat

Lifeboat is a dummy canoe node. It doesn't have a working FSM. It simply is another node in the cluster for the purpose of boosting quorum. 
An example of where this could be useful: an application where the FSM is rather resource intensive and you want to run the FSM on only 2 nodes but have uptime in case of a single node failure. 
You can start a lifeboat as part of the cluster, and lifeboat will simply act as a member of the cluster - acting as another backup point for WAL logs and Snapshots


## Arguments
* --id - ID for this canoe node. default: generate a UUID
* --cluster-id - Cluster id this canoe node is joining. default: 0x100
* --api-port - Port for the canoe configuration API. default: 1244
* --raft-port - Port for raft message transport. default: 1234
* --data-dir - Directory for storing data. default: "./lifeboat_data"
* --peers - List of peers to attempt to connect to. Either `peers` or `bootstrap` are required. default: empty
* --bootstrap - Flag for if this node is a bootstrap node

## Example

Try this out with the `kvstore` example

In different terminals:
```
./kvstore --bootstrap  --raft-port=1234 --api-port=1244 --data-dir="./data1"
./kvstore --peers="http://localhost:1244" --raft-port=1235 --api-port=1245 --data-dir="./data2"
./lifeboat --peers="http://localhost:1244,http://localhost:1245" --raft-port=1236 --api-port=1236
```
