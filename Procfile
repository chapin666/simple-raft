# Use goreman to run `go get github.com/mattn/goreman`
test1: ./simple-raft --id 1 --cluster 127.0.0.1:22379,127.0.0.1:32379 --port :12379
test2: ./simple-raft --id 2 --cluster 127.0.0.1:12379,127.0.0.1:32379 --port :22379
test3: ./simple-raft --id 3 --cluster 127.0.0.1:12379,127.0.0.1:22379 --port :32379