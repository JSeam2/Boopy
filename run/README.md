# HOW TO RUN?
## Docker VM
1. Create docker machines via 
```
docker-machine create <name of machine>
docker-machine env <name of machine>
eval $(docker-machine env <name of machine>)
```
1. Build the images by running
```
./build.sh <ID of Node>
```

1. Repeat to obtain as many virtual machines as possible. This is limited by your machine.

1. You will need to connect to the ip of the docker machine instead of the usual `0.0.0.0`
```
docker-machine ip <name of machine>
```

1. Docker might introduce additional complications in terms of the networking. The container should work fine otherwise.

## Local
1. Compile the nodes
```
go run boop_node <ID> <Address of Chord> <Front End Address>

// Example
go run boop_node 1 0.0.0.0:8001 0.0.0.0:81
go build node2.go
go build node3.go
```

# REST API
The REST endpoints are found in `boop_node.go`

# Integration Tests
Run the integration tests with `./test.sh`. Ensure you have the appropriate python libraries like requests installed.