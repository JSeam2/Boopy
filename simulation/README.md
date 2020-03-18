# HOW TO RUN?
1. Build the images by running
```
./build.sh
```

1. Compile the nodes
```
go build node1.go
go build node2.go
go build node3.go
```

1. Run `node1` docker image as follows
```
docker run -p 8001:8001 -it node1
```

1. Run the program
```
./node2
./node3
```