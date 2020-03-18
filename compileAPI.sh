protoc -I api/ \
    -I${GOPATH}/bin \
    --go_out=plugins=grpc:api \
    api/api.proto