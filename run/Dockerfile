FROM golang:latest
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go get github.com/jseam2/boopy
RUN go build boop_node.go
EXPOSE 8001 81
CMD ["/app/boop_node", "$ID", "0.0.0.0:8001", "0.0.0.0:81"]
