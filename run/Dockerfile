FROM golang:latest
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go get github.com/jseam2/boopy
RUN go build boop_node.go
EXPOSE 8000
EXPOSE 80
CMD ["./boop_node 0 0.0.0.0:8000 0.0.0.0:80"]
