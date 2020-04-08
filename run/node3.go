package main

import (
	"log"
	"math/big"
	"os"
	"os/signal"

	"github.com/jseam2/boopy"
	"github.com/jseam2/boopy/api"

	"time"
)

func createNode(id string, addr string, sister *api.Node) (*boopy.Node, error) {

	cnf := boopy.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := boopy.NewNode(cnf, sister)
	return n, err
}

func createID(id string) []byte {
	val := big.NewInt(0)
	val.SetString(id, 10)
	return val.Bytes()
}

func main() {

	joinNode := boopy.NewInode("1", ":8001")

	h, err := createNode("8", ":8003", joinNode)
	if err != nil {
		log.Fatalln(err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	h.Stop()
}
