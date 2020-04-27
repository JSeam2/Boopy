package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jseam2/boopy"
	"github.com/jseam2/boopy/api"
)

// Response describes the response from the REST server to requests
type Response struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

type SetResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type GetResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Key     string `json:"key"`
	Value   string `json:"value"`
}

type FindResponse struct {
	Message string `json:"message"`
	Error   string `json:"error"`
	Id      string `json:"id"`
	Addr    string `json:"address"`
}

// KeyValue describes the values for inserting a key-value pair into the network
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Key struct {
	Key string `json:"key"`
}

type JoinConfig struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
}

func createNode(id string, addr string, sister *api.Node) (*boopy.Node, error) {
	// Wrapper function calling the newNode function from the core API

	// Set gRPC settings for node location, timeouts, etc.
	cnf := boopy.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	// Passthrough to the boopy library for newNode
	n, err := boopy.NewNode(cnf, sister)
	return n, err
}

func main() {
	id := os.Args[1]
	chordAddr := os.Args[2]
	frontEndAddr := os.Args[3]

	node, err := createNode(id, chordAddr, nil)
	if err != nil {
		log.Fatalln(err)
		return
	}

	shut := make(chan bool)

	// REST Server

	// Basic ping function
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		res := Response{
			Message: "Pong!",
			Error:   "",
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Setter Interface: input key-value pair into network
	// submit {key: "", value: ""} -> Response
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			panic(err)
		}

		nodeErr := node.Set(kv.Key, kv.Value)
		if nodeErr != nil {
			panic(nodeErr)
			res := SetResponse{
				Message: "Set Failed",
				Error:   fmt.Sprintf("%v", err),
				Key:     kv.Key,
				Value:   kv.Value,
			}
			if err := json.NewEncoder(w).Encode(res); err != nil {
				panic(err)
			}
			return
		}

		res := SetResponse{
			Message: "Set Success",
			Error:   "",
			Key:     kv.Key,
			Value:   kv.Value,
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Search interface: given {key:""} -> Response{ID of node : "", Address:""}
	http.HandleFunc("/find", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var k Key
		err := decoder.Decode(&k)
		if err != nil {
			panic(err)
		}

		tempNode, nodeErr := node.Find(k.Key)
		if nodeErr != nil {
			panic(nodeErr)
			res := FindResponse{
				Message: "Find Failed",
				Error:   fmt.Sprintf("%v", err),
				Id:      "",
				Addr:    "",
			}
			if err := json.NewEncoder(w).Encode(res); err != nil {
				panic(err)
			}
			return
		}

		aInt := (&big.Int{}).SetBytes(tempNode.Id)

		res := FindResponse{
			Message: "Find Success",
			Error:   "",
			Id:      fmt.Sprintf("%d", aInt),
			Addr:    tempNode.Addr,
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})
	// Value finder: given {key} -> find {value} in network
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var k Key
		err := decoder.Decode(&k)
		if err != nil {
			panic(err)
		}

		val, nodeErr := node.Get(k.Key)
		if nodeErr != nil {
			res := GetResponse{
				Message: "Get Failed",
				Error:   fmt.Sprintf("%v", err),
				Key:     k.Key,
				Value:   "",
			}
			if err := json.NewEncoder(w).Encode(res); err != nil {
				panic(err)
			}
			panic(nodeErr)
			return
		}

		res := GetResponse{
			Message: "Get Success",
			Error:   "",
			Key:     k.Key,
			Value:   fmt.Sprintf("%s", val),
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Key deletion: Given {key} delete {key, value} from network
	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var k Key
		err := decoder.Decode(&k)
		if err != nil {
			panic(err)
		}

		nodeErr := node.Delete(k.Key)
		if nodeErr != nil {
			panic(nodeErr)
		}

		res := Response{
			Message: "Delete Success",
			Error:   "",
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Join
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var joinConfig JoinConfig
		err := decoder.Decode(&joinConfig)
		if err != nil {
			panic(err)
		}

		joinNode := boopy.NewInode(joinConfig.Id, joinConfig.Addr)
		if err := node.Join(joinNode); err != nil {
			res := Response{
				Message: "Join Failed",
				Error:   fmt.Sprintf("%v", err),
			}
			if err := json.NewEncoder(w).Encode(res); err != nil {
				panic(err)
			}
			return
		}

		res := Response{
			Message: "Join Success",
			Error:   "",
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Expose server
	log.Fatal(http.ListenAndServe(frontEndAddr, nil))

	// Cause os interrupts (control-c) to stop the service
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	shut <- true
	node.Stop()
}
