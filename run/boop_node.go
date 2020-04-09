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
}

// KeyValue describes the values for inserting a key-value pair into the network
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Key tells us if a key ...?
type Key struct {
	Key string `json:"key"`
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

func createID(id string) []byte {
	// Set some bigInt
	val := big.NewInt(0)
	// Convert ID to a base10 bigInt -> return byte representation
	val.SetString(id, 10)
	return val.Bytes()
}

func main() {
	// Returns a new, default-initialized node for a sister node
	// INodes: independent nodes - meant to be used only for checking
	sister := boopy.NewInode("1", ":8001")

	// We initialize our node ID and its address, along with the 'blank node'
	node, err := createNode("4", ":8002", sister)
	if err != nil {
		log.Fatalln(err)
		return
	}

	shut := make(chan bool)
	// var count int
	// go func() {
	// 	ticker := time.NewTicker(1 * time.Second)
	// 	for {
	// 		select {
	// 		case <-ticker.C:
	// 			count++
	// 			key := strconv.Itoa(count)
	// 			value := fmt.Sprintf(`{"graph_id" : %d, "nodes" : ["node-%d","node-%d","node-%d"]}`, count, count+1, count+2, count+3)
	// 			sErr := h.Set(key, value)
	// 			if sErr != nil {
	// 				log.Println("err: ", sErr)
	// 			}
	// 		case <-shut:
	// 			ticker.Stop()
	// 			return
	// 		}
	// 	}
	// }()

	// REST Server

	// Basic ping function
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		res := Response{
			Message: "Pong!",
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
			panic(err)
		}

		res := Response{
			Message: "Set Successfully",
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
			panic(err)
		}

		aInt := (&big.Int{}).SetBytes(tempNode.Id)

		res := Response{
			Message: fmt.Sprintf("Id: %s Address: %s", aInt, tempNode.Addr),
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
			panic(err)
		}

		res := Response{
			Message: fmt.Sprintf("Value: %s ", val),
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
			panic(err)
		}

		res := Response{
			Message: "Deleted Successfully!",
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			panic(err)
		}
	})

	// Expose server
	log.Fatal(http.ListenAndServe(":83", nil))

	// Cause os interrupts (control-c) to stop the service
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	shut <- true
	node.Stop()
}
