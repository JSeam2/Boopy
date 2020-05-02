package boopy

import (
	"crypto/sha1"
	"hash"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/jseam2/boopy/api"
	aurora "github.com/logrusorgru/aurora"
	"google.golang.org/grpc"
)

func BaseConfig() *Config {
	n := &Config{
		Hash:     sha1.New,
		DialOpts: make([]grpc.DialOption, 0, 5),
	}
	// n.HashSize = n.Hash().Size()
	n.HashSize = n.Hash().Size() * 8

	n.DialOpts = append(n.DialOpts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	)
	return n
}

type Config struct {
	Id   string
	Addr string // i presume import of node

	ServerOpts []grpc.ServerOption
	DialOpts   []grpc.DialOption

	Hash               func() hash.Hash // Hash function to use (for generating node ID, )
	HashSize           int              // number of fingers in finger table
	MaxTimeoutDuration time.Duration
	MaxIdleDuration    time.Duration
}

// Create a node entry, for storage in finger table
func NewInode(id string, addr string) *api.Node {
	h := sha1.New()
	_, err := h.Write([]byte(id))

	if err != nil {
		log.Printf("Error creating inode. Problems with hash function")
		return nil
	}
	val := h.Sum(nil)

	return &api.Node{
		Id:   val,
		Addr: addr,
	}
}

// Create a new node in the Chord. Check if node with same id already exists
func NewNode(cnf *Config, joinNode *api.Node) (*Node, error) {
	var nodeID string

	node := &Node{
		Node:       new(api.Node),
		shutdownCh: make(chan struct{}),
		cnf:        cnf,
		storage:    NewMapStore(cnf.Hash),
	}
	if cnf.Id != "" {
		nodeID = cnf.Id
	} else {
		nodeID = cnf.Addr
	}

	id, err := node.hashKey(nodeID)

	if err != nil {
		return nil, err
	}

	aInt := (&big.Int{}).SetBytes(id) // treating id as bytes of a big-endian unsigned integer, return the integer it represents
	log.Printf(aurora.Sprintf(aurora.Yellow("New Node ID = %d, \n"), aInt))
	node.Node.Id = id
	node.Node.Addr = cnf.Addr
	// Populate finger table (by anotating itself to be in charge of all possible hashes at the moment)
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// Start RPC server (start listening function, )
	// transport is a struct that contains grpc server and supplementary attributes (like timeout etc)
	transport, err := NewGrpcTransport(cnf)

	if err != nil {
		return nil, err
	}

	node.transport = transport

	api.RegisterChordServer(transport.server, node)
	node.transport.Start()

	// find the closest node clockwise from the id of this node (i.e. successor node)
	// adds successor to the 'successor' attribute of the node
	nodeJoinErr := node.join(joinNode)

	if nodeJoinErr != nil {
		log.Printf("Error joining node")
		return nil, err
	}

	// run routines
	// Fix fingers every 500 ms
	go node.fixFingerRoutine(500)

	// Stablize every 1000ms
	go node.stabilizeRoutine(1000)
	// Check predecessor fail every 5000 ms
	go node.checkPredecessorRoutine(2000)

	return node, nil
}

type Node struct {
	*api.Node

	cnf *Config

	predecessor *api.Node
	predMtx     sync.RWMutex

	successor *api.Node
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable   fingerTable
	ftMtx         sync.RWMutex
	storage       Storage
	stMtx         sync.RWMutex
	transport     Transport
	tsMtx         sync.RWMutex
	lastStablized time.Time
}

func (n *Node) hashKey(key string) ([]byte, error) {
	hashFunction := n.cnf.Hash()

	_, err := hashFunction.Write([]byte(key))

	if err != nil {
		return nil, err
	}

	val := hashFunction.Sum(nil)
	return val, nil
}

func (incomingNode *Node) join(joinNode *api.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	var joiningNode *api.Node
	// // Ask if our id already exists on the ring.
	if joinNode != nil {
		remoteNode, err := incomingNode.findSuccessorRPC(joinNode, incomingNode.Id)
		if err != nil {
			return err
		}

		if bytesEqual(remoteNode.Id, incomingNode.Id) {
			return ERR_NODE_EXISTS
		}
		joiningNode = joinNode
	} else {
		joiningNode = incomingNode.Node
	}

	succ, err := incomingNode.findSuccessorRPC(joiningNode, incomingNode.Id)
	if err != nil {
		return err
	}
	incomingNode.succMtx.Lock()
	incomingNode.successor = succ
	incomingNode.succMtx.Unlock()
	return nil
}

/////////////////////////////////
// Public Methods
////////////////////////////////

func (n *Node) Find(key string) (*api.Node, error) {
	return n.locate(key)
}

func (n *Node) Get(key string) ([]byte, error) {
	return n.get(key)
}
func (n *Node) Set(key, value string) error {
	return n.set(key, value)
}
func (n *Node) Delete(key string) error {
	return n.delete(key)
}

func (n *Node) Join(joinNode *api.Node) error {
	return n.join(joinNode)
}

func (n *Node) Stabilize() {
	n.stabilize()
}

func (n *Node) Stop() {
	close(n.shutdownCh)

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	n.succMtx.RLock()
	n.predMtx.RLock()
	succ := n.successor
	pred := n.predecessor
	n.succMtx.RUnlock()
	n.predMtx.RUnlock()

	if n.Node.Addr != succ.Addr && pred != nil {
		n.transferKeysFromNode(pred, succ)
		predErr := n.setPredecessorRPC(succ, pred)
		succErr := n.setSuccessorRPC(pred, succ)
		log.Println("stop errors: ", predErr, succErr)
	}

	n.transport.Stop()
}

func (n *Node) locate(key string) (*api.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}
	succ, err := n.findSuccessor(id)
	return succ, err
}

func (n *Node) get(key string) ([]byte, error) {
	node, err := n.locate(key)
	if err != nil {
		return nil, err
	}
	val, err := n.getKeyRPC(node, key)
	if err != nil {
		return nil, err
	}
	return val.Value, nil
}

func (n *Node) set(key, value string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.setKeyRPC(node, key, value)
	return err
}

func (n *Node) delete(key string) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}
	err = n.deleteKeyRPC(node, key)
	return err
}

func (n *Node) transferKeys(pred, succ *api.Node) {

	keys, _ := n.requestKeys(pred, succ)
	if len(keys) > 0 {
		log.Printf("Transfer Keys: %+v", keys)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		n.storage.Set(item.Key, item.Value)
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) transferKeysFromNode(pred, succ *api.Node) {
	keys, err := n.storage.Between(pred.Id, succ.Id)
	if len(keys) > 0 {
		log.Println("transfering: ", keys, succ, err)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		err := n.setKeyRPC(succ, item.Key, item.Value)
		if err != nil {
			log.Println("error transfering key: ", item.Key, succ.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) deleteKeys(node *api.Node, keys []string) error {
	return n.deleteKeysRPC(node, keys)
}

// When a new node joins, it requests keys from it's successor
func (n *Node) requestKeys(pred, succ *api.Node) ([]*api.KV, error) {

	if bytesEqual(n.Id, succ.Id) {
		return nil, nil
	}
	return n.requestKeysRPC(
		succ, pred.Id, n.Id,
	)
}

func (n *Node) findSuccessor(id []byte) (*api.Node, error) {
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()

	curr := n.Node
	succ := n.successor

	if succ == nil {
		return curr, nil
	}

	var err error

	if keyBetwIncludeRight(id, curr.Id, succ.Id) {
		return succ, nil
	} else {
		pred := n.closestPrecedingNode(id)
		if bytesEqual(pred.Id, n.Id) {
			succ, err = n.getSuccessorRPC(pred)

			if err != nil {
				return nil, err
			}

			if succ == nil {
				return pred, nil
			}
			return succ, nil
		}

		succ, err := n.findSuccessorRPC(pred, id)
		// fmt.Println("successor to closest node ", succ, err)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}
		return succ, nil

	}
	// return nil, nil
}

// Fig 5 implementation for closest_preceding_node
func (n *Node) closestPrecedingNode(id []byte) *api.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.Node == nil {
			continue
		}
		if between(f.Id, curr.Id, id) {
			return f.Node
		}
	}
	return curr
}

// Pseudocode in paper
// n.stabilize()
//   x = successor.predecessor
//   if (x in (n, successor))
//     successor = x
//   successor.notify(n)
func (n *Node) stabilize() {
	n.succMtx.RLock()

	succ := n.successor
	if succ == nil {
		log.Printf("No successor found")
		n.succMtx.RUnlock()
		return
	}

	n.succMtx.RUnlock()

	pred, err := n.getPredecessorRPC(succ)
	if err != nil || pred == nil {
		log.Println("Error getting predecessor, ", err, pred)
		return
	}

	// if pred.Id exists check if current node is between predecessor and successor
	if pred.Id != nil && between(pred.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		n.successor = pred
		n.succMtx.Unlock()
	}

	// call notify
	n.notify(succ, n.Node)
}

func (n *Node) checkPredecessor() {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if pred != nil {
		err := n.transport.CheckPredecessor(pred)
		if err != nil {
			log.Println("Predecessor has an error: ", err)
			n.predMtx.Lock()
			n.predecessor = nil
			n.predMtx.Unlock()
		}
	}
}
