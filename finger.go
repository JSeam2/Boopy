package boopy

import (
	"log"
	"math/big"

	"github.com/jseam2/boopy/api"
)

type fingerTable []*fingerEntry

// Generate new finger table, with empty entries
func newFingerTable(nodeStruct *api.Node, m int) fingerTable {
	ftToBeFilled := make([]*fingerEntry, m)
	for i := range ftToBeFilled {
		ftToBeFilled[i] = newFingerEntry(fingerID(nodeStruct.Id, i, m), nodeStruct)
	}

	return ftToBeFilled
}

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	Node *api.Node // RemoteNode that Start points to
	Id   []byte    // ID hash of (n + 2^i) mod (2^m)
}

// newFingerEntry returns an allocated new finger entry with the attributes set
func newFingerEntry(idHash []byte, nodeStruct *api.Node) *fingerEntry {
	return &fingerEntry{
		Node: nodeStruct,
		Id:   idHash,
	}
}

// Computes the offset by (n + 2^i) mod (2^m)
func fingerID(n []byte, i int, m int) []byte {

	// Convert the ID to a bigint
	idBigInt := (&big.Int{}).SetBytes(n)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(i)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(idBigInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod
	idBigInt.Mod(&sum, &ceil)

	// Add together
	return idBigInt.Bytes()
}

// n.fix_fingers()
//   i = random index > 1 into finger []
//   finger[i].node = find_successor(finger[i].start)
func (n *Node) fixFinger(next int) int {
	nextIndex := next + 1
	// use hashSize to figure out the next num
	nextNum := nextIndex % n.cnf.HashSize

	// Use hash to find the next entry in the finger tables
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)

	// Find successor function
	successor, err := n.findSuccessor(nextHash)

	if err != nil {
		log.Printf("Fix finger failed, unable to find successor")
		return nextNum
	}

	finger := newFingerEntry(nextHash, successor)

	// set finger table
	n.ftMtx.Lock()
	n.fingerTable[next] = finger
	n.ftMtx.Unlock()

	return nextNum
}
