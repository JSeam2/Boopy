package boopy

import (
	"github.com/jseam2/boopy/api"
	"fmt"
	"math/big"
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

// called periodically. refreshes finger table entries.
// next stores the index of the next finger to fix.
func (n *Node) fixFinger(next int) int {
	nextNum := (next + 1) % n.cnf.HashSize
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	successor, err := n.findSuccessor(nextHash)
	if err != nil || successor == nil {
		fmt.Println("error: ", err, successor)
		fmt.Printf("finger lookup failed %x %x \n", n.Id, nextHash)
		// TODO: Check how to handle retry, passing ahead for now
		return nextNum
	}

	finger := newFingerEntry(nextHash, successor)
	n.ftMtx.Lock()
	n.fingerTable[next] = finger
	n.ftMtx.Unlock()

	return nextNum
}
