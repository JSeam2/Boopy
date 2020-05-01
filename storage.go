package boopy

import (
	"hash"

	"github.com/jseam2/boopy/api"
)

// Storage defines the interface that allows the node to communicate with the underlying distributed map of [key] to [value]
type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Between([]byte, []byte) ([]*api.KV, error)
	MDelete(...string) error
}

/* mapStore defines two things:
A map matching a key (string) to a value (string)
A Hash function that the store uses*/
type mapStore struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to use

}

// NewMapStore takes in a function which produces a hash and creates an empty mapStore that uses the hash function.
func NewMapStore(hashFunc func() hash.Hash) Storage {
	return &mapStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

// hashKey generates the hash of a given key with the function used in the mapStore object
func (a *mapStore) hashKey(key string) ([]byte, error) {
	h := a.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

// Get performs a direct retrieval from the map of key-values to get the bytearray representation
func (a *mapStore) Get(key string) ([]byte, error) {
	val, ok := a.data[key]
	if !ok {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(val), nil
}

// Set adds a key to the mapStore.s
func (a *mapStore) Set(key, value string) error {
	a.data[key] = value
	return nil
}

// Delete removes a given key-value pair from the mapStore by the given key.
func (a *mapStore) Delete(key string) error {
	delete(a.data, key)
	return nil
}

// Between returns up to 10 keys that are between a given
func (storeptr *mapStore) Between(from []byte, to []byte) ([]*api.KV, error) {
	// Generate a slice of up to 10 key-value pairs
	betwVals := make([]*api.KV, 0, 10)
	for key, val := range storeptr.data {
		// generate hash of each key
		hashedKey, err := storeptr.hashKey(key)
		if err != nil {
			continue
		}

		// check if any of the hashed keys match the search range; add if it does to returned slice
		if keyBetwIncludeRight(hashedKey, from, to) {
			pair := &api.KV{
				Key:   key,
				Value: val,
			}
			betwVals = append(vals, pair)
		}
	}
	// Return all values that are between the given byte sets (hash-value wise)
	return betwVals, nil
}

// MDelete allows users to delete more than one key by providing multiple strings
func (a *mapStore) MDelete(keys ...string) error {
	for _, k := range keys {
		delete(a.data, k)
	}
	return nil
}
