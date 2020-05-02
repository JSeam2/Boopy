package boopy

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"math/rand"
	"time"
)

var (
	ERR_NO_SUCCESSOR  = errors.New("cannot find successor")
	ERR_NODE_EXISTS   = errors.New("node with id already exists")
	ERR_KEY_NOT_FOUND = errors.New("key not found")
)

func bytesEqual(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}

func isPowerOfTwo(num int) bool {
	nonZero := (num != 0)
	evenBits := (num & (num - 1)) == 0
	return nonZero && evenBits
}

func randStabilize(min, max time.Duration) time.Duration {
	random := rand.Float64()
	if max < min {
		// Catch if min > max
		return min
	}
	delta := float64(max - min)
	randDuration := (random * delta) + float64(min)
	return time.Duration(randDuration)
}

// check if key is between a and b, right inclusive
func keyBetwIncludeRight(key, a, b []byte) bool {
	return between(key, a, b) || bytes.Equal(key, b)
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 0:
		return bytes.Compare(a, key) != 0
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) >= 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) >= 0
	}
	return false
}

// For testing
func GetHashID(key string) []byte {
	h := sha1.New()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil
	}
	val := h.Sum(nil)
	return val
}
