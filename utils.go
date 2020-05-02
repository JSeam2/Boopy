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

func bytesEqual(left, right []byte) bool {
	return bytes.Compare(left, right) == 0
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

// check if key is between a and right, right inclusive
func keyBetwIncludeRight(key, left, right []byte) bool {
	return between(key, left, right) || bytes.Equal(key, right)
}

// Checks if a key is STRICTLY between two ID's exclusively
func between(key, left, right []byte) bool {
	switch bytes.Compare(left, right) {
	case 0:
		return bytes.Compare(left, key) != 0
	case 1:
		return bytes.Compare(left, key) == -1 || bytes.Compare(right, key) >= 0
	case -1:
		return bytes.Compare(left, key) == -1 && bytes.Compare(right, key) >= 0
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
