package main

import (
	"crypto/sha1"
	"fmt"
	"strconv"
)

func main() {
	for i := 0; i < 100; i++ {
		h := sha1.New()
		v := strconv.Itoa(i)
		h.Write([]byte(v))
		sum := h.Sum(nil)
		fmt.Println(sum)
	}
}
