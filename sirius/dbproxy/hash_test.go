package dbproxy

import (
	"fmt"
	"hash/crc32"
	"testing"
)

func TestHashCRC32(t *testing.T) {
	s := "AccountTable"
	r1 := HashCRC32(s)
	r2 := crc32.ChecksumIEEE([]byte(s))
	if r1 != r2 {
		t.Fatal("hash func compare failed")
	}
}

func ExampleHashCRC32() {
	s := "AccountTable"
	r1 := HashCRC32(s)
	fmt.Println(r1)

	r2 := crc32.ChecksumIEEE([]byte(s))
	fmt.Println(r2)
}
