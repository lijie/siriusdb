package dbproxy

// #include <inttypes.h>
// #include <stdio.h>
// uint32_t hash_crc32(const char *key, size_t key_length);
// uint32_t hash_crc32a(const char *key, size_t key_length);
import "C"

func HashCRC32(s string) uint32 {
	a := C.CString(s)
	r := C.hash_crc32a(a, C.size_t(len(s)))
	return uint32(r)
}
