package utils

import "unsafe"

func Str2Byte(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func Byte2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
