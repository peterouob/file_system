package utils

import (
	"crypto/rand"
	"io"
)

func NewEncryptionKey() []byte {
	keyBuf := make([]byte, 32)
	_, _ = io.ReadFull(rand.Reader, keyBuf)
	return keyBuf
}
