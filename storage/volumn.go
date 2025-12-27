package storage

import (
	"fmt"
	"os"
	"sync"
)

type KeyPair struct {
	Key    uint64
	AltKey uint32
}

type Volume struct {
	dataFile    *os.File
	index       map[KeyPair][]NeedleMeta // from the crypto file name to get
	mu          sync.RWMutex
	writeOffset int64
}

type NeedleMeta struct {
	Offset int64
	Size   uint32
}

func NewVolume(dataFile *os.File) *Volume {
	return &Volume{
		dataFile:    dataFile,
		index:       make(map[KeyPair][]NeedleMeta),
		writeOffset: 0,
	}
}

func (v *Volume) Write(n *Needle) error {

	dataBytes := n.Bytes()
	writeOffset := int64(len(dataBytes))

	v.mu.Lock()
	defer v.mu.Unlock()

	if n, err := v.dataFile.WriteAt(dataBytes, v.writeOffset); err != nil || n != len(dataBytes) {
		return fmt.Errorf("write error: %v", err)
	}

	key := KeyPair{
		Key:    n.Header.Key,
		AltKey: n.Header.AlternateKey,
	}

	v.index[key] = append(v.index[key], NeedleMeta{
		Offset: v.writeOffset,
		Size:   n.Header.Size,
	})

	v.writeOffset += writeOffset
	return nil
}
