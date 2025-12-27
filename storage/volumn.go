package storage

import (
	"errors"
	"os"
	"sync"
)

var (
	ErrWrite = errors.New("write error")
)

type Volume struct {
	dataFile    *os.File
	index       map[uint64]NeedleMeta // from the crypto file name to get
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
		index:       make(map[uint64]NeedleMeta),
		writeOffset: 0,
	}
}

func (v *Volume) Write(n *Needle) error {

	dataBytes := n.Bytes()
	writeOffset := int64(len(dataBytes))

	v.mu.Lock()
	defer v.mu.Unlock()

	if _, err := v.dataFile.WriteAt(dataBytes, v.writeOffset); err != nil {
		return ErrWrite
	}

	v.index[n.Header.Key] = NeedleMeta{
		Offset: v.writeOffset,
		Size:   uint32(len(n.Data)),
	}

	v.writeOffset += writeOffset
	return nil
}
