package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"

	errtype "github.com/peterouob/file_system/type"
)

type KeyPair struct {
	Key    uint64
	AltKey uint32
}

type Volume struct {
	dataFile    *os.File
	index       map[KeyPair]NeedleMeta
	bufferPool  *BufferPool
	writeOffset int64
	mu          sync.RWMutex
}

type NeedleMeta struct {
	Offset int64
	Size   uint32
}

var (
	bufferPool *BufferPool
	O          sync.Once
)

func NewVolume(dataFile *os.File) *Volume {

	O.Do(func() {
		bufferPool = NewBufferPool()
		bufferPool.WarnUp()
	})

	v := &Volume{
		dataFile:    dataFile,
		index:       make(map[KeyPair]NeedleMeta),
		writeOffset: 0,
		bufferPool:  bufferPool,
	}

	return v
}

func (v *Volume) Write(n *Needle) error {

	dataBytes := n.Bytes(v.bufferPool)

	defer v.bufferPool.Put(dataBytes)

	writeOffset := int64(len(dataBytes.B))

	v.mu.Lock()
	defer v.mu.Unlock()

	if n, err := v.dataFile.WriteAt(dataBytes.B, v.writeOffset); err != nil || n != len(dataBytes.B) {
		return fmt.Errorf("write error: %v", err)
	}

	key := KeyPair{
		Key:    n.Header.Key,
		AltKey: n.Header.AlternateKey,
	}

	v.index[key] = NeedleMeta{
		Offset: v.writeOffset,
		Size:   n.Header.Size,
	}

	v.writeOffset += writeOffset
	return nil
}

func (v *Volume) Read(key KeyPair, cookie uint64) ([]byte, error) {
	v.mu.RLock()
	meta, ok := v.index[key]
	v.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("read not found the data from key:,%w: %v", errtype.ErrNotFound, key)
	}

	var lastMetaSize = meta.Size

	totalSize := NeedleHeaderSize + lastMetaSize + NeedleFooterSize

	buf, err := v.bufferPool.Get(totalSize)
	buf.B = buf.B[:totalSize]

	if errors.Is(err, errtype.ErrToLarge) {
		return nil, fmt.Errorf("read error: %v", err)
	}

	defer v.bufferPool.Put(buf)

	if n, err := v.dataFile.ReadAt(buf.B, meta.Offset); err != nil || n != int(totalSize) {
		return nil, fmt.Errorf("read error: %v", err)
	}

	if err := ValidNeedleBlock(buf.B, cookie); err != nil {
		return nil, err
	}

	data, err := GetNeedleBlockInfo(totalSize, lastMetaSize, buf.B)

	if errors.Is(err, errtype.ErrCrcNotValid) {
		return nil, err
	}

	return data, nil
}
