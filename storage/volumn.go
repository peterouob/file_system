package storage

import (
	"encoding/binary"
	"errors"
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
	index       map[KeyPair]NeedleMeta
	mu          sync.RWMutex
	writeOffset int64
	bufferPool  *BufferPool
}

type NeedleMeta struct {
	Offset int64
	Size   uint32
}

func NewVolume(dataFile *os.File) *Volume {
	v := &Volume{
		dataFile:    dataFile,
		index:       make(map[KeyPair]NeedleMeta),
		writeOffset: 0,
		bufferPool:  NewBufferPool(DefaultSizes...),
	}
	v.bufferPool.WarnUp()
	return v
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

	v.index[key] = NeedleMeta{
		Offset: v.writeOffset,
		Size:   n.Header.Size,
	}

	v.writeOffset += writeOffset
	return nil
}

var (
	ErrNotFound    = errors.New("error for file not found")
	ErrMagicNumber = errors.New("error for file magic number")
	ErrCookie      = errors.New("error for file cookie")
	ErrDataDeleted = errors.New("error for file data is deleted")
	ErrCrcNotValid = errors.New("error for file crc not valid")
)

func (v *Volume) Read(key KeyPair, cookie uint64) ([]byte, error) {
	v.mu.RLock()
	meta, ok := v.index[key]
	v.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("read not found the data from key:,%w: %v", ErrNotFound, key)
	}

	var lastMetaSize = meta.Size

	totalSize := NeedleHeaderSize + lastMetaSize + NeedleFooterSize

	buf, err := v.bufferPool.Get(totalSize)
	buf.B = buf.B[:totalSize]

	if errors.Is(err, ErrToLarge) {
		return nil, fmt.Errorf("read error: %v", err)
	}

	defer v.bufferPool.Put(buf)

	if n, err := v.dataFile.ReadAt(buf.B, meta.Offset); err != nil || n != int(totalSize) {
		return nil, fmt.Errorf("read error: %v", err)
	}

	if binary.BigEndian.Uint32(buf.B[0:4]) != MagicHeader {
		return nil, ErrMagicNumber
	}

	if binary.BigEndian.Uint64(buf.B[4:12]) != cookie {
		return nil, ErrCookie
	}

	// delete byte
	if buf.B[24] == 1 {
		return nil, ErrDataDeleted
	}

	data := buf.B[:NeedleHeaderSize+lastMetaSize]
	footer := buf.B[totalSize-NeedleFooterSize:]
	storeCrc := binary.BigEndian.Uint32(footer[0:4])
	dataCrc := NewCRC(data).Value()

	if dataCrc != storeCrc {
		return nil, ErrCrcNotValid
	}

	data = buf.B[NeedleHeaderSize : NeedleHeaderSize+lastMetaSize]

	var result = make([]byte, len(data))
	copy(result, data)

	return result, nil
}
