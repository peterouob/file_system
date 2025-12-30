package storage

import (
	"errors"
	"sort"
	"sync"
)

const (
	smallSize  = 1024             // 1KB
	mediumSize = 32 * 1024        // 5KB
	largeSize  = 1024 * 1024      // 1MB
	xlargeSize = 20 * 1024 * 1024 // 20MB
)

var DefaultSizes = []uint32{smallSize, mediumSize, largeSize, xlargeSize}

// BufferPool TODO:use prometheus to metrics the pool state
type BufferPool struct {
	pools map[uint32]*sync.Pool
	size  []uint32
}

func NewBufferPool(sizes ...uint32) *BufferPool {
	pools := make(map[uint32]*sync.Pool)

	bp := &BufferPool{
		pools: pools,
		size:  sizes,
	}

	for _, size := range sizes {
		bp.pools[size] = &sync.Pool{
			New: func() any {
				return make([]byte, size)
			},
		}
	}

	return bp
}

var ErrToLarge = errors.New("too large !")

func (b *BufferPool) Get(size uint32) ([]byte, error) {

	idx := sort.Search(len(b.size), func(i int) bool { return b.size[i] >= size })

	if idx >= len(b.size) {
		return nil, ErrToLarge
	}

	targetSize := b.size[idx]
	buf := b.pools[targetSize].Get().([]byte)
	return buf, nil
}

func (b *BufferPool) Put(data []byte) {
	capacity := uint32(cap(data))
	if pool, ok := b.pools[capacity]; ok {
		data = data[:capacity]
		pool.Put(data)
	}
}

func (b *BufferPool) WarnUp() {
	for _, pool := range b.pools {
		for range 50 {
			pool.Put(pool.New())
		}
	}
}
