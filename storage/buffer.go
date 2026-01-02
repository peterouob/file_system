package storage

import (
	"sort"
	"sync"

	errtype "github.com/peterouob/file_system/type"
	"github.com/peterouob/file_system/utils"
)

const (
	xsmallSize = 64
	smallSize  = 1024             // 1KB
	mediumSize = 32 * 1024        // 5KB
	largeSize  = 1024 * 1024      // 1MB
	xlargeSize = 20 * 1024 * 1024 // 20MB
)

var (
	DefaultSizes = []uint32{xsmallSize, smallSize, mediumSize, largeSize, xlargeSize}
)

// BufferPool TODO:use prometheus to metrics the pool state
type BufferPool struct {
	pools map[uint32]*sync.Pool
	size  []uint32
}

type Buffer struct {
	B []byte
}

func NewBufferPool(sizes ...uint32) *BufferPool {

	if len(sizes) == 0 {
		sizes = DefaultSizes
	}

	pools := make(map[uint32]*sync.Pool)

	bp := &BufferPool{
		pools: pools,
		size:  sizes,
	}

	for _, size := range sizes {
		bp.pools[size] = &sync.Pool{
			New: func() any {
				b := new(Buffer)
				b.B = make([]byte, 0, size)
				return b
			},
		}
	}

	return bp
}

func (b *BufferPool) Get(size uint32) (*Buffer, error) {

	idx := sort.Search(len(b.size), func(i int) bool { return b.size[i] >= size })

	if idx >= len(b.size) {
		return nil, errtype.ErrToLarge
	}

	targetSize := b.size[idx]
	buf := b.pools[targetSize].Get().(*Buffer)
	buf.B = buf.B[:0]
	return buf, nil
}

func (b *BufferPool) Put(buf *Buffer) {
	capacity := utils.Must(utils.CIU32(cap(buf.B)))
	if pool, ok := b.pools[capacity]; ok {
		pool.Put(buf)
	}
}

func (b *BufferPool) WarnUp() {
	for _, pool := range b.pools {
		for range 50 {
			pool.Put(pool.New())
		}
	}
}
