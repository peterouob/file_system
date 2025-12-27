package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var benchPayload = make([]byte, 4096)

func setup(tb testing.TB) *os.File {
	tb.Helper()
	tmpDir := tb.TempDir()
	vPath := filepath.Join(tmpDir, "bench.vol")

	f, err := os.Create(vPath)
	assert.NoError(tb, err)

	return f
}

func teardown(f *os.File, tb testing.TB) {
	assert.NoError(tb, f.Close())
	assert.NoError(tb, os.Remove(f.Name()))
}

func BenchmarkHaystack_Write(b *testing.B) {
	f := setup(b)
	defer teardown(f, b)
	volume := NewVolume(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := &Needle{
			Header: NeedleHeader{
				Key:  uint64(i),
				Size: uint32(len(benchPayload)),
			},
			Data: benchPayload,
		}
		assert.NoError(b, volume.Write(n))
	}
	assert.NoError(b, volume.dataFile.Sync())
}

func BenchmarkOS_LooseFiles(b *testing.B) {
	tmpDir := b.TempDir()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fileName := filepath.Join(tmpDir, fmt.Sprintf("%d.dat", i))

		if err := os.WriteFile(fileName, benchPayload, 0644); err != nil {
			assert.NoError(b, err)
		}
	}
}
func TestWrite(t *testing.T) {
	f := setup(t)
	defer teardown(f, t)

	volume := NewVolume(f)
	needle := &Needle{
		Header: NeedleHeader{
			Key:          1,
			AlternateKey: 100,
			Cookie:       12345,
			MagicHeader:  MagicHeader,
			Size:         4096,
		},
		Data: make([]byte, 4096),
		Footer: NeedleFooter{
			MagicFooter: MagicFooter,
		},
	}

	assert.NoError(t, volume.Write(needle))

	key := KeyPair{Key: 1, AltKey: 100}
	metas := volume.index[key]

	assert.Equal(t, 1, len(metas), "Should have 1 needle meta")
	assert.Equal(t, int64(0), metas[0].Offset, "First offset should be 0")
	assert.Equal(t, uint32(4096), metas[0].Size, "Size should be data size only")

	// header + data + footer + padding
	// (29+ 4096 + 8 + n) % 8 = 0;total = 4136
	expectedTotalSize := int64(4136)
	assert.Equal(t, expectedTotalSize, volume.writeOffset, "Write offset calculation incorrect")

	if err := volume.Write(needle); err != nil {
		t.Fatal(err)
	}

	metas = volume.index[key]

	assert.Equal(t, 2, len(metas), "Should have 2 needle metas")
	assert.Equal(t, expectedTotalSize, metas[1].Offset, "Second offset should start after first needle")

	assert.Equal(t, expectedTotalSize*2, volume.writeOffset, "Final write offset incorrect")
}
