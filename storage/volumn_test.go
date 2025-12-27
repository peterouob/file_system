package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var benchPayload = make([]byte, 4096)

func BenchmarkHaystack_Write(b *testing.B) {
	tmpDir := b.TempDir()
	vPath := filepath.Join(tmpDir, "bench.vol")

	f, err := os.Create(vPath)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

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
		if err := volume.Write(n); err != nil {
			b.Fatal(err)
		}
	}

	if err := volume.dataFile.Sync(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOS_LooseFiles(b *testing.B) {
	tmpDir := b.TempDir()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fileName := filepath.Join(tmpDir, fmt.Sprintf("%d.dat", i))

		if err := os.WriteFile(fileName, benchPayload, 0644); err != nil {
			b.Fatal(err)
		}
	}
}
