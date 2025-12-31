package storage

import (
	"context"
	"fmt"
	rand2 "math/rand/v2"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	assert.Equal(t, int64(0), metas.Offset, "First offset should be 0")
	assert.Equal(t, uint32(4096), metas.Size, "Size should be data size only")

	// header + data + footer + padding
	// (29+ 4096 + 8 + n) % 8 = 0;total = 4136
	expectedTotalSize := int64(4136)
	assert.Equal(t, expectedTotalSize, volume.writeOffset, "Write offset calculation incorrect")

	assert.NoError(t, volume.Write(needle))
	metas = volume.index[key]

	assert.Equal(t, expectedTotalSize, metas.Offset, "Second offset should start after first needle")

	assert.Equal(t, expectedTotalSize*2, volume.writeOffset, "Final write offset incorrect")
}

func setupTestVolume(t *testing.T) (*Volume, *os.File) {
	t.Helper()
	tmpDir := t.TempDir()
	vPath := filepath.Join(tmpDir, "test_read.vol")

	f, err := os.Create(vPath)
	require.NoError(t, err)

	v := NewVolume(f)
	return v, f
}

func TestVolume_Read(t *testing.T) {
	payload := []byte("hello world data")
	keyVal := uint64(100)
	cookieVal := uint64(9999)
	altKeyVal := uint32(50)

	keyPair := KeyPair{Key: keyVal, AltKey: altKeyVal}

	needle := &Needle{
		Header: NeedleHeader{
			MagicHeader:  MagicHeader,
			Cookie:       cookieVal,
			Key:          keyVal,
			AlternateKey: altKeyVal,
			Flag:         0,
			Size:         uint32(len(payload)),
		},
		Data: payload,
		Footer: NeedleFooter{
			MagicFooter: MagicFooter,
		},
	}

	t.Run("Success_HappyPath", func(t *testing.T) {
		v, _ := setupTestVolume(t)

		err := v.Write(needle)
		require.NoError(t, err)

		gotBytes, err := v.Read(keyPair, cookieVal)
		assert.NoError(t, err)
		assert.Equal(t, gotBytes, payload)
	})

	t.Run("Error_KeyNotFound", func(t *testing.T) {
		v, _ := setupTestVolume(t)

		wrongKey := KeyPair{Key: 99999, AltKey: 0}
		_, err := v.Read(wrongKey, cookieVal)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("Error_InvalidCookie", func(t *testing.T) {
		v, _ := setupTestVolume(t)

		err := v.Write(needle)
		require.NoError(t, err)

		wrongCookie := uint64(1111)
		_, err = v.Read(keyPair, wrongCookie)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrCookie)
	})

	t.Run("Error_DataDeleted", func(t *testing.T) {
		v, f := setupTestVolume(t)

		err := v.Write(needle)
		require.NoError(t, err)

		meta := v.index[keyPair]

		flagOffset := meta.Offset + 24

		_, err = f.WriteAt([]byte{1}, flagOffset)
		require.NoError(t, err)

		_, err = v.Read(keyPair, cookieVal)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrDataDeleted)
	})

	t.Run("Error_MagicHeaderMismatch", func(t *testing.T) {
		v, f := setupTestVolume(t)

		err := v.Write(needle)
		require.NoError(t, err)

		meta := v.index[keyPair]

		badMagic := []byte{0, 0, 0, 0}
		_, err = f.WriteAt(badMagic, meta.Offset)
		require.NoError(t, err)

		_, err = v.Read(keyPair, cookieVal)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrMagicNumber)
	})

	t.Run("Error_CRC_Mismatch", func(t *testing.T) {
		v, f := setupTestVolume(t)

		err := v.Write(needle)
		require.NoError(t, err)

		meta := v.index[keyPair]

		dataStartOffset := meta.Offset + 29

		_, err = f.WriteAt([]byte{'X'}, dataStartOffset)
		require.NoError(t, err)

		_, err = v.Read(keyPair, cookieVal)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrCrcNotValid)
	})
}

const (
	BenchPayloadSize = 4096
	BenchFileCount   = 10000
)

func setupTempVolume(tb testing.TB) (*Volume, string, func()) {
	tb.Helper()
	tmpDir := tb.TempDir()
	vPath := filepath.Join(tmpDir, "bench.vol")

	f, err := os.Create(vPath)
	require.NoError(tb, err)

	v := NewVolume(f)

	cleanup := func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpDir)
	}
	return v, vPath, cleanup
}

func newRandomNeedle(key uint64, size int) *Needle {
	payload := make([]byte, size)
	payload[0] = byte(key)
	payload[size-1] = byte(key >> 8)

	return &Needle{
		Header: NeedleHeader{
			Key:         key,
			Size:        uint32(size),
			MagicHeader: MagicHeader,
		},
		Data: payload,
		Footer: NeedleFooter{
			MagicFooter: MagicFooter,
		},
	}
}

func BenchmarkHaystack_Write(b *testing.B) {
	ctx, task := trace.NewTask(context.Background(), "TASK_Haystack_Write")
	defer task.End()

	b.Run("Serial", func(b *testing.B) {
		region := trace.StartRegion(ctx, "REGION_Serial_Write")
		defer region.End()
		v, _, cleanup := setupTempVolume(b)
		defer cleanup()

		sampleNeedle := newRandomNeedle(1, BenchPayloadSize)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sampleNeedle.Header.Key = uint64(i)
			err := v.Write(sampleNeedle)
			assert.NoError(b, err)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		region := trace.StartRegion(ctx, "REGION_Serial_Parallel")
		defer region.End()
		v, _, cleanup := setupTempVolume(b)
		defer cleanup()

		b.ResetTimer()

		var i atomic.Uint64

		b.RunParallel(func(pb *testing.PB) {
			localNeedle := newRandomNeedle(0, BenchPayloadSize)

			for pb.Next() {
				i.Add(1)
				localNeedle.Header.Key = i.Load()
				err := v.Write(localNeedle)
				assert.NoError(b, err)
			}
		})
	})
}

func BenchmarkOS_Write(b *testing.B) {

	ctx, task := trace.NewTask(context.Background(), "BenchmarkOS_Write")
	defer task.End()
	payload := make([]byte, BenchPayloadSize)
	payload[0] = 1
	payload[BenchPayloadSize-1] = 255

	b.Run("Serial", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkOS_Write_Serial")
		defer region.End()
		tmpDir := b.TempDir()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			fileName := filepath.Join(tmpDir, fmt.Sprintf("%d.dat", i))
			err := os.WriteFile(fileName, payload, 0644)
			assert.NoError(b, err)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkOS_Write_Parallel")
		defer region.End()

		tmpDir := b.TempDir()
		var counter atomic.Uint64

		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := counter.Add(1)
				fileName := filepath.Join(tmpDir, fmt.Sprintf("%d.dat", id))
				err := os.WriteFile(fileName, payload, 0644)
				assert.NoError(b, err)
			}
		})
	})
}

func BenchmarkHaystack_Read(b *testing.B) {
	ctx, task := trace.NewTask(context.Background(), "BenchmarkHaystack_Read")
	defer task.End()
	v, _, cleanup := setupTempVolume(b)
	defer cleanup()

	items := make([]struct {
		key    uint64
		cookie uint64
	}, BenchFileCount)

	for i := 0; i < BenchFileCount; i++ {
		key := uint64(i)
		cookie := uint64(i * 10)
		n := newRandomNeedle(key, BenchPayloadSize)
		n.Header.Cookie = cookie

		err := v.Write(n)
		require.NoError(b, err)

		items[i] = struct{ key, cookie uint64 }{key, cookie}
	}

	_ = v.dataFile.Sync()

	b.ResetTimer()

	b.Run("Serial", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkHaystack_Read_Serial")
		defer region.End()
		rng := rand2.New(rand2.NewPCG(1, 2))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := rng.IntN(BenchFileCount)
			target := items[idx]
			_, err := v.Read(KeyPair{Key: target.key}, target.cookie)
			assert.NoError(b, err)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkHaystack_Read_Parallel")
		defer region.End()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand2.New(rand2.NewPCG(rand2.Uint64(), rand2.Uint64()))

			for pb.Next() {
				idx := rng.IntN(BenchFileCount)
				target := items[idx]
				_, err := v.Read(KeyPair{Key: target.key}, target.cookie)
				assert.NoError(b, err)

			}
		})
	})
}

func BenchmarkOS_Read(b *testing.B) {
	ctx, task := trace.NewTask(context.Background(), "BenchmarkOS_Read")
	defer task.End()

	tmpDir := b.TempDir()
	payload := make([]byte, BenchPayloadSize)

	filePaths := make([]string, BenchFileCount)

	for i := 0; i < BenchFileCount; i++ {
		fileName := fmt.Sprintf("%d.dat", i)
		fullPath := filepath.Join(tmpDir, fileName)

		err := os.WriteFile(fullPath, payload, 0644)
		require.NoError(b, err)

		filePaths[i] = fullPath
	}

	b.ResetTimer()

	b.Run("Serial", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkOS_Read_Serial")
		defer region.End()
		rng := rand2.New(rand2.NewPCG(1, 2))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.IntN(BenchFileCount)
			targetPath := filePaths[idx]

			_, err := os.ReadFile(targetPath)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		region := trace.StartRegion(ctx, "BenchmarkOS_Read_Parallel")
		defer region.End()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			rng := rand2.New(rand2.NewPCG(rand2.Uint64(), rand2.Uint64()))

			for pb.Next() {
				idx := rng.IntN(BenchFileCount)
				targetPath := filePaths[idx]

				_, err := os.ReadFile(targetPath)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
