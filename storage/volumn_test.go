package storage

import (
	"fmt"
	"math/rand"
	rand2 "math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func BenchmarkOS_Write(b *testing.B) {
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

	assert.NoError(t, volume.Write(needle))
	metas = volume.index[key]

	assert.Equal(t, 2, len(metas), "Should have 2 needle metas")
	assert.Equal(t, expectedTotalSize, metas[1].Offset, "Second offset should start after first needle")

	assert.Equal(t, expectedTotalSize*2, volume.writeOffset, "Final write offset incorrect")
}

func setupTestVolume(t *testing.T) (*Volume, *os.File) {
	t.Helper()
	tmpDir := t.TempDir()
	vPath := filepath.Join(tmpDir, "test_read.vol")

	f, err := os.Create(vPath)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = f.Close()
	})

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

		meta := v.index[keyPair][0]

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

		meta := v.index[keyPair][0]

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

		meta := v.index[keyPair][0]

		dataStartOffset := meta.Offset + 29

		_, err = f.WriteAt([]byte{'X'}, dataStartOffset)
		require.NoError(t, err)

		_, err = v.Read(keyPair, cookieVal)

		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrCrcNotValid)
	})
}

const (
	BenchFileCount = 10000
	BenchFileSize  = 4096
)

type benchItem struct {
	key    KeyPair
	cookie uint64
	path   string
}

func BenchmarkHaystack_Reader(b *testing.B) {
	b.StopTimer()
	cha8 := rand2.ChaCha8{}

	dir := b.TempDir()
	vPath := filepath.Join(dir, "bench.vol")
	f, err := os.Create(vPath)
	require.NoError(b, err)

	vol := NewVolume(f)
	b.Cleanup(func() { _ = f.Close() })

	items := make([]benchItem, BenchFileCount)
	payload := make([]byte, BenchFileSize)
	_, err = cha8.Read(payload)
	assert.NoError(b, err)
	for i := 0; i < BenchFileCount; i++ {
		key := uint64(i)
		cookie := uint64(i * 10)

		needle := &Needle{
			Header: NeedleHeader{
				MagicHeader:  MagicHeader,
				Cookie:       cookie,
				Key:          key,
				AlternateKey: 0,
				Flag:         0,
				Size:         uint32(len(payload)),
			},
			Data: payload,
			Footer: NeedleFooter{
				MagicFooter: MagicFooter,
			},
		}

		err := vol.Write(needle)
		require.NoError(b, err)

		items[i] = benchItem{
			key:    KeyPair{Key: key, AltKey: 0},
			cookie: cookie,
		}
	}

	_ = f.Sync()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		idx := rand.Intn(BenchFileCount)
		target := items[idx]

		_, err := vol.Read(target.key, target.cookie)
		assert.NoError(b, err)
	}
}

func BenchmarkOS_Read(b *testing.B) {
	b.StopTimer()
	cha8 := rand2.ChaCha8{}

	dir := b.TempDir()
	items := make([]benchItem, BenchFileCount)
	payload := make([]byte, BenchFileSize)
	_, err := cha8.Read(payload)
	assert.NoError(b, err)

	for i := 0; i < BenchFileCount; i++ {
		fileName := fmt.Sprintf("%d.dat", i)
		fullPath := filepath.Join(dir, fileName)

		err := os.WriteFile(fullPath, payload, 0644)
		require.NoError(b, err)

		items[i] = benchItem{
			path: fullPath,
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		idx := rand.Intn(BenchFileCount)
		target := items[idx]

		_, err := os.ReadFile(target.path)
		assert.NoError(b, err)
	}
}
