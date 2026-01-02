package storage

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/peterouob/file_system/utils"
)

func setupDiskTest(t *testing.T) (*DiskStore, func()) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatal(err)
	}

	disk := NewDiskStore(WithRoot(dir), WithPathTransformFunc(FileTransform))

	teardown := func() {
		_ = os.RemoveAll(dir)
	}

	return disk, teardown
}

func TestDiskStorage(t *testing.T) {
	disk, teardown := setupDiskTest(t)
	defer teardown()

	key := "peter_picture"
	data := []byte("peter_picture_data")

	if disk.Has(key) {
		t.Fatalf("expect Has key %s, but got true", key)
	}

	write, err := disk.Write(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if !disk.Has(key) {
		t.Fatalf("expect Has key %s, but got false", key)
	}

	if write != int64(len(data)) {
		t.Errorf("write wrong: got %d, want %d", write, len(data))
	}

	_, r, err := disk.Read(key)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = r.Close()
	}()

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if string(b) != string(data) {
		t.Errorf("read wrong: got %s, want %s", string(b), string(data))
	}
}

func TestDiskWithCrypto(t *testing.T) {
	s, teardown := setupDiskTest(t)
	defer teardown()

	key := "peter_picture"
	data := "peter_picture_data"
	src := bytes.NewReader([]byte(data))
	encKey := utils.NewEncryptionKey()

	if _, err := s.WriteEncrypt(encKey, key, src); err != nil {
		t.Fatal(err)
	}

	rn, r, err := s.Read(key)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = r.Close()
	}()

	if rn == int64(len(data)) {
		t.Errorf("expect Read %d, but got %d", len(data), rn+1)
	}

	dst := new(bytes.Buffer)

	if _, err := s.ReadDecrypt(encKey, key, dst); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(dst.Bytes(), []byte(data)) {
		t.Errorf("read wrong: got %s, want %s", dst.String(), data)
	}
}

//func BenchmarkDiskStore_Write_Reader(b *testing.B) {
//	s, treadDown := setupDiskTest(&testing.T{})
//	defer treadDown()
//
//	key := "batch_key"
//	data := make([]byte, 1024*1024)
//
//	b.SetBytes(int64(len(data)))
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_, err := s.Write(key, bytes.NewReader(data))
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		_, r, err := s.Read(key)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		defer r.Close()
//
//	}
//}
