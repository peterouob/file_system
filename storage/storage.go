package storage

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/peterouob/file_system/crypto"
)

type DiskStore struct {
	Opts
}

const (
	defaultRoot = "root"
)

func defaultPathTransformFunc(key string) PathKey {
	return PathKey{
		FilePath: key,
		FileName: key,
	}
}

func NewDiskStore(options ...Option) *DiskStore {
	opts := Opts{
		Root:              defaultRoot,
		PathTransformFunc: defaultPathTransformFunc,
	}

	for _, opt := range options {
		opt(&opts)
	}

	return &DiskStore{
		opts,
	}
}

type Store interface {
	Has(string) bool
	Write(key string, r io.Reader) (int64, error)
	Read(key string) (int64, io.ReadCloser, error)
}

var _ Store = (*DiskStore)(nil)

func (s *DiskStore) Has(key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.GetFullPath())

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *DiskStore) openWriteFile(key string) (*os.File, error) {
	path := s.PathTransformFunc(key)
	pathWithRoot := fmt.Sprintf("%s/%s", s.Root, path.FilePath)

	if err := os.MkdirAll(pathWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, path.GetFullPath())
	f, err := os.Create(fullPathWithRoot)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *DiskStore) openReadFile(key string) (*os.File, error) {
	path := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.Root, path.GetFullPath())

	f, err := os.Open(fullPathWithRoot)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *DiskStore) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *DiskStore) writeStream(key string, r io.Reader) (int64, error) {
	f, err := s.openWriteFile(key)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = f.Close()
	}()

	return io.Copy(f, r)
}

func (s *DiskStore) WriteEncrypt(encKey []byte, key string, r io.Reader) (int64, error) {
	f, err := s.openWriteFile(key)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = f.Close()
	}()

	n, err := crypto.CopyEnCrypto(encKey, r, f)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func (s *DiskStore) Read(key string) (int64, io.ReadCloser, error) {
	return s.readStream(key)
}

func (s *DiskStore) readStream(key string) (int64, io.ReadCloser, error) {
	f, err := s.openReadFile(key)
	if err != nil {
		return 0, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), f, nil
}

func (s *DiskStore) ReadDecrypt(encKey []byte, key string, d io.Writer) (int64, error) {
	f, err := s.openReadFile(key)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = f.Close()
	}()
	n, err := crypto.CopyDeCrypto(encKey, f, d)
	if err != nil {
		return 0, err
	}

	return int64(n), nil
}
