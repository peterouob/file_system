package storage

import "fmt"

type PathKey struct {
	FilePath string
	FileName string
}

type PathTransformFunc func(string) PathKey

func FileTransform(key string) PathKey {
	return PathKey{
		FilePath: key[:2],
		FileName: key[2:],
	}
}

func (p PathKey) GetFullPath() string {
	return fmt.Sprintf("%s/%s", p.FilePath, p.FileName)
}
