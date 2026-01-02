package utils

import "fmt"

func Must[T any](v T, err error) T {
	if err != nil {
		panic(fmt.Errorf("panic from MUST: %v", err))
	}
	return v
}
