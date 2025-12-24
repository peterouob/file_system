package utils

import "fmt"

type Result[T any] struct {
	value T
	err   error
}

func Ok[T any](v T) Result[T] {
	return Result[T]{value: v}
}

func Err[T any](err error) Result[T] {
	return Result[T]{err: err}
}

func From[T any](v T, err error) Result[T] {
	if err != nil {
		return Err[T](err)
	}
	return Ok(v)
}

func (r Result[T]) Unwrap() T {
	if r.err != nil {
		panic(r.err)
	}
	return r.value
}

func (r Result[T]) Expect(msg string) T {
	if r.err != nil {
		panic(fmt.Sprintf("%s: %v", msg, r.err))
	}
	return r.value
}

func (r Result[T]) ExpectNotPanic(msg string) Result[T] {
	if r.err != nil {
		return Err[T](fmt.Errorf("%s: %v", msg, r.err))
	}
	return Ok(r.value)
}
