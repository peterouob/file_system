package errtype

import "errors"

var (
	ErrOverflow     = errors.New("convert overflow")
	ErrNoneNegative = errors.New("convert none negative")

	ErrNotFound    = errors.New("error for file not found")
	ErrMagicNumber = errors.New("error for file magic number")
	ErrCookie      = errors.New("error for file cookie")
	ErrDataDeleted = errors.New("error for file data is deleted")
	ErrCrcNotValid = errors.New("error for file crc not valid")

	ErrToLarge = errors.New("too large")
)
