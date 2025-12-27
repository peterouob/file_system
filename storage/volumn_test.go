package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setUp(t *testing.T, payload []byte) *Needle {

	needle := &Needle{
		Header: NeedleHeader{
			MagicHeader:  uint32(MagicHeader),
			Cookie:       0x1234567890ABCDEF,
			Key:          1001,
			AlternateKey: 2,
			Flag:         0,
			Size:         uint32(len(payload)),
		},
		Data: payload,
		Footer: NeedleFooter{
			MagicFooter: uint32(MagicFooter),
		},
	}

	return needle
}

func TestNeedle_Bytes(t *testing.T) {
	dataPayload := []byte("12345")
	needle := setUp(t, dataPayload)
	getBytes := needle.Bytes()
	assert.Equal(t, len(getBytes)%8, 0) // check align 8 byte

	// before the padding size is 29+5+8 = 42
	// we want be aligned to 8 byte so actually will be 48
	assert.Equal(t, len(getBytes), 48)

	magic := binary.BigEndian.Uint32(getBytes[0:4])
	assert.Equal(t, magic, needle.Header.MagicHeader)

	cookie := binary.BigEndian.Uint64(getBytes[4:12])
	assert.Equal(t, cookie, needle.Header.Cookie)

	key := binary.BigEndian.Uint64(getBytes[12:20])
	assert.Equal(t, key, needle.Header.Key)

	altKey := binary.BigEndian.Uint32(getBytes[20:24])
	assert.Equal(t, altKey, needle.Header.AlternateKey)

	flag := getBytes[24]
	assert.Equal(t, flag, needle.Header.Flag)

	size := binary.BigEndian.Uint32(getBytes[25:29])
	assert.Equal(t, size, needle.Header.Size)

	data := getBytes[29 : 29+len(dataPayload)]
	assert.Equal(t, data, dataPayload)

	crc := binary.BigEndian.Uint32(getBytes[29+len(dataPayload) : 29+len(dataPayload)+4])
	assert.Equal(t, crc, needle.Footer.Checksum)

	magic = binary.BigEndian.Uint32(getBytes[29+len(dataPayload)+4 : 29+len(dataPayload)+8])
	assert.Equal(t, magic, needle.Footer.MagicFooter)

	ps := 29 + len(dataPayload) + 8
	paddingArea := getBytes[ps:]
	expectedPaddingLen := (8 - (42 % 8)) % 8
	assert.Equal(t, expectedPaddingLen, len(paddingArea))

	for _, b := range paddingArea {
		assert.Equal(t, byte(0), b, "Padding byte should be zero")
	}
}
