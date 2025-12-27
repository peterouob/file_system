package storage

import (
	"testing"
)

func TestCRC32(t *testing.T) {
	var paloyd = []byte("thisistestcrc32")
	crc := NewCRC(paloyd).Value()
	paloyd = append(paloyd, "wrong"...)
	crc2 := NewCRC(paloyd).Value()

	if crc == crc2 {
		t.Logf("test crc wrong, need = %d, got = %d", crc, crc2)
	}

	paloyd = []byte("thisistestcrc32")
	crc3 := NewCRC(paloyd).Value()

	if crc != crc3 {
		t.Logf("test crc wrong, need = %d, got = %d", crc, crc3)
	}
}
