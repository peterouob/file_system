package storage

import (
	"encoding/binary"
)

type Neddler interface {
	Bytes() []byte
}

var _ Neddler = (*Needle)(nil)

const MagicHeader = 0x2DCF25 >> 1
const MagicFooter = 0x2DCF25 << 1

type NeedleHeader struct {
	MagicHeader  uint32 // 4 byte
	Cookie       uint64 // 8 byte
	Key          uint64 // 8 byte
	AlternateKey uint32 // 4 byte
	Flag         byte   // 1 byte
	Size         uint32 // 4 byte
}

type Needle struct {
	Header NeedleHeader
	Data   []byte
	Footer NeedleFooter
}

type NeedleFooter struct {
	Checksum    uint32 // 4
	MagicFooter uint32 // 4
}

const NeedleHeaderSize = 29
const NeedleFooterSize = 8

func (n *Needle) Bytes() []byte {
	totalSize := NeedleHeaderSize + len(n.Data) + NeedleFooterSize
	buf := make([]byte, 0, totalSize)

	buf = binary.BigEndian.AppendUint32(buf, n.Header.MagicHeader)
	buf = binary.BigEndian.AppendUint64(buf, n.Header.Cookie)
	buf = binary.BigEndian.AppendUint64(buf, n.Header.Key)
	buf = binary.BigEndian.AppendUint32(buf, n.Header.AlternateKey)
	buf = append(buf, n.Header.Flag)
	buf = binary.BigEndian.AppendUint32(buf, n.Header.Size)

	buf = append(buf, n.Data...)

	n.Footer.Checksum = NewCRC(buf).Value()
	buf = binary.BigEndian.AppendUint32(buf, n.Footer.Checksum)
	buf = binary.BigEndian.AppendUint32(buf, n.Footer.MagicFooter)

	paddingLen := (8 - (len(buf) % 8)) % 8

	for range paddingLen {
		buf = append(buf, 0x00)
	}

	return buf
}
