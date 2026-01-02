package storage

import (
	"encoding/binary"
)

const MagicHeader = 0x2DCF25 >> 1
const MagicFooter = 0x2DCF25 << 1

type NeedleHeader struct {
	Cookie       uint64
	Key          uint64
	MagicHeader  uint32
	AlternateKey uint32
	Size         uint32
	Flag         byte
}

type Needle struct {
	Data   []byte
	Header NeedleHeader
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
