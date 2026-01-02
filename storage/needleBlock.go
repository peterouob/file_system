package storage

import (
	"encoding/binary"

	errtype "github.com/peterouob/file_system/type"
	"github.com/peterouob/file_system/utils"
)

const MagicHeader = 0x2DCF25 >> 1
const MagicFooter = 0x2DCF25 << 1

/*
NeedleHeader
+---------------+----------+----------+--------------+------+--------+
| MagicHeader   | Cookie   | Key      | AlternateKey | Flag | Size   |
| 4 bytes       | 8 bytes  | 8 bytes  | 4 bytes      | 1    | 4 bytes|
+---------------+----------+----------+--------------+------+--------+
*/
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

/*
	NeedleFooter

+-----------+-------------+
| Checksum  | MagicFooter |
| 4 bytes   | 4 bytes     |
+-----------+-------------+
*/
type NeedleFooter struct {
	Checksum    uint32 // 4
	MagicFooter uint32 // 4
}

const NeedleHeaderSize = 29
const NeedleFooterSize = 8

func (n *Needle) Bytes(bp *BufferPool) *Buffer {
	totalSize := NeedleHeaderSize + len(n.Data) + NeedleFooterSize

	size := utils.Must(utils.CIU32(totalSize))

	buf, _ := bp.Get(size)

	buf.B = binary.BigEndian.AppendUint32(buf.B, n.Header.MagicHeader)
	buf.B = binary.BigEndian.AppendUint64(buf.B, n.Header.Cookie)
	buf.B = binary.BigEndian.AppendUint64(buf.B, n.Header.Key)
	buf.B = binary.BigEndian.AppendUint32(buf.B, n.Header.AlternateKey)
	buf.B = append(buf.B, n.Header.Flag)
	buf.B = binary.BigEndian.AppendUint32(buf.B, n.Header.Size)

	buf.B = append(buf.B, n.Data...)

	n.Footer.Checksum = NewCRC(buf.B).Value()
	buf.B = binary.BigEndian.AppendUint32(buf.B, n.Footer.Checksum)
	buf.B = binary.BigEndian.AppendUint32(buf.B, n.Footer.MagicFooter)

	paddingLen := (8 - (len(buf.B) % 8)) % 8

	if paddingLen > 0 {
		for range paddingLen {
			buf.B = append(buf.B, 0)
		}
	}

	return buf
}

func ValidNeedleBlock(buf []byte, cookie uint64) error {
	if len(buf) < 25 {
		return errtype.ErrBufferTooSmall
	}

	if binary.BigEndian.Uint32(buf[:4]) != MagicHeader {
		return errtype.ErrMagicNumber
	}

	if binary.BigEndian.Uint64(buf[4:12]) != cookie {
		return errtype.ErrCookie
	}

	if buf[24] == 1 {
		return errtype.ErrDataDeleted
	}

	return nil
}

func GetNeedleBlockInfo(totalSize, metaSize uint32, buf []byte) ([]byte, error) {
	dataWithHeader := buf[:NeedleHeaderSize+metaSize]
	footer := buf[totalSize-NeedleFooterSize:]
	crc := binary.BigEndian.Uint32(footer[0:4])

	if NewCRC(dataWithHeader).Value() != crc {
		return nil, errtype.ErrCrcNotValid
	}

	data := dataWithHeader[NeedleHeaderSize : NeedleHeaderSize+metaSize]

	return data, nil
}
