package mandodb

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// Encode Buffer

type encbuf struct {
	B []byte
	C [binary.MaxVarintLen64]byte
}

func newEncbuf() *encbuf {
	return &encbuf{}
}

func (e *encbuf) Reset()        { e.B = e.B[:0] }
func (e *encbuf) Bytes() []byte { return e.B }
func (e *encbuf) Len() int      { return len(e.B) }

func (e *encbuf) MarshalUint8(b uint8) {
	e.B = append(e.B, b)
}

func (e *encbuf) MarshalUint16(u ...uint16) {
	for _, num := range u {
		binary.LittleEndian.PutUint16(e.C[:], num)
		e.B = append(e.B, e.C[:uint16Size]...)
	}
}

func (e *encbuf) MarshalUint32(u ...uint32) {
	for _, num := range u {
		binary.LittleEndian.PutUint32(e.C[:], num)
		e.B = append(e.B, e.C[:uint32Size]...)
	}
}

func (e *encbuf) MarshalUint64(u ...uint64) {
	for _, num := range u {
		binary.LittleEndian.PutUint64(e.C[:], num)
		e.B = append(e.B, e.C[:uint64Size]...)
	}
}

func (e *encbuf) MarshalBytes(b []byte) {
	e.B = append(e.B, b...)
}

func (e *encbuf) MarshalString(s string) {
	e.B = append(e.B, s...)
}

var ErrInvalidSize = errors.New("invalid size")

// Decode Buffer

type decbuf struct {
	err error
}

func newDecbuf() *decbuf {
	return &decbuf{}
}

func (d *decbuf) UnmarshalUint16(b []byte) uint16 {
	if len(b) < uint16Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (d *decbuf) UnmarshalUint32(b []byte) uint32 {
	if len(b) < uint32Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (d *decbuf) UnmarshalUint64(b []byte) uint64 {
	if len(b) < uint64Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *decbuf) UnmarshalString(b []byte) string {
	return yoloString(b)
}

func (d *decbuf) Err() error {
	return d.err
}

// 骚操作
func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
