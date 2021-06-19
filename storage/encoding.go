package storage

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// Encode Buff
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

func (e *encbuf) PutUint16(nums ...uint16) {
	for _, num := range nums {
		binary.LittleEndian.PutUint16(e.C[:], num)
		e.B = append(e.B, e.C[:uint16Size]...)
	}
}

func (e *encbuf) PutUint32(nums ...uint32) {
	for _, num := range nums {
		binary.LittleEndian.PutUint32(e.C[:], num)
		e.B = append(e.B, e.C[:uint32Size]...)
	}
}

func (e *encbuf) PutUint64(nums ...uint64) {
	for _, num := range nums {
		binary.LittleEndian.PutUint64(e.C[:], num)
		e.B = append(e.B, e.C[:uint64Size]...)
	}
}

func (e *encbuf) PutString(s string) {
	e.B = append(e.B, s...)
}

func (e *encbuf) PutBytes(b []byte) {
	e.B = append(e.B, b...)
}

var (
	ErrInvalidSize = errors.New("invalid size")
)

// Decode Buffer
type decbuf struct {
	err error
}

func newDecbuf() *decbuf {
	return &decbuf{}
}

func (d *decbuf) Uint16(b []byte) uint16 {
	if len(b) < uint16Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (d *decbuf) Uint32(b []byte) uint32 {
	if len(b) < uint32Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (d *decbuf) Uint64(b []byte) uint64 {
	if len(b) < uint64Size {
		d.err = ErrInvalidSize
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *decbuf) String(b []byte) string {
	return yoloString(b)
}

func (d *decbuf) Err() error {
	return d.err
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
