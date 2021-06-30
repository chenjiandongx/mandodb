package storage

import (
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

type BytesCompressorType int8

const (
	NoopBytesCompressor BytesCompressorType = iota
	ZstdBytesCompressor
	SnappyBytesCompressor
)

type BytesCompressor interface {
	Compress(src []byte) []byte
	Decompress(src []byte) ([]byte, error)
}

type noopBytesCompressor struct{}

func (c *noopBytesCompressor) Compress(src []byte) []byte {
	return src
}

func (c *noopBytesCompressor) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type zstdBytesCompressor struct{}

func (c *zstdBytesCompressor) Compress(src []byte) []byte {
	var encoder, _ = zstd.NewWriter(nil)
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func (c *zstdBytesCompressor) Decompress(src []byte) ([]byte, error) {
	var decoder, _ = zstd.NewReader(nil)
	return decoder.DecodeAll(src, nil)
}

type snappyBytesCompressor struct{}

func (c *snappyBytesCompressor) Compress(src []byte) []byte {
	return snappy.Encode(nil, src)
}

func (c *snappyBytesCompressor) Decompress(src []byte) ([]byte, error) {
	return snappy.Decode(nil, src)
}
