package mandodb

import (
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// BytesCompressorType 代表字节数据压缩算法类型
type BytesCompressorType int8

const (
	// NoopBytesCompressor 不压缩
	NoopBytesCompressor BytesCompressorType = iota

	// ZstdBytesCompressor 使用 ZSTD 算法压缩
	ZstdBytesCompressor

	// SnappyBytesCompressor 使用 Snappy 算法压缩
	SnappyBytesCompressor
)

// BytesCompressor 数据压缩器抽象接口
type BytesCompressor interface {
	Compress(src []byte) []byte
	Decompress(src []byte) ([]byte, error)
}

// ByteCompress 数据压缩
func ByteCompress(src []byte) []byte {
	return globalOpts.bytesCompressor.Compress(src)
}

// ByteDecompress 数据解压缩
func ByteDecompress(src []byte) ([]byte, error) {
	return globalOpts.bytesCompressor.Decompress(src)
}

type noopBytesCompressor struct{}

func newNoopBytesCompressor() BytesCompressor {
	return &noopBytesCompressor{}
}

func (c *noopBytesCompressor) Compress(src []byte) []byte {
	return src
}

func (c *noopBytesCompressor) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

type zstdBytesCompressor struct{}

func newZstdBytesCompressor() BytesCompressor {
	return &zstdBytesCompressor{}
}

func (c *zstdBytesCompressor) Compress(src []byte) []byte {
	var encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func (c *zstdBytesCompressor) Decompress(src []byte) ([]byte, error) {
	var decoder, _ = zstd.NewReader(nil)
	return decoder.DecodeAll(src, nil)
}

type snappyBytesCompressor struct{}

func newSnappyBytesCompressor() BytesCompressor {
	return &snappyBytesCompressor{}
}

func (c *snappyBytesCompressor) Compress(src []byte) []byte {
	return snappy.Encode(nil, src)
}

func (c *snappyBytesCompressor) Decompress(src []byte) ([]byte, error) {
	return snappy.Decode(nil, src)
}
