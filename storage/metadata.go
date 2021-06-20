package storage

import (
	"encoding/json"
)

type MetaSerializerType int8

const (
	JsonMetaSerializer MetaSerializerType = iota
	BinaryMetaSerializer
)

var defaultMetaSerializer = &binaryMetaSerializer{}

func MarshalMeta(meta Metadata) ([]byte, error) {
	return defaultMetaSerializer.Marshal(meta)
}

func UnmarshalMeta(data []byte, meta *Metadata) error {
	return defaultMetaSerializer.Unmarshal(data, meta)
}

type metaSeries struct {
	Sid         string `json:"sid"`
	LabelLen    uint64 `json:"labelLen"`
	StartOffset uint64 `json:"startOffset"`
	EndOffset   uint64 `json:"endOffset"`
}

type Metadata struct {
	MinTs  int64               `json:"minTs"`
	MaxTs  int64               `json:"maxTs"`
	Series []metaSeries        `json:"series"`
	Labels map[string][]uint32 `json:"labels"`
}

type MetaSerializer interface {
	Marshal(Metadata) ([]byte, error)
	Unmarshal([]byte, *Metadata) error
}

type jsonMetaSerializer struct{}

func newJSONMetaSerializer() MetaSerializer {
	return &jsonMetaSerializer{}
}

func (s *jsonMetaSerializer) Marshal(meta Metadata) ([]byte, error) {
	return json.Marshal(meta)
}

func (s *jsonMetaSerializer) Unmarshal(data []byte, meta *Metadata) error {
	return json.Unmarshal(data, meta)
}

type binaryMetaSerializer struct{}

const (
	endOfBlock uint16 = 0xffff
	uint16Size        = 2
	uint32Size        = 4
	uint64Size        = 8

	magic = "https://github.com/chenjiandongx/mandodb"
)

func newBinaryMetaSerializer() MetaSerializer {
	return &binaryMetaSerializer{}
}

func (s *binaryMetaSerializer) Marshal(meta Metadata) ([]byte, error) {
	encf := newEncbuf()

	for _, series := range meta.Series {
		encf.PutUint16(uint16(len(series.Sid)))
		encf.PutString(series.Sid)
		encf.PutUint64(series.LabelLen, series.StartOffset, series.EndOffset)
	}
	encf.PutUint16(endOfBlock)

	for name, sids := range meta.Labels {
		encf.PutUint16(uint16(len(name)))
		encf.PutString(name)
		encf.PutUint32(uint32(len(sids)))
		encf.PutUint32(sids...)
	}
	encf.PutUint16(endOfBlock)
	encf.PutUint64(uint64(meta.MinTs))
	encf.PutUint64(uint64(meta.MaxTs))
	encf.PutBytes([]byte(magic))

	return encf.Bytes(), nil
}

func (s *binaryMetaSerializer) Unmarshal(data []byte, meta *Metadata) error {
	if len(data) < len(magic) {
		return ErrInvalidSize
	}

	decf := newDecbuf()
	// 检验数据完整性
	if decf.String(data[len(data)-len(magic):]) != magic {
		return ErrInvalidSize
	}

	offset := 0
	rows := make([]metaSeries, 0)
	for {
		series := metaSeries{}

		sidLen := decf.Uint16(data[offset : offset+uint16Size])
		offset += uint16Size

		if sidLen == endOfBlock {
			break
		}

		series.Sid = decf.String(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		series.LabelLen = decf.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.StartOffset = decf.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.EndOffset = decf.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		rows = append(rows, series)
	}
	meta.Series = rows

	labels := make(map[string][]uint32)
	for {
		var sid string

		sidLen := decf.Uint16(data[offset : offset+uint16Size])
		offset += uint16Size

		if sidLen == endOfBlock {
			break
		}

		sid = decf.String(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		sidCnt := decf.Uint32(data[offset : offset+uint32Size])
		offset += uint32Size

		sidLst := make([]uint32, sidCnt)
		for i := 0; i < int(sidCnt); i++ {
			sidLst[i] = decf.Uint32(data[offset : offset+uint32Size])
			offset += uint32Size
		}
		labels[sid] = sidLst
	}

	meta.MinTs = int64(decf.Uint64(data[offset : offset+uint64Size]))
	offset += uint64Size

	meta.MaxTs = int64(decf.Uint64(data[offset : offset+uint64Size]))
	offset += uint64Size

	if decf.Err() != nil {
		return decf.Err()
	}

	meta.Labels = labels
	return nil
}
