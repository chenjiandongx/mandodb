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
	endOfBlock uint8 = 0xff
	uint8Size        = 1
	uint16Size       = 2
	uint32Size       = 4
	uint64Size       = 8

	magic = "https://github.com/chenjiandongx/mandodb"
)

func newBinaryMetaSerializer() MetaSerializer {
	return &binaryMetaSerializer{}
}

func (s *binaryMetaSerializer) Marshal(meta Metadata) ([]byte, error) {
	encf := newEncbuf()

	for _, series := range meta.Series {
		encf.MarshalUint8(uint8(len(series.Sid)))
		encf.MarshalString(series.Sid)
		encf.MarshalUint64(series.LabelLen, series.StartOffset, series.EndOffset)
	}
	encf.MarshalUint8(endOfBlock)

	for name, sids := range meta.Labels {
		encf.MarshalUint8(uint8(len(name)))
		encf.MarshalString(name)
		encf.MarshalUint32(uint32(len(sids)))
		encf.MarshalUint32(sids...)
	}
	encf.MarshalUint8(endOfBlock)
	encf.MarshalUint64(uint64(meta.MinTs))
	encf.MarshalUint64(uint64(meta.MaxTs))
	encf.MarshalString(magic)

	return encf.Bytes(), nil
}

func (s *binaryMetaSerializer) Unmarshal(data []byte, meta *Metadata) error {
	if len(data) < len(magic) {
		return ErrInvalidSize
	}

	decf := newDecbuf()
	// 检验数据完整性
	if decf.UnmarshalString(data[len(data)-len(magic):]) != magic {
		return ErrInvalidSize
	}

	offset := 0
	rows := make([]metaSeries, 0)
	for {
		series := metaSeries{}

		sidLen := data[offset]
		offset += uint8Size

		if sidLen == endOfBlock {
			break
		}

		series.Sid = decf.UnmarshalString(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		series.LabelLen = decf.UnmarshalUint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.StartOffset = decf.UnmarshalUint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.EndOffset = decf.UnmarshalUint64(data[offset : offset+uint64Size])
		offset += uint64Size

		rows = append(rows, series)
	}
	meta.Series = rows

	labels := make(map[string][]uint32)
	for {
		var sid string

		sidLen := data[offset]
		offset += uint8Size

		if sidLen == endOfBlock {
			break
		}

		sid = decf.UnmarshalString(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		sidCnt := decf.UnmarshalUint32(data[offset : offset+uint32Size])
		offset += uint32Size

		sidLst := make([]uint32, sidCnt)
		for i := 0; i < int(sidCnt); i++ {
			sidLst[i] = decf.UnmarshalUint32(data[offset : offset+uint32Size])
			offset += uint32Size
		}
		labels[sid] = sidLst
	}

	meta.MinTs = int64(decf.UnmarshalUint64(data[offset : offset+uint64Size]))
	offset += uint64Size

	meta.MaxTs = int64(decf.UnmarshalUint64(data[offset : offset+uint64Size]))
	offset += uint64Size

	if decf.Err() != nil {
		return decf.Err()
	}

	meta.Labels = labels
	return nil
}
