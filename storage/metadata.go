package storage

import (
	"encoding/json"
)

type MetaSerializerType int8

const (
	JsonMetaSerializer MetaSerializerType = iota
	BinaryMetaSerializer
)

type metaSeries struct {
	Sid         string `json:"sid"`
	LabelLen    uint64 `json:"labelLen"`
	StartOffset uint64 `json:"startOffset"`
	EndOffset   uint64 `json:"endOffset"`
}

type Metadata struct {
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
)

func newBinaryMetaSerializer() MetaSerializer {
	return &binaryMetaSerializer{}
}

func (s *binaryMetaSerializer) Marshal(meta Metadata) ([]byte, error) {
	encoder := newEncbuf()

	for _, series := range meta.Series {
		encoder.PutUint16(uint16(len(series.Sid)))
		encoder.PutString(series.Sid)
		encoder.PutUint64(series.LabelLen, series.StartOffset, series.EndOffset)
	}
	encoder.PutUint16(endOfBlock)

	for name, sids := range meta.Labels {
		encoder.PutUint16(uint16(len(name)))
		encoder.PutString(name)
		encoder.PutUint32(uint32(len(sids)))
		encoder.PutUint32(sids...)
	}
	encoder.PutUint16(endOfBlock)

	return encoder.Bytes(), nil
}

func (s *binaryMetaSerializer) Unmarshal(data []byte, meta *Metadata) error {
	offset := 0
	decoder := newDecbuf()

	rows := make([]metaSeries, 0)
	for {
		series := metaSeries{}
		sidLen := decoder.Uint16(data[offset : offset+uint16Size])
		offset += uint16Size

		if sidLen == endOfBlock {
			break
		}

		series.Sid = decoder.String(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		series.LabelLen = decoder.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.StartOffset = decoder.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.EndOffset = decoder.Uint64(data[offset : offset+uint64Size])
		offset += uint64Size

		rows = append(rows, series)
	}
	meta.Series = rows

	labels := make(map[string][]uint32)
	for {
		var sid string
		sidLen := decoder.Uint16(data[offset : offset+uint16Size])
		offset += uint16Size

		if sidLen == endOfBlock {
			break
		}

		sid = decoder.String(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		sidCnt := decoder.Uint32(data[offset : offset+uint32Size])
		offset += uint32Size

		sidLst := make([]uint32, sidCnt)
		for i := 0; i < int(sidCnt); i++ {
			sidLst[i] = decoder.Uint32(data[offset : offset+uint32Size])
			offset += uint32Size
		}
		labels[sid] = sidLst
	}

	if decoder.Err() != nil {
		return decoder.Err()
	}

	meta.Labels = labels
	return nil
}
