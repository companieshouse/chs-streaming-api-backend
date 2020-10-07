package transformer

import (
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs-streaming-api-backend/model/avro"
	"github.com/companieshouse/chs-streaming-api-backend/model/json"
)

type Serialisable interface {
	Marshal(input interface{}) ([]byte, error)
}

type Deserialisable interface {
	Unmarshal(input []byte, model interface{}) error
}

type ResourceChangedDataTransformer struct {
	deserialiser           Deserialisable
	dataDeserialiser       Deserialisable
	resourceDataSerialiser Serialisable
	responseSerialiser     Serialisable
}

type result struct {
	data   string
	offset int64
}

func NewResourceChangedDataTransformer(deserialisable Deserialisable, dataDeserialiser Deserialisable, resourceDataSerialiser Serialisable, responseSerialiser Serialisable) *ResourceChangedDataTransformer {
	return &ResourceChangedDataTransformer{
		deserialiser:           deserialisable,
		dataDeserialiser:       dataDeserialiser,
		resourceDataSerialiser: resourceDataSerialiser,
		responseSerialiser:     responseSerialiser,
	}
}

func (t *ResourceChangedDataTransformer) Transform(model *model.BackendEvent) ([]byte, error) {
	avroData := avro.ResourceChangedData{}
	if err := t.deserialiser.Unmarshal(model.Data, &avroData); err != nil {
		return nil, err
	}
	jsonData := json.ResourceChangedData{
		ResourceKind: avroData.ResourceKind,
		ResourceURI:  avroData.ResourceURI,
		ResourceID:   avroData.ResourceID,
		Event: json.Event{
			FieldsChanged: avroData.Event.FieldsChanged,
			Timepoint:     model.Offset,
			PublishedAt:   avroData.Event.PublishedAt,
			Type:          avroData.Event.Type,
		},
	}
	if err := t.dataDeserialiser.Unmarshal([]byte(avroData.Data), &jsonData.Data); err != nil {
		return nil, err
	}
	transformedData, err := t.resourceDataSerialiser.Marshal(jsonData)
	if err != nil {
		return nil, err
	}
	return t.responseSerialiser.Marshal(&result{data: string(transformedData), offset: model.Offset})
}
