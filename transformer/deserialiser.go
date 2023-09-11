package transformer

import (
	"errors"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs-streaming-api-backend/model/avro"
	"github.com/companieshouse/chs-streaming-api-backend/model/json"
)

// Deserialises the provided message into a data structure.
type Deserialiser struct {
	messageDeserialiser Unmarshallable
	dataDeserialiser    Unmarshallable
}

// Describes an object capable of unmarshalling serialised data into a data structure.
type Unmarshallable interface {
	Unmarshal(input []byte, model interface{}) error
}

// Construct a new deserialiser instance.
func NewDeserialiser(messageDeserialiser Unmarshallable, dataDeserialiser Unmarshallable) *Deserialiser {
	return &Deserialiser{
		messageDeserialiser: messageDeserialiser,
		dataDeserialiser:    dataDeserialiser,
	}
}

// Deserialise provided data into a data structure consumed by streaming API frontend users.
func (a *Deserialiser) Deserialise(model *model.BackendEvent) (*json.ResourceChangedData, error) {
	avroData := avro.ResourceChangedData{}
	if err := a.messageDeserialiser.Unmarshal(model.Data, &avroData); err != nil {
		return nil, err
	}
	if len(avroData.Data) == 0 {
		return nil, errors.New("no message data provided")
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
	if err := a.dataDeserialiser.Unmarshal([]byte(avroData.Data), &jsonData.Data); err != nil {
		return nil, err
	}
	return &jsonData, nil
}
