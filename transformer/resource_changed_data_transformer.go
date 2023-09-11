package transformer

import (
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs-streaming-api-backend/model/json"
)

// Transforms a resource changed data message into a message used by streaming API cache.
type ResourceChangedDataTransformer struct {
	deserialiser Deserialisable
	serialiser   Serialisable
}

// Describes an object capable of deserialising an incoming resource changed data message into a data structure
// that will be consumed by streaming API frontend users.
type Deserialisable interface {
	Deserialise(model *model.BackendEvent) (*json.ResourceChangedData, error)
}

// Describes an object capable of serialising a resource changed data object into a message that can be used by the
// streaming API cache.
type Serialisable interface {
	Serialise(jsonData *json.ResourceChangedData) (string, error)
}

// Construct a new resource changed data transformer instance.
func NewResourceChangedDataTransformer(deserialiser Deserialisable, serialiser Serialisable) *ResourceChangedDataTransformer {
	return &ResourceChangedDataTransformer{
		deserialiser: deserialiser,
		serialiser:   serialiser,
	}
}

// Transform the provided resource changed data message into a format usable by streaming API cache.
func (t *ResourceChangedDataTransformer) Transform(model *model.BackendEvent) (string, error) {
	jsonData, err := t.deserialiser.Deserialise(model)
	if err != nil {
		return "", err
	}
	return t.serialiser.Serialise(jsonData)
}
