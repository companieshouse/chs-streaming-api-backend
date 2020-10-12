package transformer

import "github.com/companieshouse/chs-streaming-api-backend/model/json"

//Serialises the provided message into a format that can be consumed by stremaing API cache.
type Serialiser struct {
	resourceDataSerialiser Marshallable
	resultSerialiser       Marshallable
}

//The result of the operation.
type Result struct {
	Data   string `json:"data"`
	Offset int64  `json:"offset"`
}

//Describes an object capable of marshalling a data structure into a readable data exchange format.
type Marshallable interface {
	Marshal(input interface{}) ([]byte, error)
}

//Construct a new serialiser instance.
func NewSerialiser(resourceDataSerialiser Marshallable, resultSerialiser Marshallable) *Serialiser {
	return &Serialiser{
		resourceDataSerialiser: resourceDataSerialiser,
		resultSerialiser:       resultSerialiser,
	}
}

//Serialise the provided data structure into a readable data exchange format.
func (s *Serialiser) Serialise(jsonData *json.ResourceChangedData) (string, error) {
	transformedData, err := s.resourceDataSerialiser.Marshal(jsonData)
	if err != nil {
		return "", err
	}
	result, err := s.resultSerialiser.Marshal(&Result{Data: string(transformedData), Offset: jsonData.Event.Timepoint})
	if err != nil {
		return "", err
	}
	return string(result), nil
}
