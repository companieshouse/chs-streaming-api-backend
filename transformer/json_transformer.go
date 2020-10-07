package transformer

import "encoding/json"

type JsonTransformer struct {

}

func (t JsonTransformer) Unmarshal(input []byte, target interface{}) error {
	return json.Unmarshal(input, target)
}

func (t JsonTransformer) Marshal(input interface{}) ([]byte, error) {
	return json.Marshal(input)
}

func NewJsonTransformer() *JsonTransformer {
	return &JsonTransformer{}
}