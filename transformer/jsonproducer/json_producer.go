package jsonproducer

import "encoding/json"

//Produces readable messages from a provided data structure and converts supplied messages into a data structure.
type JsonProducer struct {
}

var jsonProducer JsonProducer

//Unmarshals provided JSON into the given object reference.
func (t *JsonProducer) Unmarshal(input []byte, target interface{}) error {
	return json.Unmarshal(input, target)
}

//Marshals the provided object into JSON.
func (t *JsonProducer) Marshal(input interface{}) ([]byte, error) {
	return json.Marshal(input)
}

//Return a JsonProducer instance that can be used to marshal and unmarshal JSON.
func Instance() *JsonProducer {
	return &jsonProducer
}
