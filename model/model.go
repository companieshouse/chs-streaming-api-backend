package model

//Encapsulates information about a new offset that has been consumed by Kafka
type BackendEvent struct {
	Data   []byte
	Offset int64
}
