package avro

//An incoming resource changed data message serialised as avro
type ResourceChangedData struct {
	ResourceKind string      `avro:"resource_kind"`
	ResourceURI  string      `avro:"resource_uri"`
	ContextID    string      `avro:"context_id"`
	ResourceID   string      `avro:"resource_id"`
	Data         string      `avro:"data"`
	Event        EventRecord `avro:"event"`
}

//Event metadata within a resource changed data message
type EventRecord struct {
	PublishedAt   string   `avro:"published_at"`
	Type          string   `avro:"type"`
	FieldsChanged []string `avro:"fields_changed"`
}
