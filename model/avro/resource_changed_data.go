package avro

//ResourceChangedData represent resource change avro schema
type ResourceChangedData struct {
	ResourceKind string      `avro:"resource_kind"`
	ResourceURI  string      `avro:"resource_uri"`
	ContextID    string      `avro:"context_id"`
	ResourceID   string      `avro:"resource_id"`
	Data         string      `avro:"data"`
	Event        EventRecord `avro:"event"`
}

//EventRecord represents event details within resource-changed and resource-changed-data schema
type EventRecord struct {
	PublishedAt   string   `avro:"published_at"`
	Type          string   `avro:"type"`
	FieldsChanged []string `avro:"fields_changed"`
}
