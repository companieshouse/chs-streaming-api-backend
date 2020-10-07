package json

//ResourceChangedData represents resource change data json unmarshal
type ResourceChangedData struct {
	ResourceKind string                 `json:"resource_kind"`
	ResourceURI  string                 `json:"resource_uri"`
	ResourceID   string                 `json:"resource_id"`
	Data         map[string]interface{} `json:"data"`
	Event        Event                  `json:"event"`
}

// Event - resource changed data event
type Event struct {
	FieldsChanged []string `json:"fields_changed,omitempty"`
	Timepoint     int64    `json:"timepoint"`
	PublishedAt   string   `json:"published_at"`
	Type          string   `json:"type"`
}