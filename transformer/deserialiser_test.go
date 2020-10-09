package transformer

import (
	"errors"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs-streaming-api-backend/model/avro"
	rcd "github.com/companieshouse/chs-streaming-api-backend/model/json"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"testing"
)

type mockMessageDeserialiser struct {
	mock.Mock
	dataAbsent bool
}

type mockDataDeserialiser struct {
	mock.Mock
}

var expectedAvroData = avro.ResourceChangedData{
	ResourceKind: "ResourceKind",
	ResourceURI:  "ResourceURI",
	ContextID:    "ContextID",
	ResourceID:   "ResourceID",
	Data:         "Data",
	Event: avro.EventRecord{
		PublishedAt:   "PublishedAt",
		Type:          "Type",
		FieldsChanged: []string{"Field"},
	},
}

var expectedJsonData = rcd.ResourceChangedData{
	ResourceKind: "ResourceKind",
	ResourceURI:  "ResourceURI",
	ResourceID:   "ResourceID",
	Data:         map[string]interface{}(nil),
	Event: rcd.Event{
		FieldsChanged: []string{"Field"},
		Timepoint:     3,
		PublishedAt:   "PublishedAt",
		Type:          "Type",
	},
}

func TestCreateNewDeserialiser(t *testing.T) {
	Convey("When a new deserialiser instance is created", t, func() {
		messageDeserialiser := &mockMessageDeserialiser{}
		dataDeserialiser := &mockDataDeserialiser{}
		actual := NewDeserialiser(messageDeserialiser, dataDeserialiser)
		Convey("Then a new deserialiser reference should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.messageDeserialiser, ShouldEqual, messageDeserialiser)
			So(actual.dataDeserialiser, ShouldEqual, dataDeserialiser)
		})
	})
}

func TestDeserialiseIncomingMessage(t *testing.T) {
	Convey("Given a new deserialiser instance", t, func() {
		messageDeserialiser := &mockMessageDeserialiser{}
		messageDeserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser := &mockDataDeserialiser{}
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(nil)
		deserialiser := NewDeserialiser(messageDeserialiser, dataDeserialiser)
		Convey("When an incoming message is deserialised", func() {
			actual, err := deserialiser.Deserialise(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then it should be deserialised into the expected data structure", func() {
				So(err, ShouldBeNil)
				So(actual, ShouldResemble, &expectedJsonData)
				So(messageDeserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfMessageNotDeserialisable(t *testing.T) {
	Convey("Given a new deserialiser instance", t, func() {
		expectedError := errors.New("something went wrong")
		messageDeserialiser := &mockMessageDeserialiser{}
		messageDeserialiser.On("Unmarshal", []byte("error"), mock.Anything).Return(expectedError)
		dataDeserialiser := &mockDataDeserialiser{}
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(nil)
		deserialiser := NewDeserialiser(messageDeserialiser, dataDeserialiser)
		Convey("When an unreadable message is transformed", func() {
			actual, err := deserialiser.Deserialise(&model.BackendEvent{Data: []byte("error"), Offset: 3})
			Convey("Then an error should be returned", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(messageDeserialiser.AssertCalled(t, "Unmarshal", []byte("error"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertNotCalled(t, "Unmarshal", mock.Anything, mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfMessageDataAbsent(t *testing.T) {
	Convey("Given a new deserialiser instance", t, func() {
		messageDeserialiser := &mockMessageDeserialiser{dataAbsent: true}
		messageDeserialiser.On("Unmarshal", []byte("error"), mock.Anything).Return(nil)
		dataDeserialiser := &mockDataDeserialiser{}
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(nil)
		deserialiser := NewDeserialiser(messageDeserialiser, dataDeserialiser)
		avroData := expectedAvroData
		avroData.Data = ""
		Convey("When an unreadable message is transformed", func() {
			actual, err := deserialiser.Deserialise(&model.BackendEvent{Data: []byte("error"), Offset: 3})
			Convey("Then an error should be returned", func() {
				So(actual, ShouldBeNil)
				So(err.Error(), ShouldEqual, "no message data provided")
				So(messageDeserialiser.AssertCalled(t, "Unmarshal", []byte("error"), &avroData), ShouldBeTrue)
				So(dataDeserialiser.AssertNotCalled(t, "Unmarshal", mock.Anything, mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfDeserialisedMessageDataNotDeserialisable(t *testing.T) {
	Convey("Given a new deserialiser instance", t, func() {
		expectedError := errors.New("something went wrong")
		messageDeserialiser := &mockMessageDeserialiser{}
		messageDeserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser := &mockDataDeserialiser{}
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(expectedError)
		deserialiser := NewDeserialiser(messageDeserialiser, dataDeserialiser)
		Convey("When an incoming message is deserialised", func() {
			actual, err := deserialiser.Deserialise(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then it should be deserialised into the expected data structure", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(messageDeserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
			})
		})
	})
}

func (d *mockMessageDeserialiser) Unmarshal(input []byte, output interface{}) error {
	args := d.Called(input, output)
	myOutput := output.(*avro.ResourceChangedData)
	avroData := expectedAvroData
	if d.dataAbsent {
		avroData.Data = ""
	}
	*myOutput = avroData
	return args.Error(0)
}

func (d *mockDataDeserialiser) Unmarshal(input []byte, output interface{}) error {
	args := d.Called(input, output)
	return args.Error(0)
}
