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

type mockDeserialiser struct {
	mock.Mock
}

type mockAvroDeserialiser struct {
	mock.Mock
}

type mockSerialiser struct {
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
	Data: map[string]interface{}(nil),
	Event:        rcd.Event{
		FieldsChanged: []string{"Field"},
		Timepoint:     3,
		PublishedAt:   "PublishedAt",
		Type:          "Type",
	},
}

func TestCreateNewTransformerInstance(t *testing.T) {
	Convey("When a new transformer is created", t, func() {
		deserialiser := &mockDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		actual := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("Then a new transformer instance should be returned", func() {
			So(actual, ShouldHaveSameTypeAs, &ResourceChangedDataTransformer{})
			So(actual.deserialiser, ShouldEqual, deserialiser)
			So(actual.dataDeserialiser, ShouldEqual, dataDeserialiser)
			So(actual.resourceDataSerialiser, ShouldEqual, dataSerialiser)
			So(actual.responseSerialiser, ShouldEqual, responseSerialiser)
		})
	})
}

func TestConvertResourceChangedDataMessageToJson(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		deserialiser := &mockAvroDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		deserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(nil)
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte("dog"), nil)
		responseSerialiser.On("Marshal", mock.Anything).Return([]byte("result"), nil)
		transformer := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("When an incoming resource changed data message is transformed", func() {
			actual, err := transformer.Transform(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then the message should be deserialised with the deserialiser and reserialised with the serialiser", func() {
				So(actual, ShouldResemble, []byte("result"))
				So(err, ShouldBeNil)
				So(deserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
				So(dataSerialiser.AssertCalled(t, "Marshal", expectedJsonData), ShouldBeTrue)
				So(responseSerialiser.AssertCalled(t, "Marshal", &result{data: "dog", offset: 3}), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfMessageNotDeserialisable(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		deserialiser := &mockAvroDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		expectedError := errors.New("something went wrong")
		deserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(expectedError)
		transformer := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("When an unreadable message is transformed", func() {
			actual, err := transformer.Transform(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then an error should be returned", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertNotCalled(t, "Unmarshal", mock.Anything, mock.Anything), ShouldBeTrue)
				So(dataSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
				So(responseSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfDeserialisedMessageDataNotDeserialisable(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		deserialiser := &mockAvroDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		expectedError := errors.New("something went wrong")
		deserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser.On("Unmarshal", mock.Anything, mock.Anything).Return(expectedError)
		transformer := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("When an unreadable message is transformed", func() {
			actual, err := transformer.Transform(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then an error should be returned", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
				So(dataSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
				So(responseSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfDeserialisedMessageNotSerialisable(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		deserialiser := &mockAvroDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		expectedError := errors.New("something went wrong")
		deserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser.On("Unmarshal", mock.Anything, mock.Anything).Return(nil)
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte(nil), expectedError)
		transformer := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("When an incoming resource changed data message is transformed", func() {
			actual, err := transformer.Transform(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then the message should be deserialised with the deserialiser and reserialised with the serialiser", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
				So(dataSerialiser.AssertCalled(t, "Marshal", expectedJsonData), ShouldBeTrue)
				So(responseSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfMappedMessageNotSerialisable(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		deserialiser := &mockAvroDeserialiser{}
		dataDeserialiser := &mockDeserialiser{}
		dataSerialiser := &mockSerialiser{}
		responseSerialiser := &mockSerialiser{}
		expectedError := errors.New("something went wrong")
		deserialiser.On("Unmarshal", []byte("cat"), mock.Anything).Return(nil)
		dataDeserialiser.On("Unmarshal", []byte("Data"), mock.Anything).Return(nil)
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte("dog"), nil)
		responseSerialiser.On("Marshal", mock.Anything).Return([]byte(nil), expectedError)
		transformer := NewResourceChangedDataTransformer(deserialiser, dataDeserialiser, dataSerialiser, responseSerialiser)
		Convey("When an incoming resource changed data message is transformed", func() {
			actual, err := transformer.Transform(&model.BackendEvent{Data: []byte("cat"), Offset: 3})
			Convey("Then the message should be deserialised with the deserialiser and reserialised with the serialiser", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Unmarshal", []byte("cat"), &expectedAvroData), ShouldBeTrue)
				So(dataDeserialiser.AssertCalled(t, "Unmarshal", []byte("Data"), &expectedJsonData.Data), ShouldBeTrue)
				So(dataSerialiser.AssertCalled(t, "Marshal", expectedJsonData), ShouldBeTrue)
				So(responseSerialiser.AssertCalled(t, "Marshal", &result{data: "dog", offset: 3}), ShouldBeTrue)
			})
		})
	})
}

func (d *mockDeserialiser) Unmarshal(input []byte, output interface{}) error {
	args := d.Called(input, output)
	return args.Error(0)
}

func (d *mockAvroDeserialiser) Unmarshal(input []byte, output interface{}) error {
	args := d.Called(input, output)
	myOutput := output.(*avro.ResourceChangedData)
	*myOutput = expectedAvroData
	return args.Error(0)
}

func (s *mockSerialiser) Marshal(input interface{}) ([]byte, error) {
	args := s.Called(input)
	return args.Get(0).([]byte), args.Error(1)
}
