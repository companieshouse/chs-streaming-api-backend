package transformer

import (
	"errors"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	rcd "github.com/companieshouse/chs-streaming-api-backend/model/json"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"testing"
)

type mockDeserialiser struct {
	mock.Mock
}

type mockSerialiser struct {
	mock.Mock
}

func TestCreateNewTransformerInstance(t *testing.T) {
	Convey("When a new transformer is created", t, func() {
		deserialiser := &mockDeserialiser{}
		serialiser := &mockSerialiser{}
		actual := NewResourceChangedDataTransformer(deserialiser, serialiser)
		Convey("Then a new transformer instance should be returned", func() {
			So(actual, ShouldHaveSameTypeAs, &ResourceChangedDataTransformer{})
			So(actual.deserialiser, ShouldEqual, deserialiser)
			So(actual.serialiser, ShouldEqual, serialiser)
		})
	})
}

func TestTransformResourceChangedDataMessage(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		event := &model.BackendEvent{
			Data:   []byte("data"),
			Offset: 3,
		}
		data := &rcd.ResourceChangedData{}
		deserialiser := &mockDeserialiser{}
		deserialiser.On("Deserialise", mock.Anything).Return(data, nil)
		serialiser := &mockSerialiser{}
		serialiser.On("Serialise", mock.Anything).Return([]byte("result"), nil)
		transformer := NewResourceChangedDataTransformer(deserialiser, serialiser)
		Convey("When a resource changed data message is transformed", func() {
			actual, err := transformer.Transform(event)
			Convey("Then the expected result should be returned", func() {
				So(actual, ShouldResemble, []byte("result"))
				So(err, ShouldBeNil)
				So(deserialiser.AssertCalled(t, "Deserialise", event), ShouldBeTrue)
				So(serialiser.AssertCalled(t, "Serialise", data), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfDeserialisationFails(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		expectedError := errors.New("something went wrong")
		event := &model.BackendEvent{
			Data:   []byte("data"),
			Offset: 3,
		}
		deserialiser := &mockDeserialiser{}
		deserialiser.On("Deserialise", mock.Anything).Return((*rcd.ResourceChangedData)(nil), expectedError)
		serialiser := &mockSerialiser{}
		transformer := NewResourceChangedDataTransformer(deserialiser, serialiser)
		Convey("When a resource changed data message is transformed", func() {
			actual, err := transformer.Transform(event)
			Convey("Then the expected result should be returned", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Deserialise", event), ShouldBeTrue)
				So(serialiser.AssertNotCalled(t, "Serialise", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfSerialisationFails(t *testing.T) {
	Convey("Given a new transformer has been created", t, func() {
		expectedError := errors.New("something went wrong")
		event := &model.BackendEvent{
			Data:   []byte("data"),
			Offset: 3,
		}
		data := &rcd.ResourceChangedData{}
		deserialiser := &mockDeserialiser{}
		deserialiser.On("Deserialise", mock.Anything).Return(data, nil)
		serialiser := &mockSerialiser{}
		serialiser.On("Serialise", mock.Anything).Return([]byte(nil), expectedError)
		transformer := NewResourceChangedDataTransformer(deserialiser, serialiser)
		Convey("When a resource changed data message is transformed", func() {
			actual, err := transformer.Transform(event)
			Convey("Then the expected result should be returned", func() {
				So(actual, ShouldBeNil)
				So(err, ShouldEqual, expectedError)
				So(deserialiser.AssertCalled(t, "Deserialise", event), ShouldBeTrue)
				So(serialiser.AssertCalled(t, "Serialise", data), ShouldBeTrue)
			})
		})
	})
}

func (d *mockDeserialiser) Deserialise(model *model.BackendEvent) (*rcd.ResourceChangedData, error) {
	args := d.Called(model)
	return args.Get(0).(*rcd.ResourceChangedData), args.Error(1)
}

func (s *mockSerialiser) Serialise(jsonData *rcd.ResourceChangedData) ([]byte, error) {
	args := s.Called(jsonData)
	return args.Get(0).([]byte), args.Error(1)
}
