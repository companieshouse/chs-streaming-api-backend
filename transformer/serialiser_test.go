package transformer

import (
	"errors"
	rcd "github.com/companieshouse/chs-streaming-api-backend/model/json"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"testing"
)

type mockDataSerialiser struct {
	mock.Mock
}

type mockResultSerialiser struct {
	mock.Mock
}

func TestCreateNewSerialiser(t *testing.T) {
	Convey("When a new serialiser instance is created", t, func() {
		dataSerialiser := &mockDataSerialiser{}
		resultSerialiser := &mockResultSerialiser{}
		actual := NewSerialiser(dataSerialiser, resultSerialiser)
		Convey("Then a new serialiser refereence should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.resourceDataSerialiser, ShouldEqual, dataSerialiser)
			So(actual.resultSerialiser, ShouldEqual, resultSerialiser)
		})
	})
}

func TestSerialiseResourceChangedDataMessage(t *testing.T) {
	Convey("Given a new serialiser instance", t, func() {
		dataSerialiser := &mockDataSerialiser{}
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte("data"), nil)
		resultSerialiser := &mockResultSerialiser{}
		resultSerialiser.On("Marshal", mock.Anything).Return([]byte("result"), nil)
		data := &rcd.ResourceChangedData{Event: rcd.Event{Timepoint: 3}}
		serialiser := NewSerialiser(dataSerialiser, resultSerialiser)
		Convey("When a resource changed data message is serialised as a result", func() {
			actual, err := serialiser.Serialise(data)
			Convey("Then the serialised result should be returned", func() {
				So(actual, ShouldNotBeNil)
				So(err, ShouldBeNil)
				So(actual, ShouldResemble, "result")
				So(dataSerialiser.AssertCalled(t, "Marshal", data), ShouldBeTrue)
				So(resultSerialiser.AssertCalled(t, "Marshal", &result{
					data:   "data",
					offset: 3,
				}), ShouldBeTrue)
			})
		})
	})
}

func TestRaiseErrorIfDataCannotBeSerialised(t *testing.T) {
	Convey("Given a new serialiser instance", t, func() {
		expectedError := errors.New("something went wrong")
		dataSerialiser := &mockDataSerialiser{}
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte(nil), expectedError)
		resultSerialiser := &mockResultSerialiser{}
		data := &rcd.ResourceChangedData{Event: rcd.Event{Timepoint: 3}}
		serialiser := NewSerialiser(dataSerialiser, resultSerialiser)
		Convey("When a resource changed data message is serialised as a result", func() {
			_, err := serialiser.Serialise(data)
			Convey("Then the serialised result should be returned", func() {
				So(err, ShouldEqual, expectedError)
				So(dataSerialiser.AssertCalled(t, "Marshal", data), ShouldBeTrue)
				So(resultSerialiser.AssertNotCalled(t, "Marshal", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func TestRaiseErrorIfResultCannotBeSerialised(t *testing.T) {
	Convey("Given a new serialiser instance", t, func() {
		expectedError := errors.New("something went wrong")
		dataSerialiser := &mockDataSerialiser{}
		dataSerialiser.On("Marshal", mock.Anything).Return([]byte("data"), nil)
		resultSerialiser := &mockResultSerialiser{}
		resultSerialiser.On("Marshal", mock.Anything).Return([]byte(nil), expectedError)
		data := &rcd.ResourceChangedData{Event: rcd.Event{Timepoint: 3}}
		serialiser := NewSerialiser(dataSerialiser, resultSerialiser)
		Convey("When a resource changed data message is serialised as a result", func() {
			_, err := serialiser.Serialise(data)
			Convey("Then the serialised result should be returned", func() {
				So(err, ShouldEqual, expectedError)
				So(dataSerialiser.AssertCalled(t, "Marshal", data), ShouldBeTrue)
				So(resultSerialiser.AssertCalled(t, "Marshal", &result{
					data:   "data",
					offset: 3,
				}), ShouldBeTrue)
			})
		})
	})
}

func (s *mockDataSerialiser) Marshal(input interface{}) ([]byte, error) {
	args := s.Called(input)
	return args.Get(0).([]byte), args.Error(1)
}

func (s *mockResultSerialiser) Marshal(input interface{}) ([]byte, error) {
	args := s.Called(input)
	return args.Get(0).([]byte), args.Error(1)
}
