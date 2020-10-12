package jsonproducer

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type greeting struct {
	Greeting string `json:"greeting"`
}

func TestCreateNewJsonTransformerInstance(t *testing.T) {
	Convey("When a new JsonProducer instance is created", t, func() {
		actual := Instance()
		Convey("Then a new JsonProducer instance should be returned", func() {
			So(actual, ShouldHaveSameTypeAs, &JsonProducer{})
		})
	})
}

func TestUnmarshalByteArrayIntoStruct(t *testing.T) {
	Convey("Given a new JsonProducer instance has been created", t, func() {
		transformer := Instance()
		actual := &greeting{}
		Convey("When valid JSON is unmarshalled", func() {
			err := transformer.Unmarshal([]byte(`{"greeting": "hello"}`), actual)
			Convey("Then the result should be deserialised into the provided struct", func() {
				So(err, ShouldBeNil)
				So(actual.Greeting, ShouldEqual, "hello")
			})
		})
	})
}

func TestReturnErrorIfJsonMalformed(t *testing.T) {
	Convey("Given a new JsonProducer instance has been created", t, func() {
		transformer := Instance()
		actual := &greeting{}
		Convey("When malformed JSON is unmarshalled", func() {
			err := transformer.Unmarshal([]byte(`:)`), actual)
			Convey("Then the result should be deserialised into the provided struct", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMarshalStructIntoByteArray(t *testing.T) {
	Convey("Given a new JsonProducer instance has been created", t, func() {
		transformer := Instance()
		Convey("When an object is marshalled", func() {
			actual, err := transformer.Marshal(&greeting{Greeting: "hello"})
			Convey("Then a serialised representation should be returned and no errors should be raised", func() {
				So(err, ShouldBeNil)
				So(actual, ShouldResemble, []byte(`{"greeting":"hello"}`))
			})
		})
	})
}

func TestReturnErrorIfJsonCannotBeMarshaled(t *testing.T) {
	Convey("Given a new JsonProducer instance has been created", t, func() {
		transformer := Instance()
		Convey("When an unserialisable object is marshalled", func() {
			_, err := transformer.Marshal(func() {})
			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestSingletonInstanceReturned(t *testing.T) {
	Convey("Given a new JsonProducer instance has already been created", t, func() {
		transformer := Instance()
		Convey("When further JsonProducer instances are created", func() {
			actual := Instance()
			Convey("Then the same instance should be returned", func() {
				So(transformer, ShouldPointTo, actual)
			})
		})
	})
}
