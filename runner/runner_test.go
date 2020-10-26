package runner

import (
	"github.com/companieshouse/chs-streaming-api-backend/consumer"
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs.go/avro"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"testing"
)

type mockRunnable struct {
	mock.Mock
	done chan bool
}

func TestCreateNewFactoryInstance(t *testing.T) {
	Convey("Given a configuration object with all fields specified", t, func() {
		config := &Config{KafkaBroker: []string{"0.0.0.0"}, Topic: "topic", Schema: &avro.Schema{}}
		Convey("When a new runner instance is created", func() {
			actual := NewFactory(config)
			Convey("Then a new runner instance should be returned", func() {
				So(actual, ShouldNotBeNil)
				So(actual.kafkaBrokerAddr, ShouldResemble, config.KafkaBroker)
				So(actual.topic, ShouldEqual, config.Topic)
				So(actual.schema, ShouldEqual, config.Schema)
			})
		})
	})
}

func TestStartNewConsumer(t *testing.T) {
	Convey("Given a new runner instance has been created", t, func() {
		config := &Config{KafkaBroker: []string{"0.0.0.0"}, Topic: "topic", Schema: &avro.Schema{}}
		factory := NewFactory(config)
		done := make(chan bool)
		runnable := &mockRunnable{done: done}
		runnable.On("Run").Return()
		runnable.On("HasStarted").Return(true)
		factory.constructor = mockConsumerConstructor(runnable)
		Convey("When a new consumer instance is obtained", func() {
			actual, err := factory.StartConsumer(3)
			<-done
			Convey("Then a new consumer instance should be constructed from the provided details", func() {
				So(err, ShouldBeNil)
				So(actual, ShouldNotBeNil)
				So(actual.(*ConsumerController).runtime, ShouldEqual, runnable)
				So(runnable.AssertCalled(t, "Run"), ShouldBeTrue)
			})
		})
	})
}

func TestReturnErrorIfConsumerNotStarted(t *testing.T) {
	Convey("Given a new runner instance has been created", t, func() {
		config := &Config{KafkaBroker: []string{"0.0.0.0"}, Topic: "topic", Schema: &avro.Schema{}}
		factory := NewFactory(config)
		done := make(chan bool)
		runnable := &mockRunnable{done: done}
		runnable.On("Run").Return()
		runnable.On("HasStarted").Return(false)
		factory.constructor = mockConsumerConstructor(runnable)
		Convey("When a new consumer instance is obtained", func() {
			actual, err := factory.StartConsumer(3)
			<-done
			Convey("Then a new consumer instance should be constructed from the provided details", func() {
				So(err.Error(), ShouldEqual, "failed to start consumer")
				So(actual, ShouldBeNil)
				So(runnable.AssertCalled(t, "Run"), ShouldBeTrue)
			})
		})
	})
}

func (r *mockRunnable) Run() {
	r.Called()
	r.done <- true
}

func (r *mockRunnable) Shutdown(msg string) {
	r.Called(msg)
}

func (r *mockRunnable) HasStarted() bool {
	args := r.Called()
	return args.Bool(0)
}

func mockConsumerConstructor(r consumer.Runnable) func(consumer consumer.KafkaPartitionConsumable, messageTransformer consumer.Transformable, publisher consumer.Publishable, partition int32, offset int64, logger logger.Logger) consumer.Runnable {
	return func(consumer consumer.KafkaPartitionConsumable, messageTransformer consumer.Transformable, publisher consumer.Publishable, partition int32, offset int64, logger logger.Logger) consumer.Runnable {
		return r
	}
}
