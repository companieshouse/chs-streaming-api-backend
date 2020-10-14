package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"net/http"
	"sync"
	"testing"
)

type mockKafkaConsumer struct {
	mock.Mock
}

type mockTransformer struct {
	mock.Mock
}

type mockPublisher struct {
	mock.Mock
}

type mockLogger struct {
	mock.Mock
}

func TestCreateNewConsumer(t *testing.T) {
	Convey("When a new consumer instance is created", t, func() {
		actual := NewConsumer(&mockKafkaConsumer{}, &mockTransformer{}, &mockPublisher{}, 1, -1, &mockLogger{}).(*KafkaMessageConsumer)
		Convey("Then a new consumer instance with a partition consumer and a transformed should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.kafkaConsumer, ShouldNotBeNil)
			So(actual.messageTransformer, ShouldNotBeNil)
			So(actual.publisher, ShouldNotBeNil)
			So(actual.shutdown, ShouldNotBeNil)
			So(actual.wg, ShouldBeNil)
			So(actual.partition, ShouldEqual, 1)
			So(actual.offset, ShouldEqual, -1)
		})
	})
}

func TestReceiveMessageFromKafka(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(nil)
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", mock.Anything).Return("123", nil)
		mockPublisher := &mockPublisher{}
		mockPublisher.On("Publish", mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 0, -1, &mockLogger{}).(*KafkaMessageConsumer)
		consumer.wg = new(sync.WaitGroup)
		consumer.wg.Add(1)
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc"), Offset: 3}
			consumer.wg.Wait()
			Convey("Then an event should be published and the message should be transformed and published to the publisher", func() {
				So(<-consumer.started, ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(0), int64(-1)), ShouldBeTrue)
				So(mockPublisher.AssertCalled(t, "Publish", "123"), ShouldBeTrue)
				So(mockTransformer.AssertCalled(t, "Transform", &model.BackendEvent{Data: []byte("abc"), Offset: 3}), ShouldBeTrue)
			})
		})
	})
}

func TestReceiveShutdownSignal(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(nil)
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockKafkaConsumer.On("Close").Return(nil)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		mockLogger := &mockLogger{}
		mockLogger.On("Info", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 1, -1, mockLogger).(*KafkaMessageConsumer)
		consumer.wg = new(sync.WaitGroup)
		consumer.wg.Add(1)
		go consumer.Run()
		Convey("When a system signal is received", func() {
			consumer.Shutdown("user disconnected")
			consumer.wg.Wait()
			Convey("Then an event should be published and the consumer should be closed", func() {
				So(<-consumer.started, ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(1), int64(-1)), ShouldBeTrue)
				So(mockLogger.AssertCalled(t, "Info", "shutting down consumer: user disconnected", []log.Data(nil)), ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "Close"), ShouldBeTrue)
			})
		})
	})
}

func TestLogErrorsReceivedFromKafka(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		theError := &sarama.ConsumerError{
			Topic: "the-topic",
			Err:   errors.New("something went wrong"),
		}
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(nil)
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		logger := &mockLogger{}
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 0, -1, logger).(*KafkaMessageConsumer)
		consumer.wg = new(sync.WaitGroup)
		consumer.wg.Add(1)
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			errorChannel <- theError
			consumer.wg.Wait()
			Convey("Then an event should be published and the message should be transformed and published to the publisher", func() {
				So(<-consumer.started, ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(0), int64(-1)), ShouldBeTrue)
				So(logger.AssertCalled(t, "Error", theError, []log.Data{{"topic": "the-topic"}}), ShouldBeTrue)
			})
		})
	})
}

func TestSkipMessageIfTransformerReturnsError(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		theError := errors.New("something went wrong")
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(nil)
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", mock.Anything).Return("", theError)
		mockPublisher := &mockPublisher{}
		mockLogger := &mockLogger{}
		mockLogger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 0, -1, mockLogger).(*KafkaMessageConsumer)
		consumer.wg = new(sync.WaitGroup)
		consumer.wg.Add(1)
		go consumer.Run()
		Convey("When an untransformable message is consumed from Kafka", func() {
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc"), Offset: 3}
			consumer.wg.Wait()
			Convey("Then an event should be published, the error should be logged and no further processing should be done", func() {
				So(<-consumer.started, ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(0), int64(-1)), ShouldBeTrue)
				So(mockTransformer.AssertCalled(t, "Transform", &model.BackendEvent{Data: []byte("abc"), Offset: 3}), ShouldBeTrue)
				So(mockPublisher.AssertNotCalled(t, "Publish", mock.Anything), ShouldBeTrue)
				So(mockLogger.AssertCalled(t, "Error", theError, []log.Data{{}}), ShouldBeTrue)
			})
		})
	})
}

func TestLogErrorWhenClosingKafkaConsumer(t *testing.T) {
	Convey("Given a consumer is running", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		theError := errors.New("something went wrong")
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(nil)
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockKafkaConsumer.On("Close").Return(theError)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		logger := &mockLogger{}
		logger.On("Info", mock.Anything, mock.Anything).Return()
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 0, -1, logger).(*KafkaMessageConsumer)
		consumer.wg = new(sync.WaitGroup)
		consumer.wg.Add(1)
		go consumer.Run()
		Convey("When the consumer is shutdown", func() {
			consumer.Shutdown("user disconnected")
			consumer.wg.Wait()
			Convey("Then an event should be published and the error should be logged", func() {
				So(<-consumer.started, ShouldBeTrue)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(0), int64(-1)), ShouldBeTrue)
				So(logger.AssertCalled(t, "Info", "shutting down consumer: user disconnected", []log.Data(nil)), ShouldBeTrue)
				So(logger.AssertCalled(t, "Error", theError, []log.Data{{}}), ShouldBeTrue)
			})
		})
	})
}

func TestNotifyNotStartedIfErrorReturnedWhenConsumingPartition(t *testing.T) {
	Convey("Given a new consumer has been created and the partition consumer will return an error", t, func() {
		expectedError := errors.New("something went wrong")
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("ConsumePartition", mock.Anything, mock.Anything).Return(expectedError)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		mockLogger := &mockLogger{}
		mockLogger.On("Error", mock.Anything, []log.Data(nil)).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, 0, -1, mockLogger).(*KafkaMessageConsumer)
		Convey("When the consumer is started", func() {
			go consumer.Run()
			Convey("Then a message should be published indicating the consumer hasn't started", func() {
				So(consumer.HasStarted(), ShouldBeFalse)
				So(mockKafkaConsumer.AssertCalled(t, "ConsumePartition", int32(0), int64(-1)), ShouldBeTrue)
				So(mockLogger.AssertCalled(t, "Error", expectedError, []log.Data(nil)), ShouldBeTrue)
			})
		})
	})
}

func (k *mockKafkaConsumer) ConsumePartition(partition int32, offset int64) error {
	args := k.Called(partition, offset)
	return args.Error(0)
}

func (k *mockKafkaConsumer) Close() error {
	args := k.Called()
	return args.Error(0)
}

func (k *mockKafkaConsumer) Messages() <-chan *sarama.ConsumerMessage {
	args := k.Called()
	return args.Get(0).(chan *sarama.ConsumerMessage)
}

func (k *mockKafkaConsumer) Errors() <-chan *sarama.ConsumerError {
	args := k.Called()
	return args.Get(0).(chan *sarama.ConsumerError)
}

func (t *mockTransformer) Transform(message *model.BackendEvent) (string, error) {
	args := t.Called(message)
	return args.String(0), args.Error(1)
}

func (b *mockPublisher) Publish(msg string) {
	b.Called(msg)
}

func (l *mockLogger) Error(err error, data ...log.Data) {
	l.Called(err, data)
}

func (l *mockLogger) Info(msg string, data ...log.Data) {
	l.Called(msg, data)
}

func (l *mockLogger) InfoR(req *http.Request, msg string, data ...log.Data) {
	l.Called(req, msg, data)
}

func (l *mockLogger) ErrorR(req *http.Request, err error, data ...log.Data) {
	l.Called(req, err, data)
}
