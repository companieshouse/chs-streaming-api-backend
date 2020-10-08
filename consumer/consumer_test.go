package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/companieshouse/chs.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"net/http"
	"sync"
	"syscall"
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
		actual := NewConsumer(&mockKafkaConsumer{}, &mockTransformer{}, &mockPublisher{}, &mockLogger{})
		Convey("Then a new consumer instance with a partition consumer and a transformed should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.kafkaConsumer, ShouldNotBeNil)
			So(actual.messageTransformer, ShouldNotBeNil)
			So(actual.publisher, ShouldNotBeNil)
			So(actual.systemEvents, ShouldNotBeNil)
			So(actual.wg, ShouldBeNil)
		})
	})
}

func TestReceiveMessageFromKafka(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", []byte("abc")).Return("123", nil)
		mockPublisher := &mockPublisher{}
		mockPublisher.On("Publish", "123").Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, &mockLogger{})
		consumer.wg = new(sync.WaitGroup)
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			consumer.wg.Add(1)
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc")}
			consumer.wg.Wait()
			Convey("Then an event should be published and the message should be transformed and published to the publisher", func() {
				So(mockPublisher.AssertCalled(t, "Publish", "123"), ShouldBeTrue)
				So(mockTransformer.AssertCalled(t, "Transform", []byte("abc")), ShouldBeTrue)
			})
		})
	})
}

func TestReceiveSystemSignal(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		mockKafkaConsumer := &mockKafkaConsumer{}
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockKafkaConsumer.On("Close").Return(nil)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, &mockLogger{})
		consumer.wg = new(sync.WaitGroup)
		go consumer.Run()
		Convey("When a system signal is received", func() {
			consumer.wg.Add(1)
			consumer.systemEvents <- syscall.SIGINT
			consumer.wg.Wait()
			Convey("Then an event should be published and the consumer should be closed", func() {
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
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		logger := &mockLogger{}
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, logger)
		consumer.wg = new(sync.WaitGroup)
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			consumer.wg.Add(1)
			errorChannel <- theError
			consumer.wg.Wait()
			Convey("Then an event should be published and the message should be transformed and published to the publisher", func() {
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
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", []byte("abc")).Return("", theError)
		mockPublisher := &mockPublisher{}
		mockLogger := &mockLogger{}
		mockLogger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, mockLogger)
		consumer.wg = new(sync.WaitGroup)
		go consumer.Run()
		Convey("When an untransformable message is consumed from Kafka", func() {
			consumer.wg.Add(1)
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc")}
			consumer.wg.Wait()
			Convey("Then an event should be published, the error should be logged and no further processing should be done", func() {
				So(mockTransformer.AssertCalled(t, "Transform", []byte("abc")), ShouldBeTrue)
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
		mockKafkaConsumer.On("Messages").Return(msgChannel)
		mockKafkaConsumer.On("Errors").Return(errorChannel)
		mockKafkaConsumer.On("Close").Return(theError)
		mockTransformer := &mockTransformer{}
		mockPublisher := &mockPublisher{}
		logger := &mockLogger{}
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(mockKafkaConsumer, mockTransformer, mockPublisher, logger)
		consumer.wg = new(sync.WaitGroup)
		go consumer.Run()
		Convey("When a system signal has been received", func() {
			consumer.wg.Add(1)
			consumer.systemEvents <- syscall.SIGINT
			consumer.wg.Wait()
			Convey("Then an event should be published and the error should be logged", func() {
				So(logger.AssertCalled(t, "Error", theError, []log.Data{{}}), ShouldBeTrue)
			})
		})
	})
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

func (t *mockTransformer) Transform(message []byte) (string, error) {
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
