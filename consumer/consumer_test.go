package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/companieshouse/chs.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"syscall"
	"testing"
)

type mockKafkaConsumer struct {
	mock.Mock
}

type mockTransformer struct {
	mock.Mock
}

type mockBroker struct {
	mock.Mock
}

type mockLogger struct {
	mock.Mock
}

func TestCreateNewConsumer(t *testing.T) {
	Convey("When a new consumer instance is created", t, func() {
		actual := NewConsumer(&mockKafkaConsumer{}, &mockTransformer{}, &mockBroker{}, &mockLogger{})
		Convey("Then a new consumer instance with a partition consumer and a transformed should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.kafkaConsumer, ShouldNotBeNil)
			So(actual.messageTransformer, ShouldNotBeNil)
			So(actual.broker, ShouldNotBeNil)
			So(actual.systemEvents, ShouldNotBeNil)
		})
	})
}

func TestReceiveMessageFromKafka(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		kafkaConsumer := &mockKafkaConsumer{}
		kafkaConsumer.On("Messages").Return(msgChannel)
		kafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", []byte("abc")).Return("123", nil)
		mockBroker := &mockBroker{}
		mockBroker.On("Publish", "123").Return()
		consumer := NewConsumer(kafkaConsumer, mockTransformer, mockBroker, &mockLogger{})
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc")}
			Convey("Then an event should be published and the message should be transformed and published to the broker", func() {
				So(<-consumer.event, ShouldNotBeNil)
				So(mockBroker.AssertCalled(t, "Publish", "123"), ShouldBeTrue)
			})
		})
	})
}

func TestReceiveSystemSignal(t *testing.T) {
	Convey("Given a new consumer has been created", t, func() {
		msgChannel := make(chan *sarama.ConsumerMessage)
		errorChannel := make(chan *sarama.ConsumerError)
		kafkaConsumer := &mockKafkaConsumer{}
		kafkaConsumer.On("Messages").Return(msgChannel)
		kafkaConsumer.On("Errors").Return(errorChannel)
		kafkaConsumer.On("Close").Return(nil)
		mockTransformer := &mockTransformer{}
		mockBroker := &mockBroker{}
		consumer := NewConsumer(kafkaConsumer, mockTransformer, mockBroker, &mockLogger{})
		go consumer.Run()
		Convey("When a system signal is received", func() {
			consumer.systemEvents <- syscall.SIGINT
			Convey("Then an event should be published and the consumer should be closed", func() {
				So(<-consumer.event, ShouldNotBeNil)
				So(kafkaConsumer.AssertCalled(t, "Close"), ShouldBeTrue)
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
		kafkaConsumer := &mockKafkaConsumer{}
		kafkaConsumer.On("Messages").Return(msgChannel)
		kafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockBroker := &mockBroker{}
		logger := &mockLogger{}
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(kafkaConsumer, mockTransformer, mockBroker, logger)
		go consumer.Run()
		Convey("When a message is consumed from Kafka", func() {
			errorChannel <- theError
			Convey("Then an event should be published and the message should be transformed and published to the broker", func() {
				So(<-consumer.event, ShouldNotBeNil)
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
		kafkaConsumer := &mockKafkaConsumer{}
		kafkaConsumer.On("Messages").Return(msgChannel)
		kafkaConsumer.On("Errors").Return(errorChannel)
		mockTransformer := &mockTransformer{}
		mockTransformer.On("Transform", []byte("abc")).Return("", theError)
		mockBroker := &mockBroker{}
		mockLogger := &mockLogger{}
		mockLogger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(kafkaConsumer, mockTransformer, mockBroker, mockLogger)
		go consumer.Run()
		Convey("When an untransformable message is consumed from Kafka", func() {
			msgChannel <- &sarama.ConsumerMessage{Value: []byte("abc")}
			Convey("Then an event should be published, the error should be logged and no further processing should be done", func() {
				So(<-consumer.event, ShouldNotBeNil)
				So(mockBroker.AssertNotCalled(t, "Publish", mock.Anything), ShouldBeTrue)
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
		kafkaConsumer := &mockKafkaConsumer{}
		kafkaConsumer.On("Messages").Return(msgChannel)
		kafkaConsumer.On("Errors").Return(errorChannel)
		kafkaConsumer.On("Close").Return(theError)
		mockTransformer := &mockTransformer{}
		mockBroker := &mockBroker{}
		logger := &mockLogger{}
		logger.On("Error", mock.Anything, mock.Anything).Return()
		consumer := NewConsumer(kafkaConsumer, mockTransformer, mockBroker, logger)
		go consumer.Run()
		Convey("When a system signal has been received", func() {
			consumer.systemEvents <- syscall.SIGINT
			Convey("Then an event should be published and the error should be logged", func() {
				So(<-consumer.event, ShouldNotBeNil)
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

func (b *mockBroker) Publish(msg string) {
	b.Called(msg)
}

func (l *mockLogger) Error(err error, data ...log.Data) {
	l.Called(err, data)
}

func (l *mockLogger) Info(msg string, data ...log.Data) {
	l.Called(msg, data)
}
