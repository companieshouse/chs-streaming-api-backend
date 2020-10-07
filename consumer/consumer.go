package consumer

import (
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs.go/kafka/consumer"
	"github.com/companieshouse/chs.go/log"
	"os"
	"os/signal"
	"syscall"
)

type Transformer interface {
	Transform(message []byte) (string, error)
}

type Broker interface {
	Publish(msg string)
}

type KafkaMessageConsumer struct {
	kafkaConsumer      consumer.PConsumer
	messageTransformer Transformer
	broker             Broker
	systemEvents       chan os.Signal
	logger             logger.Logger
	event              chan struct{}
}

func NewConsumer(consumer consumer.PConsumer, messageTransformer Transformer, broker Broker, logger logger.Logger) *KafkaMessageConsumer {
	systemEvents := make(chan os.Signal)
	signal.Notify(systemEvents, syscall.SIGINT, syscall.SIGTERM)
	return &KafkaMessageConsumer{
		kafkaConsumer:      consumer,
		messageTransformer: messageTransformer,
		broker:             broker,
		systemEvents:       systemEvents,
		logger:             logger,
		event:              make(chan struct{}),
	}
}

func (c *KafkaMessageConsumer) Run() {
	for {
		select {
		case message := <-c.kafkaConsumer.Messages():
			result, err := c.messageTransformer.Transform(message.Value)
			if err != nil {
				c.logger.Error(err, log.Data{})
				c.event <- struct{}{}
				continue
			}
			c.broker.Publish(result)
			c.event <- struct{}{}
		case <-c.systemEvents:
			if err := c.kafkaConsumer.Close(); err != nil {
				c.logger.Error(err, log.Data{})
			}
			c.event <- struct{}{}
			return
		case err := <-c.kafkaConsumer.Errors():
			c.logger.Error(err, log.Data{"topic": err.Topic})
			c.event <- struct{}{}
		}
	}
}
