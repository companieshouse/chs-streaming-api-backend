package consumer

import (
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs.go/kafka/consumer"
	"github.com/companieshouse/chs.go/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

//Describes an object capable of transforming on given representation into another.
type Transformable interface {
	Transform(message *model.BackendEvent) (string, error)
}

//Describes an object capable of publishing a message.
type Publishable interface {
	Publish(msg string)
}

type KafkaConsumer interface {
	consumer.PConsumer
	ConsumePartition(partition int32, offset int64) error
}

//Consumes messages from the associated partition consumer, transforms these into a desired format and publishes
//transformed messages to clients.
type KafkaMessageConsumer struct {
	kafkaConsumer      KafkaConsumer
	messageTransformer Transformable
	publisher          Publishable
	systemEvents       chan os.Signal
	logger             logger.Logger
	wg                 *sync.WaitGroup
}

//Create a new consumer wrapper instance.
func NewConsumer(consumer KafkaConsumer, messageTransformer Transformable, publisher Publishable, logger logger.Logger) *KafkaMessageConsumer {
	systemEvents := make(chan os.Signal)
	signal.Notify(systemEvents, syscall.SIGINT, syscall.SIGTERM)
	return &KafkaMessageConsumer{
		kafkaConsumer:      consumer,
		messageTransformer: messageTransformer,
		publisher:          publisher,
		systemEvents:       systemEvents,
		logger:             logger,
	}
}

//Run this consumer instance.
func (c *KafkaMessageConsumer) Run() {
	if err := c.kafkaConsumer.ConsumePartition(0, -1); err != nil {
		panic(err) //TODO
	}
	for {
		select {
		case message := <-c.kafkaConsumer.Messages():
			result, err := c.messageTransformer.Transform(&model.BackendEvent{
				Data:   message.Value,
				Offset: message.Offset,
			})
			if err != nil {
				c.logger.Error(err, log.Data{})
				if c.wg != nil {
					c.wg.Done()
				}
				continue
			}
			c.publisher.Publish(result)
			if c.wg != nil {
				c.wg.Done()
			}
		case <-c.systemEvents:
			if err := c.kafkaConsumer.Close(); err != nil {
				c.logger.Error(err, log.Data{})
			}
			if c.wg != nil {
				c.wg.Done()
			}
			return
		case err := <-c.kafkaConsumer.Errors():
			c.logger.Error(err, log.Data{"topic": err.Topic})
			if c.wg != nil {
				c.wg.Done()
			}
		}
	}
}
