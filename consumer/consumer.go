package consumer

import (
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/model"
	"github.com/companieshouse/chs.go/kafka/consumer"
	"github.com/companieshouse/chs.go/log"
	"sync"
)

//Describes an object capable of transforming on given representation into another.
type Transformable interface {
	Transform(message *model.BackendEvent) (string, error)
}

//Describes an object capable of publishing a message.
type Publishable interface {
	Publish(msg string)
}

type Runnable interface {
	Run()
	HasStarted() bool
	Shutdown(msg string)
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
	shutdown           chan string
	logger             logger.Logger
	wg                 *sync.WaitGroup
	partition          int32
	offset             int64
	started            chan bool
}

//Create a new consumer wrapper instance.
func NewConsumer(consumer KafkaConsumer, messageTransformer Transformable, publisher Publishable, partition int32, offset int64, logger logger.Logger) Runnable {
	return &KafkaMessageConsumer{
		kafkaConsumer:      consumer,
		messageTransformer: messageTransformer,
		publisher:          publisher,
		shutdown:           make(chan string),
		partition:          partition,
		offset:             offset,
		logger:             logger,
		started:            make(chan bool),
	}
}

//Run this consumer instance.
func (c *KafkaMessageConsumer) Run() {
	if err := c.kafkaConsumer.ConsumePartition(c.partition, c.offset); err != nil {
		c.logger.Error(err)
		go c.notifyStarted(false)
		return
	}
	go c.notifyStarted(true)
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
		case msg := <-c.shutdown:
			c.logger.Info("shutting down consumer: " + msg)
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

func (c *KafkaMessageConsumer) HasStarted() bool {
	return <-c.started
}

func (c *KafkaMessageConsumer) Shutdown(msg string) {
	c.shutdown <- msg
}

func (c *KafkaMessageConsumer) notifyStarted(started bool) {
	c.started <- started
}
