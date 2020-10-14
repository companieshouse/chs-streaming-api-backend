package runner

import (
	"errors"
	backendconsumer "github.com/companieshouse/chs-streaming-api-backend/consumer"
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/transformer"
	"github.com/companieshouse/chs-streaming-api-backend/transformer/jsonproducer"
	"github.com/companieshouse/chs.go/avro"
	"github.com/companieshouse/chs.go/kafka/consumer"
)

type Config struct {
	KafkaBroker []string
	Topic       string
	Schema      *avro.Schema
	Broker      backendconsumer.Publishable
}

type Runner struct {
	kafkaBrokerAddr []string
	topic           string
	schema          *avro.Schema
	broker          backendconsumer.Publishable
	offset          int64
	partition       int32
	constructor     func(backendconsumer.KafkaConsumer, backendconsumer.Transformable, backendconsumer.Publishable, int32, int64, logger.Logger) backendconsumer.Runnable
}

type publisher struct {
	data chan string
}

type ConsumerController struct {
	runtime backendconsumer.Runnable
	data    chan string
}

type Controllable interface {
	Stop(msg string)
	Data() <-chan string
}

func NewFactory(cfg *Config) *Runner {
	factory := &Runner{
		kafkaBrokerAddr: cfg.KafkaBroker,
		topic:           cfg.Topic,
		schema:          cfg.Schema,
		broker:          cfg.Broker,
		constructor:     backendconsumer.NewConsumer,
	}
	return factory
}

func (f *Runner) StartConsumer(offset int64) (Controllable, error) {
	data := make(chan string)
	publisher := &publisher{data}
	backendConsumer := f.constructor(
		consumer.NewPartitionConsumer(&consumer.Config{
			BrokerAddr: f.kafkaBrokerAddr,
			Topics:     []string{f.topic},
		}),
		transformer.NewResourceChangedDataTransformer(
			transformer.NewDeserialiser(f.schema, jsonproducer.Instance()),
			transformer.NewSerialiser(jsonproducer.Instance(), jsonproducer.Instance())),
		publisher,
		f.partition,
		offset,
		logger.NewLogger())
	go backendConsumer.Run()
	if !backendConsumer.HasStarted() {
		return nil, errors.New("failed to start consumer")
	}
	return &ConsumerController{backendConsumer, data}, nil
}

func (c *ConsumerController) Stop(msg string) {
	c.runtime.Shutdown(msg)
	close(c.data)
}

func (c *ConsumerController) Data() <-chan string {
	return c.data
}

func (n *publisher) Publish(msg string) {
	n.data <- msg
}
