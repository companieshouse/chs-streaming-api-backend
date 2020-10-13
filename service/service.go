package service

import (
	"github.com/companieshouse/chs-streaming-api-backend/broker"
	"github.com/companieshouse/chs-streaming-api-backend/config"
	backendconsumer "github.com/companieshouse/chs-streaming-api-backend/consumer"
	"github.com/companieshouse/chs-streaming-api-backend/handler"
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/transformer"
	"github.com/companieshouse/chs-streaming-api-backend/transformer/jsonproducer"
	"github.com/companieshouse/chs.go/avro"
	"github.com/companieshouse/chs.go/kafka/consumer"
	"github.com/gorilla/mux"
	"github.com/gorilla/pat"
	"net/http"
)

type BackendService struct {
	broker       *broker.Broker
	consumer     *backendconsumer.KafkaMessageConsumer
	kafkaBrokers []string
	schema       *avro.Schema
	router       *pat.Router
}

type Router interface {
	Get(path string, handler http.HandlerFunc) *mux.Route
}

type BackendConfiguration struct {
	Configuration *config.Config
	Schema        *avro.Schema
	Router        *pat.Router
}

func NewBackendService(cfg *BackendConfiguration) *BackendService {
	return &BackendService{
		broker:       broker.NewBroker(),
		schema:       cfg.Schema,
		kafkaBrokers: cfg.Configuration.KafkaBroker,
		router:       cfg.Router,
	}
}

func (s *BackendService) WithTopic(topic string) *BackendService {
	s.consumer = backendconsumer.NewConsumer(
		consumer.NewPartitionConsumer(&consumer.Config{
			BrokerAddr: s.kafkaBrokers,
			Topics:     []string{topic},
		}),
		transformer.NewResourceChangedDataTransformer(
			transformer.NewDeserialiser(s.schema, jsonproducer.Instance()),
			transformer.NewSerialiser(jsonproducer.Instance(), jsonproducer.Instance())),
		s.broker,
		0,
		-1,
		logger.NewLogger())
	return s
}

func (s *BackendService) WithPath(path string) *BackendService {
	s.router.Path(path).Methods("GET").HandlerFunc(handler.NewRequestHandler(s.broker, logger.NewLogger()).HandleRequest)
	return s
}

func (s *BackendService) Start() {
	go s.consumer.Run()
	go s.broker.Run()
}
