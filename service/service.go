package service

import (
	"github.com/companieshouse/chs-streaming-api-backend/config"
	"github.com/companieshouse/chs-streaming-api-backend/handler"
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/runner"
	"github.com/companieshouse/chs.go/avro"
	"github.com/gorilla/mux"
	"github.com/gorilla/pat"
	"net/http"
)

type BackendService struct {
	kafkaBroker []string
	schema      *avro.Schema
	factory     *runner.Runner
	router      *pat.Router
}

type Router interface {
	Get(path string, handler http.HandlerFunc) *mux.Route
}

type BackendConfiguration struct {
	Configuration *config.Config
	Schema        *avro.Schema
	Router        *pat.Router
	Topic         string
}

func NewBackendService(cfg *BackendConfiguration) *BackendService {
	return &BackendService{
		router:      cfg.Router,
		kafkaBroker: cfg.Configuration.KafkaBroker,
		schema:      cfg.Schema,
	}
}

func (s *BackendService) WithTopic(topic string) *BackendService {
	s.factory = runner.NewFactory(&runner.Config{
		KafkaBroker: s.kafkaBroker,
		Schema:      s.schema,
		Topic:       topic,
	})
	return s
}

func (s *BackendService) WithPath(path string) *BackendService {
	s.router.Path(path).Methods(http.MethodGet).HandlerFunc(handler.NewRequestHandler(s.factory, logger.NewLogger()).HandleRequest)
	return s
}
