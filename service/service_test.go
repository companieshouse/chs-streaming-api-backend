package service

import (
	"github.com/companieshouse/chs-streaming-api-backend/config"
	backendhandler "github.com/companieshouse/chs-streaming-api-backend/handler"
	"github.com/companieshouse/chs.go/avro"
	"github.com/gorilla/mux"
	"github.com/gorilla/pat"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCreateNewService(t *testing.T) {
	Convey("When a new service instance is constructed", t, func() {
		configuration := &BackendConfiguration{
			Configuration: &config.Config{
				KafkaBroker: []string{"0.0.0.0"},
			},
			Schema: &avro.Schema{},
			Router: pat.New(),
		}
		actual := NewBackendService(configuration)
		Convey("Then a new service instance should be returned", func() {
			So(actual, ShouldNotBeNil)
			So(actual.schema, ShouldEqual, configuration.Schema)
			So(actual.router, ShouldEqual, configuration.Router)
			So(actual.kafkaBroker, ShouldResemble, configuration.Configuration.KafkaBroker)
		})
	})
}

func TestBindKafkaTopic(t *testing.T) {
	Convey("Given a new service instance has been constructed", t, func() {
		configuration := &BackendConfiguration{
			Configuration: &config.Config{
				KafkaBroker: []string{"0.0.0.0"},
			},
			Schema: &avro.Schema{},
			Router: pat.New(),
		}
		service := NewBackendService(configuration)
		Convey("When a kafka topic is bound to it", func() {
			actual := service.WithTopic("topic")
			Convey("Then a new consumer factory should be allocated to the service", func() {
				So(actual, ShouldEqual, service)
				So(actual.factory, ShouldNotBeNil)
			})
		})
	})
}

func TestAttachRequestHandler(t *testing.T) {
	Convey("Given a new service instance has been constructed", t, func() {
		configuration := &BackendConfiguration{
			Configuration: &config.Config{
				KafkaBroker: []string{"0.0.0.0"},
			},
			Schema: &avro.Schema{},
			Router: pat.New(),
		}
		service := NewBackendService(configuration)
		Convey("When a request handler is attached to it", func() {
			actual := service.WithPath("/path")
			Convey("Then a new request handler should be allocated to the service", func() {
				So(actual, ShouldEqual, service)
				_ = configuration.Router.Walk(func(r *mux.Route, o *mux.Router, u []*mux.Route) error {
					path, _ := r.GetPathTemplate()
					methods, _ := r.GetMethods()
					handler := r.GetHandler()
					So(path, ShouldEqual, "/path")
					So(methods, ShouldResemble, []string{"GET"})
					So(handler, ShouldEqual, (&backendhandler.RequestHandler{}).HandleRequest)
					return nil
				})
			})
		})
	})
}
