package main

import (
	"fmt"
	chsconfig "github.com/companieshouse/chs-streaming-api-backend/config"
	"github.com/companieshouse/chs-streaming-api-backend/service"
	"github.com/companieshouse/chs.go/avro"
	"github.com/companieshouse/chs.go/avro/schema"
	chslog "github.com/companieshouse/chs.go/log"
	chsservice "github.com/companieshouse/chs.go/service"
	chshandler "github.com/companieshouse/chs.go/service/handlers/requestID"
	"github.com/justinas/alice"
	"net/http"
)

const (
	schemaName              = "resource-changed-data"
	filingHistoryStream     = "stream-filing-history"
	companyProfileStream    = "stream-company-profile"
	companyInsolvencyStream = "stream-company-insolvency"
	companyChargesStream    = "stream-company-charges"
	companyOfficersStream   = "stream-company-officers"
	companyPSCStream        = "stream-company-psc"
)

func main() {
	chsservice.DefaultMiddleware = []alice.Constructor{chshandler.Handler(20), chslog.Handler}
	config, err := chsconfig.Get()
	if err != nil {
		panic(err)
	}
	svc := chsservice.New(config.ServiceConfig())
	chslog.Info("fetching avro schema from schema registry", chslog.Data{"schema_name": schemaName})
	s, err := schema.Get(config.SchemaRegistryURL, schemaName)
	if err != nil {
		chslog.Error(fmt.Errorf("error receiving %s schema: %s", schemaName, err))
		panic(err)
	}
	rcdAvroSchema := &avro.Schema{
		Definition: s,
	}
	backendConfiguration := &service.BackendConfiguration{
		Configuration: config,
		Schema:        rcdAvroSchema,
		Router:        svc.Router(),
	}

	service.NewBackendService(backendConfiguration).WithTopic(filingHistoryStream).WithPath("/filings").Start()
	service.NewBackendService(backendConfiguration).WithTopic(companyProfileStream).WithPath("/companies").Start()
	service.NewBackendService(backendConfiguration).WithTopic(companyInsolvencyStream).WithPath("/insolvency-cases").Start()
	service.NewBackendService(backendConfiguration).WithTopic(companyChargesStream).WithPath("/charges").Start()
	service.NewBackendService(backendConfiguration).WithTopic(companyOfficersStream).WithPath("/officers").Start()
	service.NewBackendService(backendConfiguration).WithTopic(companyPSCStream).WithPath("/persons-with-significant-control").Start()

	svc.Router().Path("/healthcheck").Methods("GET").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	svc.Start()
}
