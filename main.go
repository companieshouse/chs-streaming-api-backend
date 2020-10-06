package main

import (
	backend_config "github.com/companieshouse/chs-streaming-api-backend/config"
	"log"
	"net/http"
)

func main() {
	config, err := backend_config.Get()
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	log.Fatal(http.ListenAndServe(":"+config.BindAddress, nil))
}
