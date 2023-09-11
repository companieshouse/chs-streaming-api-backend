package config_test

import (
	"encoding/json"
	"os"

	"github.com/companieshouse/chs-streaming-api-backend/config"
	"regexp"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// key constants
const (
	BINDADDRCONST            = `BIND_ADDRESS`
	CERTFILECONST            = `CERT_FILE`
	KEYFILECONST             = `KEY_FILE`
	STREAMINGBROKERADDRCONST = `KAFKA_STREAMING_BROKER_ADDR`
	SCHEMAREGISTRYURLCONST   = `SCHEMA_REGISTRY_URL`
)

// value constants
const (
	bindAddrConst            = `bind-addr`
	certFileConst            = `cert-file`
	keyFileConst             = `key-file`
	streamingBrokerAddrConst = `streaming-broker-addr`
	schemaRegistryURLConst   = `schema-registry-url`
)

func TestConfig(t *testing.T) {
	t.Parallel()
	os.Clearenv()
	var (
		err           error
		configuration *config.Config
		envVars       = map[string]string{
			BINDADDRCONST:            bindAddrConst,
			CERTFILECONST:            certFileConst,
			STREAMINGBROKERADDRCONST: streamingBrokerAddrConst,
			KEYFILECONST:             keyFileConst,
			SCHEMAREGISTRYURLCONST:   schemaRegistryURLConst,
		}
		builtConfig = config.Config{
			BindAddress:       bindAddrConst,
			CertFile:          certFileConst,
			KafkaBroker:       []string{streamingBrokerAddrConst},
			KeyFile:           keyFileConst,
			SchemaRegistryURL: schemaRegistryURLConst,
		}
		bindAddrRegex            = regexp.MustCompile(bindAddrConst)
		certFileRegex            = regexp.MustCompile(certFileConst)
		streamingBrokerAddrRegex = regexp.MustCompile(streamingBrokerAddrConst)
		keyFileRegex             = regexp.MustCompile(keyFileConst)
		schemaRegistryURLRegex   = regexp.MustCompile(schemaRegistryURLConst)
	)

	// set test env variables
	for varName, varValue := range envVars {
		os.Setenv(varName, varValue)
		defer os.Unsetenv(varName)
	}

	Convey("Given an environment with no environment variables set", t, func() {

		Convey("Then configuration should be nil", func() {
			So(configuration, ShouldBeNil)
		})

		Convey("When the config values are retrieved", func() {

			Convey("Then there should be no error returned, and values are as expected", func() {
				configuration, err = config.Get()

				So(err, ShouldBeNil)
				So(configuration, ShouldResemble, &builtConfig)
			})

			Convey("The generated JSON string from configuration should not contain sensitive data", func() {
				jsonByte, err := json.Marshal(builtConfig)

				So(err, ShouldBeNil)
				So(bindAddrRegex.Match(jsonByte), ShouldEqual, true)
				So(certFileRegex.Match(jsonByte), ShouldEqual, false)
				So(keyFileRegex.Match(jsonByte), ShouldEqual, false)
				So(streamingBrokerAddrRegex.Match(jsonByte), ShouldEqual, true)
				So(schemaRegistryURLRegex.Match(jsonByte), ShouldEqual, true)
			})
		})
	})
}
