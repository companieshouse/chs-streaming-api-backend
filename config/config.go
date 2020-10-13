package config

import "github.com/ian-kent/gofigure"

type Config struct {
	gofigure          interface{} `order:"env,flag"`
	BindAddress       string      `env:"BIND_ADDRESS" flag:"bind-address"`
	KafkaBroker       []string    `env:"KAFKA_BROKER_ADDR" flag:"kafka-broker-addr"`
	SchemaRegistryURL string      `env:"SCHEMA_REGISTRY_URL" flag:"schema-registry-url"`
}

var config *Config

func Get() (*Config, error) {
	if config == nil {
		config = &Config{}
		if err := gofigure.Gofigure(config); err != nil {
			return nil, err
		}
	}
	return config, nil
}
