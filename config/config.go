package config

import "github.com/companieshouse/gofigure"

type Config struct {
	gofigure          interface{} `order:"env,flag"`
	BindAddress       string      `env:"BIND_ADDRESS" flag:"bind-address"`
	CertFile          string      `env:"CERT_FILE" flag:"cert-file" json:"-"`
	KafkaBroker       []string    `env:"KAFKA_STREAMING_BROKER_ADDR" flag:"kafka-broker-addr"`
	KeyFile           string      `env:"KEY_FILE" flag:"key-file" json:"-"`
	SchemaRegistryURL string      `env:"SCHEMA_REGISTRY_URL" flag:"schema-registry-url"`
}

// ServiceConfig returns a ServiceConfig interface for Config.
func (c Config) ServiceConfig() ServiceConfig {
	return ServiceConfig{c}
}

// ServiceConfig wraps Config to implement service.Config.
type ServiceConfig struct {
	Config
}

// BindAddr implements service.Config.BindAddr.
func (cfg ServiceConfig) BindAddr() string {
	return cfg.Config.BindAddress
}

// CertFile implements service.Config.CertFile.
func (cfg ServiceConfig) CertFile() string {
	return cfg.Config.CertFile
}

// KeyFile implements service.Config.KeyFile.
func (cfg ServiceConfig) KeyFile() string {
	return cfg.Config.KeyFile
}

// Namespace implements service.Config.Namespace.
func (cfg ServiceConfig) Namespace() string {
	return "chs-streaming-api-backend"
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
