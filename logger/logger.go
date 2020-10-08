package logger

import "github.com/companieshouse/chs.go/log"

type Logger interface {
	Error(err error, data ...log.Data)
	Info(msg string, data ...log.Data)
}
