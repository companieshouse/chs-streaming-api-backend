package logger

import (
	"github.com/companieshouse/chs.go/log"
	"net/http"
)

type Logger interface {
	Error(err error, data ...log.Data)
	Info(msg string, data ...log.Data)
	InfoR(req *http.Request, message string, data ...log.Data)
}
