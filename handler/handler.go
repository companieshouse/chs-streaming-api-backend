package handler

import (
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"github.com/companieshouse/chs-streaming-api-backend/runner"
	"net/http"
	"strconv"
	"sync"
)

const (
	offsetRequestParam  = "offset"
	msgUserConnected    = "user connected"
	msgUserDisconnected = "user disconnected"
)

type RequestHandler struct {
	runner Controllable
	logger logger.Logger
	wg     *sync.WaitGroup
}

type Controllable interface {
	StartConsumer(offset int64) (runner.Controllable, error)
}

func NewRequestHandler(runner Controllable, logger logger.Logger) *RequestHandler {
	return &RequestHandler{
		runner: runner,
		logger: logger,
	}
}

func (h *RequestHandler) HandleRequest(writer http.ResponseWriter, request *http.Request) {
	var offset int64 = -1
	var err error
	h.logger.InfoR(request, msgUserConnected)
	if offsetParam := request.URL.Query().Get(offsetRequestParam); offsetParam != "" {
		offset, err = strconv.ParseInt(offsetParam, 10, 64)
		if err != nil {
			h.logger.ErrorR(request, err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	controller, err := h.runner.StartConsumer(offset)
	if err != nil {
		h.logger.ErrorR(request, err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	for {
		select {
		case msg := <-controller.Data():
			_, _ = writer.Write([]byte(msg))
			writer.(http.Flusher).Flush()
			if h.wg != nil {
				h.wg.Done()
			}
		case <-request.Context().Done():
			controller.Stop(msgUserDisconnected)
			h.logger.InfoR(request, msgUserDisconnected)
			if h.wg != nil {
				h.wg.Done()
			}
			return
		}
	}
}
