package handler

import (
	"github.com/companieshouse/chs-streaming-api-backend/logger"
	"net/http"
	"sync"
)

type Subscribable interface {
	Subscribe() (chan string, error)
	Unsubscribe(chan string) error
}

type RequestHandler struct {
	broker Subscribable
	logger logger.Logger
	wg     *sync.WaitGroup
}

func NewRequestHandler(broker Subscribable, logger logger.Logger) *RequestHandler {
	return &RequestHandler{
		broker: broker,
		logger: logger,
	}
}

func (h *RequestHandler) HandleRequest(writer http.ResponseWriter, request *http.Request) {
	h.logger.InfoR(request, "User connected")
	subscription, _ := h.broker.Subscribe()
	for {
		select {
		case msg := <-subscription:
			_, _ = writer.Write([]byte(msg))
			writer.(http.Flusher).Flush()
			if h.wg != nil {
				h.wg.Done()
			}
		case <-request.Context().Done():
			_ = h.broker.Unsubscribe(subscription)
			h.logger.InfoR(request, "User disconnected")
			if h.wg != nil {
				h.wg.Done()
			}
		}
	}
}
