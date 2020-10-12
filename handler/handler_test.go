package handler

import (
	"github.com/companieshouse/chs.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type mockBroker struct {
	mock.Mock
}

type mockContext struct {
	mock.Mock
}

type mockLogger struct {
	mock.Mock
}

func TestCreateNewRequestHandler(t *testing.T) {
	Convey("Given an existing broker instance", t, func() {
		broker := &mockBroker{}
		logger := &mockLogger{}
		Convey("When a new request handler instance is created", func() {
			actual := NewRequestHandler(broker, logger)
			Convey("Then a new request handler instance should be returned", func() {
				So(actual, ShouldNotBeNil)
				So(actual.broker, ShouldEqual, broker)
				So(actual.logger, ShouldEqual, logger)
				So(actual.wg, ShouldBeNil)
			})
		})
	})
}

func TestWritePublishedMessageToResponseWriter(t *testing.T) {
	Convey("Given a running request handler", t, func() {
		subscription := make(chan string)
		broker := &mockBroker{}
		broker.On("Subscribe").Return(subscription, nil)
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		requestHandler := NewRequestHandler(broker, logger)
		waitGroup := new(sync.WaitGroup)
		requestHandler.wg = waitGroup
		request := httptest.NewRequest("GET", "/endpoint", nil)
		request.Header.Add("X-Request-Id", "123")
		response := httptest.NewRecorder()
		go requestHandler.HandleRequest(response, request)
		Convey("When a new message is published", func() {
			waitGroup.Add(1)
			subscription <- "Hello world"
			waitGroup.Wait()
			output, _ := response.Body.ReadString('\n')
			Convey("Then the message should be written to the output stream", func() {
				So(logger.AssertCalled(t, "InfoR", request, "User connected", mock.Anything), ShouldBeTrue)
				So(broker.AssertCalled(t, "Subscribe"), ShouldBeTrue)
				So(output, ShouldEqual, "Hello world")
			})
		})
	})
}

func TestHandlerUnsubscribesIfUserDisconnects(t *testing.T) {
	Convey("Given a running request handler", t, func() {
		subscription := make(chan string)
		requestComplete := make(chan struct{})
		broker := &mockBroker{}
		broker.On("Subscribe").Return(subscription, nil)
		broker.On("Unsubscribe", subscription).Return(nil)
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		context := &mockContext{}
		context.On("Done").Return(requestComplete)
		requestHandler := NewRequestHandler(broker, logger)
		waitGroup := new(sync.WaitGroup)
		requestHandler.wg = waitGroup
		request := httptest.NewRequest("GET", "/endpoint", nil).WithContext(context)
		request.Header.Add("X-Request-Id", "123")
		response := httptest.NewRecorder()
		go requestHandler.HandleRequest(response, request)
		Convey("When the user disconnects", func() {
			waitGroup.Add(1)
			requestComplete <- struct{}{}
			waitGroup.Wait()
			Convey("Then the broker should be unsubscribed from the broker", func() {
				So(logger.AssertCalled(t, "InfoR", request, "User connected", mock.Anything), ShouldBeTrue)
				So(broker.AssertCalled(t, "Subscribe"), ShouldBeTrue)
				So(broker.AssertCalled(t, "Unsubscribe", subscription), ShouldBeTrue)
				So(logger.AssertCalled(t, "InfoR", request, "User disconnected", mock.Anything), ShouldBeTrue)
			})
		})
	})
}

func (b *mockBroker) Subscribe() (chan string, error) {
	args := b.Called()
	return args.Get(0).(chan string), args.Error(1)
}

func (b *mockBroker) Unsubscribe(subscription chan string) error {
	args := b.Called(subscription)
	return args.Error(0)
}

func (c *mockContext) Deadline() (deadline time.Time, ok bool) {
	args := c.Called()
	return args.Get(0).(time.Time), args.Bool(1)
}

func (c *mockContext) Done() <-chan struct{} {
	args := c.Called()
	return args.Get(0).(chan struct{})
}

func (c *mockContext) Err() error {
	args := c.Called()
	return args.Error(0)
}

func (c *mockContext) Value(key interface{}) interface{} {
	args := c.Called(key)
	return args.Get(0)
}

func (l *mockLogger) Info(msg string, data ...log.Data) {
	l.Called(msg, data)
}

func (l *mockLogger) InfoR(req *http.Request, msg string, data ...log.Data) {
	l.Called(req, msg, data)
}

func (l *mockLogger) Error(err error, data ...log.Data) {
	l.Called(err, data)
}
