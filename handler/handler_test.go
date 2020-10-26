package handler

import (
	"errors"
	"github.com/companieshouse/chs-streaming-api-backend/runner"
	"github.com/companieshouse/chs.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type mockConsumerRunner struct {
	mock.Mock
}

type mockController struct {
	mock.Mock
}

type mockContext struct {
	mock.Mock
}

type mockLogger struct {
	mock.Mock
}

func TestCreateNewRequestHandler(t *testing.T) {
	Convey("Given an existing consumer runner", t, func() {
		consumerRunner := &mockConsumerRunner{}
		logger := &mockLogger{}
		Convey("When a new request handler instance is created", func() {
			actual := NewRequestHandler(consumerRunner, logger)
			Convey("Then a new request handler instance should be returned", func() {
				So(actual, ShouldNotBeNil)
				So(actual.runner, ShouldEqual, consumerRunner)
				So(actual.logger, ShouldEqual, logger)
				So(actual.wg, ShouldBeNil)
			})
		})
	})
}

func TestWritePublishedMessageToResponseWriter(t *testing.T) {
	Convey("Given a user is connected to the request handler", t, func() {
		consumerManager := &mockConsumerRunner{}
		subscription := make(chan string)
		mockController := &mockController{}
		consumerManager.On("StartConsumer", mock.Anything).Return(mockController, nil)
		mockController.On("Data").Return(subscription)
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		requestHandler := NewRequestHandler(consumerManager, logger)
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
			Convey("Then the message should be written to the response body", func() {
				So(response.Code, ShouldEqual, 200)
				So(output, ShouldEqual, "Hello world")
				So(logger.AssertCalled(t, "InfoR", request, "user connected", []log.Data(nil)), ShouldBeTrue)
				So(consumerManager.AssertCalled(t, "StartConsumer", int64(-1)), ShouldBeTrue)
				So(mockController.AssertCalled(t, "Data"), ShouldBeTrue)
			})
		})
	})
}

func TestHandlerUnsubscribesIfUserDisconnects(t *testing.T) {
	Convey("Given a user is connected to the request handler", t, func() {
		requestComplete := make(chan struct{})
		subscription := make(chan string)
		mockController := &mockController{}
		mockController.On("Data").Return(subscription)
		mockController.On("Stop", mock.Anything).Return()
		consumerManager := &mockConsumerRunner{}
		consumerManager.On("StartConsumer", mock.Anything).Return(mockController, nil)
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		context := &mockContext{}
		context.On("Done").Return(requestComplete)
		requestHandler := NewRequestHandler(consumerManager, logger)
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
			Convey("Then the consumer should be stopped", func() {
				So(response.Code, ShouldEqual, 200)
				So(mockController.AssertCalled(t, "Stop", "user disconnected"), ShouldBeTrue)
				So(logger.AssertCalled(t, "InfoR", request, "user connected", []log.Data(nil)), ShouldBeTrue)
				So(consumerManager.AssertCalled(t, "StartConsumer", int64(-1)), ShouldBeTrue)
				So(logger.AssertCalled(t, "InfoR", request, "user disconnected", []log.Data(nil)), ShouldBeTrue)
			})
		})
	})
}

func TestHandlerReturnsBadRequestIfInvalidOffsetFormatSpecified(t *testing.T) {
	Convey("Given a request handler instance", t, func() {
		consumerManager := &mockConsumerRunner{}
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		logger.On("ErrorR", mock.Anything, mock.Anything, mock.Anything).Return()
		requestHandler := NewRequestHandler(consumerManager, logger)
		waitGroup := new(sync.WaitGroup)
		requestHandler.wg = waitGroup
		request := httptest.NewRequest("GET", "/endpoint?offset=q", nil)
		request.Header.Add("X-Request-Id", "123")
		response := httptest.NewRecorder()
		Convey("When a request specifying an invalid offset format is made", func() {
			requestHandler.HandleRequest(response, request)
			Convey("Then the response should be HTTP 400 Bad Request", func() {
				So(logger.AssertCalled(t, "InfoR", request, "user connected", []log.Data(nil)), ShouldBeTrue)
				So(consumerManager.AssertNotCalled(t, "StartConsumer", mock.Anything), ShouldBeTrue)
				So(logger.AssertCalled(t, "ErrorR", request, mock.Anything, []log.Data(nil)), ShouldBeTrue)
				So(response.Code, ShouldEqual, http.StatusBadRequest)
			})
		})
	})
}

func TestHandlerReturnsInternalServerErrorIfConsumerReturnsError(t *testing.T) {
	Convey("Given an error will be raised when the consumer is launched by the request handler", t, func() {
		expectedError := errors.New("something went wrong")
		consumerManager := &mockConsumerRunner{}
		mockController := &mockController{}
		consumerManager.On("StartConsumer", mock.Anything).Return(mockController, expectedError)
		logger := &mockLogger{}
		logger.On("InfoR", mock.Anything, mock.Anything, mock.Anything).Return()
		logger.On("ErrorR", mock.Anything, mock.Anything, mock.Anything).Return()
		requestHandler := NewRequestHandler(consumerManager, logger)
		waitGroup := new(sync.WaitGroup)
		requestHandler.wg = waitGroup
		request := httptest.NewRequest("GET", "/endpoint?offset=3", nil)
		request.Header.Add("X-Request-Id", "123")
		response := httptest.NewRecorder()
		Convey("When a request is made", func() {
			requestHandler.HandleRequest(response, request)
			Convey("Then the response should be HTTP 500 Internal Server Error", func() {
				So(logger.AssertCalled(t, "InfoR", request, "user connected", []log.Data(nil)), ShouldBeTrue)
				So(consumerManager.AssertCalled(t, "StartConsumer", mock.Anything), ShouldBeTrue)
				So(logger.AssertCalled(t, "ErrorR", request, expectedError, []log.Data(nil)), ShouldBeTrue)
				So(response.Code, ShouldEqual, http.StatusInternalServerError)
			})
		})
	})
}

func (s *mockConsumerRunner) StartConsumer(offset int64) (runner.Controllable, error) {
	args := s.Called(offset)
	return args.Get(0).(runner.Controllable), args.Error(1)
}

func (c *mockController) Stop(msg string) {
	c.Called(msg)
}

func (c *mockController) Data() <-chan string {
	return c.Called().Get(0).(chan string)
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

func (l *mockLogger) ErrorR(req *http.Request, err error, data ...log.Data) {
	l.Called(req, err, data)
}
