package broker

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCreateNewBrokerInstance(t *testing.T) {
	Convey("When a new broker instance is created", t, func() {
		actual := NewBroker()
		Convey("Then a new broker instance should be returned", func() {
			So(actual, ShouldNotBeNil)
		})
	})
}

func TestSubscribeConsumer(t *testing.T) {
	Convey("Given a running broker instance", t, func() {
		broker := NewBroker()
		go broker.Run()
		Convey("When a consumer subscribes to the broker", func() {
			consumer, err := broker.Subscribe()
			Convey("Then a new subscription should be created", func() {
				So(consumer, ShouldNotBeNil)
				So(err, ShouldBeNil)
				So(broker.consumers, ShouldContainKey, consumer)
			})
		})
	})
}

func TestUnsubscribeConsumer(t *testing.T) {
	Convey("Given a running broker instance with a subscribed consumer", t, func() {
		broker := NewBroker()
		go broker.Run()
		consumer, _ := broker.Subscribe()
		Convey("When the consumer unsubscribes", func() {
			err := broker.Unsubscribe(consumer)
			Convey("Then the user should be removed from the list of subscribers", func() {
				So(broker.consumers, ShouldNotContainKey, consumer)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestUnsubscribeConsumerReturnsErrorIfNotSubscribed(t *testing.T) {
	Convey("Given a running broker instance with no subscribed consumers", t, func() {
		broker := NewBroker()
		go broker.Run()
		Convey("When an unsubscribed consumer attempts to unsubscribe", func() {
			err := broker.Unsubscribe(make(chan string))
			Convey("Then an error should be returned", func() {
				So(err.Error(), ShouldEqual, "Attempted to unsubscribe a consumer that was not subscribed")
			})
		})
	})
}

func TestPublishMessage(t *testing.T) {
	Convey("Given a running broker instance with a subscribed consumer", t, func() {
		broker := NewBroker()
		go broker.Run()
		consumer, _ := broker.Subscribe()
		Convey("When a message is published", func() {
			broker.Publish("Hello world!")
			Convey("Then the message should be published to all subscribers", func() {
				So(<-consumer, ShouldEqual, "Hello world!")
			})
		})
	})
}
