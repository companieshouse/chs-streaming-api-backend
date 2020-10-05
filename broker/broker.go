package broker

import "errors"

//A broker to which producers will send messages published to all subscribed consumers.
type Broker struct {
	consumerSubscribed   chan *Event
	consumerUnsubscribed chan *Event
	consumers            map[chan string]bool
	data                 chan string
}

//An event that has been emitted to the given broker instance.
type Event struct {
	stream chan string
	result chan *Result
}

//The result of the event after it has been handled by the event handler.
type Result struct {
	hasErrors bool
	msg       string
}

//Create a new broker instance.
func NewBroker() *Broker {
	return &Broker{
		consumerSubscribed:   make(chan *Event),
		consumerUnsubscribed: make(chan *Event),
		consumers:            make(map[chan string]bool),
		data:                 make(chan string),
	}
}

//Subscribe a consumer to this broker.
func (b *Broker) Subscribe() (chan string, error) {
	stream := make(chan string)
	subscription := &Event{
		stream: stream,
		result: make(chan *Result),
	}
	b.consumerSubscribed <- subscription
	<-subscription.result
	close(subscription.result)
	return stream, nil
}

//Unsubscribe a consumer from this broker. If the consumer isn't subscribed to this broker then an error will be
//returned.
func (b *Broker) Unsubscribe(consumer chan string) error {
	subscription := &Event{
		stream: consumer,
		result: make(chan *Result),
	}
	defer close(subscription.result)
	b.consumerUnsubscribed <- subscription
	result := <-subscription.result
	if result.hasErrors {
		return errors.New(result.msg)
	}
	return nil
}

//Publish a message to all subscribed consumers.
func (b *Broker) Publish(msg string) {
	b.data <- msg
}

//Run this broker instance.
func (b *Broker) Run() {
	for {
		select {
		case subscriber := <-b.consumerSubscribed:
			b.consumers[subscriber.stream] = true
			subscriber.result <- &Result{}
		case unsubscribed := <-b.consumerUnsubscribed:
			if _, ok := b.consumers[unsubscribed.stream]; !ok {
				unsubscribed.result <- &Result{
					hasErrors: true,
					msg:       "Attempted to unsubscribe a consumer that was not subscribed",
				}
				continue
			}
			delete(b.consumers, unsubscribed.stream)
			close(unsubscribed.stream)
			unsubscribed.result <- &Result{}
		case data := <-b.data:
			for consumer := range b.consumers {
				consumer <- data
			}
		}
	}
}
