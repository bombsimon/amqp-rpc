package main

import (
	"context"
	"fmt"
	"time"

	amqprpc "github.com/0x4b53/amqp-rpc"

	"github.com/streadway/amqp"
)

//nolint:gochecknoglobals // OK for this example
var timesCalled = 0

const (
	exchangeName = "cool-exchange"
	routingKey   = "times_called"
	uri          = "amqp://guest:guest@localhost:5672/"
)

func main() {
	qds := amqprpc.QueueDeclareSettings{
		Exclusive: true,
	}

	s1 := amqprpc.NewServer(uri).WithQueueDeclareSettings(qds)
	s2 := amqprpc.NewServer(uri).WithQueueDeclareSettings(qds)
	s3 := amqprpc.NewServer(uri).WithQueueDeclareSettings(qds)

	// No need for three handlers but it's just to show that different methods
	// will be called.
	s1.Bind(amqprpc.FanoutBinding(exchangeName, fanoutHandlerOne))
	s2.Bind(amqprpc.FanoutBinding(exchangeName, fanoutHandlerTwo))
	s3.Bind(amqprpc.FanoutBinding(exchangeName, fanoutHandlerThree))

	s1.Bind(amqprpc.DirectBinding(routingKey, timesCalledHandler))

	go s1.ListenAndServe()
	go s2.ListenAndServe()
	go s3.ListenAndServe()

	time.Sleep(1 * time.Second)

	c := amqprpc.NewClient(uri)
	r := amqprpc.NewRequest().
		WithExchange(exchangeName).
		WithBody("Seding fanout").
		WithResponse(false)

	_, err := c.Send(r)
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	time.Sleep(1 * time.Second)

	response, err := c.Send(amqprpc.NewRequest().WithRoutingKey(routingKey))
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	fmt.Printf("The fanout call has been handled %s times\n", response.Body)
}

func fanoutHandlerOne(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "First server handled request")
}

func fanoutHandlerTwo(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Second server handled request")
}

func fanoutHandlerThree(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Third server handled request")
}

func timesCalledHandler(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, timesCalled)
}
