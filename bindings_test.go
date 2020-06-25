package amqprpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanout(t *testing.T) {
	var (
		timesCalled int64
	)

	fanoutHandler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		atomic.AddInt64(&timesCalled, 1)
	}

	for range make([]struct{}, 3) {
		s := NewServer(testURL)
		s.Bind(FanoutBinding("fanout-exchange", fanoutHandler))

		stop := startAndWait(s)

		defer stop()
	}

	c := NewClient(testURL)
	defer c.Stop()

	_, err := c.Send(NewRequest().WithExchange("fanout-exchange").WithResponse(false))
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&timesCalled) == int64(3)
	}, 3*time.Second, 10*time.Millisecond)
}

func TestTopic(t *testing.T) {
	s := NewServer(testURL)
	c := NewClient(testURL)

	defer c.Stop()

	wasCalled := make(chan struct{})

	s.Bind(TopicBinding("", "foo.#", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		wasCalled <- struct{}{}
	}))

	stop := startAndWait(s)
	defer stop()

	_, err := c.Send(NewRequest().
		WithRoutingKey("foo.bar.baz").
		WithExchange("amq.topic").
		WithResponse(false))

	require.NoError(t, err)

	select {
	case <-wasCalled:
	// Yay!
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for request")
	}
}

func TestHeaders(t *testing.T) {
	s := NewServer(testURL)
	c := NewClient(testURL)

	defer c.Stop()

	handler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Hello, world")
	}

	h := amqp.Table{
		"x-match": "all",
		"foo":     "bar",
	}

	s.Bind(HeadersBinding("", h, handler))

	stop := startAndWait(s)
	defer stop()

	// Ensure 'somewhere.*' matches 'somewhere.there'.
	response, err := c.Send(NewRequest().WithExchange("amq.match").WithHeaders(amqp.Table{"foo": "bar"}))

	assert.Nil(t, err, "no errors occurred")
	assert.Equal(t, []byte("Hello, world"), response.Body, "correct request body")
}
