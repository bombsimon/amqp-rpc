package amqprpc

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// Request is a requet to perform with the client
type Request struct {
	// Exchange is the exchange to which the rquest will be published when
	// passing it to the clients send function.
	Exchange string

	// Routing key is the routing key that will be used in the amqp.Publishing
	// request.
	RoutingKey string

	// Reply is a boolean value telling if the request should wait for a reply
	// or just send the request without waiting.
	Reply bool

	// Timeout is the time we should wait after a request is sent before
	// we assume the request got lost.
	Timeout time.Duration

	// Publishing is the publising that are going to be sent.
	Publishing amqp.Publishing

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request. This is only executed for the specific request.
	middlewares []ClientMiddlewareFunc

	// These channels are used by the repliesConsumer and correlcationIdMapping and will send the
	// replies to this Request here.
	response chan *amqp.Delivery
	errChan  chan error // If we get a client error (e.g we can't publish) it will end up here.

	// the number of times that the publisher should retry.
	numRetries int
}

// NewRequest will generate a new request to be published. The default request
// will use the content type "text/plain" and always wait for reply.
func NewRequest(rk string) *Request {
	r := Request{
		RoutingKey:  rk,
		Reply:       true,
		middlewares: []ClientMiddlewareFunc{},
		Publishing: amqp.Publishing{
			ContentType: "text/plain",
			Headers:     amqp.Table{},
		},
	}

	return &r
}

// Write will write the response Body of the amqp.Publishing.
// It is safe to call Write multiple times.
func (r *Request) Write(p []byte) (int, error) {
	r.Publishing.Body = append(r.Publishing.Body, p...)
	return len(p), nil
}

// WriteHeader will write a header for the specified key.
func (r *Request) WriteHeader(header string, value interface{}) {
	r.Publishing.Headers[header] = value
}

// WithExchange will set the exchange on to which the request will be published.
func (r *Request) WithExchange(e string) *Request {
	r.Exchange = e

	return r
}

// WithHeaders will set the full amqp.Table as the headers for the request.
// Note that this will overwrite anything previously set on the headers.
func (r *Request) WithHeaders(h amqp.Table) *Request {
	r.Publishing.Headers = h
	return r
}

// WithTimeout will set the client timeout used when publishing messages.
// t will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (r *Request) WithTimeout(t time.Duration) *Request {
	r.Timeout = t.Round(time.Millisecond)
	return r
}

// WithResponse sets the value determening wether the request should wait for a
// response or not. A request that does not require a response will only catch
// errors occuring before the reuqest has been published.
func (r *Request) WithResponse(wr bool) *Request {
	r.Reply = wr

	return r
}

// WithContentType will update the content type passed in the header of the
// request. This value will bee set as the ContentType in the amqp.Publishing
// type but also preserved as a header value.
func (r *Request) WithContentType(ct string) *Request {
	r.Publishing.ContentType = ct
	return r
}

// WithBody will convert a string to a byte slice and add as the body
// passed for the request.
func (r *Request) WithBody(b string) *Request {
	r.Publishing.Body = []byte(b)

	return r
}

// AddMiddleware will add a middleware which will be executed when the request
// is sent.
func (r *Request) AddMiddleware(m ClientMiddlewareFunc) *Request {
	r.middlewares = append(r.middlewares, m)

	return r
}

// startTimeout will start the timeout counter by using Duration.After.
// Is will also set the Expiration field for the Publishing so that amqp won't
// hold on to the message in the queue after the timeout has happened.
func (r *Request) startTimeout() <-chan time.Time {
	r.Publishing.Expiration = fmt.Sprintf("%d", r.Timeout.Nanoseconds()/1e6)
	return time.After(r.Timeout)
}
