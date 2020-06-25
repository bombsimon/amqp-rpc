package amqprpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Request is a requet to perform with the client.
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

	// Timeout is the time we should wait after a request is published before
	// we assume the request got lost.
	Timeout time.Duration

	// timeoutAt is the exact time when the request times out. This is set by
	// the client when starting the countdown.
	timeoutAt time.Time

	// Publishing is the publising that are going to be published.
	Publishing amqp.Publishing

	// Context is a context which you can use to pass data from where the
	// request is created to middlewares. By default this will be a
	// context.Background()
	Context context.Context

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request. This is only executed for the specific request.
	middlewares []ClientMiddlewareFunc

	// These channels are used by the repliesConsumer and correlcationIdMapping and will send the
	// replies to this Request here.
	response chan *amqp.Delivery
	errChan  chan error // If we get a client error (e.g we can't publish) it will end up here.

	// the number of times that the publisher should retry.
	numRetries int

	deliveryTag uint64

	confirmed chan struct{}
	returned  *amqp.Return
}

// NewRequest will generate a new request to be published. The default request
// will use the content type "text/plain" and always wait for reply.
func NewRequest() *Request {
	r := Request{
		Context:     context.Background(),
		Reply:       true,
		middlewares: []ClientMiddlewareFunc{},
		Publishing: amqp.Publishing{
			ContentType: "text/plain",
			Headers:     amqp.Table{},
		},
	}

	return &r
}

// WithRoutingKey will set the routing key for the request.
func (r *Request) WithRoutingKey(rk string) *Request {
	r.RoutingKey = rk

	return r
}

// WithCorrelationID will add/overwrite the correlation ID used for the
// request and set it on the Publishing.
func (r *Request) WithCorrelationID(id string) *Request {
	r.Publishing.CorrelationId = id

	return r
}

// WithContext will set the context on the request.
func (r *Request) WithContext(ctx context.Context) *Request {
	r.Context = ctx

	return r
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

// WithResponse sets the value determining wether the request should wait for a
// response or not. A request that does not require a response will only catch
// errors occurring before the reuqest has been published.
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

// Write will write the response Body of the amqp.Publishing.
// It is safe to call Write multiple times.
func (r *Request) Write(p []byte) (int, error) {
	r.Publishing.Body = append(r.Publishing.Body, p...)

	return len(p), nil
}

// AddMiddleware will add a middleware which will be executed when the request
// is published.
func (r *Request) AddMiddleware(m ClientMiddlewareFunc) *Request {
	r.middlewares = append(r.middlewares, m)

	return r
}

// startTimeout will start the timeout counter. Is will also set the Expiration
// field for the Publishing so that amqp won't hold on to the message in the
// queue after the timeout has happened.
func (r *Request) startTimeout(defaultTimeout time.Duration) {
	if r.Timeout.Nanoseconds() == 0 {
		r.WithTimeout(defaultTimeout)
	}

	if r.Reply {
		r.Publishing.Expiration = fmt.Sprintf("%d", r.Timeout.Nanoseconds()/1e6)
	}

	r.timeoutAt = time.Now().Add(r.Timeout)
}

// AfterTimeout waits for the duration of the timeout.
func (r *Request) AfterTimeout() <-chan time.Time {
	return time.After(time.Until(r.timeoutAt))
}

// RequestMap keeps track of requests based on their DeliveryTag and/or
// CorrelationID.
type RequestMap struct {
	byDeliveryTag   map[uint64]*Request
	byCorrelationID map[string]*Request
	mu              sync.RWMutex
}

// GetByCorrelationID returns the request with the provided correlation id.
func (m *RequestMap) GetByCorrelationID(key string) (*Request, bool) {
	m.mu.RLock()
	r, ok := m.byCorrelationID[key]
	m.mu.RUnlock()

	return r, ok
}

// GetByDeliveryTag returns the request with the provided delivery tag.
func (m *RequestMap) GetByDeliveryTag(key uint64) (*Request, bool) {
	m.mu.RLock()
	r, ok := m.byDeliveryTag[key]
	m.mu.RUnlock()

	return r, ok
}

// Set will add r to m so it can be fetched later using it's correlation id or
// delivery tag.
func (m *RequestMap) Set(r *Request) {
	m.mu.Lock()
	m.byDeliveryTag[r.deliveryTag] = r
	m.byCorrelationID[r.Publishing.CorrelationId] = r
	m.mu.Unlock()
}

// Delete will remove r from m.
func (m *RequestMap) Delete(r *Request) {
	m.mu.Lock()
	delete(m.byDeliveryTag, r.deliveryTag)
	delete(m.byCorrelationID, r.Publishing.CorrelationId)
	m.mu.Unlock()
}
