package amqprpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

const (
	// chanSendWaitTime is the maximum time we will wait when sending a
	// response, confirm or error on the corresponding channels. This is so that
	// we won't block forever if the listening goroutine has stopped listening.
	chanSendWaitTime = 1 * time.Second
)

var (
	// ErrRequestReturned can be returned by Client#Send() when the server
	// returns the message. For example when mandatory is set but the message
	// can not be routed.
	ErrRequestReturned = errors.New("publishing returned")

	// ErrRequestRejected can be returned by Client#Send() when the server Nacks
	// the message. This can happen if there is some problem inside the amqp
	// server. To check if the error returned is a ErrRequestReturned error, use
	// errors.Is(err, ErrRequestRejected).
	ErrRequestRejected = errors.New("publishing Nacked")

	// ErrRequestTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration. To check if the
	// error returned is an ErrRequestTimeout error, use errors.Is(err,
	// ErrRequestTimeout).
	ErrRequestTimeout = errors.New("request timed out")
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// timeout is the time we should wait after a request is published before
	// we assume the request got lost.
	timeout time.Duration

	// maxRetries is the amount of times a request will be retried before
	// giving up.
	maxRetries int

	// requests is a single channel used whenever we want to publish a message.
	// The channel is consumed in a separate go routine which allows us to add
	// messages to the channel that we don't want replies from without the need
	// to wait for on going requests.
	requests chan *Request

	// requestsMap will keep track of requests waiting for confirmations,
	// replies or returns. it maps each correlation ID and delivery tag of a
	// message to the actual Request. This is to ensure that no matter the order
	// of a request and response, we will always publish the response to the
	// correct consumer.
	requestsMap RequestMap

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dialer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dial function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// queueDeclareSettings is configuration used when declaring a RabbitMQ
	// queue.
	queueDeclareSettings QueueDeclareSettings

	// consumeSettings is configuration used when consuming from the message
	// bus.
	consumeSettings ConsumeSettings

	// publishSettings is the configuration used when publishing a message with
	// the client
	publishSettings PublishSettings

	// replyToQueueName can be used to avoid generating queue names on the
	// message bus and use a pre defined name throughout the usage of a client.
	replyToQueueName string

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request.
	middlewares []ClientMiddlewareFunc

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// didStopChan will close when the client has finished shutdown.
	didStopChan chan struct{}

	// isRunning is 1 when the server is running.
	isRunning int32

	// wantStop tells the runForever function to exit even on connection errors.
	wantStop int32

	// errorLog specifies an optional logger for amqp errors, unexpected
	// behavior etc. If nil, logging is done via the log package's standard
	// logger.
	errorLog LogFunc

	// debugLog specifies an optional logger for debugging, this logger will
	// print most of what is happening internally.
	// If nil, logging is not done.
	debugLog LogFunc

	// Sender is the main send function called after all middlewares has been
	// chained and called. This field can be overridden to simplify testing.
	Sender SendFunc

	// onStarteds will all be executed after the client has connected.
	onStarteds []OnStartedFunc
}

// NewClient will return a pointer to a new Client. There are two ways to manage
// the connection that will be used by the client (i.e. when using TLS).
//
// The first one is to use the Certificates type and just pass the filenames to
// the client certificate, key and the server CA. If this is done the function
// will handle the reading of the files.
//
// It is also possible to create a custom amqp.Config with whatever
// configuration desired and that will be used as dial configuration when
// connection to the message bus.
func NewClient(url string) *Client {
	c := &Client{
		url: url,
		dialconfig: amqp.Config{
			Dial: DefaultDialer,
		},
		requests: make(chan *Request),
		requestsMap: RequestMap{
			byDeliveryTag:   make(map[uint64]*Request),
			byCorrelationID: make(map[string]*Request),
		},
		replyToQueueName: "reply-to-" + uuid.New().String(),
		middlewares:      []ClientMiddlewareFunc{},
		timeout:          time.Second * 10,
		maxRetries:       10,
		errorLog:         log.Printf,                                  // use the standard logger default.
		debugLog:         func(format string, args ...interface{}) {}, // don't print anything default.
	}

	c.Sender = c.send

	// Set default values to use when creating channels and consumers.
	c.setDefaults()

	return c
}

/*
OnStarted can be used to hook into the connections/channels that the client is
using. This can be useful if you want more control over amqp directly.
Note that since the client is lazy and won't connect until the first .Send()
the provided OnStartedFunc won't be called until then. Also note that this
is blocking and the client won't continue it's startup until this function has
finished executing.

	client := NewClient(url)
	client.OnStarted(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
		// Do something with amqp connections/channels.
	})
*/
func (c *Client) OnStarted(f OnStartedFunc) {
	c.onStarteds = append(c.onStarteds, f)
}

// WithDialConfig sets the dial config used for the client.
//nolint:gocritic // No need to pass config pointer.
func (c *Client) WithDialConfig(dc amqp.Config) *Client {
	c.dialconfig = dc

	return c
}

// WithTLS sets the TLS config in the dial config for the client.
func (c *Client) WithTLS(tlsConfig *tls.Config) *Client {
	c.dialconfig.TLSClientConfig = tlsConfig

	return c
}

// WithErrorLogger sets the logger to use for error logging.
func (c *Client) WithErrorLogger(f LogFunc) *Client {
	c.errorLog = f

	return c
}

// WithDebugLogger sets the logger to use for debug logging.
func (c *Client) WithDebugLogger(f LogFunc) *Client {
	c.debugLog = f

	return c
}

// WithQueueDeclareSettings will set the settings used when declaring queues
// for the client globally.
func (c *Client) WithQueueDeclareSettings(s QueueDeclareSettings) *Client {
	c.queueDeclareSettings = s

	return c
}

// WithConsumeSettings will set the settings used when consuming in the client
// globally.
func (c *Client) WithConsumeSettings(s ConsumeSettings) *Client {
	c.consumeSettings = s

	return c
}

// WithPublishSettings will set the client publishing settings when publishing
// messages.
func (c *Client) WithPublishSettings(s PublishSettings) *Client {
	c.publishSettings = s

	return c
}

// WithConfirmMode sets the confirm-mode on the client. This causes the client
// to wait for confirmations, and if none arrives or the confirmation is marked
// as Nack, Client#Send() returns a corresponding error.
func (c *Client) WithConfirmMode(confirmMode bool) *Client {
	c.publishSettings.ConfirmMode = confirmMode

	return c
}

// WithTimeout will set the client timeout used when publishing messages.
// t will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (c *Client) WithTimeout(t time.Duration) *Client {
	c.timeout = t.Round(time.Millisecond)

	return c
}

// WithMaxRetries sets the maximum amount of times the client will retry
// sending the request before giving up and returning the error to the caller
// of c.Send(). This retry will persist during reconnects.
func (c *Client) WithMaxRetries(n int) *Client {
	c.maxRetries = n

	return c
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *Client) AddMiddleware(m ClientMiddlewareFunc) *Client {
	c.middlewares = append(c.middlewares, m)

	return c
}

func (c *Client) setDefaults() {
	c.queueDeclareSettings = QueueDeclareSettings{
		Durable:          true,
		DeleteWhenUnused: false,
		Exclusive:        false,
		Args: map[string]interface{}{
			// Ensure the queue is deleted automatically when it's unused for
			// more than the set time. This is to ensure that messages that
			// are in flight during a reconnect doesn't get lost (which might
			// happen when using `DeleteWhenUnused`).
			"x-expires": 1 * 60 * 1000, // 1 minute.
		},
	}

	c.consumeSettings = ConsumeSettings{
		Consumer:  "",
		AutoAck:   true,
		Exclusive: true,
		Args:      nil,
	}

	c.publishSettings = PublishSettings{
		Mandatory:   true,
		Immediate:   false,
		ConfirmMode: true,
	}
}

// runForever will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also automatically restart
// the setup if the underlying connection or socket isn't gracefully closed.
// This will also block until the client is gracefully stopped.
func (c *Client) runForever() {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		// Already running.
		return
	}

	// Always assume that we don't want to stop initially.
	atomic.StoreInt32(&c.wantStop, 0)

	c.stopChan = make(chan struct{})
	c.didStopChan = make(chan struct{})

	go func() {
		for {
			c.debugLog("client: connecting...")

			err := c.runOnce()
			if err == nil {
				c.debugLog("client: finished gracefully")
				break
			}

			if atomic.LoadInt32(&c.wantStop) == 1 {
				c.debugLog("client: finished with error %s", err.Error())
				break
			}

			c.errorLog("client: got error: %s, will reconnect in %v second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
		}

		// Tell c.Close() that we have finished shutdown and that it can return.
		close(c.didStopChan)

		// Ensure we can start again.
		atomic.StoreInt32(&c.isRunning, 0)
	}()
}

// runOnce will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *Client) runOnce() error {
	c.debugLog("client: starting up...")

	inputConn, outputConn, err := createConnections(c.url, c.dialconfig)
	if err != nil {
		return err
	}

	defer inputConn.Close()
	defer outputConn.Close()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return err
	}

	defer inputCh.Close()
	defer outputCh.Close()

	// Notify everyone that the client has started. Runs sequentially so there
	// isn't any race conditions when working with the connections or channels.
	for _, onStarted := range c.onStarteds {
		onStarted(inputConn, outputConn, inputCh, outputCh)
	}

	err = c.runRepliesConsumer(inputCh)
	if err != nil {
		return err
	}

	if c.publishSettings.ConfirmMode {
		// ConfirmMode is wanted, tell the amqp-server that we want to enable
		// confirm-mode on this channel and start the confirms consumer.
		err = outputCh.Confirm(
			false, // no-wait.
		)

		if err != nil {
			return err
		}

		go c.runConfirmsConsumer(
			outputCh.NotifyPublish(make(chan amqp.Confirmation)),
			outputCh.NotifyReturn(make(chan amqp.Return)),
		)
	}

	go c.runPublisher(outputCh)

	err = monitorAndWait(
		c.stopChan,
		inputConn.NotifyClose(make(chan *amqp.Error)),
		outputConn.NotifyClose(make(chan *amqp.Error)),
		inputCh.NotifyClose(make(chan *amqp.Error)),
		outputCh.NotifyClose(make(chan *amqp.Error)),
	)

	if err != nil {
		return err
	}

	return nil
}

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in chan requests unless we have retried to many times.
func (c *Client) runPublisher(ouputChan *amqp.Channel) {
	c.debugLog("client: running publisher...")

	// Monitor the closing of this channel. We need to do this in a separate,
	// goroutine to ensure we won't get a deadlock inside the select below
	// which can itself close this channel.
	onClose := make(chan struct{})

	go func() {
		<-ouputChan.NotifyClose(make(chan *amqp.Error))
		close(onClose)
	}()

	// Delivery tags always starts at 1 but we increase it before we do any
	// .Publish() on the channel.
	nextDeliveryTag := uint64(0)

	for {
		select {
		case <-onClose:
			// The channels for publishing responses was closed, once the
			// client has started again. This loop will be restarted.
			c.debugLog("client: publisher stopped after channel was closed")
			return
		case request := <-c.requests:
			// Set the ReplyTo if needed, or ensure it's empty if it's not.
			if request.Reply {
				request.Publishing.ReplyTo = c.replyToQueueName
			} else {
				request.Publishing.ReplyTo = ""
			}

			c.debugLog("client: publishing %s", request.Publishing.CorrelationId)

			// Setup the delivery tag for this request.
			nextDeliveryTag++
			request.deliveryTag = nextDeliveryTag

			// Ensure the replies, returns and confirms consumers can get a hold
			// of this request once they come in.
			c.requestsMap.Set(request)

			err := ouputChan.Publish(
				request.Exchange,
				request.RoutingKey,
				c.publishSettings.Mandatory,
				c.publishSettings.Immediate,
				request.Publishing,
			)

			if err != nil {
				ouputChan.Close()

				c.retryRequest(request, err)

				c.errorLog(
					"client: publisher stopped because of error: %s, request: %s",
					err.Error(),
					stringifyRequestForLog(request),
				)

				return
			}

			if !c.publishSettings.ConfirmMode {
				// We're not in confirm mode so we confirm that we have sent
				// the request here.
				c.confirmRequest(request)

				if !request.Reply {
					// Since we won't get a confirmation of this request and
					// we don't want to have a reply, just return nil to the
					// caller.
					c.respondToRequest(request, nil)
				}
			}
		}
	}
}

// retryRequest will retry the provided request, unless the request already
// has been retried too many times. Then the provided error will be sent to the
// caller instead.
func (c *Client) retryRequest(request *Request, err error) {
	if request.numRetries >= c.maxRetries {
		// We have already retried too many times
		c.errorLog(
			"client: could not publish, giving up: reason: %s, %s",
			err.Error(),
			stringifyRequestForLog(request),
		)

		// We shouldn't wait for confirmations any more because they will never
		// arrive.
		c.confirmRequest(request)

		// Return whatever error .Publish returned to the caller.
		c.respondErrorToRequest(request, err)

		return
	}

	request.numRetries++

	go func() {
		c.debugLog("client: queuing request for retry: reason: %s, %s", err.Error(), stringifyRequestForLog(request))

		select {
		case c.requests <- request:
		case <-request.AfterTimeout():
			c.errorLog(
				"client: request timed out while waiting for retry reason: %s, %s",
				err.Error(),
				stringifyRequestForLog(request),
			)
		}
	}()
}

// runConfirmsConsumer will consume both confirmations and returns and since
// returns always arrives before confirmations we want to finish handling any
// return before we handle any confirmations.
func (c *Client) runConfirmsConsumer(confirms chan amqp.Confirmation, returns chan amqp.Return) {
	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				return
			}

			request, ok := c.requestsMap.GetByCorrelationID(ret.CorrelationId)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout. But since returns are normally very fast that
				// would mean that something isn't quite right on the amqp server.
				c.errorLog("client: got return for unknown request: %s", stringifyReturnForLog(ret))
				continue
			}

			c.debugLog("client: publishing is returned by server: %s", ret.CorrelationId)

			request.returned = &ret

		case confirm, ok := <-confirms:
			if !ok {
				return
			}

			request, ok := c.requestsMap.GetByDeliveryTag(confirm.DeliveryTag)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout. But since confirmations are normally very fast that
				// would mean that something isn't quite right on the amqp server.
				// Unfortunately there isn't any way of getting more information
				// than the delivery tag from a confirmation.
				c.errorLog("client: got confirmation of unknown request: %d", confirm.DeliveryTag)
				continue
			}

			c.debugLog("client: confirming request %s", request.Publishing.CorrelationId)

			c.confirmRequest(request)

			if !confirm.Ack {
				c.errorLog("client: publishing is rejected by server: %s", stringifyRequestForLog(request))

				c.respondErrorToRequest(request, ErrRequestRejected)

				// Doesn't matter if the request wants the nil reply below because
				// we gave it an error instead.
				continue
			}

			// Check if the request was also returned.
			if request.returned != nil {
				c.respondErrorToRequest(
					request,
					fmt.Errorf("%w: %d, %s",
						ErrRequestReturned,
						request.returned.ReplyCode,
						request.returned.ReplyText,
					),
				)

				continue
			}

			if !request.Reply {
				// The request isn't expecting a reply so we need give a nil
				// response instead to signal that we're done.
				c.debugLog(
					"client: sending nil response after confirmation due to no reply wanted %s",
					request.Publishing.CorrelationId,
				)

				c.respondToRequest(request, nil)
			}
		}
	}
}

// respondErrorToRequest will return the provided response to the caller.
func (c *Client) respondToRequest(request *Request, response *amqp.Delivery) {
	select {
	case request.response <- response:
		return
	case <-time.After(chanSendWaitTime):
		c.errorLog(
			"client: nobody is waiting for response on: %s, response: %s",
			stringifyRequestForLog(request),
			stringifyDeliveryForLog(response),
		)
	}
}

// respondErrorToRequest will return the provided error to the caller.
func (c *Client) respondErrorToRequest(request *Request, err error) {
	select {
	case request.errChan <- err:
		return
	case <-time.After(chanSendWaitTime):
		c.errorLog(
			"nobody is waiting for error on: %s, error: %s",
			stringifyRequestForLog(request),
			err.Error(),
		)
	}
}

// confirmRequest will mark the provided request as confirmed by the amqp
// server.
func (c *Client) confirmRequest(request *Request) {
	select {
	case request.confirmed <- struct{}{}:
		return
	case <-time.After(chanSendWaitTime):
		c.errorLog("nobody is waiting for confirmation on: %s", stringifyRequestForLog(request))
	}
}

// runRepliesConsumer will declare and start consuming from the queue where we
// expect replies to come back. The method will stop consuming if the
// underlying amqp channel is closed for any reason.
func (c *Client) runRepliesConsumer(inChan *amqp.Channel) error {
	queue, err := inChan.QueueDeclare(
		c.replyToQueueName,
		c.queueDeclareSettings.Durable,
		c.queueDeclareSettings.DeleteWhenUnused,
		c.queueDeclareSettings.Exclusive,
		false, // no-wait.
		c.queueDeclareSettings.Args,
	)

	if err != nil {
		return err
	}

	messages, err := inChan.Consume(
		queue.Name,
		c.consumeSettings.Consumer,
		c.consumeSettings.AutoAck,
		c.consumeSettings.Exclusive,
		false, // no-local.
		false, // no-wait.
		c.consumeSettings.Args,
	)

	if err != nil {
		return err
	}

	go func() {
		c.debugLog("client: running replies consumer...")

		for response := range messages {
			request, ok := c.requestsMap.GetByCorrelationID(response.CorrelationId)
			if !ok {
				c.errorLog(
					"client: could not find where to reply. CorrelationId: %s",
					stringifyDeliveryForLog(&response),
				)

				continue
			}

			c.debugLog("client: forwarding reply %s", response.CorrelationId)

			responseCopy := response

			select {
			case request.response <- &responseCopy:
			case <-time.After(chanSendWaitTime):
				c.errorLog("client: could not send to reply response chan: %s", stringifyRequestForLog(request))
			}
		}

		c.debugLog("client: replies consumer is done")
	}()

	return nil
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.Sender, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// Ensure that the publisher is running.
	c.runForever()

	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after publisher has Published the
	// message.
	r.response = make(chan *amqp.Delivery)

	// This channel is sent to when the request is confirmed. This can happen
	// both when confirm-mode is set. And if not set, it's automatically
	// confirmed once the request is published.
	r.confirmed = make(chan struct{})

	// This is where we get any (client) errors if they occur before we could
	// even send the request.
	r.errChan = make(chan error)

	// Set the correlation id on the publishing if not yet set.
	if r.Publishing.CorrelationId == "" {
		r.Publishing.CorrelationId = uuid.New().String()
	}

	defer c.requestsMap.Delete(r)

	r.startTimeout(c.timeout)
	timeoutChan := r.AfterTimeout()

	c.debugLog("client: queuing request %s", r.Publishing.CorrelationId)

	select {
	case c.requests <- r:
		// successful send.
	case <-timeoutChan:
		c.debugLog("client: timeout while waiting for request queue %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for request queue", ErrRequestTimeout)
	}

	c.debugLog("client: waiting for reply of %s", r.Publishing.CorrelationId)

	// We hang here until the request has been published (or when confirm-mode
	// is on; confirmed).
	select {
	case <-r.confirmed:
		// got confirmation.
	case <-timeoutChan:
		c.debugLog("client: timeout while waiting for request confirmation %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for confirmation", ErrRequestTimeout)
	}

	// All responses are published on the requests response channel. Hang here
	// until a response is received and close the channel when it's read.
	select {
	case err := <-r.errChan:
		c.debugLog("client: error for %s, %s", r.Publishing.CorrelationId, err.Error())
		return nil, err

	case <-timeoutChan:
		c.debugLog("client: timeout for %s", r.Publishing.CorrelationId)
		return nil, fmt.Errorf("%w while waiting for response", ErrRequestTimeout)

	case delivery := <-r.response:
		c.debugLog("client: got delivery for %s", r.Publishing.CorrelationId)
		return delivery, nil
	}
}

// Stop will gracefully disconnect from AMQP. It is not guaranteed that all
// in flight requests or responses are handled before the disconnect. Instead
// the user should ensure that all calls to c.Send() has returned before calling
// c.Stop().
func (c *Client) Stop() {
	if atomic.LoadInt32(&c.isRunning) != 1 {
		return
	}

	atomic.StoreInt32(&c.wantStop, 1)

	close(c.stopChan)
	<-c.didStopChan
}
