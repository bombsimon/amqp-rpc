package amqprpc

import (
	"context"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"

	"github.com/bombsimon/amqp-rpc/logger"
)

type ctxKey string

const (
	// CtxQueueName can be used to get the queue name from the context.Context
	// inside the HandlerFunc.
	CtxQueueName ctxKey = "queue_name"
)

// HandlerFunc is the function that handles all request based on the routing key.
type HandlerFunc func(context.Context, *ResponseWriter, amqp.Delivery)

// processedRequest is used to add the response from a handler func combined
// with a amqp.Delivery. The reasone we need to combine those is that we reply
// to each request in a separate go routine and the delivery is required to
// determine on which queue to reply.
type processedRequest struct {
	replyTo    string
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

// Server represents an AMQP server used within the RPC framework. The
// server uses bindings to keep a list of handler functions.
type Server struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// bindings is a list of HandlerBinding that holds information about the
	// bindings and it's handlers.
	bindings []HandlerBinding

	// middlewares are chained and executed on request.
	middlewares []ServerMiddlewareFunc

	// Every processed request will be responded to in a separate go routine.
	// The server holds a chanel on which all the responses from a handler func
	// is added.
	responses chan processedRequest

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection. By default the dialconfig will
	// include a dail function implemented in connection/dialer.go.
	dialconfig amqp.Config

	// exchangeDelcareSettings is configurations used when declaring a RabbitMQ
	// exchange.
	exchangeDelcareSettings ExchangeDeclareSettings

	// queueDeclareSettings is configuration used when declaring a RabbitMQ
	// queue.
	queueDeclareSettings QueueDeclareSettings

	// consumeSetting is configuration used when consuming from the message
	// bus.
	consumeSettings ConsumeSettings

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}
}

// NewServer will return a pointer to a new Server.
func NewServer(url string) *Server {
	server := Server{
		url:         url,
		bindings:    []HandlerBinding{},
		middlewares: []ServerMiddlewareFunc{},
		dialconfig: amqp.Config{
			Dial: DefaultDialer,
		},
		exchangeDelcareSettings: ExchangeDeclareSettings{Durable: true},
		queueDeclareSettings:    QueueDeclareSettings{},
		consumeSettings:         ConsumeSettings{},
	}

	return &server
}

// WithDialConfig sets the dial config used for the server.
func (s *Server) WithDialConfig(c amqp.Config) *Server {
	s.dialconfig = c

	return s
}

// AddMiddleware will add a ServerMiddleware to the list of middlewares to be
// triggered before the handle func for each request.
func (s *Server) AddMiddleware(m ServerMiddlewareFunc) *Server {
	s.middlewares = append(s.middlewares, m)

	return s
}

// Bind will add a HandlerBinding to the list of servers to serve.
func (s *Server) Bind(binding HandlerBinding) {
	s.bindings = append(s.bindings, binding)
}

// ListenAndServe will dial the RabbitMQ message bus, set up all the channels,
// consume from all RPC server queues and monitor to connection to ensure the
// server is always connected.
func (s *Server) ListenAndServe() {
	s.responses = make(chan processedRequest)
	s.stopChan = make(chan struct{})

	for {
		err := s.listenAndServe()
		if err != nil {
			logger.Warnf("server: got error: %s, will reconnect in %v second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logger.Info("server: listener exiting gracefully")
		break
	}
}

func (s *Server) listenAndServe() error {
	logger.Infof("server: staring listener: %s", s.url)

	// We are using two different connections here because:
	// "It's advisable to use separate connections for Channel.Publish and
	// Channel.Consume so not to have TCP pushback on publishing affect the
	// ability to consume messages [...]"
	// -- https://godoc.org/github.com/streadway/amqp#Channel.Consume
	inputConn, outputConn, err := createConnections(s.url, s.dialconfig)
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

	// Setup a WaitGroup for use by consume(). This WaitGroup will be 0
	// when all consumers are finished consuming messages.
	consumersWg := sync.WaitGroup{}

	// consumerTags is used when we later want to tell AMQP that we want to cancel our consumers.
	consumerTags, err := s.startConsumers(inputCh, &consumersWg)
	if err != nil {
		return err
	}

	// This WaitGroup will reach 0 when the responder() has finished sending all responses.
	responderWg := sync.WaitGroup{}
	go s.responder(outputCh, &responderWg)

	err = monitorAndWait(
		s.stopChan,
		inputConn.NotifyClose(make(chan *amqp.Error)),
		outputConn.NotifyClose(make(chan *amqp.Error)),
		inputCh.NotifyClose(make(chan *amqp.Error)),
		outputCh.NotifyClose(make(chan *amqp.Error)),
	)
	if err != nil {
		return err
	}

	// 1. Tell amqp we want to shut down by cancelling all the consumers.
	for _, consumerTag := range consumerTags {
		err = inputCh.Cancel(consumerTag, false)
		if err != nil {
			return err
		}
	}

	// 2. We've told amqp to stop delivering messages, now we wait for all
	// the consumers to finish inflight messages.
	consumersWg.Wait()

	// 3. Close the responses chan and wait until the consumers are finished.
	// We might still have responses we want to send.
	close(s.responses)
	responderWg.Wait()

	// 4. We have no more messages incoming and we've sent all our responses.
	// The closing of connections and channels are defered so we can just return now.

	return nil
}

func (s *Server) startConsumers(inputCh *amqp.Channel, wg *sync.WaitGroup) ([]string, error) {
	consumerTags := []string{}
	for _, binding := range s.bindings {
		consumerTag, err := s.consume(binding, inputCh, wg)
		if err != nil {
			return []string{}, err
		}

		consumerTags = append(consumerTags, consumerTag)
	}

	return consumerTags, nil
}

func (s *Server) consume(binding HandlerBinding, inputCh *amqp.Channel, wg *sync.WaitGroup) (string, error) {
	queueName, err := s.declareAndBind(inputCh, binding)
	if err != nil {
		return "", err
	}

	consumerTag := uuid.Must(uuid.NewV4()).String()
	deliveries, err := inputCh.Consume(
		queueName,
		consumerTag,
		s.consumeSettings.AutoAck,
		s.consumeSettings.Exclusive,
		s.consumeSettings.NoLocal,
		s.consumeSettings.NoWait,
		s.consumeSettings.Args,
	)

	if err != nil {
		return "", err
	}

	// Attach the middlewares to the handler.
	handler := ServerMiddlewareChain(binding.Handler, s.middlewares...)

	go s.runHandler(handler, deliveries, queueName, wg)

	return consumerTag, nil
}

func (s *Server) runHandler(handler HandlerFunc, deliveries <-chan amqp.Delivery, queueName string, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	logger.Infof("server: waiting for messages on queue '%s'", queueName)

	for delivery := range deliveries {
		// Add one delta to the wait group each time a delivery is handled so
		// we can end by marking it as done. This will ensure that we don't
		// close the responses channel until the very last go routin handling a
		// delivery is finished even though we handle them concurrently.
		wg.Add(1)

		logger.Infof("server: got delivery on queue %v correlation id %v", queueName, delivery.CorrelationId)

		rw := ResponseWriter{
			publishing: &amqp.Publishing{
				CorrelationId: delivery.CorrelationId,
				Body:          []byte{},
			},
		}

		ctx := context.WithValue(context.Background(), CtxQueueName, queueName)

		// Use the default provided Acknowledger for the delivery
		// (amqp.Channel) and add our ack aware acknowledger which can tell if
		// a message has been acknowledged (ack, nack or rejected).
		aac := ackAwareChannel{
			ch:      delivery.Acknowledger,
			handled: false,
		}

		delivery.Acknowledger = &aac

		go func(delivery amqp.Delivery) {
			handler(ctx, &rw, delivery)

			if !aac.IsHandled() {
				delivery.Ack(false)
			}

			s.responses <- processedRequest{
				replyTo:    delivery.ReplyTo,
				mandatory:  rw.mandatory,
				immediate:  rw.immediate,
				publishing: *rw.publishing,
			}

			// Mark the specific delivery as finished.
			wg.Done()
		}(delivery)
	}

	logger.Infof("server: stopped waiting for messages on queue '%s'", queueName)
}

func (s *Server) declareAndBind(inputCh *amqp.Channel, binding HandlerBinding) (string, error) {
	queue, err := inputCh.QueueDeclare(
		binding.RoutingKey,
		s.queueDeclareSettings.Durable,
		s.queueDeclareSettings.DeleteWhenUnused,
		s.queueDeclareSettings.Exclusive,
		s.queueDeclareSettings.NoWait,
		s.queueDeclareSettings.Args,
	)

	if err != nil {
		return "", err
	}

	if binding.ExchangeName == "" {
		return queue.Name, nil
	}

	err = inputCh.ExchangeDeclare(
		binding.ExchangeName,
		binding.ExchangeType,
		s.exchangeDelcareSettings.Durable,
		s.exchangeDelcareSettings.AutoDelete,
		s.exchangeDelcareSettings.Internal,
		s.exchangeDelcareSettings.NoWait,
		s.exchangeDelcareSettings.Args,
	)

	if err != nil {
		return "", err
	}

	err = inputCh.QueueBind(
		queue.Name,
		binding.RoutingKey,
		binding.ExchangeName,
		s.queueDeclareSettings.NoWait, // Use same value as for declaring.
		binding.BindHeaders,
	)

	if err != nil {
		return "", err
	}

	return queue.Name, nil
}

func (s *Server) responder(outCh *amqp.Channel, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for response := range s.responses {
		err := outCh.Publish(
			"", // exchange
			response.replyTo,
			response.mandatory,
			response.immediate,
			response.publishing,
		)

		if err != nil {
			logger.Warnf("server: could not publish response: %s", err.Error())
			outCh.Close()

			// We resend the response here so that other running goroutines
			// that hav a working outCh can pick up this response.
			s.responses <- response
			return
		}
	}
}

// Stop will gracefully disconnect from AMQP after draining first incoming then
// outgoing messages. This method won't wait for server shutdown to complete,
// you should instead wait for ListenAndServe to exit.
func (s *Server) Stop() {
	close(s.stopChan)
}
