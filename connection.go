package amqprpc

import (
	"errors"

	"github.com/streadway/amqp"
)

var (
	// ErrUnexpectedConnClosed is returned by ListenAndServe() if the server
	// shuts down without calling Stop() and if AMQP does not give an error
	// when said shutdown happens.
	ErrUnexpectedConnClosed = errors.New("unexpected connection close without specific error")
)

// OnStartedFunc can be registered at Server.OnStarted(f) and
// Client.OnStarted(f). This is used when you want to do more setup on the
// connections and/or channels from amqp, for example setting Qos,
// NotifyPublish etc.
type OnStartedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

// ExchangeDeclareSettings is the settings that will be used when a handler
// is mapped to a fanout exchange and an exchange is declared.
type ExchangeDeclareSettings struct {
	// Durable sets the durable flag. Durable exchanges survives server restart.
	Durable bool

	// AutoDelete sets the auto-delete flag, this ensures the exchange is
	// deleted when it isn't bound to any more.
	AutoDelete bool

	// Args sets the arguments table used.
	Args amqp.Table
}

// QueueDeclareSettings is the settings that will be used when the response
// any kind of queue is declared. Se documentation for amqp.QueueDeclare
// for more information about these settings.
type QueueDeclareSettings struct {
	// DeleteWhenUnused sets the auto-delete flag. It's recommended to have this
	// set to false so that amqp-rpc can reconnect and use the same queue while
	// keeping any messages in the queue.
	DeleteWhenUnused bool

	// Durable sets the durable flag. It's recommended to have this set to false
	// and instead use ha-mode for queues and messages.
	Durable bool

	// Exclusive sets the exclusive flag when declaring queues. This flag has
	// no effect on Clients reply-to queues which are never exclusive so it
	// can support reconnects properly.
	Exclusive bool

	// Args sets the arguments table used.
	Args amqp.Table
}

// ConsumeSettings is the settings that will be used when the consumption
// on a specified queue is started.
type ConsumeSettings struct {
	// Consumer sets the consumer tag used when consuming.
	Consumer string

	// AutoAck sets the auto-ack flag. When this is set to false, you must
	// manually ack any deliveries. This is always true for the Client when
	// consuming replies.
	AutoAck bool

	// Exclusive sets the exclusive flag. When this is set to true, no other
	// instances can consume from a given queue. This has no affect on the
	// Client when consuming replies where it's always set to true so that no
	// two clients can consume from the same reply-to queue.
	Exclusive bool

	// QoSPrefetchCount sets the prefetch-count. Set this to a value to ensure
	// that amqp-rpc won't prefetch all messages in the queue. This has no
	// effect on the Client which will always try to fetch everything.
	QoSPrefetchCount int

	// QoSPrefetchSize sets the prefetch-size. Set this to a value to ensure
	// that amqp-rpc won't prefetch all messages in the queue.
	QoSPrefetchSize int

	// Args sets the arguments table used.
	Args amqp.Table
}

// PublishSettings is the settings that will be used when a message is about
// to be published to the message bus. These settings are only used by the
// Client and never by the Server. For the server, Mandatory or Immediate
// can be set on the ResponseWriter instead.
type PublishSettings struct {
	// Mandatory sets the mandatory flag. When this is true a Publish call will
	// be returned if it's not routable by the exchange.
	Mandatory bool

	// Immediate sets the immediate flag. When this is true a Publish call will
	// be returned if a consumer isn't directly available.
	Immediate bool

	// ConfirmMode puts the channel that messages are published over in
	// confirm mode. This makes sending requests more reliable at the cost
	// of some performance. Each publishing must be confirmed by the server.
	// See https://www.rabbitmq.com/confirms.html#publisher-confirms
	ConfirmMode bool
}

func monitorAndWait(stopChan chan struct{}, amqpErrs ...chan *amqp.Error) error {
	result := make(chan error, len(amqpErrs))

	// Setup monitoring for connections and channels, can be several connections and several channels.
	// The first one closed will yield the error.
	for _, errCh := range amqpErrs {
		go func(c chan *amqp.Error) {
			err, ok := <-c
			if !ok {
				result <- ErrUnexpectedConnClosed
				return
			}
			result <- err
		}(errCh)
	}

	select {
	case err := <-result:
		return err
	case <-stopChan:
		return nil
	}
}

func createConnections(url string, config amqp.Config) (conn1, conn2 *amqp.Connection, err error) {
	conn1, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, nil, err
	}

	conn2, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, nil, err
	}

	return conn1, conn2, nil
}

func createChannels(inputConn, outputConn *amqp.Connection) (inputCh, outputCh *amqp.Channel, err error) {
	inputCh, err = inputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	outputCh, err = outputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return inputCh, outputCh, nil
}
