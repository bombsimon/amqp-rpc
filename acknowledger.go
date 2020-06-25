package amqprpc

import (
	"github.com/streadway/amqp"
)

// AwareAcknowledger implements the amqp.Acknowledger interface with the
// addition that it can tell if a message has been acked in any way.
type AwareAcknowledger struct {
	Acknowledger amqp.Acknowledger
	Handled      bool
}

// NewAwareAcknowledger returns the passed acknowledger as an AwareAcknowledger.
func NewAwareAcknowledger(acknowledger amqp.Acknowledger) *AwareAcknowledger {
	return &AwareAcknowledger{
		Acknowledger: acknowledger,
	}
}

// Ack passes the Ack down to the underlying Acknowledger.
func (a *AwareAcknowledger) Ack(tag uint64, multiple bool) error {
	a.Handled = true
	return a.Acknowledger.Ack(tag, multiple)
}

// Nack passes the Nack down to the underlying Acknowledger.
func (a *AwareAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.Handled = true
	return a.Acknowledger.Nack(tag, multiple, requeue)
}

// Reject passes the Reject down to the underlying Acknowledger.
func (a *AwareAcknowledger) Reject(tag uint64, requeue bool) error {
	a.Handled = true
	return a.Acknowledger.Reject(tag, requeue)
}
