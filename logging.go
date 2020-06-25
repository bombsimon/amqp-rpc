package amqprpc

import (
	"fmt"
	"sort"
	"strings"

	"github.com/streadway/amqp"
)

/*
LogFunc is used for logging in amqp-rpc. It makes it possible to define your own logging.

Here is an example where the logger from the log package is used:

	debugLogger := log.New(os.Stdout, "DEBUG - ", log.LstdFlags)
	errorLogger := log.New(os.Stdout, "ERROR - ", log.LstdFlags)

	server := NewServer(url)
	server.WithErrorLogger(errorLogger.Printf)
	server.WithDebugLogger(debugLogger.Printf)

It can also be used with for example a Logrus logger:

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.Formatter = &logrus.JSONFormatter{}

	s.WithErrorLogger(logger.Warnf)
	s.WithDebugLogger(logger.Debugf)

	client := NewClient(url)
	client.WithErrorLogger(logger.Errorf)
	client.WithDebugLogger(logger.Debugf)

*/
type LogFunc func(format string, args ...interface{})

func stringifyTableForLog(v amqp.Table) string {
	if len(v) == 0 {
		return "[]"
	}

	vals := []string{}

	for key, val := range v {
		if inner, ok := val.(amqp.Table); ok {
			val = stringifyTableForLog(inner)
		}

		strVal := fmt.Sprintf("%v", val)

		if strVal == "" {
			continue
		}

		vals = append(vals, fmt.Sprintf("%s=%s", key, strVal))
	}

	sort.Strings(vals)

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyDeliveryForLog(v *amqp.Delivery) string {
	if v == nil {
		return "[nil]"
	}

	vals := []string{}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationId=%s", v.CorrelationId))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyPublishingForLog(v amqp.Publishing) string {
	vals := []string{}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationID=%s", v.CorrelationId))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyRequestForLog(v *Request) string {
	if v == nil {
		return "[nil]"
	}

	vals := []string{}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	vals = append(vals, fmt.Sprintf("Publishing=%s", stringifyPublishingForLog(v.Publishing)))

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}

func stringifyReturnForLog(v amqp.Return) string {
	vals := []string{
		fmt.Sprintf("ReplyCode=%d", v.ReplyCode),
		fmt.Sprintf("ReplyText=%s", v.ReplyText),
	}

	if v.Exchange != "" {
		vals = append(vals, fmt.Sprintf("Exchange=%s", v.Exchange))
	}

	if v.RoutingKey != "" {
		vals = append(vals, fmt.Sprintf("RoutingKey=%s", v.RoutingKey))
	}

	if v.CorrelationId != "" {
		vals = append(vals, fmt.Sprintf("CorrelationID=%s", v.CorrelationId))
	}

	if v.Type != "" {
		vals = append(vals, fmt.Sprintf("Type=%s", v.Type))
	}

	if v.AppId != "" {
		vals = append(vals, fmt.Sprintf("AppId=%s", v.AppId))
	}

	if v.UserId != "" {
		vals = append(vals, fmt.Sprintf("UserId=%s", v.UserId))
	}

	if len(v.Headers) > 0 {
		vals = append(vals, fmt.Sprintf("Headers=%s", stringifyTableForLog(v.Headers)))
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ", "))
}
