package amqprpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	testURL                  = "amqp://guest:guest@localhost:5672/"
	serverAPITestURL         = "http://guest:guest@localhost:15672/api"
	defaultTestQueue         = "test.queue"
	testClientConnectionName = "test-client"
	testServerConnectionName = "test-server"
)

// MockAcknowledger is a mocked amqp.Acknowledger, useful for tests.
type MockAcknowledger struct {
	Acks    int
	Nacks   int
	Rejects int
}

// Ack increases Acks.
func (ma *MockAcknowledger) Ack(tag uint64, multiple bool) error {
	ma.Acks++
	return nil
}

// Nack increases Nacks.
func (ma *MockAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	ma.Nacks++
	return nil
}

// Reject increases Rejects.
func (ma *MockAcknowledger) Reject(tag uint64, requeue bool) error {
	ma.Rejects++
	return nil
}

// startAndWait will start s by running ListenAndServe, it will then block
// until the server is started.
func startAndWait(s *Server) func() {
	started := make(chan struct{})
	once := sync.Once{}

	s.OnStarted(func(_, _ *amqp.Connection, _, _ *amqp.Channel) {
		once.Do(func() {
			close(started)
		})
	})

	done := make(chan struct{})

	go func() {
		s.ListenAndServe()
		close(done)
	}()

	<-started

	return func() {
		s.Stop()
		<-done
	}
}

func deleteQueue(name string) {
	queueURL := fmt.Sprintf("%s/queues/%s/%s", serverAPITestURL, url.PathEscape("/"), url.PathEscape(name))

	req, err := http.NewRequest("DELETE", queueURL, nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	_ = resp.Body.Close()
}

func closeConnections(names ...string) {
	var (
		connectionsURL = fmt.Sprintf("%s/connections", serverAPITestURL)
		connections    []map[string]interface{}
	)

	// It takes a while (0.5s - 4s) for the management plugin to discover the
	// connections so we loop until we've found some.
	for i := 0; i < 20; i++ {
		resp, err := http.Get(connectionsURL) //nolint:gosec // This URL is fine!
		if err != nil {
			panic(err)
		}

		err = json.NewDecoder(resp.Body).Decode(&connections)
		if err != nil {
			panic(err)
		}

		_ = resp.Body.Close()

		if len(connections) == 0 {
			time.Sleep(time.Duration(i*100) * time.Millisecond)
			continue
		}

		break
	}

	for _, conn := range connections {
		// Should we close this connection?
		shouldRemove := false

		for _, name := range names {
			if conn["user_provided_name"] == name {
				shouldRemove = true
				break
			}
		}

		if !shouldRemove {
			continue
		}

		connectionURL := fmt.Sprintf("%s/connections/%s", serverAPITestURL, url.PathEscape(conn["name"].(string)))

		req, err := http.NewRequest("DELETE", connectionURL, nil)
		if err != nil {
			panic(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}

		_ = resp.Body.Close()

		fmt.Println("closed", conn["user_provided_name"])
	}
}

func testServer() *Server {
	server := NewServer(testURL).
		WithDebugLogger(testLogFunc("debug")).
		WithErrorLogger(testLogFunc("ERROR")).
		WithDialConfig(
			amqp.Config{
				Properties: amqp.Table{
					"connection_name": testServerConnectionName,
				},
			},
		)

	server.Bind(DirectBinding(defaultTestQueue, func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	return server
}

func testClient() *Client {
	return NewClient(testURL).
		WithDebugLogger(testLogFunc("debug")).
		WithErrorLogger(testLogFunc("ERROR")).
		WithDialConfig(
			amqp.Config{
				Properties: amqp.Table{
					"connection_name": testClientConnectionName,
				},
			},
		)
}

func initTest() (server *Server, client *Client, start, stop func()) {
	deleteQueue(defaultTestQueue) // Ensure queue is clean from the start.

	server = testServer()
	client = testClient()

	var stopServer func()

	stop = func() {
		client.Stop()
		stopServer()
	}

	start = func() {
		stopServer = startAndWait(server)

		// Ensure the client is started.
		_, err := client.Send(
			NewRequest().
				WithRoutingKey(defaultTestQueue).
				WithResponse(false),
		)

		if err != nil {
			panic(err)
		}

		stop = func() {
			stopServer()
			client.Stop()
		}
	}

	return
}

func testLogFunc(prefix string) LogFunc {
	startTime := time.Now()

	return func(format string, args ...interface{}) {
		format = fmt.Sprintf("[%s] %s: %s\n", prefix, time.Since(startTime).String(), format)
		fmt.Printf(format, args...)
	}
}
