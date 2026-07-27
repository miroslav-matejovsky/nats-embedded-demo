package natsinprocess

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Connect establishes a nats.go client connection to the embedded instance
// through its in-process connection. No network address is dialed.
func Connect(instance *server.Server, name string) (*nats.Conn, error) {
	if instance == nil {
		return nil, errors.New("embedded server instance is required")
	}
	if name == "" {
		return nil, errors.New("connection name is required")
	}
	conn, err := nats.Connect("", nats.InProcessServer(instance), nats.Name(name))
	if err != nil {
		return nil, fmt.Errorf("connect to in-process server: %w", err)
	}
	return conn, nil
}
