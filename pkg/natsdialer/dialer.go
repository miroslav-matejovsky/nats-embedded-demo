package natsdialer

import (
	"errors"
	"fmt"
	"net"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// InProcessDialer implements nats.CustomDialer by returning in-process
// connections from an embedded server instead of dialing a network address.
type InProcessDialer struct {
	instance *server.Server
}

// NewInProcessDialer creates a dialer bound to instance.
func NewInProcessDialer(instance *server.Server) (*InProcessDialer, error) {
	if instance == nil {
		return nil, errors.New("embedded server instance is required")
	}
	return &InProcessDialer{instance: instance}, nil
}

// Dial ignores network and address and returns an in-process connection.
func (d *InProcessDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := d.instance.InProcessConn()
	if err != nil {
		return nil, fmt.Errorf("create in-process connection: %w", err)
	}
	return conn, nil
}

// Connect establishes a nats.go client connection to the embedded instance
// through an InProcessDialer. The connection URL is a placeholder; the dialer
// never uses it.
func Connect(instance *server.Server, name string) (*nats.Conn, error) {
	if name == "" {
		return nil, errors.New("connection name is required")
	}
	dialer, err := NewInProcessDialer(instance)
	if err != nil {
		return nil, err
	}
	conn, err := nats.Connect(
		"nats://in-process",
		nats.SetCustomDialer(dialer),
		nats.Name(name),
	)
	if err != nil {
		return nil, fmt.Errorf("connect through custom dialer: %w", err)
	}
	return conn, nil
}
