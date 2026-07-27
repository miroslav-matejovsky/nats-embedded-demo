package embeddedserver

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const readyTimeout = 5 * time.Second

// Config configures one local embedded NATS server instance.
type Config struct {
	// Name uniquely identifies the server in a cluster.
	Name string
	// ClientPort is the loopback client listener port. Use server.RANDOM_PORT
	// to select an available port.
	ClientPort int
	// ClusterName identifies the cluster when Routes is not empty.
	ClusterName string
	// ClusterPort is the loopback route listener port when Routes is not empty.
	// Use server.RANDOM_PORT to select an available port.
	ClusterPort int
	// Routes is a comma-separated list of loopback NATS route URLs. Clustering
	// is disabled when Routes is empty.
	Routes string
	// JetStream enables JetStream storage and APIs.
	JetStream bool
	// StoreDir holds JetStream data when JetStream is enabled.
	StoreDir string
}

// Start validates config, starts an embedded server, and waits for readiness.
func Start(config Config) (*server.Server, error) {
	if err := validate(config); err != nil {
		return nil, err
	}

	routes, err := localRoutes(config.Routes)
	if err != nil {
		return nil, err
	}
	opts := &server.Options{
		ServerName: config.Name,
		Host:       "127.0.0.1",
		Port:       config.ClientPort,
		NoSigs:     true,
		NoLog:      true,
		JetStream:  config.JetStream,
		StoreDir:   config.StoreDir,
		Routes:     routes,
	}
	if len(routes) > 0 {
		opts.Cluster = server.ClusterOpts{
			Name: config.ClusterName,
			Host: "127.0.0.1",
			Port: config.ClusterPort,
		}
	}

	instance, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create embedded NATS server: %w", err)
	}
	instance.Start()
	if !instance.ReadyForConnections(readyTimeout) {
		instance.Shutdown()
		return nil, fmt.Errorf("embedded NATS server was not ready within %s", readyTimeout)
	}
	return instance, nil
}

// ClusterAddress returns the route listener address or "disabled".
func ClusterAddress(instance *server.Server) string {
	if instance == nil || instance.ClusterAddr() == nil {
		return "disabled"
	}
	return instance.ClusterAddr().String()
}

func validate(config Config) error {
	if strings.TrimSpace(config.Name) == "" {
		return errors.New("server name is required")
	}
	if err := validatePort("client", config.ClientPort); err != nil {
		return err
	}
	if strings.TrimSpace(config.Routes) != "" {
		if strings.TrimSpace(config.ClusterName) == "" {
			return errors.New("cluster name is required when routes are configured")
		}
		if err := validatePort("cluster", config.ClusterPort); err != nil {
			return err
		}
		if config.ClientPort == config.ClusterPort && config.ClientPort != server.RANDOM_PORT {
			return errors.New("client and cluster ports must differ")
		}
	}
	if config.JetStream && strings.TrimSpace(config.StoreDir) == "" {
		return errors.New("JetStream store directory is required")
	}
	return nil
}

func validatePort(kind string, port int) error {
	if port == server.RANDOM_PORT {
		return nil
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s port must be -1 or between 1 and 65535", kind)
	}
	return nil
}

func localRoutes(value string) ([]*url.URL, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	routes := server.RoutesFromStr(value)
	if len(routes) == 0 {
		return nil, fmt.Errorf("no valid routes in %q", value)
	}
	for _, route := range routes {
		host := route.Hostname()
		if host != "localhost" && !isLoopbackIP(host) {
			return nil, fmt.Errorf("route %q is not local", route)
		}
	}
	return routes, nil
}

func isLoopbackIP(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
