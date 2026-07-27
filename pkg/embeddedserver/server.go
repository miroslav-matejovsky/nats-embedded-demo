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
//
// There is no client port to configure. Clients reach the server through
// server.Server.InProcessConn, so a standalone server is started with
// DontListen and opens no client listener at all. A clustered server has to
// open one anyway; see Start for why, and note that nothing ever connects to
// it.
type Config struct {
	// Name uniquely identifies the server in a cluster.
	Name string
	// ClusterName identifies the cluster when Routes is not empty.
	ClusterName string
	// RoutePort is the loopback TCP port on which this server accepts route
	// connections from the other embedded servers listed in their Routes.
	// It is used only when Routes is not empty, and it carries server-to-server
	// traffic only, never client traffic. Use server.RANDOM_PORT to select an
	// available port.
	RoutePort int
	// Routes is a comma-separated list of loopback NATS route URLs pointing at
	// the RoutePort of other embedded servers. Clustering is disabled when
	// Routes is empty.
	Routes string
	// JetStream enables JetStream storage and APIs.
	JetStream bool
	// StoreDir holds JetStream data when JetStream is enabled.
	StoreDir string
}

// Start validates config, starts an embedded server, and waits for readiness.
//
// The client listener is disabled with DontListen whenever clustering is off,
// which proves the demo's point: the nats.go client needs no client port.
//
// Clustering is the one exception. server.Server.Start only launches routing
// after the client accept loop has opened its port (it hands StartRouting a
// clientListenReady channel that AcceptLoop closes), and AcceptLoop does not
// run under DontListen. A clustered server started with DontListen therefore
// never opens its route listener and never becomes ready. So when routes are
// configured this opens a client listener on an unpredictable loopback port
// that the demo never advertises and nothing ever connects to; the client
// still attaches in process.
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
		DontListen: true,
		NoSigs:     true,
		NoLog:      true,
		JetStream:  config.JetStream,
		StoreDir:   config.StoreDir,
		Routes:     routes,
	}
	if len(routes) > 0 {
		opts.DontListen = false
		opts.Port = server.RANDOM_PORT
		opts.Cluster = server.ClusterOpts{
			Name: config.ClusterName,
			Host: "127.0.0.1",
			Port: config.RoutePort,
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

// RouteAddress returns the route listener address or "disabled".
func RouteAddress(instance *server.Server) string {
	if instance == nil || instance.ClusterAddr() == nil {
		return "disabled"
	}
	return instance.ClusterAddr().String()
}

func validate(config Config) error {
	if strings.TrimSpace(config.Name) == "" {
		return errors.New("server name is required")
	}
	if strings.TrimSpace(config.Routes) != "" {
		if strings.TrimSpace(config.ClusterName) == "" {
			return errors.New("cluster name is required when routes are configured")
		}
		if err := validatePort("route", config.RoutePort); err != nil {
			return err
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
