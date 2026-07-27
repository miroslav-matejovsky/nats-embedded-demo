package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/miroslav-matejovsky/nats-demo/internal/demo"
	"github.com/miroslav-matejovsky/nats-demo/pkg/embeddedserver"
	"github.com/miroslav-matejovsky/nats-demo/pkg/natswire"
	"github.com/nats-io/nats-server/v2/server"
)

const interestPollInterval = 50 * time.Millisecond

type config struct {
	name        string
	clientPort  int
	clusterName string
	clusterPort int
	routes      string
	mode        string
	subject     string
	message     string
	count       int
	timeout     time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "nats demo: %v\n", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, output io.Writer) (runErr error) {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	mode, err := demo.ParseMode(cfg.mode)
	if err != nil {
		return err
	}
	if cfg.count < 1 {
		return errors.New("count must be at least 1")
	}

	ctx, cancel := context.WithTimeout(parent, cfg.timeout)
	defer cancel()

	instance, err := embeddedserver.Start(embeddedserver.Config{
		Name:        cfg.name,
		ClientPort:  cfg.clientPort,
		ClusterName: cfg.clusterName,
		ClusterPort: cfg.clusterPort,
		Routes:      cfg.routes,
	})
	if err != nil {
		return err
	}
	defer instance.Shutdown()

	rawConn, err := instance.InProcessConn()
	if err != nil {
		return fmt.Errorf("create in-process connection: %w", err)
	}
	conn, err := natswire.Connect(ctx, rawConn, cfg.name+"-app")
	if err != nil {
		closeErr := rawConn.Close()
		return errors.Join(err, closeErr)
	}
	defer func() {
		runErr = errors.Join(runErr, conn.Close())
	}()

	_, _ = fmt.Fprintf(
		output,
		"server=%s client=%s cluster=%s mode=%s\n",
		instance.Name(),
		instance.ClientURL(),
		embeddedserver.ClusterAddress(instance),
		mode,
	)

	var sid string
	if mode.Subscribes() {
		sid, err = conn.Subscribe(cfg.subject)
		if err != nil {
			return err
		}
		if err := conn.Flush(ctx); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "subscribed subject=%s\n", cfg.subject)
	}

	if mode.Publishes() {
		if mode == demo.ModePublisher {
			if err := waitForInterest(ctx, instance, cfg.subject); err != nil {
				return err
			}
		}
		if err := conn.Publish(cfg.subject, "", []byte(cfg.message)); err != nil {
			return err
		}
		if err := conn.Flush(ctx); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "published subject=%s data=%q\n", cfg.subject, cfg.message)
	}

	if mode.Subscribes() {
		for range cfg.count {
			msg, err := conn.Next(ctx, sid)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(output, "received subject=%s data=%q\n", msg.Subject, msg.Data)
		}
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("nats", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.name, "name", "nats-a", "unique embedded server name")
	flags.IntVar(&cfg.clientPort, "client-port", 4222, "local NATS client listener port")
	flags.StringVar(&cfg.clusterName, "cluster", "embedded-demo", "NATS cluster name")
	flags.IntVar(&cfg.clusterPort, "cluster-port", 6222, "local route listener port")
	flags.StringVar(&cfg.routes, "routes", "", "comma-separated local seed routes")
	flags.StringVar(&cfg.mode, "mode", string(demo.ModeBoth), "both, publisher, or subscriber")
	flags.StringVar(&cfg.subject, "subject", "demo.core", "NATS subject")
	flags.StringVar(&cfg.message, "message", "hello from embedded NATS", "message to publish")
	flags.IntVar(&cfg.count, "count", 1, "messages to receive")
	flags.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "maximum demo run time")
	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}
	if cfg.timeout <= 0 {
		return config{}, errors.New("timeout must be positive")
	}
	return cfg, nil
}

func waitForInterest(ctx context.Context, instance *server.Server, subject string) error {
	ticker := time.NewTicker(interestPollInterval)
	defer ticker.Stop()

	for {
		if instance.GlobalAccount().SubscriptionInterest(subject) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for subscriber interest on %q: %w", subject, ctx.Err())
		case <-ticker.C:
		}
	}
}
