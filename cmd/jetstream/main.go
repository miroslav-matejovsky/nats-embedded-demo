package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/miroslav-matejovsky/nats-demo/internal/demo"
	"github.com/miroslav-matejovsky/nats-demo/pkg/embeddedserver"
	"github.com/miroslav-matejovsky/nats-demo/pkg/jetstream"
	"github.com/miroslav-matejovsky/nats-demo/pkg/natswire"
)

const pullExpires = 2 * time.Second

type config struct {
	name        string
	clientPort  int
	clusterName string
	clusterPort int
	routes      string
	storeDir    string
	mode        string
	stream      string
	consumer    string
	subject     string
	message     string
	count       int
	replicas    int
	timeout     time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "jetstream demo: %v\n", err)
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
	if cfg.replicas < 1 || cfg.replicas > 5 {
		return errors.New("replicas must be between 1 and 5")
	}
	if cfg.storeDir == "" {
		cfg.storeDir = filepath.Join("data", "jetstream", cfg.name)
	}

	ctx, cancel := context.WithTimeout(parent, cfg.timeout)
	defer cancel()

	instance, err := embeddedserver.Start(embeddedserver.Config{
		Name:        cfg.name,
		ClientPort:  cfg.clientPort,
		ClusterName: cfg.clusterName,
		ClusterPort: cfg.clusterPort,
		Routes:      cfg.routes,
		JetStream:   true,
		StoreDir:    cfg.storeDir,
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
	js, err := jetstream.New(conn)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(
		output,
		"server=%s client=%s cluster=%s store=%s mode=%s\n",
		instance.Name(),
		instance.ClientURL(),
		embeddedserver.ClusterAddress(instance),
		cfg.storeDir,
		mode,
	)
	if err := js.WaitReady(ctx); err != nil {
		return err
	}
	if err := js.EnsureStream(ctx, cfg.stream, cfg.subject, cfg.replicas); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(output, "stream=%s subject=%s replicas=%d\n", cfg.stream, cfg.subject, cfg.replicas)

	if mode.Subscribes() {
		if err := js.EnsurePullConsumer(ctx, cfg.stream, cfg.consumer, cfg.subject); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(output, "consumer=%s ready\n", cfg.consumer)
	}

	if mode.Publishes() {
		sequence, err := js.Publish(ctx, cfg.subject, []byte(cfg.message))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(
			output,
			"stored stream=%s sequence=%d data=%q\n",
			cfg.stream,
			sequence,
			cfg.message,
		)
	}

	if mode.Subscribes() {
		for received := 0; received < cfg.count; {
			msg, err := js.Next(ctx, cfg.stream, cfg.consumer, pullExpires)
			if isPullTimeout(err) {
				continue
			}
			if err != nil {
				return err
			}
			if err := js.Ack(msg); err != nil {
				return err
			}
			received++
			_, _ = fmt.Fprintf(output, "received subject=%s data=%q acked=true\n", msg.Subject, msg.Data)
		}
		if err := conn.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("jetstream", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.name, "name", "js-a", "unique embedded server name")
	flags.IntVar(&cfg.clientPort, "client-port", 4222, "local NATS client listener port")
	flags.StringVar(&cfg.clusterName, "cluster", "embedded-js-demo", "NATS cluster name")
	flags.IntVar(&cfg.clusterPort, "cluster-port", 6222, "local route listener port")
	flags.StringVar(&cfg.routes, "routes", "", "comma-separated local seed routes")
	flags.StringVar(&cfg.storeDir, "store-dir", "", "JetStream data directory")
	flags.StringVar(&cfg.mode, "mode", string(demo.ModeBoth), "both, publisher, or subscriber")
	flags.StringVar(&cfg.stream, "stream", "DEMO", "JetStream stream name")
	flags.StringVar(&cfg.consumer, "consumer", "demo-consumer", "durable pull consumer name")
	flags.StringVar(&cfg.subject, "subject", "demo.jetstream", "stream subject")
	flags.StringVar(&cfg.message, "message", "hello from embedded JetStream", "message to publish")
	flags.IntVar(&cfg.count, "count", 1, "messages to receive")
	flags.IntVar(&cfg.replicas, "replicas", 1, "stream replica count")
	flags.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "maximum demo run time")
	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}
	if cfg.timeout <= 0 {
		return config{}, errors.New("timeout must be positive")
	}
	return cfg, nil
}

func isPullTimeout(err error) bool {
	var statusErr *natswire.StatusError
	return errors.As(err, &statusErr) && statusErr.Code == 408
}
