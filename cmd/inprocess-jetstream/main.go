// Command inprocess-jetstream runs the JetStream demo using the official
// nats.go client and its jetstream package, connected through the
// nats.Options InProcessServer field.
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

	"github.com/miroslav-matejovsky/nats-embedded-demo/internal/demo"
	"github.com/miroslav-matejovsky/nats-embedded-demo/pkg/embeddedserver"
	"github.com/miroslav-matejovsky/nats-embedded-demo/pkg/natsinprocess"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pullExpires        = 2 * time.Second
	readyPollInterval  = 100 * time.Millisecond
	readyProbeDuration = time.Second
)

type config struct {
	name        string
	clusterName string
	routePort   int
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
		_, _ = fmt.Fprintf(os.Stderr, "in-process jetstream demo: %v\n", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, output io.Writer) error {
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
		ClusterName: cfg.clusterName,
		RoutePort:   cfg.routePort,
		Routes:      cfg.routes,
		JetStream:   true,
		StoreDir:    cfg.storeDir,
	})
	if err != nil {
		return err
	}
	defer instance.Shutdown()

	conn, err := natsinprocess.Connect(instance, cfg.name+"-app")
	if err != nil {
		return err
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}

	_, _ = fmt.Fprintf(
		output,
		"server=%s client=in-process route=%s store=%s mode=%s\n",
		instance.Name(),
		embeddedserver.RouteAddress(instance),
		cfg.storeDir,
		mode,
	)

	if err := waitReady(ctx, js); err != nil {
		return err
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.stream,
		Subjects: []string{cfg.subject},
		Storage:  jetstream.FileStorage,
		Replicas: cfg.replicas,
	})
	if err != nil {
		return fmt.Errorf("create stream %q: %w", cfg.stream, err)
	}
	_, _ = fmt.Fprintf(output, "stream=%s subject=%s replicas=%d\n", cfg.stream, cfg.subject, cfg.replicas)

	var consumer jetstream.Consumer
	if mode.Subscribes() {
		consumer, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       cfg.consumer,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: cfg.subject,
			ReplayPolicy:  jetstream.ReplayInstantPolicy,
		})
		if err != nil {
			return fmt.Errorf("create consumer %q: %w", cfg.consumer, err)
		}
		_, _ = fmt.Fprintf(output, "consumer=%s ready\n", cfg.consumer)
	}

	if mode.Publishes() {
		ack, err := js.Publish(ctx, cfg.subject, []byte(cfg.message))
		if err != nil {
			return fmt.Errorf("publish JetStream message: %w", err)
		}
		_, _ = fmt.Fprintf(
			output,
			"stored stream=%s sequence=%d data=%q\n",
			ack.Stream,
			ack.Sequence,
			cfg.message,
		)
	}

	if mode.Subscribes() {
		for received := 0; received < cfg.count; {
			batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(pullExpires))
			if err != nil {
				return fmt.Errorf("pull JetStream message: %w", err)
			}
			for msg := range batch.Messages() {
				if err := msg.Ack(); err != nil {
					return fmt.Errorf("acknowledge JetStream message: %w", err)
				}
				received++
				_, _ = fmt.Fprintf(output, "received subject=%s data=%q acked=true\n", msg.Subject(), msg.Data())
			}
			if err := batch.Error(); err != nil {
				return fmt.Errorf("pull JetStream message: %w", err)
			}
		}
		if err := conn.FlushWithContext(ctx); err != nil {
			return fmt.Errorf("flush acknowledgements: %w", err)
		}
	}
	return nil
}

// waitReady polls the JetStream account API until it responds, which covers
// clustered starts where the meta leader is not elected yet.
func waitReady(ctx context.Context, js jetstream.JetStream) error {
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()

	for {
		probeCtx, cancel := context.WithTimeout(ctx, readyProbeDuration)
		_, err := js.AccountInfo(probeCtx)
		cancel()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for JetStream readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func parseConfig(args []string) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("inprocess-jetstream", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.name, "name", "js-a", "unique embedded server name")
	flags.StringVar(&cfg.clusterName, "cluster", "embedded-js-demo", "NATS cluster name")
	flags.IntVar(&cfg.routePort, "route-port", 6222,
		"loopback TCP port for the server-to-server route listener; used only with --routes")
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
