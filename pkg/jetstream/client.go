package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/miroslav-matejovsky/nats-demo/pkg/natswire"
	"github.com/nats-io/nats-server/v2/server"
)

const retryInterval = 100 * time.Millisecond

// Client calls the JetStream subject-based JSON API without nats.go.
type Client struct {
	conn *natswire.Conn
}

// New creates a JetStream API client over conn.
func New(conn *natswire.Conn) (*Client, error) {
	if conn == nil {
		return nil, errors.New("NATS wire connection is required")
	}
	return &Client{conn: conn}, nil
}

// WaitReady waits until the JetStream account API responds successfully.
func (c *Client) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		requestCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := c.conn.Request(requestCtx, server.JSApiAccountInfo, nil)
		cancel()
		if err == nil {
			var response server.JSApiAccountInfoResponse
			if unmarshalErr := json.Unmarshal(msg.Data, &response); unmarshalErr == nil && response.Error == nil {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for JetStream readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// EnsureStream creates or verifies a file-backed stream.
func (c *Client) EnsureStream(
	ctx context.Context,
	name string,
	subject string,
	replicas int,
) error {
	config := server.StreamConfigRequest{
		StreamConfig: server.StreamConfig{
			Name:     name,
			Subjects: []string{subject},
			Storage:  server.FileStorage,
			Replicas: replicas,
		},
	}
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode stream config: %w", err)
	}
	msg, err := c.conn.Request(ctx, fmt.Sprintf(server.JSApiStreamCreateT, name), data)
	if err != nil {
		return fmt.Errorf("create stream %q: %w", name, err)
	}
	var response server.JSApiStreamCreateResponse
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		return fmt.Errorf("decode stream %q response: %w", name, err)
	}
	if response.Error != nil {
		return fmt.Errorf("create stream %q: %w", name, response.Error)
	}
	return nil
}

// EnsurePullConsumer creates or verifies a durable pull consumer.
func (c *Client) EnsurePullConsumer(
	ctx context.Context,
	streamName string,
	consumerName string,
	filterSubject string,
) error {
	request := server.CreateConsumerRequest{
		Stream: streamName,
		Config: server.ConsumerConfig{
			Durable:       consumerName,
			Name:          consumerName,
			DeliverPolicy: server.DeliverAll,
			AckPolicy:     server.AckExplicit,
			FilterSubject: filterSubject,
			ReplayPolicy:  server.ReplayInstant,
		},
		Action: server.ActionCreateOrUpdate,
	}
	data, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode consumer config: %w", err)
	}
	subject := fmt.Sprintf(server.JSApiConsumerCreateExT, streamName, consumerName, filterSubject)
	msg, err := c.conn.Request(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("create consumer %q: %w", consumerName, err)
	}
	var response server.JSApiConsumerCreateResponse
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		return fmt.Errorf("decode consumer %q response: %w", consumerName, err)
	}
	if response.Error != nil {
		return fmt.Errorf("create consumer %q: %w", consumerName, response.Error)
	}
	return nil
}

// Publish persists data and returns the assigned stream sequence.
func (c *Client) Publish(ctx context.Context, subject string, data []byte) (uint64, error) {
	msg, err := c.conn.Request(ctx, subject, data)
	if err != nil {
		return 0, fmt.Errorf("publish JetStream message: %w", err)
	}
	var response server.JSPubAckResponse
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		return 0, fmt.Errorf("decode publish acknowledgement: %w", err)
	}
	if response.Error != nil {
		return 0, fmt.Errorf("publish JetStream message: %w", response.Error)
	}
	if response.PubAck == nil {
		return 0, errors.New("publish acknowledgement has no stream details")
	}
	return response.Sequence, nil
}

// Next requests one message from a durable pull consumer.
func (c *Client) Next(
	ctx context.Context,
	streamName string,
	consumerName string,
	expires time.Duration,
) (natswire.Message, error) {
	request := server.JSApiConsumerGetNextRequest{
		Expires: expires,
		Batch:   1,
	}
	data, err := json.Marshal(request)
	if err != nil {
		return natswire.Message{}, fmt.Errorf("encode pull request: %w", err)
	}
	subject := fmt.Sprintf(server.JSApiRequestNextT, streamName, consumerName)
	msg, err := c.conn.Request(ctx, subject, data)
	if err != nil {
		return natswire.Message{}, fmt.Errorf("pull JetStream message: %w", err)
	}
	if msg.Reply == "" {
		return natswire.Message{}, errors.New("JetStream delivery has no acknowledgement subject")
	}
	return msg, nil
}

// Ack acknowledges successful processing of a delivered JetStream message.
func (c *Client) Ack(message natswire.Message) error {
	if message.Reply == "" {
		return errors.New("message has no acknowledgement subject")
	}
	if err := c.conn.Publish(message.Reply, "", nil); err != nil {
		return fmt.Errorf("acknowledge JetStream message: %w", err)
	}
	return nil
}
