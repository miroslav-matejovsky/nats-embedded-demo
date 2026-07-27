package natsdialer

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestConnectRequiresInstanceAndName(t *testing.T) {
	_, err := NewInProcessDialer(nil)
	require.Error(t, err)

	_, err = Connect(nil, "client")
	require.Error(t, err)

	instance := startServer(t, false)
	_, err = Connect(instance, "")
	require.Error(t, err)
}

func TestPublishSubscribeOverInProcessConnection(t *testing.T) {
	instance := startServer(t, false)
	conn, err := Connect(instance, t.Name())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	sub, err := conn.SubscribeSync("test.core")
	require.NoError(t, err)
	require.NoError(t, conn.Flush())
	require.NoError(t, conn.Publish("test.core", []byte("hello")))

	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), msg.Data)
}

func TestJetStreamPublishFetchAndAck(t *testing.T) {
	instance := startServer(t, true)
	conn, err := Connect(instance, t.Name())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.events"},
		Storage:  jetstream.FileStorage,
	})
	require.NoError(t, err)

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "worker",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	ack, err := js.Publish(ctx, "test.events", []byte("event"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), ack.Sequence)

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	require.NoError(t, err)
	received := 0
	for msg := range batch.Messages() {
		require.Equal(t, []byte("event"), msg.Data())
		require.NoError(t, msg.Ack())
		received++
	}
	require.NoError(t, batch.Error())
	require.Equal(t, 1, received)
}

func startServer(t *testing.T, withJetStream bool) *server.Server {
	t.Helper()

	instance, err := server.NewServer(&server.Options{
		DontListen: true,
		NoSigs:     true,
		NoLog:      true,
		JetStream:  withJetStream,
		StoreDir:   t.TempDir(),
	})
	require.NoError(t, err)
	instance.Start()
	require.True(t, instance.ReadyForConnections(5*time.Second))
	t.Cleanup(instance.Shutdown)
	return instance
}
