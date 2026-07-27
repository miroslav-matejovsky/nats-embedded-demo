package jetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/miroslav-matejovsky/nats-demo/pkg/natswire"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

func TestPublishPullAndAckOverInProcessConnection(t *testing.T) {
	instance := startJetStreamServer(t)
	conn := connectClient(t, instance)
	client, err := New(conn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, client.WaitReady(ctx))
	require.NoError(t, client.EnsureStream(ctx, "TEST", "test.events", 1))
	require.NoError(t, client.EnsurePullConsumer(ctx, "TEST", "worker", "test.events"))

	sequence, err := client.Publish(ctx, "test.events", []byte("event"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), sequence)

	msg, err := client.Next(ctx, "TEST", "worker", time.Second)
	require.NoError(t, err)
	require.Equal(t, []byte("event"), msg.Data)
	require.NotEmpty(t, msg.Reply)
	require.NoError(t, client.Ack(msg))
	require.NoError(t, conn.Flush(ctx))
}

func startJetStreamServer(t *testing.T) *server.Server {
	t.Helper()

	instance, err := server.NewServer(&server.Options{
		DontListen: true,
		NoSigs:     true,
		NoLog:      true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	})
	require.NoError(t, err)
	instance.Start()
	require.True(t, instance.ReadyForConnections(5*time.Second))
	t.Cleanup(instance.Shutdown)
	return instance
}

func connectClient(t *testing.T, instance *server.Server) *natswire.Conn {
	t.Helper()

	rawConn, err := instance.InProcessConn()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := natswire.Connect(ctx, rawConn, t.Name())
	if err != nil {
		require.NoError(t, errors.Join(err, rawConn.Close()))
	}
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return conn
}
