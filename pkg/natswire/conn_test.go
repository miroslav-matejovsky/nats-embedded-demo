package natswire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

func TestCorePublishSubscribeOverInProcessConnection(t *testing.T) {
	instance := startTestServer(t, false)
	conn := connectTestClient(t, instance)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sid, err := conn.Subscribe("test.core")
	require.NoError(t, err)
	require.NoError(t, conn.Flush(ctx))
	require.NoError(t, conn.Publish("test.core", "", []byte("hello")))

	msg, err := conn.Next(ctx, sid)
	require.NoError(t, err)
	require.Equal(t, "test.core", msg.Subject)
	require.Equal(t, []byte("hello"), msg.Data)
}

func TestRequestReturnsNoRespondersStatus(t *testing.T) {
	instance := startTestServer(t, false)
	conn := connectTestClient(t, instance)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := conn.Request(ctx, "test.no.responders", nil)
	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, 503, statusErr.Code)
}

func startTestServer(t *testing.T, jetStream bool) *server.Server {
	t.Helper()

	opts := &server.Options{
		DontListen: true,
		NoSigs:     true,
		NoLog:      true,
		JetStream:  jetStream,
	}
	if jetStream {
		opts.StoreDir = t.TempDir()
	}
	instance, err := server.NewServer(opts)
	require.NoError(t, err)
	instance.Start()
	require.True(t, instance.ReadyForConnections(5*time.Second))
	t.Cleanup(instance.Shutdown)
	return instance
}

func connectTestClient(t *testing.T, instance *server.Server) *Conn {
	t.Helper()

	rawConn, err := instance.InProcessConn()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Connect(ctx, rawConn, t.Name())
	if err != nil {
		require.NoError(t, errors.Join(err, rawConn.Close()))
	}
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	return conn
}
