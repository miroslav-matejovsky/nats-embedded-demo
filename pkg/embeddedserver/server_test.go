package embeddedserver

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

func TestLocalRoutesRejectsNonLoopbackAddress(t *testing.T) {
	t.Parallel()

	_, err := localRoutes("nats://192.0.2.1:6222")
	require.EqualError(t, err, `route "nats://192.0.2.1:6222" is not local`)
}

func TestStartWithoutRoutesDisablesClustering(t *testing.T) {
	t.Parallel()

	instance, err := Start(Config{Name: "standalone"})
	require.NoError(t, err)
	t.Cleanup(instance.Shutdown)

	require.Equal(t, "disabled", RouteAddress(instance))
}

func TestStartOpensNoClientListener(t *testing.T) {
	t.Parallel()

	instance, err := Start(Config{Name: "no-client-listener"})
	require.NoError(t, err)
	t.Cleanup(instance.Shutdown)

	require.Nil(t, instance.Addr(), "embedded server must not accept client TCP connections")
}

// TestStartWithRoutesBecomesReady guards the DontListen and clustering
// interaction described on Start: nats-server only starts routing once the
// client accept loop is up, so a clustered server started with DontListen
// would hang instead of opening its route listener.
func TestStartWithRoutesBecomesReady(t *testing.T) {
	t.Parallel()

	instance, err := Start(Config{
		Name:        "clustered",
		ClusterName: "embedded-test",
		RoutePort:   server.RANDOM_PORT,
		Routes:      "nats://127.0.0.1:6222",
	})
	require.NoError(t, err)
	t.Cleanup(instance.Shutdown)

	require.NotEqual(t, "disabled", RouteAddress(instance))
}
