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

	instance, err := Start(Config{
		Name:       "standalone",
		ClientPort: server.RANDOM_PORT,
	})
	require.NoError(t, err)
	t.Cleanup(instance.Shutdown)

	require.Equal(t, "disabled", ClusterAddress(instance))
}
