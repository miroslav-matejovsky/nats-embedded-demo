// Package embeddedserver starts locally bound embedded NATS servers for the
// demo applications. Servers never open a client listener: clients connect
// through server.Server.InProcessConn. The only TCP listener a server can open
// is the route listener used to cluster with other embedded servers, and it
// accepts only loopback routes.
package embeddedserver
