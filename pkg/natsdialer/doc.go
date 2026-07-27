// Package natsdialer connects the official nats.go client to an embedded
// NATS server through a custom dialer, following
// https://docs.nats.io/using-nats/developer/tutorials/custom_dialer.
//
// The dialer ignores the network address and returns the server's in-process
// connection, so the client library speaks the NATS protocol over a net.Pipe
// instead of a TCP socket. Compare with pkg/natsinprocess, which reaches the
// same connection through the nats.Options InProcessServer field and is the
// recommended wiring.
package natsdialer
