// Package natsinprocess connects the official nats.go client to an embedded
// NATS server with the nats.Options InProcessServer field, the recommended
// way to use nats.go without any network connection.
//
// The client library obtains its transport from Server.InProcessConn()
// directly, so no custom dialer and no placeholder URL are needed. Compare
// with pkg/natsdialer, which reaches the same connection through
// nats.SetCustomDialer.
package natsinprocess
