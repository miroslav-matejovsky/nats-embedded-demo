package natswire

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var inboxSequence atomic.Uint64

// Message is a message delivered by a NATS server.
type Message struct {
	// Subject is the subject on which the message was delivered.
	Subject string
	// Reply is the optional reply or JetStream acknowledgement subject.
	Reply string
	// Subscription is the protocol subscription ID that received the message.
	Subscription string
	// Header contains the raw NATS header block.
	Header []byte
	// Data contains the message payload.
	Data []byte
	// StatusCode is the optional status code from a header-only response.
	StatusCode int
	// StatusText describes StatusCode.
	StatusText string
}

// StatusError reports a status message returned by the server.
type StatusError struct {
	// Code is the NATS status code.
	Code int
	// Text is the NATS status description.
	Text string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("NATS status %d: %s", e.Code, e.Text)
}

// Conn is a synchronous connection that implements a limited NATS protocol
// client over a caller-provided net.Conn.
type Conn struct {
	conn    net.Conn
	reader  *bufio.Reader
	nextSID uint64
	pending []Message
}

// Connect completes the NATS protocol handshake on conn.
func Connect(ctx context.Context, conn net.Conn, name string) (*Conn, error) {
	if conn == nil {
		return nil, errors.New("connection is required")
	}
	if name == "" {
		return nil, errors.New("connection name is required")
	}

	c := &Conn{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
	if err := c.setDeadline(ctx); err != nil {
		return nil, err
	}
	defer c.clearDeadline()

	line, err := c.readLine()
	if err != nil {
		return nil, fmt.Errorf("read server INFO: %w", err)
	}
	if !strings.HasPrefix(line, "INFO ") {
		return nil, fmt.Errorf("expected INFO, got %q", line)
	}

	connect, err := json.Marshal(struct {
		Verbose      bool   `json:"verbose"`
		Pedantic     bool   `json:"pedantic"`
		Headers      bool   `json:"headers"`
		NoResponders bool   `json:"no_responders"`
		Name         string `json:"name"`
	}{
		Headers:      true,
		NoResponders: true,
		Name:         name,
	})
	if err != nil {
		return nil, fmt.Errorf("encode CONNECT: %w", err)
	}
	if err := c.writeRaw(fmt.Appendf(nil, "CONNECT %s\r\nPING\r\n", connect)); err != nil {
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	if err := c.readUntilPong(); err != nil {
		return nil, fmt.Errorf("complete handshake: %w", err)
	}

	return c, nil
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close NATS connection: %w", err)
	}
	return nil
}

// Subscribe creates a subscription and returns its protocol subscription ID.
func (c *Conn) Subscribe(subject string) (string, error) {
	if err := validateSubject(subject); err != nil {
		return "", err
	}
	c.nextSID++
	sid := strconv.FormatUint(c.nextSID, 10)
	if err := c.writeRaw(fmt.Appendf(nil, "SUB %s %s\r\n", subject, sid)); err != nil {
		return "", fmt.Errorf("subscribe to %q: %w", subject, err)
	}
	return sid, nil
}

// Publish sends data to subject with an optional reply subject.
func (c *Conn) Publish(subject, reply string, data []byte) error {
	if err := validateSubject(subject); err != nil {
		return err
	}
	if reply != "" {
		if err := validateSubject(reply); err != nil {
			return fmt.Errorf("invalid reply subject: %w", err)
		}
	}

	var command []byte
	if reply == "" {
		command = fmt.Appendf(nil, "PUB %s %d\r\n", subject, len(data))
	} else {
		command = fmt.Appendf(nil, "PUB %s %s %d\r\n", subject, reply, len(data))
	}
	command = append(command, data...)
	command = append(command, "\r\n"...)
	if err := c.writeRaw(command); err != nil {
		return fmt.Errorf("publish to %q: %w", subject, err)
	}
	return nil
}

// Flush waits until the server has processed all commands written before it.
func (c *Conn) Flush(ctx context.Context) error {
	if err := c.setDeadline(ctx); err != nil {
		return err
	}
	defer c.clearDeadline()

	if err := c.writeRaw([]byte("PING\r\n")); err != nil {
		return fmt.Errorf("write PING: %w", err)
	}
	if err := c.readUntilPong(); err != nil {
		return fmt.Errorf("wait for PONG: %w", err)
	}
	return nil
}

// Next waits for the next message on sid.
func (c *Conn) Next(ctx context.Context, sid string) (Message, error) {
	if sid == "" {
		return Message{}, errors.New("subscription ID is required")
	}
	for i, msg := range c.pending {
		if msg.Subscription == sid {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return messageResult(msg)
		}
	}

	if err := c.setDeadline(ctx); err != nil {
		return Message{}, err
	}
	defer c.clearDeadline()

	for {
		frame, err := c.readFrame()
		if err != nil {
			return Message{}, fmt.Errorf("read message: %w", err)
		}
		switch frame.kind {
		case frameMessage:
			if frame.message.Subscription == sid {
				return messageResult(frame.message)
			}
			c.pending = append(c.pending, frame.message)
		case framePing:
			if err := c.writeRaw([]byte("PONG\r\n")); err != nil {
				return Message{}, fmt.Errorf("reply to server PING: %w", err)
			}
		}
	}
}

// Request sends a request and waits for one reply.
func (c *Conn) Request(ctx context.Context, subject string, data []byte) (Message, error) {
	if err := validateSubject(subject); err != nil {
		return Message{}, err
	}
	inbox := fmt.Sprintf("_INBOX.embedded.%d.%d", os.Getpid(), inboxSequence.Add(1))
	sid, err := c.Subscribe(inbox)
	if err != nil {
		return Message{}, err
	}
	if err := c.writeRaw(fmt.Appendf(nil, "UNSUB %s 1\r\n", sid)); err != nil {
		return Message{}, fmt.Errorf("limit request subscription: %w", err)
	}
	if err := c.Publish(subject, inbox, data); err != nil {
		return Message{}, err
	}
	msg, err := c.Next(ctx, sid)
	if err != nil {
		return Message{}, fmt.Errorf("request %q: %w", subject, err)
	}
	return msg, nil
}

type frameKind uint8

const (
	frameOther frameKind = iota
	frameMessage
	framePing
	framePong
)

type frame struct {
	kind    frameKind
	message Message
}

func (c *Conn) readUntilPong() error {
	for {
		frame, err := c.readFrame()
		if err != nil {
			return err
		}
		switch frame.kind {
		case framePong:
			return nil
		case framePing:
			if err := c.writeRaw([]byte("PONG\r\n")); err != nil {
				return err
			}
		case frameMessage:
			c.pending = append(c.pending, frame.message)
		}
	}
}

func (c *Conn) readFrame() (frame, error) {
	line, err := c.readLine()
	if err != nil {
		return frame{}, err
	}
	switch {
	case line == "PING":
		return frame{kind: framePing}, nil
	case line == "PONG":
		return frame{kind: framePong}, nil
	case line == "+OK", strings.HasPrefix(line, "INFO "):
		return frame{kind: frameOther}, nil
	case strings.HasPrefix(line, "-ERR"):
		return frame{}, fmt.Errorf("server error: %s", strings.TrimSpace(strings.TrimPrefix(line, "-ERR")))
	case strings.HasPrefix(line, "MSG "):
		msg, err := c.readMSG(strings.Fields(line))
		return frame{kind: frameMessage, message: msg}, err
	case strings.HasPrefix(line, "HMSG "):
		msg, err := c.readHMSG(strings.Fields(line))
		return frame{kind: frameMessage, message: msg}, err
	default:
		return frame{}, fmt.Errorf("unsupported server protocol line %q", line)
	}
}

func (c *Conn) readMSG(fields []string) (Message, error) {
	if len(fields) != 4 && len(fields) != 5 {
		return Message{}, fmt.Errorf("invalid MSG arguments %q", fields)
	}
	sizeIndex := len(fields) - 1
	size, err := parseSize(fields[sizeIndex])
	if err != nil {
		return Message{}, err
	}
	data, err := c.readPayload(size)
	if err != nil {
		return Message{}, err
	}
	msg := Message{Subject: fields[1], Subscription: fields[2], Data: data}
	if len(fields) == 5 {
		msg.Reply = fields[3]
	}
	return msg, nil
}

func (c *Conn) readHMSG(fields []string) (Message, error) {
	if len(fields) != 5 && len(fields) != 6 {
		return Message{}, fmt.Errorf("invalid HMSG arguments %q", fields)
	}
	headerIndex := len(fields) - 2
	headerSize, err := parseSize(fields[headerIndex])
	if err != nil {
		return Message{}, err
	}
	totalSize, err := parseSize(fields[headerIndex+1])
	if err != nil {
		return Message{}, err
	}
	if headerSize > totalSize {
		return Message{}, fmt.Errorf("header size %d exceeds total size %d", headerSize, totalSize)
	}
	payload, err := c.readPayload(totalSize)
	if err != nil {
		return Message{}, err
	}

	msg := Message{
		Subject:      fields[1],
		Subscription: fields[2],
		Header:       payload[:headerSize],
		Data:         payload[headerSize:],
	}
	if len(fields) == 6 {
		msg.Reply = fields[3]
	}
	msg.StatusCode, msg.StatusText = parseStatus(msg.Header)
	return msg, nil
}

func (c *Conn) readLine() (string, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("protocol line does not end in CRLF")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (c *Conn) readPayload(size int) ([]byte, error) {
	payload := make([]byte, size+2)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, err
	}
	if payload[size] != '\r' || payload[size+1] != '\n' {
		return nil, errors.New("message payload does not end in CRLF")
	}
	return payload[:size], nil
}

func (c *Conn) writeRaw(data []byte) error {
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (c *Conn) setDeadline(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set connection deadline: %w", err)
	}
	return nil
}

func (c *Conn) clearDeadline() {
	_ = c.conn.SetDeadline(noDeadline)
}

func validateSubject(subject string) error {
	if subject == "" {
		return errors.New("subject is required")
	}
	if strings.ContainsAny(subject, " \t\r\n") {
		return fmt.Errorf("subject %q contains whitespace", subject)
	}
	return nil
}

func parseSize(value string) (int, error) {
	size, err := strconv.Atoi(value)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid message size %q", value)
	}
	return size, nil
}

func parseStatus(header []byte) (int, string) {
	line, _, _ := strings.Cut(string(header), "\r\n")
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "NATS/1.0" {
		return 0, ""
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, ""
	}
	return code, strings.Join(fields[2:], " ")
}

func messageResult(msg Message) (Message, error) {
	if msg.StatusCode >= 300 {
		return Message{}, &StatusError{Code: msg.StatusCode, Text: msg.StatusText}
	}
	return msg, nil
}

var noDeadline time.Time
