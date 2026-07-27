package demo

import (
	"fmt"
	"strings"
)

// Mode selects the application behavior for one embedded server process.
type Mode string

const (
	// ModeBoth publishes and receives in one process.
	ModeBoth Mode = "both"
	// ModePublisher only publishes.
	ModePublisher Mode = "publisher"
	// ModeSubscriber only receives.
	ModeSubscriber Mode = "subscriber"
)

// ParseMode validates a command-line mode.
func ParseMode(value string) (Mode, error) {
	mode := Mode(strings.ToLower(strings.TrimSpace(value)))
	switch mode {
	case ModeBoth, ModePublisher, ModeSubscriber:
		return mode, nil
	default:
		return "", fmt.Errorf("mode must be %q, %q, or %q", ModeBoth, ModePublisher, ModeSubscriber)
	}
}

// Publishes reports whether mode publishes messages.
func (m Mode) Publishes() bool {
	return m == ModeBoth || m == ModePublisher
}

// Subscribes reports whether mode receives messages.
func (m Mode) Subscribes() bool {
	return m == ModeBoth || m == ModeSubscriber
}
