// Package logging configures the structured JSON logger used by every binary.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON slog logger at the requested level. Accepted levels are
// debug, info, warn, and error.
func New(level string) (*slog.Logger, error) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info", "":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler), nil
}
