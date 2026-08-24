package logging

import (
	"fmt"
	"io"
	"log/slog"
)

func New(output io.Writer, level, format string) (*slog.Logger, error) {
	var minimumLevel slog.Level
	if err := minimumLevel.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("parse log level: %w", err)
	}

	options := &slog.HandlerOptions{Level: minimumLevel}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(output, options)
	case "text":
		handler = slog.NewTextHandler(output, options)
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
	return slog.New(handler), nil
}
