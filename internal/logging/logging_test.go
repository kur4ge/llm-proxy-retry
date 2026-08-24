package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewJSONLoggerEmitsAllConfiguredLevels(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(&output, "debug", "json")
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	logger.Debug("debug event")
	logger.Info("info event")
	logger.Warn("warn event")
	logger.Error("error event")

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected four log lines, got %d: %q", len(lines), output.String())
	}

	wantLevels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	for index, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode line %d: %v", index, err)
		}
		if record["level"] != wantLevels[index] {
			t.Fatalf("line %d has level %v, want %s", index, record["level"], wantLevels[index])
		}
	}
}

func TestNewTextLoggerRespectsMinimumLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(&output, "warn", "text")
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}

	logger.Debug("hidden debug")
	logger.Info("hidden info")
	logger.Warn("visible warning")
	logger.Error("visible error")

	logs := output.String()
	if strings.Contains(logs, "hidden") {
		t.Fatalf("log level filter leaked lower-level events: %q", logs)
	}
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "level=ERROR") {
		t.Fatalf("expected WARN and ERROR text records, got %q", logs)
	}
}
