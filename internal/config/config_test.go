package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndNormalizesPrefix(t *testing.T) {
	cfg := loadTestConfig(t, `
routes:
  - prefix: /A/
    backends:
      - name: primary
        url: https://example.com/base
`)

	if cfg.Server.Listen != ":9917" {
		t.Fatalf("unexpected default listen address: %q", cfg.Server.Listen)
	}
	if cfg.Logging.Level != "info" || cfg.Logging.Format != "json" {
		t.Fatalf("unexpected logging defaults: level=%q format=%q", cfg.Logging.Level, cfg.Logging.Format)
	}
	if cfg.Routes[0].Prefix != "/A" {
		t.Fatalf("unexpected normalized prefix: %q", cfg.Routes[0].Prefix)
	}
	backend := cfg.Routes[0].Backends[0]
	if backend.Weight != 1 {
		t.Fatalf("unexpected default weight: %d", backend.Weight)
	}
	if backend.RetryDelay.Duration != time.Second {
		t.Fatalf("unexpected default retry delay: %s", backend.RetryDelay.Duration)
	}
	if backend.MaxRetryDuration.Duration != 600*time.Second {
		t.Fatalf("unexpected default retry duration: %s", backend.MaxRetryDuration.Duration)
	}
	if len(backend.RetryStatuses) != 1 || backend.RetryStatuses[0] != 429 {
		t.Fatalf("unexpected default retry statuses: %v", backend.RetryStatuses)
	}
	if !backend.ShouldRetryNetworkErrors() {
		t.Fatal("network errors should be retried by default")
	}
}

func TestLoadNormalizesLoggingConfig(t *testing.T) {
	cfg := loadTestConfig(t, `
logging:
  level: DEBUG
  format: TEXT
routes:
  - prefix: /
    backends:
      - name: primary
        url: http://example.com
`)

	if cfg.Logging.Level != "debug" || cfg.Logging.Format != "text" {
		t.Fatalf("unexpected logging config: level=%q format=%q", cfg.Logging.Level, cfg.Logging.Format)
	}
}

func TestLoadRejectsInvalidLoggingConfig(t *testing.T) {
	tests := []struct {
		name      string
		logging   string
		wantError string
	}{
		{
			name:      "level",
			logging:   "level: trace\n  format: json",
			wantError: "logging.level must be one of debug, info, warn, or error",
		},
		{
			name:      "format",
			logging:   "level: info\n  format: console",
			wantError: "logging.format must be either json or text",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := writeTestConfig(t, `
logging:
  `+test.logging+`
routes:
  - prefix: /
    backends:
      - name: primary
        url: http://example.com
`)

			_, err := Load(filename)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected %q, got %v", test.wantError, err)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	filename := writeTestConfig(t, `
server:
  listen: ":9917"
  typo: true
routes:
  - prefix: /
    backends:
      - name: primary
        url: http://example.com
`)

	_, err := Load(filename)
	if err == nil || !strings.Contains(err.Error(), "field typo not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestLoadRejectsDuplicateNormalizedPrefixes(t *testing.T) {
	filename := writeTestConfig(t, `
routes:
  - prefix: /A
    backends:
      - name: one
        url: http://example.com
  - prefix: /A/
    backends:
      - name: two
        url: http://example.net
`)

	_, err := Load(filename)
	if err == nil || !strings.Contains(err.Error(), `duplicate route prefix "/A"`) {
		t.Fatalf("expected duplicate prefix error, got %v", err)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Fatalf("load example config: %v", err)
	}
}

func loadTestConfig(t *testing.T, contents string) *Config {
	t.Helper()
	cfg, err := Load(writeTestConfig(t, contents))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

func writeTestConfig(t *testing.T, contents string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return filename
}
