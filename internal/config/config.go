package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultLogLevel                = "info"
	defaultLogFormat               = "json"
	defaultMaxRequestBodyBytes     = int64(64 << 20)
	defaultMemoryRequestBodyBytes  = int64(1 << 20)
	defaultMaxInspectResponseBytes = int64(4 << 20)
	defaultRetryDelay              = time.Second
	defaultMaxRetryDuration        = 600 * time.Second
	defaultAttemptTimeout          = 60 * time.Second
	defaultReadHeaderTimeout       = 10 * time.Second
	defaultIdleTimeout             = 120 * time.Second
	defaultShutdownTimeout         = 15 * time.Second
	defaultDialTimeout             = 10 * time.Second
	defaultTLSHandshakeTimeout     = 10 * time.Second
	defaultIdleConnTimeout         = 90 * time.Second
	defaultMaxIdleConns            = 256
	defaultMaxIdleConnsPerHost     = 64
	defaultExpectContinueTimeout   = time.Second
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a string such as 2s or 600ms")
	}
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	d.Duration = value
	return nil
}

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Logging   LoggingConfig   `yaml:"logging"`
	Transport TransportConfig `yaml:"transport"`
	Routes    []RouteConfig   `yaml:"routes"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type ServerConfig struct {
	Listen                      string   `yaml:"listen"`
	ReadHeaderTimeout           Duration `yaml:"read_header_timeout"`
	IdleTimeout                 Duration `yaml:"idle_timeout"`
	ShutdownTimeout             Duration `yaml:"shutdown_timeout"`
	MaxRequestBodyBytes         int64    `yaml:"max_request_body_bytes"`
	MemoryRequestBodyBytes      int64    `yaml:"memory_request_body_bytes"`
	MaxInspectResponseBodyBytes int64    `yaml:"max_inspect_response_body_bytes"`
	TempDir                     string   `yaml:"temp_dir"`
}

type TransportConfig struct {
	DialTimeout           Duration `yaml:"dial_timeout"`
	TLSHandshakeTimeout   Duration `yaml:"tls_handshake_timeout"`
	IdleConnTimeout       Duration `yaml:"idle_conn_timeout"`
	ExpectContinueTimeout Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int      `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int      `yaml:"max_idle_conns_per_host"`
}

type RouteConfig struct {
	Prefix      string          `yaml:"prefix"`
	StripPrefix bool            `yaml:"strip_prefix"`
	Backends    []BackendConfig `yaml:"backends"`
}

type BackendConfig struct {
	Name               string   `yaml:"name"`
	URL                string   `yaml:"url"`
	Weight             int      `yaml:"weight"`
	RetryDelay         Duration `yaml:"retry_delay"`
	MaxRetryDuration   Duration `yaml:"max_retry_duration"`
	AttemptTimeout     Duration `yaml:"attempt_timeout"`
	RetryStatuses      []int    `yaml:"retry_statuses"`
	RetryKeywords      []string `yaml:"retry_keywords"`
	RetryNetworkErrors *bool    `yaml:"retry_network_errors"`
	PreserveHost       bool     `yaml:"preserve_host"`
}

func Load(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	cfg := defaultConfig()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode config: multiple YAML documents are not supported")
		}
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if err := cfg.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func defaultConfig() Config {
	return Config{
		Logging: LoggingConfig{
			Level:  defaultLogLevel,
			Format: defaultLogFormat,
		},
		Server: ServerConfig{
			Listen:                      ":9917",
			ReadHeaderTimeout:           Duration{defaultReadHeaderTimeout},
			IdleTimeout:                 Duration{defaultIdleTimeout},
			ShutdownTimeout:             Duration{defaultShutdownTimeout},
			MaxRequestBodyBytes:         defaultMaxRequestBodyBytes,
			MemoryRequestBodyBytes:      defaultMemoryRequestBodyBytes,
			MaxInspectResponseBodyBytes: defaultMaxInspectResponseBytes,
		},
		Transport: TransportConfig{
			DialTimeout:           Duration{defaultDialTimeout},
			TLSHandshakeTimeout:   Duration{defaultTLSHandshakeTimeout},
			IdleConnTimeout:       Duration{defaultIdleConnTimeout},
			ExpectContinueTimeout: Duration{defaultExpectContinueTimeout},
			MaxIdleConns:          defaultMaxIdleConns,
			MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
		},
	}
}

func (cfg *Config) applyDefaultsAndValidate() error {
	cfg.Logging.Level = strings.ToLower(strings.TrimSpace(cfg.Logging.Level))
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = defaultLogLevel
	}
	switch cfg.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("logging.level must be one of debug, info, warn, or error")
	}

	cfg.Logging.Format = strings.ToLower(strings.TrimSpace(cfg.Logging.Format))
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = defaultLogFormat
	}
	switch cfg.Logging.Format {
	case "json", "text":
	default:
		return errors.New("logging.format must be either json or text")
	}

	if strings.TrimSpace(cfg.Server.Listen) == "" {
		return errors.New("server.listen must not be empty")
	}
	if cfg.Server.ReadHeaderTimeout.Duration <= 0 {
		return errors.New("server.read_header_timeout must be greater than zero")
	}
	if cfg.Server.IdleTimeout.Duration <= 0 {
		return errors.New("server.idle_timeout must be greater than zero")
	}
	if cfg.Server.ShutdownTimeout.Duration <= 0 {
		return errors.New("server.shutdown_timeout must be greater than zero")
	}
	if cfg.Server.MaxRequestBodyBytes <= 0 {
		return errors.New("server.max_request_body_bytes must be greater than zero")
	}
	if cfg.Server.MemoryRequestBodyBytes < 0 {
		return errors.New("server.memory_request_body_bytes must not be negative")
	}
	if cfg.Server.MemoryRequestBodyBytes > cfg.Server.MaxRequestBodyBytes {
		return errors.New("server.memory_request_body_bytes must not exceed max_request_body_bytes")
	}
	if cfg.Server.MaxInspectResponseBodyBytes <= 0 {
		return errors.New("server.max_inspect_response_body_bytes must be greater than zero")
	}
	if cfg.Transport.DialTimeout.Duration <= 0 {
		return errors.New("transport.dial_timeout must be greater than zero")
	}
	if cfg.Transport.TLSHandshakeTimeout.Duration <= 0 {
		return errors.New("transport.tls_handshake_timeout must be greater than zero")
	}
	if cfg.Transport.IdleConnTimeout.Duration <= 0 {
		return errors.New("transport.idle_conn_timeout must be greater than zero")
	}
	if cfg.Transport.ExpectContinueTimeout.Duration <= 0 {
		return errors.New("transport.expect_continue_timeout must be greater than zero")
	}
	if cfg.Transport.MaxIdleConns <= 0 {
		return errors.New("transport.max_idle_conns must be greater than zero")
	}
	if cfg.Transport.MaxIdleConnsPerHost <= 0 {
		return errors.New("transport.max_idle_conns_per_host must be greater than zero")
	}
	if len(cfg.Routes) == 0 {
		return errors.New("at least one route is required")
	}

	prefixes := make(map[string]struct{}, len(cfg.Routes))
	for routeIndex := range cfg.Routes {
		route := &cfg.Routes[routeIndex]
		route.Prefix = normalizePrefix(route.Prefix)
		if route.Prefix == "" {
			return fmt.Errorf("routes[%d].prefix must be a clean absolute path without a query or fragment", routeIndex)
		}
		if _, exists := prefixes[route.Prefix]; exists {
			return fmt.Errorf("duplicate route prefix %q", route.Prefix)
		}
		prefixes[route.Prefix] = struct{}{}
		if len(route.Backends) == 0 {
			return fmt.Errorf("route %q must contain at least one backend", route.Prefix)
		}

		names := make(map[string]struct{}, len(route.Backends))
		for backendIndex := range route.Backends {
			backend := &route.Backends[backendIndex]
			if err := applyBackendDefaultsAndValidate(backend, route.Prefix, backendIndex); err != nil {
				return err
			}
			if _, exists := names[backend.Name]; exists {
				return fmt.Errorf("route %q has duplicate backend name %q", route.Prefix, backend.Name)
			}
			names[backend.Name] = struct{}{}
		}
	}
	return nil
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#") {
		return ""
	}
	if prefix != "/" {
		prefix = strings.TrimRight(prefix, "/")
		if prefix == "" {
			return ""
		}
	}
	if path.Clean(prefix) != prefix {
		return ""
	}
	return prefix
}

func applyBackendDefaultsAndValidate(backend *BackendConfig, routePrefix string, index int) error {
	backend.Name = strings.TrimSpace(backend.Name)
	if backend.Name == "" {
		return fmt.Errorf("route %q backends[%d].name must not be empty", routePrefix, index)
	}

	target, err := url.Parse(backend.URL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return fmt.Errorf("route %q backend %q has invalid url %q", routePrefix, backend.Name, backend.URL)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return fmt.Errorf("route %q backend %q url must use http or https", routePrefix, backend.Name)
	}
	if target.Fragment != "" {
		return fmt.Errorf("route %q backend %q url must not contain a fragment", routePrefix, backend.Name)
	}

	if backend.Weight == 0 {
		backend.Weight = 1
	}
	if backend.Weight < 0 {
		return fmt.Errorf("route %q backend %q weight must be greater than zero", routePrefix, backend.Name)
	}
	if backend.RetryDelay.Duration == 0 {
		backend.RetryDelay.Duration = defaultRetryDelay
	}
	if backend.RetryDelay.Duration <= 0 {
		return fmt.Errorf("route %q backend %q retry_delay must be greater than zero", routePrefix, backend.Name)
	}
	if backend.MaxRetryDuration.Duration == 0 {
		backend.MaxRetryDuration.Duration = defaultMaxRetryDuration
	}
	if backend.MaxRetryDuration.Duration <= 0 {
		return fmt.Errorf("route %q backend %q max_retry_duration must be greater than zero", routePrefix, backend.Name)
	}
	if backend.AttemptTimeout.Duration == 0 {
		backend.AttemptTimeout.Duration = defaultAttemptTimeout
	}
	if backend.AttemptTimeout.Duration <= 0 {
		return fmt.Errorf("route %q backend %q attempt_timeout must be greater than zero", routePrefix, backend.Name)
	}
	if len(backend.RetryStatuses) == 0 {
		backend.RetryStatuses = []int{429}
	}
	seenStatuses := make(map[int]struct{}, len(backend.RetryStatuses))
	for _, status := range backend.RetryStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("route %q backend %q contains invalid retry status %d", routePrefix, backend.Name, status)
		}
		if _, exists := seenStatuses[status]; exists {
			return fmt.Errorf("route %q backend %q contains duplicate retry status %d", routePrefix, backend.Name, status)
		}
		seenStatuses[status] = struct{}{}
	}
	for _, keyword := range backend.RetryKeywords {
		if keyword == "" {
			return fmt.Errorf("route %q backend %q contains an empty retry keyword", routePrefix, backend.Name)
		}
	}
	return nil
}

func (backend BackendConfig) ShouldRetryNetworkErrors() bool {
	return backend.RetryNetworkErrors == nil || *backend.RetryNetworkErrors
}
