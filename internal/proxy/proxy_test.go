package proxy

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"llm-proxy-retry/internal/config"
)

func TestProxyRetriesStatusAndReplaysRequest(t *testing.T) {
	var attempts atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempt := attempts.Add(1)
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read downstream request: %v", err)
		}
		if request.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", request.Method)
		}
		if request.URL.EscapedPath() != "/base/v1/a%2Fb" {
			t.Errorf("unexpected path: %s", request.URL.EscapedPath())
		}
		if request.URL.RawQuery != "base=1&client=2" {
			t.Errorf("unexpected query: %s", request.URL.RawQuery)
		}
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization header was not preserved")
		}
		if string(body) != `{"prompt":"hello"}` {
			t.Errorf("unexpected body: %q", body)
		}

		if attempt < 3 {
			writer.Header().Set("X-Downstream-Error", "rate-limited")
			writer.WriteHeader(http.StatusTooManyRequests)
			_, _ = writer.Write([]byte("try again"))
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL+"/base?base=1", func(backend *config.BackendConfig) {
		backend.RetryDelay = duration(2 * time.Millisecond)
		backend.RetryStatuses = []int{http.StatusTooManyRequests}
	}, 1<<20)

	request, err := http.NewRequest(
		http.MethodPost,
		proxyServer.URL+"/A/v1/a%2Fb?client=2",
		strings.NewReader(`{"prompt":"hello"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if response.StatusCode != http.StatusOK || string(body) != `{"ok":true}` {
		t.Fatalf("unexpected response: status=%d body=%q", response.StatusCode, body)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 downstream attempts, got %d", attempts.Load())
	}
}

func TestProxyRetriesKeywordInNonSSEResponse(t *testing.T) {
	var attempts atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"error":"provider overloaded"}`))
			return
		}
		_, _ = writer.Write([]byte(`{"result":"ok"}`))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, func(backend *config.BackendConfig) {
		backend.RetryDelay = duration(time.Millisecond)
		backend.RetryKeywords = []string{"overloaded"}
	}, 1<<20)

	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if string(body) != `{"result":"ok"}` {
		t.Fatalf("unexpected response body: %q", body)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 downstream attempts, got %d", attempts.Load())
	}
}

func TestProxyReturnsLastDownstreamErrorAtDeadline(t *testing.T) {
	var attempts atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writer.Header().Set("X-Downstream-Error", "preserved")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, func(backend *config.BackendConfig) {
		backend.RetryDelay = duration(50 * time.Millisecond)
		backend.MaxRetryDuration = duration(15 * time.Millisecond)
	}, 1<<20)

	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", response.StatusCode)
	}
	if response.Header.Get("X-Downstream-Error") != "preserved" {
		t.Fatalf("downstream response header was not preserved")
	}
	if string(body) != `{"error":"rate limited"}` {
		t.Fatalf("unexpected body: %q", body)
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected one attempt, got %d", attempts.Load())
	}
}

func TestProxyDoesNotInspectSuccessfulSSEForKeywords(t *testing.T) {
	var attempts atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = writer.Write([]byte("data: provider overloaded\n\n"))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, func(backend *config.BackendConfig) {
		backend.RetryKeywords = []string{"overloaded"}
	}, 1<<20)

	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if string(body) != "data: provider overloaded\n\n" {
		t.Fatalf("unexpected SSE body: %q", body)
	}
	if attempts.Load() != 1 {
		t.Fatalf("SSE response was retried %d times", attempts.Load())
	}
}

func TestProxyFlushesSSEBeforeDownstreamCloses(t *testing.T) {
	release := make(chan struct{})
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: first\n\n"))
		writer.(http.Flusher).Flush()
		<-release
		_, _ = writer.Write([]byte("data: second\n\n"))
	}))
	defer downstream.Close()
	defer close(release)

	proxyServer := newTestProxy(t, downstream.URL, nil, 1<<20)
	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	lineResult := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(response.Body).ReadString('\n')
		lineResult <- line
	}()

	select {
	case line := <-lineResult:
		if line != "data: first\n" {
			t.Fatalf("unexpected first SSE line: %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("first SSE event was not flushed")
	}
}

func TestProxyRetriesSSEByStatusBeforeStreaming(t *testing.T) {
	var attempts atomic.Int32
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writer.WriteHeader(http.StatusTooManyRequests)
			_, _ = writer.Write([]byte("busy"))
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: ready\n\n"))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, func(backend *config.BackendConfig) {
		backend.RetryDelay = duration(time.Millisecond)
	}, 1<<20)

	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusOK || string(body) != "data: ready\n\n" {
		t.Fatalf("unexpected response: status=%d body=%q", response.StatusCode, body)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
}

func TestProxyRetriesOversizedResponseByStatus(t *testing.T) {
	var attempts atomic.Int32
	responseBody := bytes.Repeat([]byte("x"), 65)
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writer.WriteHeader(http.StatusTooManyRequests)
			_, _ = writer.Write(responseBody)
			return
		}
		_, _ = writer.Write([]byte("ok"))
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, nil, 64)
	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("unexpected response: status=%d body=%q", response.StatusCode, body)
	}
	if attempts.Load() != 2 {
		t.Fatalf("oversized retry status should be retried, got %d attempts", attempts.Load())
	}
}

func TestProxyReturnsOversizedLastErrorAtDeadline(t *testing.T) {
	var attempts atomic.Int32
	responseBody := bytes.Repeat([]byte("last-error"), 16)
	downstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writer.Header().Set("X-Downstream-Error", "large")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write(responseBody)
	}))
	defer downstream.Close()

	proxyServer := newTestProxy(t, downstream.URL, func(backend *config.BackendConfig) {
		backend.RetryDelay = duration(50 * time.Millisecond)
		backend.MaxRetryDuration = duration(15 * time.Millisecond)
	}, 64)
	response, err := http.Get(proxyServer.URL + "/A/chat")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusTooManyRequests || !bytes.Equal(body, responseBody) {
		t.Fatalf("last response was not preserved: status=%d body=%q", response.StatusCode, body)
	}
	if response.Header.Get("X-Downstream-Error") != "large" {
		t.Fatal("last response header was not preserved")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected one attempt, got %d", attempts.Load())
	}
}

func newTestProxy(
	t *testing.T,
	backendURL string,
	mutate func(*config.BackendConfig),
	maxInspectBytes int64,
) *httptest.Server {
	t.Helper()
	backend := config.BackendConfig{
		Name:               "test",
		URL:                backendURL,
		Weight:             1,
		RetryDelay:         duration(2 * time.Millisecond),
		MaxRetryDuration:   duration(500 * time.Millisecond),
		AttemptTimeout:     duration(250 * time.Millisecond),
		RetryStatuses:      []int{http.StatusTooManyRequests},
		RetryNetworkErrors: boolPointer(true),
	}
	if mutate != nil {
		mutate(&backend)
	}
	cfg := &config.Config{
		Server: config.ServerConfig{
			MaxRequestBodyBytes:         1 << 20,
			MemoryRequestBodyBytes:      32,
			MaxInspectResponseBodyBytes: maxInspectBytes,
		},
		Transport: config.TransportConfig{
			DialTimeout:           duration(time.Second),
			TLSHandshakeTimeout:   duration(time.Second),
			IdleConnTimeout:       duration(time.Second),
			ExpectContinueTimeout: duration(time.Second),
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   8,
		},
		Routes: []config.RouteConfig{{
			Prefix:      "/A",
			StripPrefix: true,
			Backends:    []config.BackendConfig{backend},
		}},
	}
	handler, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		handler.CloseIdleConnections()
	})
	return server
}

func duration(value time.Duration) config.Duration {
	return config.Duration{Duration: value}
}

func boolPointer(value bool) *bool {
	return &value
}
