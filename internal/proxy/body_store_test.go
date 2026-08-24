package proxy

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestBodyStoreSpillsToDiskAndReplays(t *testing.T) {
	store, err := readRequestBody(
		io.NopCloser(strings.NewReader("request-body")),
		1024,
		4,
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	filename := store.filename
	if filename == "" {
		t.Fatal("expected request body to spill to disk")
	}

	for range 2 {
		reader, err := store.open()
		if err != nil {
			t.Fatalf("open body store: %v", err)
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("read replayed body: %v", err)
		}
		if string(body) != "request-body" {
			t.Fatalf("unexpected replayed body: %q", body)
		}
	}

	if err := store.close(); err != nil {
		t.Fatalf("close body store: %v", err)
	}
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Fatalf("temp file still exists: %v", err)
	}
}

func TestBodyStoreRejectsOversizedBody(t *testing.T) {
	_, err := readRequestBody(
		io.NopCloser(strings.NewReader("too-large")),
		4,
		4,
		t.TempDir(),
	)
	if err != errRequestBodyTooLarge {
		t.Fatalf("expected body size error, got %v", err)
	}
}
