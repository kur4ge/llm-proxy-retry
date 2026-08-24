package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

var errRequestBodyTooLarge = errors.New("request body exceeds configured limit")

type bodyStore struct {
	data     []byte
	filename string
	size     int64
}

func readRequestBody(body io.ReadCloser, maxBytes, memoryBytes int64, tempDir string) (*bodyStore, error) {
	store := &bodyStore{}
	if body == nil || body == http.NoBody {
		return store, nil
	}
	defer body.Close()

	var memory bytes.Buffer
	var file *os.File
	cleanupOnError := func() {
		if file != nil {
			_ = file.Close()
		}
		if store.filename != "" {
			_ = os.Remove(store.filename)
		}
	}

	buffer := make([]byte, 32<<10)
	for {
		n, readErr := body.Read(buffer)
		if n > 0 {
			if int64(n) > maxBytes-store.size {
				cleanupOnError()
				return nil, errRequestBodyTooLarge
			}

			if file == nil && int64(n) > memoryBytes-store.size {
				var err error
				file, err = os.CreateTemp(tempDir, "llm-proxy-request-*")
				if err != nil {
					cleanupOnError()
					return nil, fmt.Errorf("create request body temp file: %w", err)
				}
				store.filename = file.Name()
				if _, err := file.Write(memory.Bytes()); err != nil {
					cleanupOnError()
					return nil, fmt.Errorf("write request body temp file: %w", err)
				}
				memory.Reset()
			}

			var writeErr error
			if file != nil {
				_, writeErr = file.Write(buffer[:n])
			} else {
				_, writeErr = memory.Write(buffer[:n])
			}
			if writeErr != nil {
				cleanupOnError()
				return nil, fmt.Errorf("buffer request body: %w", writeErr)
			}
			store.size += int64(n)
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			cleanupOnError()
			return nil, fmt.Errorf("read request body: %w", readErr)
		}
	}

	if file != nil {
		if err := file.Close(); err != nil {
			cleanupOnError()
			return nil, fmt.Errorf("close request body temp file: %w", err)
		}
	} else {
		store.data = append([]byte(nil), memory.Bytes()...)
	}
	return store, nil
}

func (s *bodyStore) open() (io.ReadCloser, error) {
	if s.filename != "" {
		file, err := os.Open(s.filename)
		if err != nil {
			return nil, fmt.Errorf("open buffered request body: %w", err)
		}
		return file, nil
	}
	if s.size == 0 {
		return http.NoBody, nil
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func (s *bodyStore) close() error {
	if s.filename == "" {
		return nil
	}
	err := os.Remove(s.filename)
	s.filename = ""
	return err
}
