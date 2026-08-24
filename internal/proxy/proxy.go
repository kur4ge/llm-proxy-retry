package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"llm-proxy-retry/internal/config"
)

type Handler struct {
	router                      *router
	transport                   *http.Transport
	logger                      *slog.Logger
	maxRequestBodyBytes         int64
	memoryRequestBodyBytes      int64
	maxInspectResponseBodyBytes int64
	tempDir                     string
	requestSequence             atomic.Uint64
}

type bufferedResponse struct {
	statusCode int
	header     http.Header
	trailer    http.Header
	body       []byte
}

type failure struct {
	response *bufferedResponse
	err      error
	timeout  bool
}

type attemptControl struct {
	cancel   context.CancelFunc
	timer    *time.Timer
	timedOut atomic.Bool
}

type prefixedReadCloser struct {
	io.Reader
	closer io.Closer
}

func New(cfg *config.Config, logger *slog.Logger) (*Handler, error) {
	requestRouter, err := newRouter(cfg.Routes)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}

	dialer := &net.Dialer{
		Timeout:   cfg.Transport.DialTimeout.Duration,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.Transport.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.Transport.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.Transport.IdleConnTimeout.Duration,
		TLSHandshakeTimeout:   cfg.Transport.TLSHandshakeTimeout.Duration,
		ExpectContinueTimeout: cfg.Transport.ExpectContinueTimeout.Duration,
		DisableCompression:    true,
	}

	return &Handler{
		router:                      requestRouter,
		transport:                   transport,
		logger:                      logger,
		maxRequestBodyBytes:         cfg.Server.MaxRequestBodyBytes,
		memoryRequestBodyBytes:      cfg.Server.MemoryRequestBodyBytes,
		maxInspectResponseBodyBytes: cfg.Server.MaxInspectResponseBodyBytes,
		tempDir:                     cfg.Server.TempDir,
	}, nil
}

func (h *Handler) CloseIdleConnections() {
	h.transport.CloseIdleConnections()
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	requestID := h.requestSequence.Add(1)
	selectedRoute := h.router.match(request.URL.Path)
	if selectedRoute == nil {
		http.NotFound(writer, request)
		return
	}

	if request.ContentLength > h.maxRequestBodyBytes {
		http.Error(writer, "request body exceeds configured limit", http.StatusRequestEntityTooLarge)
		return
	}
	requestBody, err := readRequestBody(
		request.Body,
		h.maxRequestBodyBytes,
		h.memoryRequestBodyBytes,
		h.tempDir,
	)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			http.Error(writer, err.Error(), http.StatusRequestEntityTooLarge)
		} else if request.Context().Err() == nil {
			http.Error(writer, "failed to read request body", http.StatusBadRequest)
			h.logger.Warn("failed to buffer request body",
				"request_id", requestID,
				"method", request.Method,
				"path", request.URL.Path,
				"error", err,
			)
		}
		return
	}
	defer func() {
		if err := requestBody.close(); err != nil {
			h.logger.Warn("failed to remove request body temp file",
				"request_id", requestID,
				"error", err,
			)
		}
	}()

	selectedBackend := selectedRoute.choose(time.Now())
	deadline := started.Add(selectedBackend.maxRetryDuration)
	attempts := 0
	var lastFailure failure

	for {
		if err := selectedBackend.waitUntilReady(request.Context(), deadline); err != nil {
			if request.Context().Err() != nil {
				return
			}
			h.writeFailure(writer, lastFailure)
			h.logFinished(requestID, request, selectedRoute, selectedBackend, attempts, started, lastFailure)
			return
		}
		if !time.Now().Before(deadline) {
			h.writeFailure(writer, lastFailure)
			h.logFinished(requestID, request, selectedRoute, selectedBackend, attempts, started, lastFailure)
			return
		}

		outgoing, err := h.newOutgoingRequest(request, selectedRoute, selectedBackend, requestBody)
		if err != nil {
			if request.Context().Err() == nil {
				http.Error(writer, "failed to prepare downstream request", http.StatusInternalServerError)
				h.logger.Error("failed to prepare downstream request",
					"request_id", requestID,
					"backend", selectedBackend.name,
					"error", err,
				)
			}
			return
		}

		attempts++
		response, control, err := h.roundTrip(request.Context(), outgoing, selectedBackend, deadline)
		if err != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			lastFailure = failure{
				err:     err,
				timeout: control.timedOut.Load() || errors.Is(err, context.DeadlineExceeded),
			}
			control.stop()
			if request.Context().Err() != nil {
				return
			}
			if !selectedBackend.retryNetworkErrors || !time.Now().Before(deadline) {
				h.writeFailure(writer, lastFailure)
				h.logFinished(requestID, request, selectedRoute, selectedBackend, attempts, started, lastFailure)
				return
			}
			selectedBackend.startCooldown(time.Now())
			h.logRetry(requestID, selectedRoute, selectedBackend, attempts, "network_error", err)
			continue
		}

		statusRetry := selectedBackend.retriesStatus(response.StatusCode)
		isEventStream := responseIsEventStream(response)
		if response.StatusCode == http.StatusOK && isEventStream {
			control.stopTimer()
			h.writeLiveResponse(writer, response, true)
			control.stop()
			h.logSuccess(requestID, request, selectedRoute, selectedBackend, attempts, started, response.StatusCode)
			return
		}
		if isEventStream && !statusRetry {
			control.stopTimer()
			h.writeLiveResponse(writer, response, true)
			control.stop()
			h.logSuccess(requestID, request, selectedRoute, selectedBackend, attempts, started, response.StatusCode)
			return
		}
		if !statusRetry && (len(selectedBackend.retryKeywords) == 0 || responseIsEncoded(response)) {
			control.stopTimer()
			h.writeLiveResponse(writer, response, false)
			control.stop()
			h.logSuccess(requestID, request, selectedRoute, selectedBackend, attempts, started, response.StatusCode)
			return
		}

		bodyPrefix, exceeded, readErr := readResponseProbe(response.Body, h.maxInspectResponseBodyBytes)
		if exceeded {
			control.stopTimer()
			response.Body = &prefixedReadCloser{
				Reader: io.MultiReader(bytes.NewReader(bodyPrefix), response.Body),
				closer: response.Body,
			}
			if statusRetry {
				selectedBackend.startCooldown(time.Now())
				waitErr := selectedBackend.waitUntilReady(request.Context(), deadline)
				if request.Context().Err() != nil {
					_ = response.Body.Close()
					control.stop()
					return
				}
				if waitErr != nil || !time.Now().Before(deadline) {
					h.writeLiveResponse(writer, response, false)
					control.stop()
					h.logFinished(
						requestID,
						request,
						selectedRoute,
						selectedBackend,
						attempts,
						started,
						failure{response: &bufferedResponse{statusCode: response.StatusCode}},
					)
					return
				}
				_ = response.Body.Close()
				control.stop()
				h.logRetry(requestID, selectedRoute, selectedBackend, attempts, "status", nil)
				continue
			}
			h.writeLiveResponse(writer, response, false)
			control.stop()
			h.logSuccess(requestID, request, selectedRoute, selectedBackend, attempts, started, response.StatusCode)
			return
		}
		_ = response.Body.Close()
		control.stopTimer()

		snapshot := snapshotResponse(response, bodyPrefix)
		if readErr != nil {
			control.stop()
			lastFailure = failure{
				err:     fmt.Errorf("read downstream response: %w", readErr),
				timeout: control.timedOut.Load(),
			}
			if request.Context().Err() != nil {
				return
			}
			if !selectedBackend.retryNetworkErrors || !time.Now().Before(deadline) {
				h.writeFailure(writer, lastFailure)
				h.logFinished(requestID, request, selectedRoute, selectedBackend, attempts, started, lastFailure)
				return
			}
			selectedBackend.startCooldown(time.Now())
			h.logRetry(requestID, selectedRoute, selectedBackend, attempts, "response_read_error", readErr)
			continue
		}
		control.stop()

		keywordRetry := !isEventStream && selectedBackend.containsRetryKeyword(bodyPrefix)
		if !statusRetry && !keywordRetry {
			h.writeBufferedResponse(writer, snapshot)
			h.logSuccess(requestID, request, selectedRoute, selectedBackend, attempts, started, response.StatusCode)
			return
		}

		lastFailure = failure{response: snapshot}
		if !time.Now().Before(deadline) {
			h.writeFailure(writer, lastFailure)
			h.logFinished(requestID, request, selectedRoute, selectedBackend, attempts, started, lastFailure)
			return
		}

		selectedBackend.startCooldown(time.Now())
		reason := "status"
		if keywordRetry && !statusRetry {
			reason = "keyword"
		}
		h.logRetry(requestID, selectedRoute, selectedBackend, attempts, reason, nil)
	}
}

func (h *Handler) newOutgoingRequest(
	incoming *http.Request,
	selectedRoute *route,
	selectedBackend *backend,
	body *bodyStore,
) (*http.Request, error) {
	requestBody, err := body.open()
	if err != nil {
		return nil, err
	}

	outgoing := incoming.Clone(incoming.Context())
	outgoing.Body = requestBody
	outgoing.GetBody = body.open
	outgoing.ContentLength = incoming.ContentLength
	outgoing.TransferEncoding = append([]string(nil), incoming.TransferEncoding...)
	outgoing.Trailer = incoming.Trailer.Clone()
	outgoing.Close = false
	outgoing.RequestURI = ""
	outgoing.Header = incoming.Header.Clone()
	removeHopByHopHeaders(outgoing.Header)

	outgoingURL := *incoming.URL
	pathValue, rawPathValue := selectedRoute.outgoingPath(incoming.URL)
	outgoingURL.Scheme = selectedBackend.target.Scheme
	outgoingURL.Host = selectedBackend.target.Host
	outgoingURL.Path, outgoingURL.RawPath = joinTargetPath(selectedBackend.target, pathValue, rawPathValue)
	outgoingURL.RawQuery = joinQuery(selectedBackend.target.RawQuery, incoming.URL.RawQuery)
	outgoingURL.ForceQuery = selectedBackend.target.ForceQuery || incoming.URL.ForceQuery
	outgoingURL.Fragment = ""
	outgoingURL.RawFragment = ""
	outgoingURL.Opaque = ""
	outgoing.URL = &outgoingURL

	if selectedBackend.preserveHost {
		outgoing.Host = incoming.Host
	} else {
		outgoing.Host = selectedBackend.target.Host
	}
	return outgoing, nil
}

func (h *Handler) roundTrip(
	parent context.Context,
	request *http.Request,
	selectedBackend *backend,
	deadline time.Time,
) (*http.Response, *attemptControl, error) {
	attemptContext, cancel := context.WithCancel(parent)
	control := &attemptControl{cancel: cancel}
	budget := selectedBackend.attemptTimeout
	if remaining := time.Until(deadline); remaining < budget {
		budget = remaining
	}
	if budget <= 0 {
		control.timedOut.Store(true)
		cancel()
		return nil, control, context.DeadlineExceeded
	}

	control.timer = time.AfterFunc(budget, func() {
		control.timedOut.Store(true)
		cancel()
	})
	response, err := h.transport.RoundTrip(request.WithContext(attemptContext))
	return response, control, err
}

func (c *attemptControl) stopTimer() {
	if c.timer != nil {
		stopAndDrainTimer(c.timer)
	}
}

func (c *attemptControl) stop() {
	c.stopTimer()
	c.cancel()
}

func (c *prefixedReadCloser) Close() error {
	return c.closer.Close()
}

func (b *backend) retriesStatus(status int) bool {
	_, exists := b.retryStatuses[status]
	return exists
}

func (b *backend) containsRetryKeyword(body []byte) bool {
	for _, keyword := range b.retryKeywords {
		if bytes.Contains(body, keyword) {
			return true
		}
	}
	return false
}

func readResponseProbe(body io.Reader, maxBytes int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxBytes))
	if err != nil || int64(len(data)) < maxBytes {
		return data, false, err
	}

	var extra [1]byte
	n, extraErr := io.ReadFull(body, extra[:])
	if n > 0 {
		data = append(data, extra[0])
		return data, true, nil
	}
	if errors.Is(extraErr, io.EOF) {
		return data, false, nil
	}
	return data, false, extraErr
}

func snapshotResponse(response *http.Response, body []byte) *bufferedResponse {
	header := response.Header.Clone()
	removeHopByHopHeaders(header)
	return &bufferedResponse{
		statusCode: response.StatusCode,
		header:     header,
		trailer:    response.Trailer.Clone(),
		body:       append([]byte(nil), body...),
	}
}

func responseIsEventStream(response *http.Response) bool {
	contentType := response.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil {
		return strings.EqualFold(mediaType, "text/event-stream")
	}
	if separator := strings.IndexByte(contentType, ';'); separator >= 0 {
		contentType = contentType[:separator]
	}
	return strings.EqualFold(strings.TrimSpace(contentType), "text/event-stream")
}

func responseIsEncoded(response *http.Response) bool {
	encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	return encoding != "" && !strings.EqualFold(encoding, "identity")
}

func joinQuery(targetQuery, requestQuery string) string {
	switch {
	case targetQuery == "":
		return requestQuery
	case requestQuery == "":
		return targetQuery
	default:
		return targetQuery + "&" + requestQuery
	}
}

func (h *Handler) writeLiveResponse(writer http.ResponseWriter, response *http.Response, flush bool) {
	defer response.Body.Close()

	header := response.Header.Clone()
	removeHopByHopHeaders(header)
	copyHeader(writer.Header(), header)
	announceTrailers(writer.Header(), response.Trailer)
	writer.WriteHeader(response.StatusCode)

	if flush {
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
	}

	buffer := make([]byte, 32<<10)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if _, writeErr := writer.Write(buffer[:n]); writeErr != nil {
				return
			}
			if flush {
				if flusher, ok := writer.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	writeTrailers(writer.Header(), response.Trailer)
}

func (h *Handler) writeBufferedResponse(writer http.ResponseWriter, response *bufferedResponse) {
	copyHeader(writer.Header(), response.header)
	announceTrailers(writer.Header(), response.trailer)
	writer.WriteHeader(response.statusCode)
	_, _ = writer.Write(response.body)
	writeTrailers(writer.Header(), response.trailer)
}

func (h *Handler) writeFailure(writer http.ResponseWriter, last failure) {
	if last.response != nil {
		h.writeBufferedResponse(writer, last.response)
		return
	}
	status := http.StatusBadGateway
	message := "downstream request failed"
	if last.timeout || errors.Is(last.err, context.DeadlineExceeded) || last.err == nil {
		status = http.StatusGatewayTimeout
		message = "downstream retry deadline exceeded"
	}
	if last.err != nil {
		message += ": " + last.err.Error()
	}
	http.Error(writer, message, status)
}

func announceTrailers(header, trailer http.Header) {
	for name := range trailer {
		header.Add("Trailer", name)
	}
}

func writeTrailers(header, trailer http.Header) {
	for name, values := range trailer {
		header[http.TrailerPrefix+name] = append([]string(nil), values...)
	}
}

func (h *Handler) logRetry(
	requestID uint64,
	selectedRoute *route,
	selectedBackend *backend,
	attempt int,
	reason string,
	err error,
) {
	attributes := []any{
		"request_id", requestID,
		"route", selectedRoute.prefix,
		"backend", selectedBackend.name,
		"attempt", attempt,
		"reason", reason,
		"retry_delay", selectedBackend.retryDelay,
	}
	if err != nil {
		attributes = append(attributes, "error", err)
	}
	h.logger.Warn("retrying downstream request", attributes...)
}

func (h *Handler) logSuccess(
	requestID uint64,
	request *http.Request,
	selectedRoute *route,
	selectedBackend *backend,
	attempts int,
	started time.Time,
	status int,
) {
	h.logger.Info("request completed",
		"request_id", requestID,
		"method", request.Method,
		"path", request.URL.Path,
		"route", selectedRoute.prefix,
		"backend", selectedBackend.name,
		"attempts", attempts,
		"status", status,
		"duration", time.Since(started),
	)
}

func (h *Handler) logFinished(
	requestID uint64,
	request *http.Request,
	selectedRoute *route,
	selectedBackend *backend,
	attempts int,
	started time.Time,
	last failure,
) {
	status := http.StatusBadGateway
	if last.response != nil {
		status = last.response.statusCode
	} else if last.timeout || last.err == nil {
		status = http.StatusGatewayTimeout
	}
	attributes := []any{
		"request_id", requestID,
		"method", request.Method,
		"path", request.URL.Path,
		"route", selectedRoute.prefix,
		"backend", selectedBackend.name,
		"attempts", attempts,
		"status", status,
		"duration", time.Since(started),
	}
	if last.err != nil {
		attributes = append(attributes, "error", last.err)
	}
	h.logger.Warn("request completed after retry exhaustion", attributes...)
}
