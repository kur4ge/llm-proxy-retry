package proxy

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"llm-proxy-retry/internal/config"
)

type router struct {
	routes []*route
}

type route struct {
	prefix      string
	stripPrefix bool
	backends    []*backend

	mu sync.Mutex
}

type backend struct {
	name               string
	target             *url.URL
	weight             int64
	currentWeight      int64
	retryDelay         time.Duration
	maxRetryDuration   time.Duration
	attemptTimeout     time.Duration
	retryStatuses      map[int]struct{}
	retryKeywords      [][]byte
	retryNetworkErrors bool
	preserveHost       bool

	cooldownMu    sync.Mutex
	cooldownUntil time.Time
}

func newRouter(routeConfigs []config.RouteConfig) (*router, error) {
	routes := make([]*route, 0, len(routeConfigs))
	for _, routeConfig := range routeConfigs {
		item := &route{
			prefix:      routeConfig.Prefix,
			stripPrefix: routeConfig.StripPrefix,
			backends:    make([]*backend, 0, len(routeConfig.Backends)),
		}
		for _, backendConfig := range routeConfig.Backends {
			target, err := url.Parse(backendConfig.URL)
			if err != nil {
				return nil, fmt.Errorf("parse backend %q URL: %w", backendConfig.Name, err)
			}
			statuses := make(map[int]struct{}, len(backendConfig.RetryStatuses))
			for _, status := range backendConfig.RetryStatuses {
				statuses[status] = struct{}{}
			}
			keywords := make([][]byte, 0, len(backendConfig.RetryKeywords))
			for _, keyword := range backendConfig.RetryKeywords {
				keywords = append(keywords, []byte(keyword))
			}
			item.backends = append(item.backends, &backend{
				name:               backendConfig.Name,
				target:             target,
				weight:             int64(backendConfig.Weight),
				retryDelay:         backendConfig.RetryDelay.Duration,
				maxRetryDuration:   backendConfig.MaxRetryDuration.Duration,
				attemptTimeout:     backendConfig.AttemptTimeout.Duration,
				retryStatuses:      statuses,
				retryKeywords:      keywords,
				retryNetworkErrors: backendConfig.ShouldRetryNetworkErrors(),
				preserveHost:       backendConfig.PreserveHost,
			})
		}
		routes = append(routes, item)
	}
	sort.Slice(routes, func(i, j int) bool {
		return len(routes[i].prefix) > len(routes[j].prefix)
	})
	return &router{routes: routes}, nil
}

func (r *router) match(requestPath string) *route {
	for _, candidate := range r.routes {
		if prefixMatches(candidate.prefix, requestPath) {
			return candidate
		}
	}
	return nil
}

func prefixMatches(prefix, requestPath string) bool {
	if prefix == "/" {
		return strings.HasPrefix(requestPath, "/")
	}
	if requestPath == prefix {
		return true
	}
	return strings.HasPrefix(requestPath, prefix) &&
		len(requestPath) > len(prefix) &&
		requestPath[len(prefix)] == '/'
}

// choose applies weighted selection among ready backends. If all backends are
// cooling down, it selects the one that becomes ready first.
func (r *route) choose(now time.Time) *backend {
	r.mu.Lock()
	defer r.mu.Unlock()

	ready := make([]*backend, 0, len(r.backends))
	for _, candidate := range r.backends {
		if candidate.ready(now) {
			ready = append(ready, candidate)
		}
	}
	if len(ready) == 0 {
		earliest := r.backends[0]
		earliestDeadline := earliest.cooldownDeadline()
		for _, candidate := range r.backends[1:] {
			if candidateDeadline := candidate.cooldownDeadline(); candidateDeadline.Before(earliestDeadline) {
				earliest = candidate
				earliestDeadline = candidateDeadline
			}
		}
		ready = []*backend{earliest}
	}

	var selected *backend
	var totalWeight int64
	for _, candidate := range ready {
		candidate.currentWeight += candidate.weight
		totalWeight += candidate.weight
		if selected == nil || candidate.currentWeight > selected.currentWeight {
			selected = candidate
		}
	}
	selected.currentWeight -= totalWeight
	return selected
}

func (b *backend) ready(now time.Time) bool {
	b.cooldownMu.Lock()
	defer b.cooldownMu.Unlock()
	return !now.Before(b.cooldownUntil)
}

func (b *backend) cooldownDeadline() time.Time {
	b.cooldownMu.Lock()
	defer b.cooldownMu.Unlock()
	return b.cooldownUntil
}

func (b *backend) startCooldown(now time.Time) {
	until := now.Add(b.retryDelay)
	b.cooldownMu.Lock()
	if until.After(b.cooldownUntil) {
		b.cooldownUntil = until
	}
	b.cooldownMu.Unlock()
}

func (b *backend) waitUntilReady(ctx context.Context, deadline time.Time) error {
	for {
		b.cooldownMu.Lock()
		until := b.cooldownUntil
		b.cooldownMu.Unlock()

		now := time.Now()
		if !now.Before(deadline) {
			return context.DeadlineExceeded
		}
		if !now.Before(until) {
			return nil
		}

		wakeAt := until
		if deadline.Before(wakeAt) {
			wakeAt = deadline
		}
		timer := time.NewTimer(time.Until(wakeAt))
		select {
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (r *route) outgoingPath(incoming *url.URL) (pathValue, rawPathValue string) {
	pathValue = incoming.Path
	rawPathValue = incoming.RawPath
	if !r.stripPrefix {
		return pathValue, rawPathValue
	}

	pathValue = strings.TrimPrefix(pathValue, r.prefix)
	if pathValue == "" {
		pathValue = "/"
	}

	escapedPrefix := (&url.URL{Path: r.prefix}).EscapedPath()
	escapedPath := incoming.EscapedPath()
	if strings.HasPrefix(escapedPath, escapedPrefix) {
		rawPathValue = strings.TrimPrefix(escapedPath, escapedPrefix)
		if rawPathValue == "" {
			rawPathValue = "/"
		}
		if decoded, err := url.PathUnescape(rawPathValue); err != nil || decoded != pathValue {
			rawPathValue = ""
		}
	} else {
		rawPathValue = ""
	}
	return pathValue, rawPathValue
}

func joinTargetPath(target *url.URL, requestPath, requestRawPath string) (string, string) {
	requestURL := &url.URL{Path: requestPath, RawPath: requestRawPath}
	if target.RawPath == "" && requestURL.RawPath == "" {
		return singleJoiningSlash(target.Path, requestURL.Path), ""
	}

	targetEscaped := target.EscapedPath()
	requestEscaped := requestURL.EscapedPath()
	targetSlash := strings.HasSuffix(targetEscaped, "/")
	requestSlash := strings.HasPrefix(requestEscaped, "/")
	switch {
	case targetSlash && requestSlash:
		return target.Path + requestURL.Path[1:], targetEscaped + requestEscaped[1:]
	case !targetSlash && !requestSlash:
		return target.Path + "/" + requestURL.Path, targetEscaped + "/" + requestEscaped
	default:
		return target.Path + requestURL.Path, targetEscaped + requestEscaped
	}
}

func singleJoiningSlash(left, right string) string {
	leftSlash := strings.HasSuffix(left, "/")
	rightSlash := strings.HasPrefix(right, "/")
	switch {
	case leftSlash && rightSlash:
		return left + right[1:]
	case !leftSlash && !rightSlash:
		return left + "/" + right
	}
	return left + right
}
