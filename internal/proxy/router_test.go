package proxy

import (
	"net/url"
	"testing"
	"time"

	"llm-proxy-retry/internal/config"
)

func TestRouterUsesLongestSegmentPrefix(t *testing.T) {
	requestRouter, err := newRouter([]config.RouteConfig{
		testRouteConfig("/", "root", 1),
		testRouteConfig("/A", "a", 1),
		testRouteConfig("/A/deep", "deep", 1),
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	tests := []struct {
		path   string
		prefix string
	}{
		{path: "/A/deep/chat", prefix: "/A/deep"},
		{path: "/A/chat", prefix: "/A"},
		{path: "/ABC", prefix: "/"},
		{path: "/", prefix: "/"},
	}
	for _, test := range tests {
		matched := requestRouter.match(test.path)
		if matched == nil || matched.prefix != test.prefix {
			t.Errorf("path %q: expected %q, got %#v", test.path, test.prefix, matched)
		}
	}
}

func TestWeightedSelectionPrefersReadyBackends(t *testing.T) {
	requestRouter, err := newRouter([]config.RouteConfig{{
		Prefix: "/",
		Backends: []config.BackendConfig{
			testBackendConfig("heavy", 3),
			testBackendConfig("light", 1),
		},
	}})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	selectedRoute := requestRouter.routes[0]

	counts := map[string]int{}
	for range 4 {
		counts[selectedRoute.choose(time.Now()).name]++
	}
	if counts["heavy"] != 3 || counts["light"] != 1 {
		t.Fatalf("unexpected weighted distribution: %v", counts)
	}

	heavy := selectedRoute.backends[0]
	heavy.startCooldown(time.Now())
	for range 4 {
		if selected := selectedRoute.choose(time.Now()); selected.name != "light" {
			t.Fatalf("selected cooling backend %q", selected.name)
		}
	}
}

func TestSelectionUsesEarliestBackendWhenAllAreCooling(t *testing.T) {
	requestRouter, err := newRouter([]config.RouteConfig{{
		Prefix: "/",
		Backends: []config.BackendConfig{
			testBackendConfig("early", 1),
			testBackendConfig("late", 100),
		},
	}})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	selectedRoute := requestRouter.routes[0]
	now := time.Now()
	selectedRoute.backends[0].cooldownUntil = now.Add(time.Second)
	selectedRoute.backends[1].cooldownUntil = now.Add(2 * time.Second)

	if selected := selectedRoute.choose(now); selected.name != "early" {
		t.Fatalf("expected earliest backend, got %q", selected.name)
	}
}

func TestRoutePathRewritePreservesEscaping(t *testing.T) {
	selectedRoute := &route{prefix: "/A", stripPrefix: true}
	incoming, err := url.Parse("http://proxy/A/v1/a%2Fb")
	if err != nil {
		t.Fatal(err)
	}
	pathValue, rawPathValue := selectedRoute.outgoingPath(incoming)
	target, err := url.Parse("https://backend.example/base")
	if err != nil {
		t.Fatal(err)
	}
	pathValue, rawPathValue = joinTargetPath(target, pathValue, rawPathValue)

	if pathValue != "/base/v1/a/b" {
		t.Fatalf("unexpected decoded path: %q", pathValue)
	}
	if rawPathValue != "/base/v1/a%2Fb" {
		t.Fatalf("unexpected escaped path: %q", rawPathValue)
	}
}

func testRouteConfig(prefix, name string, weight int) config.RouteConfig {
	return config.RouteConfig{
		Prefix: prefix,
		Backends: []config.BackendConfig{
			testBackendConfig(name, weight),
		},
	}
}

func testBackendConfig(name string, weight int) config.BackendConfig {
	return config.BackendConfig{
		Name:             name,
		URL:              "http://example.com",
		Weight:           weight,
		RetryDelay:       config.Duration{Duration: time.Second},
		MaxRetryDuration: config.Duration{Duration: time.Minute},
		AttemptTimeout:   config.Duration{Duration: time.Second},
		RetryStatuses:    []int{429},
	}
}
