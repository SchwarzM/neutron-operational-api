package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type capPublisher struct {
	events []any
}

func (c *capPublisher) Publish(v any) { c.events = append(c.events, v) }

func TestLoadGenSendsRequests(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"pod":            "test-pod",
			"requestsServed": 1,
		})
	}))
	defer srv.Close()

	pub := &capPublisher{}
	k := &K8s{pub: pub, inFlight: make(map[string]int), podSpecs: make(map[string]*corev1.Pod)}
	lg := NewLoadGen(pub, k, srv.URL)
	lg.SetRPS(10)
	lg.Start()
	time.Sleep(500 * time.Millisecond)
	lg.Stop()

	if hits.Load() < 3 {
		t.Fatalf("expected >=3 requests in 500ms at 10rps, got %d", hits.Load())
	}

	// At least one RequestEvent must have been published
	found := false
	for _, ev := range pub.events {
		if req, ok := ev.(RequestEvent); ok && req.PodName == "test-pod" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no RequestEvent with podName=test-pod published")
	}
}
