package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadgenSendsRequestsToTarget(t *testing.T) {
	var hits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"pod": "test-pod"}) //nolint:errcheck
	}))
	defer target.Close()

	var mu sync.Mutex
	var received []map[string]any
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev map[string]any
		if json.NewDecoder(r.Body).Decode(&ev) == nil {
			mu.Lock()
			received = append(received, ev)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cp.Close()

	lg := &loadgen{targetURL: target.URL, sessionID: "test123", controlPlaneURL: cp.URL}
	lg.rps.Store(10)
	lg.Start()
	time.Sleep(400 * time.Millisecond)
	lg.Stop()

	if hits.Load() < 3 {
		t.Fatalf("expected >=3 hits at 10rps/400ms, got %d", hits.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, ev := range received {
		if ev["type"] == "request" && ev["podName"] == "test-pod" {
			return
		}
	}
	t.Fatal("no request event with podName=test-pod received by control plane")
}

func TestLoadgenControlAPIClampsRPS(t *testing.T) {
	lg := &loadgen{}
	lg.SetRPS(200)
	if lg.rps.Load() != maxRPS {
		t.Errorf("rps = %d, want %d (maxRPS)", lg.rps.Load(), maxRPS)
	}
	lg.SetRPS(0)
	if lg.rps.Load() != minRPS {
		t.Errorf("rps = %d, want %d (minRPS)", lg.rps.Load(), minRPS)
	}
}

func TestLoadgenSetDurationClampsNegative(t *testing.T) {
	lg := &loadgen{}
	lg.SetDurationMs(-100)
	if lg.durationMs.Load() != 0 {
		t.Errorf("durationMs = %d, want 0", lg.durationMs.Load())
	}
}

func TestLoadgenStartStopIdempotent(t *testing.T) {
	lg := &loadgen{targetURL: "http://127.0.0.1:0", sessionID: "x", controlPlaneURL: "http://127.0.0.1:0"}
	lg.rps.Store(1)
	lg.Start()
	lg.Start() // second Start must not panic or create extra goroutines
	lg.Stop()
	lg.Stop() // second Stop must not panic
}
