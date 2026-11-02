package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := newServer("test-pod")
	return httptest.NewServer(s.mux)
}

func TestReadyzDefaultReady(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
}

func TestReadyzToggle(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	body := bytes.NewBufferString(`{"ready":false}`)
	resp, err := http.Post(srv.URL+"/admin/ready", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin/ready: got %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz after toggle: got %d, want 503", resp.StatusCode)
	}
}

func TestLivezAlwaysOK(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	// Toggle readiness off — livez must still return 200
	http.Post(srv.URL+"/admin/ready", "application/json", bytes.NewBufferString(`{"ready":false}`)) //nolint:errcheck // test setup call

	resp, err := http.Get(srv.URL + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
}

func TestRootReturnsPodName(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Pod string `json:"pod"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Pod != "test-pod" {
		t.Fatalf("got pod %q, want %q", body.Pod, "test-pod")
	}
}

func TestDelaySlowsResponse(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	http.Post(srv.URL+"/admin/delay", "application/json", bytes.NewBufferString(`{"ms":100}`)) //nolint:errcheck // test setup call

	start := time.Now()
	http.Get(srv.URL + "/") //nolint:errcheck // test call to verify timing, response not needed
	elapsed := time.Since(start)

	if elapsed < 90*time.Millisecond {
		t.Fatalf("expected >=100ms delay, got %v", elapsed)
	}
}
