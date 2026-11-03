package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

// TestHandlerRegistration ensures Handler() does not panic on route registration.
// A conflicting mux pattern would panic at startup before serving any request.
func TestHandlerRegistration(t *testing.T) {
	sm := NewSessionManager(fake.NewClientset(), nil, "http://localhost:9090")
	srv := NewAPIServer(sm)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Handler() panicked: %v", r)
		}
	}()
	h := srv.Handler()
	if h == nil {
		t.Fatal("Handler() returned nil")
	}
}

// TestHealthz verifies the /healthz endpoint returns 200.
func TestHealthz(t *testing.T) {
	sm := NewSessionManager(fake.NewClientset(), nil, "http://localhost:9090")
	srv := NewAPIServer(sm)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", w.Code)
	}
}

// TestRootRedirectsToSession verifies GET / creates a session and redirects.
func TestRootRedirectsToSession(t *testing.T) {
	sm := NewSessionManager(fake.NewClientset(), nil, "http://localhost:9090")
	srv := NewAPIServer(sm)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusTemporaryRedirect {
		t.Errorf("GET / = %d, want %d", w.Code, http.StatusTemporaryRedirect)
	}
	loc := w.Header().Get("Location")
	if loc == "" {
		t.Error("GET / did not set Location header")
	}
}
