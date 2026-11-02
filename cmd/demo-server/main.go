package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type server struct {
	podName        string
	ready          atomic.Bool
	delayMs        atomic.Int64
	requestsServed atomic.Int64
	accepting      atomic.Bool
	inFlight       sync.WaitGroup
	mux            *http.ServeMux
}

func newServer(podName string) *server {
	s := &server{
		podName: podName,
		mux:     http.NewServeMux(),
	}
	s.ready.Store(true)
	s.accepting.Store(true)

	s.mux.HandleFunc("GET /", s.handleRoot)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /livez", s.handleLivez)
	s.mux.HandleFunc("POST /admin/ready", s.handleAdminReady)
	s.mux.HandleFunc("POST /admin/delay", s.handleAdminDelay)
	return s
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if !s.accepting.Load() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	s.inFlight.Add(1)
	defer s.inFlight.Done()

	waitMs := int64(0)
	if wq := r.URL.Query().Get("wait"); wq != "" {
		if n, err := strconv.ParseInt(wq, 10, 64); err == nil {
			waitMs = n
		}
	}
	totalDelay := s.delayMs.Load() + waitMs
	if totalDelay > 0 {
		select {
		case <-time.After(time.Duration(totalDelay) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
	s.requestsServed.Add(1)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"pod":            s.podName,
		"ready":          s.ready.Load(),
		"delayMs":        s.delayMs.Load(),
		"requestsServed": s.requestsServed.Load(),
	})
}

func (s *server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (s *server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleAdminReady(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.ready.Store(body.Ready)
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleAdminDelay(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ms int64 `json:"ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.delayMs.Store(body.Ms)
	w.WriteHeader(http.StatusOK)
}

func (s *server) shutdown() {
	log.Println("demo-server: SIGTERM received, draining in-flight requests")
	s.accepting.Store(false)
	s.inFlight.Wait()
	log.Println("demo-server: all in-flight requests complete")
}

func main() {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "unknown"
	}

	s := newServer(podName)
	srv := &http.Server{Addr: ":8080", Handler: s.mux}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	shutdownDone := make(chan struct{})
	go func() {
		<-sigCh
		s.shutdown()
		srv.Shutdown(context.Background())
		close(shutdownDone)
	}()

	log.Printf("demo-server: listening on :8080 pod=%s", podName)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-shutdownDone // wait for Shutdown to drain active connections before exiting
}
