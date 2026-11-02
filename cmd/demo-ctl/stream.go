package main

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Hub broadcasts SSE events to all connected browser clients.
// It implements Publisher so backend components can call Publish without
// importing net/http.
type Hub struct {
	mu             sync.Mutex
	clients        map[chan []byte]struct{}
	lastCluster    []byte
	lastDeployment []byte
	lastEndpoints  []byte
	lastPods       map[string][]byte // pod name → last PodEvent JSON
}

func NewHub() *Hub {
	return &Hub{
		clients:  make(map[chan []byte]struct{}),
		lastPods: make(map[string][]byte),
	}
}

// Publish JSON-encodes v and sends it to every connected SSE client.
// Slow clients are dropped (non-blocking send).
// ClusterEvent, DeploymentEvent, EndpointsEvent, and PodEvent are stored as
// sticky state and replayed to new subscribers on connect so late-connecting
// browsers don't miss existing state.
func (h *Hub) Publish(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	switch ev := v.(type) {
	case ClusterEvent:
		h.lastCluster = data
	case DeploymentEvent:
		h.lastDeployment = data
	case EndpointsEvent:
		h.lastEndpoints = data
	case PodEvent:
		if ev.Deleted {
			delete(h.lastPods, ev.Name)
		} else {
			h.lastPods[ev.Name] = data
		}
	}
	for ch := range h.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

// subscribe registers a new client channel and returns an unsubscribe func.
// All sticky state is sent immediately so reconnecting browsers see current
// state without waiting for the next event.
func (h *Hub) subscribe() (ch chan []byte, unsub func()) {
	ch = make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	if h.lastCluster != nil {
		ch <- h.lastCluster
	}
	if h.lastDeployment != nil {
		ch <- h.lastDeployment
	}
	if h.lastEndpoints != nil {
		ch <- h.lastEndpoints
	}
	for _, data := range h.lastPods {
		ch <- data
	}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
		close(ch)
	}
}

// ServeHTTP handles GET /api/stream — one long-lived SSE connection per browser tab.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	ch, unsub := h.subscribe()
	defer unsub()

	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write([]byte("data: ")) //nolint:errcheck // SSE: client disconnect handled via context
			_, _ = w.Write(data)             //nolint:errcheck
			_, _ = w.Write([]byte("\n\n"))   //nolint:errcheck
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
