package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHubPublishReachesSubscriber(t *testing.T) {
	h := NewHub()
	ch, unsub := h.subscribe()
	defer unsub()

	h.Publish(ClusterEvent{Type: EventCluster, State: "ready"})

	select {
	case data := <-ch:
		var ev ClusterEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if ev.State != "ready" {
			t.Fatalf("got state %q, want %q", ev.State, "ready")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout: no message received")
	}
}

func TestHubUnsubscribeStopsMessages(t *testing.T) {
	h := NewHub()
	ch, unsub := h.subscribe()
	unsub()

	h.Publish(ClusterEvent{Type: EventCluster, State: "ready"})

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed, got message")
		}
	default:
	}
}

func TestHubStickyStateReplayedOnSubscribe(t *testing.T) {
	h := NewHub()

	// Publish state before any subscriber exists.
	h.Publish(ClusterEvent{Type: EventCluster, State: "ready"})
	h.Publish(DeploymentEvent{Type: EventDeployment, Replicas: 2})
	h.Publish(EndpointsEvent{Type: EventEndpoints, Pods: []string{"demo-abc"}})
	h.Publish(PodEvent{Type: EventPod, Name: "demo-abc", Phase: "Running", Ready: true})

	// Late subscriber should immediately receive all sticky events.
	ch, unsub := h.subscribe()
	defer unsub()

	seen := map[string]bool{}
	deadline := time.After(200 * time.Millisecond)
	for len(seen) < 4 {
		select {
		case data := <-ch:
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			seen[m["type"].(string)] = true
		case <-deadline:
			t.Fatalf("timeout: only got types %v, want cluster/deployment/endpoints/pod", seen)
		}
	}
}

func TestHubPodDeletedRemovedFromSticky(t *testing.T) {
	h := NewHub()

	h.Publish(PodEvent{Type: EventPod, Name: "demo-abc", Phase: "Running"})
	h.Publish(PodEvent{Type: EventPod, Name: "demo-abc", Deleted: true})

	ch, unsub := h.subscribe()
	defer unsub()

	// Deleted pod must not be replayed.
	select {
	case data := <-ch:
		var m map[string]any
		json.Unmarshal(data, &m) //nolint:errcheck
		t.Fatalf("unexpected replay of deleted pod: %v", m)
	case <-time.After(50 * time.Millisecond):
		// correct — nothing replayed
	}
}

func TestHubSSEEndpoint(t *testing.T) {
	h := NewHub()

	srv := httptest.NewServer(http.HandlerFunc(h.ServeHTTP))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type: got %q, want %q", ct, "text/event-stream")
	}
}
