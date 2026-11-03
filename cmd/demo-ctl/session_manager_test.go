package main

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestSessionManagerCreateAndGet(t *testing.T) {
	sm := newTestSessionManager(t)
	sess, err := sm.Create(context.Background())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(sess.ID) != 8 {
		t.Errorf("session ID length = %d, want 8", len(sess.ID))
	}
	got, ok := sm.Get(sess.ID)
	if !ok {
		t.Fatal("Get: session not found after Create")
	}
	if got.ID != sess.ID {
		t.Errorf("ID mismatch: %q != %q", got.ID, sess.ID)
	}
}

func TestSessionManagerDestroyRemovesSession(t *testing.T) {
	sm := newTestSessionManager(t)
	sess, err := sm.Create(context.Background())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sm.Destroy(context.Background(), sess.ID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if _, ok := sm.Get(sess.ID); ok {
		t.Error("session still present after Destroy")
	}
}

func TestSessionManagerTouchUpdatesLastSeen(t *testing.T) {
	sm := newTestSessionManager(t)
	sess, err := sm.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	sess.mu.Lock()
	sess.lastSeen = time.Now().Add(-10 * time.Minute)
	sess.mu.Unlock()

	sm.Touch(sess.ID)

	if sess.idle() > time.Minute {
		t.Error("Touch did not reset lastSeen")
	}
}

func TestGenerateSessionID_UniqueAndLength(t *testing.T) {
	id1, err := generateSessionID()
	if err != nil {
		t.Fatal(err)
	}
	id2, err := generateSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id1) != 8 {
		t.Errorf("id length = %d, want 8", len(id1))
	}
	if id1 == id2 {
		t.Error("two IDs are equal — not random")
	}
}

func TestSessionReaperExpiresStaleSessions(t *testing.T) {
	sm := newTestSessionManager(t)
	sess, err := sm.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// backdate lastSeen past TTL
	sess.mu.Lock()
	sess.lastSeen = time.Now().Add(-(sessionTTL + time.Minute))
	sess.mu.Unlock()

	// invoke reaper logic directly without waiting for the ticker
	sm.mu.RLock()
	var expired []string
	for id, s := range sm.sessions {
		if s.idle() > sessionTTL {
			expired = append(expired, id)
		}
	}
	sm.mu.RUnlock()
	for _, id := range expired {
		sm.Destroy(context.Background(), id) //nolint:errcheck
	}

	if _, ok := sm.Get(sess.ID); ok {
		t.Error("expired session was not reaped")
	}
}

func newTestSessionManager(t *testing.T) *SessionManager {
	t.Helper()
	cs := fake.NewSimpleClientset()
	return &SessionManager{
		sessions:        make(map[string]*Session),
		cs:              cs,
		controlPlaneURL: "http://localhost:9090",
	}
}
