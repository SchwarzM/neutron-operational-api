package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	sessionCookieName = "demo-session"
	sessionTTL        = 30 * time.Minute
	reaperInterval    = 5 * time.Minute
)

// Session holds all state for one active demo session.
type Session struct {
	ID     string
	hub    *Hub
	k8s    *K8s
	cancel context.CancelFunc

	mu       sync.Mutex
	lastSeen time.Time

	inFlight *atomic.Int64
}

func (s *Session) touch() {
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
}

func (s *Session) idle() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastSeen)
}

// SessionManager creates and tracks sessions. Safe for concurrent use.
type SessionManager struct {
	mu              sync.RWMutex
	sessions        map[string]*Session
	cs              kubernetes.Interface
	rcfg            *rest.Config
	controlPlaneURL string
}

func NewSessionManager(cs kubernetes.Interface, rcfg *rest.Config, controlPlaneURL string) *SessionManager {
	sm := &SessionManager{
		sessions:        make(map[string]*Session),
		cs:              cs,
		rcfg:            rcfg,
		controlPlaneURL: controlPlaneURL,
	}
	go sm.reaper()
	return sm
}

// Create allocates a session ID, creates the k8s namespace with all resources,
// starts the k8s watchers, and registers the session.
func (sm *SessionManager) Create(ctx context.Context) (*Session, error) {
	id, err := generateSessionID()
	if err != nil {
		return nil, fmt.Errorf("generate session ID: %w", err)
	}

	if err := createSessionNamespace(ctx, sm.cs, id, sm.controlPlaneURL); err != nil {
		return nil, fmt.Errorf("create session namespace: %w", err)
	}

	hub := NewHub()
	ctx2, cancel := context.WithCancel(context.Background())
	inFlight := new(atomic.Int64)

	k8s := NewK8s(hub, sm.cs, sm.rcfg, sessionNamespace(id), func() int {
		return int(inFlight.Load())
	})

	hub.Publish(ClusterEvent{Type: EventCluster, State: "ready"})

	sess := &Session{
		ID:       id,
		hub:      hub,
		k8s:      k8s,
		cancel:   cancel,
		lastSeen: time.Now(),
		inFlight: inFlight,
	}

	go k8s.WatchPods(ctx2)
	go k8s.WatchEndpoints(ctx2)
	go k8s.WatchDeployment(ctx2)

	sm.mu.Lock()
	sm.sessions[id] = sess
	sm.mu.Unlock()

	return sess, nil
}

// Get returns the session for the given ID, or false if not found.
func (sm *SessionManager) Get(id string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.sessions[id]
	return s, ok
}

// Touch resets the inactivity timer for a session.
func (sm *SessionManager) Touch(id string) {
	sm.mu.RLock()
	s, ok := sm.sessions[id]
	sm.mu.RUnlock()
	if ok {
		s.touch()
	}
}

// Destroy stops the session's watchers, removes it from the map, and deletes its namespace.
func (sm *SessionManager) Destroy(ctx context.Context, id string) error {
	sm.mu.Lock()
	sess, ok := sm.sessions[id]
	if ok {
		delete(sm.sessions, id)
	}
	sm.mu.Unlock()

	if !ok {
		return nil
	}

	sess.cancel()
	return destroySession(ctx, sm.cs, id)
}

func (sm *SessionManager) reaper() {
	tick := time.NewTicker(reaperInterval)
	defer tick.Stop()
	for range tick.C {
		sm.mu.RLock()
		var expired []string
		for id, s := range sm.sessions {
			if s.idle() > sessionTTL {
				expired = append(expired, id)
			}
		}
		sm.mu.RUnlock()

		for _, id := range expired {
			if err := sm.Destroy(context.Background(), id); err != nil {
				log.Printf("reaper: destroy session %s: %v", id, err)
			}
		}
	}
}

func generateSessionID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
