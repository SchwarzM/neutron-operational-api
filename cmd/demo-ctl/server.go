package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
)

//go:embed all:assets
var assetsFS embed.FS

// APIServer wires the HTTP API for all sessions.
type APIServer struct {
	sessions  *SessionManager
	indexHTML []byte
}

func NewAPIServer(sessions *SessionManager) *APIServer {
	indexBytes, err := assetsFS.ReadFile("assets/index.html")
	if err != nil {
		log.Fatalf("read embedded index.html: %v", err)
	}
	return &APIServer{sessions: sessions, indexHTML: indexBytes}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// Health probe (used by demo-ctl Deployment in demo-system)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Landing: check cookie, create or resume session, redirect
	mux.HandleFunc("GET /", s.handleRoot)

	// Static assets — session-independent, served globally
	assetsSub, err := fs.Sub(assetsFS, "assets/assets")
	if err != nil {
		log.Fatalf("assets sub: %v", err)
	}
	mux.Handle("GET /assets/",
		http.StripPrefix("/assets/", http.FileServer(http.FS(assetsSub))))

	// Session SSE stream (registered before the catch-all below)
	mux.HandleFunc("GET /session/{id}/api/stream", s.handleStream)

	// Pod control
	mux.HandleFunc("PUT /session/{id}/api/pod/{name}/ready", s.handlePodReady)
	mux.HandleFunc("PUT /session/{id}/api/pod/{name}/delay", s.handlePodDelay)

	// Traffic control (proxied to loadgen-svc)
	mux.HandleFunc("POST /session/{id}/api/traffic/start", s.handleTrafficStart)
	mux.HandleFunc("POST /session/{id}/api/traffic/stop", s.handleTrafficStop)
	mux.HandleFunc("PUT /session/{id}/api/traffic/rps", s.handleTrafficRPS)
	mux.HandleFunc("PUT /session/{id}/api/traffic/duration", s.handleTrafficDuration)

	// Deployment control
	mux.HandleFunc("PUT /session/{id}/api/deployment/replicas", s.handleScaleReplicas)
	mux.HandleFunc("PUT /session/{id}/api/deployment/strategy", s.handleSetStrategy)
	mux.HandleFunc("PUT /session/{id}/api/deployment/grace", s.handleSetGrace)
	mux.HandleFunc("PUT /session/{id}/api/deployment/prestop", s.handleSetPreStop)
	mux.HandleFunc("POST /session/{id}/api/deployment/rollout", s.handleRollout)

	// Session destroy
	mux.HandleFunc("POST /session/{id}/api/cluster/destroy", s.handleClusterDestroy)

	// Loadgen event callback — reachable via ingress; session ID in the URL is the only access control.
	mux.HandleFunc("POST /session/{id}/internal/event", s.handleInternalEvent)

	// SPA root + fallback (must be last — catch-all for client-side routes)
	mux.HandleFunc("GET /session/{id}/", s.handleSessionUI)
	mux.HandleFunc("GET /session/{id}/{path...}", s.handleSessionUI)

	return mux
}

// handleRoot checks the demo-session cookie and redirects to the session UI.
// If the session is missing or expired, a new session is created first.
func (s *APIServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	var sessionID string
	if c, err := r.Cookie(sessionCookieName); err == nil {
		if _, exists := s.sessions.Get(c.Value); exists {
			sessionID = c.Value
		}
	}
	if sessionID == "" {
		sess, err := s.sessions.Create(r.Context())
		if err != nil {
			http.Error(w, "failed to create session: "+err.Error(), http.StatusInternalServerError)
			return
		}
		sessionID = sess.ID
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    sessionID,
			Path:     "/",
			MaxAge:   86400,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
	}
	http.Redirect(w, r, "/session/"+sessionID+"/", http.StatusTemporaryRedirect)
}

// handleSessionUI serves the Svelte SPA for any /session/{id}/* path.
// If the session no longer exists (expired), redirect to / to get a new one.
func (s *APIServer) handleSessionUI(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}
	s.sessions.Touch(id)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(s.indexHTML) //nolint:errcheck
}

func (s *APIServer) handleStream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, ok := s.sessions.Get(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.sessions.Touch(id)
	sess.hub.ServeHTTP(w, r)
}

// handleInternalEvent receives RequestEvent and StatsEvent POSTs from the loadgen pod
// and forwards them to the session's SSE hub.
func (s *APIServer) handleInternalEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, ok := s.sessions.Get(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, 65536))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typed); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch typed.Type {
	case string(EventRequest):
		var ev RequestEvent
		if err := json.Unmarshal(data, &ev); err == nil {
			sess.hub.Publish(ev)
		}
	case string(EventStats):
		var ev StatsEvent
		if err := json.Unmarshal(data, &ev); err == nil {
			sess.hub.Publish(ev)
			sess.inFlight.Store(int64(ev.InFlight))
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// loadgenURL returns the ClusterIP service URL for the session's loadgen pod.
func loadgenURL(sessionID, path string) string {
	return fmt.Sprintf("http://loadgen-svc.demo-%s.svc.cluster.local:8080%s", sessionID, path)
}

// loadgenCall proxies a control request to the loadgen ClusterIP service.
func loadgenCall(ctx context.Context, sessionID, method, path string, body []byte) error {
	var br io.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, loadgenURL(sessionID, path), br)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("loadgen returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// sessionOp is a helper: looks up the session, touches it, and returns
// (session, true) on success or writes a 404 and returns (nil, false).
func (s *APIServer) sessionOp(w http.ResponseWriter, r *http.Request) (*Session, bool) {
	id := r.PathValue("id")
	sess, ok := s.sessions.Get(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return nil, false
	}
	s.sessions.Touch(id)
	return sess, true
}

func (s *APIServer) handlePodReady(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.SetReady(r.Context(), name, body.Ready); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handlePodDelay(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	var body struct {
		Ms int64 `json:"ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.SetDelay(r.Context(), name, body.Ms); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.sessions.Touch(id)
	if err := loadgenCall(r.Context(), id, "POST", "/start", nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.sessions.Touch(id)
	if err := loadgenCall(r.Context(), id, "POST", "/stop", nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficRPS(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.sessions.Touch(id)
	body, err := io.ReadAll(io.LimitReader(r.Body, 256))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := loadgenCall(r.Context(), id, "PUT", "/rps", body); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficDuration(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.sessions.Touch(id)
	body, err := io.ReadAll(io.LimitReader(r.Body, 256))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := loadgenCall(r.Context(), id, "PUT", "/duration", body); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleScaleReplicas(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	var body struct {
		Replicas int32 `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.ScaleDeployment(r.Context(), body.Replicas); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetStrategy(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	var body struct {
		Strategy string `json:"strategy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.SetStrategy(r.Context(), body.Strategy); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetGrace(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	var body struct {
		Seconds int32 `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.SetGracePeriod(r.Context(), body.Seconds); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetPreStop(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	var body struct {
		Seconds int32 `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := sess.k8s.SetPreStopSleep(r.Context(), body.Seconds); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleRollout(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionOp(w, r)
	if !ok {
		return
	}
	if err := sess.k8s.TriggerRollout(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *APIServer) handleClusterDestroy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.sessions.Get(id); !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	go func() {
		if err := s.sessions.Destroy(context.Background(), id); err != nil {
			log.Printf("destroy session %s: %v", id, err)
		}
	}()
	w.WriteHeader(http.StatusAccepted)
}
