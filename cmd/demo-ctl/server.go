package main

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strings"
)

//go:embed all:assets
var assetsFS embed.FS

// APIServer wires the HTTP API and static file serving.
type APIServer struct {
	hub      *Hub
	cluster  *Cluster
	k8s      *K8s
	loadgen  *LoadGen
	shutdown func() // called after cluster destroy to trigger clean exit
}

func NewAPIServer(hub *Hub, cluster *Cluster, k8s *K8s, lg *LoadGen, shutdown func()) *APIServer {
	return &APIServer{hub: hub, cluster: cluster, k8s: k8s, loadgen: lg, shutdown: shutdown}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// SSE stream
	mux.Handle("GET /api/stream", s.hub)

	// Pod control
	mux.HandleFunc("PUT /api/pod/{name}/ready", s.handlePodReady)
	mux.HandleFunc("PUT /api/pod/{name}/delay", s.handlePodDelay)

	// Traffic control
	mux.HandleFunc("POST /api/traffic/start", s.handleTrafficStart)
	mux.HandleFunc("POST /api/traffic/stop", s.handleTrafficStop)
	mux.HandleFunc("PUT /api/traffic/rps", s.handleTrafficRPS)
	mux.HandleFunc("PUT /api/traffic/duration", s.handleTrafficDuration)

	// Deployment control
	mux.HandleFunc("PUT /api/deployment/replicas", s.handleScaleReplicas)
	mux.HandleFunc("PUT /api/deployment/strategy", s.handleSetStrategy)
	mux.HandleFunc("PUT /api/deployment/grace", s.handleSetGrace)
	mux.HandleFunc("PUT /api/deployment/prestop", s.handleSetPreStop)
	mux.HandleFunc("POST /api/deployment/rollout", s.handleRollout)

	// Cluster control
	mux.HandleFunc("POST /api/cluster/destroy", s.handleClusterDestroy)

	// Static Svelte app — serve from embedded assets/
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		log.Fatalf("assets sub: %v", err)
	}
	fileServer := http.FileServer(http.FS(sub))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// SPA fallback: any path without a dot → serve index.html
		if !strings.Contains(r.URL.Path, ".") && r.URL.Path != "/" {
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/"
			fileServer.ServeHTTP(w, r2)
			return
		}
		fileServer.ServeHTTP(w, r)
	})

	return mux
}

func (s *APIServer) handlePodReady(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.SetReady(r.Context(), name, body.Ready); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handlePodDelay(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var body struct {
		Ms int64 `json:"ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.SetDelay(r.Context(), name, body.Ms); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficStart(w http.ResponseWriter, _ *http.Request) {
	s.loadgen.Start()
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficStop(w http.ResponseWriter, _ *http.Request) {
	s.loadgen.Stop()
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficRPS(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RPS int `json:"rps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.loadgen.SetRPS(body.RPS)
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleTrafficDuration(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ms int64 `json:"ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.loadgen.SetDurationMs(body.Ms)
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleScaleReplicas(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Replicas int32 `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.ScaleDeployment(r.Context(), body.Replicas); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetStrategy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Strategy string `json:"strategy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.SetStrategy(r.Context(), body.Strategy); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetGrace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Seconds int32 `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.SetGracePeriod(r.Context(), body.Seconds); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleSetPreStop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Seconds int32 `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.k8s.SetPreStopSleep(r.Context(), body.Seconds); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) handleRollout(w http.ResponseWriter, r *http.Request) {
	if err := s.k8s.TriggerRollout(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *APIServer) handleClusterDestroy(w http.ResponseWriter, r *http.Request) {
	go func() {
		if err := s.cluster.Destroy(context.Background()); err != nil {
			log.Printf("destroy cluster: %v", err)
		}
		s.shutdown()
	}()
	w.WriteHeader(http.StatusAccepted)
}
