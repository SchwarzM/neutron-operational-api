# K8s Readiness Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained `make demo` tool that spins up a local kind cluster, lets the user spawn/control k8s pods from a browser UI, sends live HTTP traffic through nginx-ingress, and animates every request as a colored dot on an SVG traffic map — teaching zero-request-missed maintenance through direct observation.

**Architecture:** Two Go binaries (`demo-server` runs inside pods; `demo-ctl` runs on the host and owns the kind cluster, k8s API, load generator, SSE hub, and static file server) plus a Svelte frontend compiled and embedded in `demo-ctl`. The `Publisher` interface in `events.go` is the single coupling point between all backend components and the SSE hub.

**Tech Stack:** Go 1.27, k8s.io/client-go v0.37.2, Svelte 5 + Vite 6, kind v0.33, nginx-ingress-controller v1.11.0.

**Spec:** `docs/superpowers/specs/2026-10-28-k8s-readiness-demo-design.md`

## Global Constraints

- Go module path: `github.com/schwarzm/neutron-operational-api`
- k8s.io/client-go v0.37.2, k8s.io/api v0.37.2, k8s.io/apimachinery v0.37.2
- nginx-ingress manifest: `https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.0/deploy/static/provider/kind/deploy.yaml` — downloaded once and embedded at `cmd/demo-ctl/nginx-ingress-kind.yaml`
- kind cluster name: `k8s-readiness-demo`
- All demo k8s objects live in namespace `demo`
- demo-ctl listens on `:9090`; kind maps host port `8080 → ingress port 80`
- Svelte build output: `cmd/demo-ctl/assets/` (embedded via `go:embed`)
- `demo-server` image tag: `demo-server:local`
- Visual style: dark/light theme matching pheme — CSS custom properties, same color tokens (`--add`, `--del`, `--warn`, `--accent`)

## Parallel execution map

After Task 1 and Task 2, three agents can work in parallel:
- **Agent A:** Tasks 3–4 (demo-server binary + Docker image)
- **Agent B:** Tasks 5–10 (full demo-ctl backend)
- **Agent C:** Tasks 11–13 (full Svelte frontend)
Task 14 (Makefile + integration) runs after all three agents finish.

---

## File Map

```
neutron-operational-api/
├── cmd/
│   ├── demo-ctl/
│   │   ├── assets/              ← go:embed target; vite outputs here
│   │   ├── nginx-ingress-kind.yaml  ← downloaded once, embedded
│   │   ├── kind-config.yaml     ← embedded kind cluster config
│   │   ├── events.go            ← shared SSE event types + Publisher interface
│   │   ├── stream.go            ← SSE hub (implements Publisher)
│   │   ├── cluster.go           ← kind cluster lifecycle
│   │   ├── k8s.go               ← pod/service/endpoint watch + per-pod proxy
│   │   ├── loadgen.go           ← HTTP load generator goroutines
│   │   ├── server.go            ← HTTP API handlers + static file serving
│   │   └── main.go              ← entry point, wires everything
│   └── demo-server/
│       └── main.go              ← in-pod HTTP server
├── web/
│   ├── src/
│   │   ├── store.js             ← SSE state + API helpers
│   │   ├── App.svelte           ← layout shell + SSE bootstrap
│   │   ├── Sidebar.svelte       ← controls + k8s event log
│   │   ├── SystemMap.svelte     ← SVG map + dot animation
│   │   └── PodNode.svelte       ← single pod SVG group
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── Dockerfile.demo-server
├── Makefile
├── go.mod
└── go.sum
```

---

## Task 1: Go module + project scaffolding

**Files:**
- Create: `go.mod`
- Create: `cmd/demo-ctl/events.go`
- Create: `cmd/demo-server/main.go` (stub — replaced in Task 3)
- Create: `cmd/demo-ctl/main.go` (stub — replaced in Task 10)

- [ ] **Step 1: Create directory tree**

```bash
mkdir -p cmd/demo-ctl/assets cmd/demo-server web/src docs/superpowers/plans docs/superpowers/specs
```

- [ ] **Step 2: Write go.mod**

```
module github.com/schwarzm/neutron-operational-api

go 1.27

require (
	k8s.io/api v0.37.2
	k8s.io/apimachinery v0.37.2
	k8s.io/client-go v0.37.2
)
```

Save as `go.mod`.

- [ ] **Step 3: Download nginx-ingress manifest**

```bash
curl -sL https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.0/deploy/static/provider/kind/deploy.yaml \
  -o cmd/demo-ctl/nginx-ingress-kind.yaml
```

- [ ] **Step 4: Write kind cluster config**

Save as `cmd/demo-ctl/kind-config.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 8080
    protocol: TCP
```

- [ ] **Step 5: Write cmd/demo-ctl/events.go**

```go
package main

// EventType identifies the SSE event shape.
type EventType string

const (
	EventCluster   EventType = "cluster"
	EventPod       EventType = "pod"
	EventEndpoints EventType = "endpoints"
	EventRequest   EventType = "request"
	EventSignal    EventType = "signal"
	EventStats     EventType = "stats"
)

// ClusterEvent reports cluster bootstrap state.
type ClusterEvent struct {
	Type  EventType `json:"type"`
	State string    `json:"state"` // "creating" | "ready" | "destroying"
}

// PodEvent is emitted whenever a pod's observed state changes.
type PodEvent struct {
	Type           EventType `json:"type"`
	Name           string    `json:"name"`
	Phase          string    `json:"phase"`
	Ready          bool      `json:"ready"`
	Terminating    bool      `json:"terminating"`
	GraceRemaining int       `json:"graceRemaining"` // seconds remaining in grace period
	InFlight       int       `json:"inFlight"`        // requests currently in-flight to this pod
}

// EndpointsEvent lists which pods are currently in the endpoint slice.
type EndpointsEvent struct {
	Type EventType `json:"type"`
	Pods []string  `json:"pods"`
}

// RequestEvent is emitted for every completed HTTP request by the load generator.
type RequestEvent struct {
	Type       EventType `json:"type"`
	ID         string    `json:"id"`
	PodName    string    `json:"podName"`
	Status     int       `json:"status"`
	DurationMs int64     `json:"durationMs"`
	Slow       bool      `json:"slow"`
}

// SignalKind identifies the k8s signal type for SignalEvent.
type SignalKind string

const (
	SignalProbeOK       SignalKind = "PROBE_OK"
	SignalProbeFail     SignalKind = "PROBE_FAIL"
	SignalEndpointAdd   SignalKind = "ENDPOINT_ADD"
	SignalEndpointRemove SignalKind = "ENDPOINT_REMOVE"
	SignalSIGTERM       SignalKind = "SIGTERM"
	SignalDrained       SignalKind = "DRAINED"
)

// SignalEvent reports a notable k8s lifecycle event for a pod.
type SignalEvent struct {
	Type    EventType  `json:"type"`
	Kind    SignalKind `json:"kind"`
	PodName string     `json:"podName"`
	Message string     `json:"message"`
}

// StatsEvent is emitted by the load generator every second.
type StatsEvent struct {
	Type        EventType `json:"type"`
	RPS         float64   `json:"rps"`
	SuccessRate float64   `json:"successRate"`
	P50Ms       int64     `json:"p50Ms"`
	P95Ms       int64     `json:"p95Ms"`
	InFlight    int       `json:"inFlight"`
}

// Publisher is implemented by the SSE Hub. All backend components publish
// events through this interface so they are decoupled from the hub.
type Publisher interface {
	Publish(v any)
}
```

- [ ] **Step 6: Write stub main files**

`cmd/demo-server/main.go`:
```go
package main

func main() {}
```

`cmd/demo-ctl/main.go`:
```go
package main

func main() {}
```

- [ ] **Step 7: Download Go dependencies**

```bash
go mod tidy
```

This will resolve and download k8s.io/client-go and its transitive dependencies. Expect it to take 1–2 minutes.

- [ ] **Step 8: Verify build compiles**

```bash
go build ./...
```

Expected: no errors (stubs compile).

- [ ] **Step 9: Commit**

```bash
git add go.mod go.sum cmd/ docs/
git commit -m "scaffold: go module, event types, kind config, nginx-ingress manifest"
```

---

## Task 2: SSE hub (stream.go)

**Files:**
- Create: `cmd/demo-ctl/stream.go`
- Create: `cmd/demo-ctl/stream_test.go`

**Interfaces:**
- Consumes: `Publisher` interface from `events.go`
- Produces: `*Hub` with methods `Publish(v any)`, `ServeHTTP(w, r)`, `subscribe() (chan []byte, func())`

- [ ] **Step 1: Write the failing tests**

`cmd/demo-ctl/stream_test.go`:
```go
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./cmd/demo-ctl/ -run "TestHub" -v
```

Expected: compile error `NewHub undefined`.

- [ ] **Step 3: Implement stream.go**

`cmd/demo-ctl/stream.go`:
```go
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
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[chan []byte]struct{})}
}

// Publish JSON-encodes v and sends it to every connected SSE client.
// Slow clients are dropped (non-blocking send).
func (h *Hub) Publish(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

// subscribe registers a new client channel and returns an unsubscribe func.
func (h *Hub) subscribe() (chan []byte, func()) {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
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
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(data)
			_, _ = w.Write([]byte("\n\n"))
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./cmd/demo-ctl/ -run "TestHub" -v
```

Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/demo-ctl/stream.go cmd/demo-ctl/stream_test.go
git commit -m "feat(demo-ctl): SSE hub with subscribe/publish/unsubscribe"
```

---

## Task 3: demo-server binary [Agent A]

**Files:**
- Replace: `cmd/demo-server/main.go` (full implementation)
- Create: `cmd/demo-server/main_test.go`

**Interfaces:**
- Produces: HTTP server listening on `:8080` with endpoints per spec

- [ ] **Step 1: Write failing tests**

`cmd/demo-server/main_test.go`:
```go
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
	http.Post(srv.URL+"/admin/ready", "application/json", bytes.NewBufferString(`{"ready":false}`))

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

	http.Post(srv.URL+"/admin/delay", "application/json", bytes.NewBufferString(`{"ms":100}`))

	start := time.Now()
	http.Get(srv.URL + "/")
	elapsed := time.Since(start)

	if elapsed < 90*time.Millisecond {
		t.Fatalf("expected >=100ms delay, got %v", elapsed)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./cmd/demo-server/ -v
```

Expected: compile error `newServer undefined`.

- [ ] **Step 3: Implement cmd/demo-server/main.go**

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	if d := s.delayMs.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
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

	go func() {
		<-sigCh
		s.shutdown()
		srv.Shutdown(context.Background())
	}()

	log.Printf("demo-server: listening on :8080 pod=%s", podName)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./cmd/demo-server/ -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/demo-server/
git commit -m "feat(demo-server): HTTP server with readiness toggle, delay, graceful shutdown"
```

---

## Task 4: Dockerfile + image build [Agent A, after Task 3]

**Files:**
- Create: `Dockerfile.demo-server`

- [ ] **Step 1: Write Dockerfile.demo-server**

```dockerfile
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/demo-server/ ./cmd/demo-server/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /demo-server ./cmd/demo-server/

FROM scratch
COPY --from=build /demo-server /demo-server
ENTRYPOINT ["/demo-server"]
```

- [ ] **Step 2: Verify Docker is running and build the image**

```bash
docker build -f Dockerfile.demo-server -t demo-server:local .
```

Expected: image builds successfully. The final image should be under 20MB (scratch base + static binary).

- [ ] **Step 3: Smoke test the image**

```bash
docker run --rm -e POD_NAME=test -p 18080:8080 demo-server:local &
sleep 1
curl -s http://localhost:18080/readyz && echo " OK"
curl -s http://localhost:18080/livez && echo " OK"
curl -s http://localhost:18080/ | python3 -m json.tool
kill %1
```

Expected: `/readyz` and `/livez` return 200; `/` returns JSON with `"pod":"test"`.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile.demo-server
git commit -m "feat(demo-server): scratch Dockerfile, ~5MB image"
```

---

## Task 5: kind cluster lifecycle (cluster.go) [Agent B]

**Files:**
- Create: `cmd/demo-ctl/cluster.go`

**Interfaces:**
- Consumes: `Publisher` from `events.go`, `ClusterEvent`, `EventCluster`
- Produces: `*Cluster` with `Bootstrap(ctx) error`, `Destroy(ctx) error`, `Clientset() *kubernetes.Clientset`, `RESTConfig() *rest.Config`

- [ ] **Step 1: Write cluster.go**

`cmd/demo-ctl/cluster.go`:
```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	_ "embed"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

//go:embed kind-config.yaml
var kindConfigYAML []byte

//go:embed nginx-ingress-kind.yaml
var nginxIngressYAML []byte

const (
	clusterName   = "k8s-readiness-demo"
	demoNamespace = "demo"
)

// Cluster owns the kind cluster lifecycle and bootstraps all k8s objects.
type Cluster struct {
	pub  Publisher
	cs   *kubernetes.Clientset
	rcfg *rest.Config
}

func NewCluster(pub Publisher) *Cluster {
	return &Cluster{pub: pub}
}

// Bootstrap creates the kind cluster, installs nginx-ingress, and creates
// the demo namespace, Service, and Ingress. Blocks until the cluster is ready.
func (c *Cluster) Bootstrap(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "creating"})

	cfgFile, err := os.CreateTemp("", "kind-config-*.yaml")
	if err != nil {
		return fmt.Errorf("write kind config: %w", err)
	}
	defer os.Remove(cfgFile.Name())
	cfgFile.Write(kindConfigYAML)
	cfgFile.Close()

	if err := kindRun(ctx, "create", "cluster", "--name", clusterName, "--config", cfgFile.Name()); err != nil {
		return fmt.Errorf("kind create cluster: %w", err)
	}

	kubeconfig, err := kindOutput(ctx, "get", "kubeconfig", "--name", clusterName)
	if err != nil {
		return fmt.Errorf("get kubeconfig: %w", err)
	}

	c.rcfg, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("parse kubeconfig: %w", err)
	}
	c.cs, err = kubernetes.NewForConfig(c.rcfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	if err := kubectlApply(ctx, kubeconfig, nginxIngressYAML); err != nil {
		return fmt.Errorf("apply nginx-ingress: %w", err)
	}

	if err := c.waitForIngressController(ctx); err != nil {
		return fmt.Errorf("wait for ingress-controller ready: %w", err)
	}

	if err := c.createNamespace(ctx); err != nil {
		return err
	}
	if err := c.createService(ctx); err != nil {
		return err
	}
	if err := c.createIngress(ctx); err != nil {
		return err
	}

	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "ready"})
	return nil
}

// Destroy deletes the kind cluster.
func (c *Cluster) Destroy(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "destroying"})
	return kindRun(ctx, "delete", "cluster", "--name", clusterName)
}

// Clientset returns the k8s client (valid after Bootstrap).
func (c *Cluster) Clientset() *kubernetes.Clientset { return c.cs }

// RESTConfig returns the REST config (valid after Bootstrap).
func (c *Cluster) RESTConfig() *rest.Config { return c.rcfg }

func (c *Cluster) waitForIngressController(ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, 3*time.Second, 4*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			pods, err := c.cs.CoreV1().Pods("ingress-nginx").List(ctx, metav1.ListOptions{
				LabelSelector: "app.kubernetes.io/component=controller",
			})
			if err != nil || len(pods.Items) == 0 {
				return false, nil
			}
			for _, p := range pods.Items {
				for _, cond := range p.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						return true, nil
					}
				}
			}
			return false, nil
		})
}

func (c *Cluster) createNamespace(ctx context.Context) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: demoNamespace}}
	_, err := c.cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *Cluster) createService(ctx context.Context) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-svc", Namespace: demoNamespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := c.cs.CoreV1().Services(demoNamespace).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *Cluster) createIngress(ctx context.Context) error {
	pathType := networkingv1.PathTypePrefix
	className := "nginx"
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: demoNamespace,
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &className,
			Rules: []networkingv1.IngressRule{{
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: "demo-svc",
									Port: networkingv1.ServiceBackendPort{Number: 80},
								},
							},
						}},
					},
				},
			}},
		},
	}
	_, err := c.cs.NetworkingV1().Ingresses(demoNamespace).Create(ctx, ing, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func kindRun(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "kind", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func kindOutput(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "kind", args...).Output()
}

func kubectlApply(ctx context.Context, kubeconfig, manifest []byte) error {
	f, err := os.CreateTemp("", "kubeconfig-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	f.Write(kubeconfig)
	f.Close()

	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", f.Name(), "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader(manifest)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/demo-ctl/cluster.go
git commit -m "feat(demo-ctl): kind cluster lifecycle with nginx-ingress bootstrap"
```

---

## Task 6: k8s pod management + watchers (k8s.go) [Agent B, after Task 5]

**Files:**
- Create: `cmd/demo-ctl/k8s.go`

**Interfaces:**
- Consumes: `Publisher`, all event types, `*kubernetes.Clientset`, `*rest.Config`
- Produces: `*K8s` with methods:
  - `SpawnPod(ctx) (string, error)` → returns pod name
  - `DeletePod(ctx, name string) error`
  - `SetReady(ctx, name string, ready bool) error`
  - `SetDelay(ctx, name string, ms int64) error`
  - `WatchPods(ctx)` → runs indefinitely, publishes PodEvent + SignalEvent
  - `WatchEndpoints(ctx)` → runs indefinitely, publishes EndpointsEvent + SignalEvent
  - `InFlightInc(podName string)`, `InFlightDec(podName string)` → tracked by loadgen

- [ ] **Step 1: Write k8s.go**

`cmd/demo-ctl/k8s.go`:
```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	terminationGracePeriod = 30
	demoImageName          = "demo-server:local"
)

// K8s manages demo pods and watches their state.
type K8s struct {
	pub  Publisher
	cs   *kubernetes.Clientset
	rcfg *rest.Config

	mu       sync.Mutex
	inFlight map[string]int // pod name → in-flight request count
	podSpecs map[string]*corev1.Pod // pod name → last observed pod
}

func NewK8s(pub Publisher, cs *kubernetes.Clientset, rcfg *rest.Config) *K8s {
	return &K8s{
		pub:      pub,
		cs:       cs,
		rcfg:     rcfg,
		inFlight: make(map[string]int),
		podSpecs: make(map[string]*corev1.Pod),
	}
}

// SpawnPod creates a new demo pod and returns its name.
func (k *K8s) SpawnPod(ctx context.Context) (string, error) {
	name := fmt.Sprintf("demo-%s", randSuffix(6))
	grace := int64(terminationGracePeriod)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: demoNamespace,
			Labels:    map[string]string{"app": "demo"},
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &grace,
			Containers: []corev1.Container{{
				Name:            "demo-server",
				Image:           demoImageName,
				ImagePullPolicy: corev1.PullNever,
				Env: []corev1.EnvVar{{
					Name:  "POD_NAME",
					Value: name,
				}},
				Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/readyz",
							Port: intstrFromInt(8080),
						},
					},
					PeriodSeconds:    2,
					FailureThreshold: 1,
					SuccessThreshold: 1,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/livez",
							Port: intstrFromInt(8080),
						},
					},
					PeriodSeconds:    10,
					FailureThreshold: 3,
				},
			}},
		},
	}
	_, err := k.cs.CoreV1().Pods(demoNamespace).Create(ctx, pod, metav1.CreateOptions{})
	return name, err
}

// DeletePod deletes a pod gracefully (SIGTERM → grace period → SIGKILL).
func (k *K8s) DeletePod(ctx context.Context, name string) error {
	err := k.cs.CoreV1().Pods(demoNamespace).Delete(ctx, name, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

// SetReady calls POST /admin/ready on the pod via the API server pod proxy.
func (k *K8s) SetReady(ctx context.Context, name string, ready bool) error {
	body, _ := json.Marshal(map[string]bool{"ready": ready})
	return k.podProxy(ctx, name, "POST", "admin/ready", body)
}

// SetDelay calls POST /admin/delay on the pod via the API server pod proxy.
func (k *K8s) SetDelay(ctx context.Context, name string, ms int64) error {
	body, _ := json.Marshal(map[string]int64{"ms": ms})
	return k.podProxy(ctx, name, "POST", "admin/delay", body)
}

// InFlightInc increments the in-flight counter for a pod (called by loadgen).
func (k *K8s) InFlightInc(podName string) {
	k.mu.Lock()
	k.inFlight[podName]++
	k.mu.Unlock()
}

// InFlightDec decrements the in-flight counter and re-publishes the pod event.
func (k *K8s) InFlightDec(podName string) {
	k.mu.Lock()
	if k.inFlight[podName] > 0 {
		k.inFlight[podName]--
	}
	k.mu.Unlock()
}

func (k *K8s) getInFlight(podName string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.inFlight[podName]
}

// WatchPods watches all demo pods and publishes PodEvent + SignalEvent on changes.
// Restarts the watch automatically on errors. Runs until ctx is cancelled.
func (k *K8s) WatchPods(ctx context.Context) {
	prevReady := make(map[string]bool)
	for {
		if ctx.Err() != nil {
			return
		}
		w, err := k.cs.CoreV1().Pods(demoNamespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: "app=demo",
		})
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			pod, ok := ev.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			ready := isPodReady(pod)
			terminating := pod.DeletionTimestamp != nil

			graceRemaining := 0
			if terminating && pod.DeletionTimestamp != nil {
				deadline := pod.DeletionTimestamp.Time.Add(terminationGracePeriod * time.Second)
				if rem := time.Until(deadline); rem > 0 {
					graceRemaining = int(rem.Seconds())
				}
			}

			k.pub.Publish(PodEvent{
				Type:           EventPod,
				Name:           pod.Name,
				Phase:          string(pod.Status.Phase),
				Ready:          ready,
				Terminating:    terminating,
				GraceRemaining: graceRemaining,
				InFlight:       k.getInFlight(pod.Name),
			})

			if ev.Type == watch.Modified {
				if was, seen := prevReady[pod.Name]; seen && was != ready {
					if ready {
						k.pub.Publish(SignalEvent{
							Type:    EventSignal,
							Kind:    SignalProbeOK,
							PodName: pod.Name,
							Message: fmt.Sprintf("%s: readiness probe passing", pod.Name),
						})
					} else {
						k.pub.Publish(SignalEvent{
							Type:    EventSignal,
							Kind:    SignalProbeFail,
							PodName: pod.Name,
							Message: fmt.Sprintf("%s: readiness probe failed", pod.Name),
						})
					}
				}
				if terminating {
					// Emit SIGTERM signal once (first time we see DeletionTimestamp)
					k.mu.Lock()
					_, alreadySeen := k.podSpecs[pod.Name]
					k.mu.Unlock()
					if alreadySeen {
						wasTerm := false
						k.mu.Lock()
						if prev, ok := k.podSpecs[pod.Name]; ok {
							wasTerm = prev.DeletionTimestamp != nil
						}
						k.mu.Unlock()
						if !wasTerm {
							k.pub.Publish(SignalEvent{
								Type:    EventSignal,
								Kind:    SignalSIGTERM,
								PodName: pod.Name,
								Message: fmt.Sprintf("%s: SIGTERM sent (grace: %ds)", pod.Name, terminationGracePeriod),
							})
						}
					}
				}
			}

			if ev.Type == watch.Deleted {
				k.pub.Publish(SignalEvent{
					Type:    EventSignal,
					Kind:    SignalDrained,
					PodName: pod.Name,
					Message: fmt.Sprintf("%s: pod exited", pod.Name),
				})
				k.mu.Lock()
				delete(k.podSpecs, pod.Name)
				delete(k.inFlight, pod.Name)
				k.mu.Unlock()
			} else {
				k.mu.Lock()
				podCopy := pod.DeepCopy()
				k.podSpecs[pod.Name] = podCopy
				k.mu.Unlock()
			}

			prevReady[pod.Name] = ready
		}
		time.Sleep(2 * time.Second) // brief pause before reconnect
	}
}

// WatchEndpoints watches the EndpointSlice for demo-svc and publishes EndpointsEvent + SignalEvent.
func (k *K8s) WatchEndpoints(ctx context.Context) {
	prevPods := map[string]bool{}
	for {
		if ctx.Err() != nil {
			return
		}
		w, err := k.cs.DiscoveryV1().EndpointSlices(demoNamespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: "kubernetes.io/service-name=demo-svc",
		})
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			eps, ok := ev.Object.(*discoveryv1.EndpointSlice)
			if !ok {
				continue
			}
			currentPods := map[string]bool{}
			for _, ep := range eps.Endpoints {
				if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
					for _, hint := range ep.TargetRef != nil && ep.TargetRef.Name != "" {
						currentPods[ep.TargetRef.Name] = true
						_ = hint
					}
					if ep.TargetRef != nil {
						currentPods[ep.TargetRef.Name] = true
					}
				}
			}

			names := make([]string, 0, len(currentPods))
			for n := range currentPods {
				names = append(names, n)
			}
			k.pub.Publish(EndpointsEvent{Type: EventEndpoints, Pods: names})

			// Emit signals for pods that transitioned in or out
			for name := range currentPods {
				if !prevPods[name] {
					k.pub.Publish(SignalEvent{
						Type:    EventSignal,
						Kind:    SignalEndpointAdd,
						PodName: name,
						Message: fmt.Sprintf("%s: added to endpoints", name),
					})
				}
			}
			for name := range prevPods {
				if !currentPods[name] {
					k.pub.Publish(SignalEvent{
						Type:    EventSignal,
						Kind:    SignalEndpointRemove,
						PodName: name,
						Message: fmt.Sprintf("%s: removed from endpoints", name),
					})
				}
			}
			prevPods = currentPods
		}
		time.Sleep(2 * time.Second)
	}
}

// podProxy calls an admin endpoint on a specific pod via the k8s API server proxy.
func (k *K8s) podProxy(ctx context.Context, podName, method, path string, body []byte) error {
	req := k.cs.CoreV1().RESTClient().Verb(method).
		Namespace(demoNamespace).
		Resource("pods").
		Name(podName).
		SubResource("proxy").
		Suffix(path).
		Body(bytes.NewReader(body))
	result := req.Do(ctx)
	return result.Error()
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func randSuffix(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// intstrFromInt returns a k8s IntOrString from an int.
func intstrFromInt(v int32) intstr.IntOrString {
	return intstr.FromInt32(v)
}
```

NOTE: The `for _, hint := range ep.TargetRef != nil && ...` pattern is a typo. Replace the endpoint pod name extraction loop with:
```go
if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" {
    currentPods[ep.TargetRef.Name] = true
}
```
And remove the inner `for _, hint` loop.

- [ ] **Step 2: Add missing imports and fix the endpoint loop**

The full corrected endpoint loop in `WatchEndpoints`:
```go
for _, ep := range eps.Endpoints {
    if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
        if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" {
            currentPods[ep.TargetRef.Name] = true
        }
    }
}
```

Also add `"k8s.io/apimachinery/pkg/util/intstr"` to the import block.

- [ ] **Step 3: Verify it compiles**

```bash
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/demo-ctl/k8s.go
git commit -m "feat(demo-ctl): pod spawn/delete/watch and endpoint slice watcher"
```

---

## Task 7: Load generator (loadgen.go) [Agent B, after Task 6]

**Files:**
- Create: `cmd/demo-ctl/loadgen.go`
- Create: `cmd/demo-ctl/loadgen_test.go`

**Interfaces:**
- Consumes: `Publisher`, `RequestEvent`, `StatsEvent`, `*K8s` (for InFlightInc/Dec)
- Produces: `*LoadGen` with `Start()`, `Stop()`, `SetRPS(n int)`, `SetDurationMs(ms int64)`

- [ ] **Step 1: Write failing test**

`cmd/demo-ctl/loadgen_test.go`:
```go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type capPublisher struct {
	events []any
}

func (c *capPublisher) Publish(v any) { c.events = append(c.events, v) }

func TestLoadGenSendsRequests(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"pod":            "test-pod",
			"requestsServed": 1,
		})
	}))
	defer srv.Close()

	pub := &capPublisher{}
	k := &K8s{pub: pub, inFlight: make(map[string]int), podSpecs: make(map[string]*corev1.Pod)}
	lg := NewLoadGen(pub, k, srv.URL)
	lg.SetRPS(10)
	lg.Start()
	time.Sleep(500 * time.Millisecond)
	lg.Stop()

	if hits.Load() < 3 {
		t.Fatalf("expected >=3 requests in 500ms at 10rps, got %d", hits.Load())
	}

	// At least one RequestEvent must have been published
	found := false
	for _, ev := range pub.events {
		if req, ok := ev.(RequestEvent); ok && req.PodName == "test-pod" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no RequestEvent with podName=test-pod published")
	}
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
go test ./cmd/demo-ctl/ -run TestLoadGenSendsRequests -v
```

Expected: compile error `NewLoadGen undefined`.

- [ ] **Step 3: Implement loadgen.go**

`cmd/demo-ctl/loadgen.go`:
```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LoadGen sends HTTP traffic to the ingress and publishes RequestEvent and StatsEvent.
type LoadGen struct {
	pub        Publisher
	k8s        *K8s
	targetURL  string
	rps        atomic.Int64
	durationMs atomic.Int64

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool

	// metrics ring buffer (last 60 samples)
	mu2     sync.Mutex
	samples []sample
}

type sample struct {
	ts         time.Time
	durationMs int64
	success    bool
	podName    string
}

func NewLoadGen(pub Publisher, k8s *K8s, targetURL string) *LoadGen {
	lg := &LoadGen{pub: pub, k8s: k8s, targetURL: targetURL}
	lg.rps.Store(5)
	return lg
}

// SetRPS sets the target requests per second (1–50).
func (lg *LoadGen) SetRPS(n int) {
	if n < 1 {
		n = 1
	}
	if n > 50 {
		n = 50
	}
	lg.rps.Store(int64(n))
}

// SetDurationMs sets the ?wait= parameter sent to demo-server (0 = fast).
func (lg *LoadGen) SetDurationMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	lg.durationMs.Store(ms)
}

// Start begins load generation. Safe to call multiple times.
func (lg *LoadGen) Start() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if lg.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	lg.cancel = cancel
	lg.running = true
	go lg.run(ctx)
	go lg.statsLoop(ctx)
}

// Stop halts load generation.
func (lg *LoadGen) Stop() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.running {
		return
	}
	lg.cancel()
	lg.running = false
}

func (lg *LoadGen) run(ctx context.Context) {
	for {
		rps := lg.rps.Load()
		interval := time.Duration(float64(time.Second) / float64(rps))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			go lg.doRequest(ctx)
		}
	}
}

func (lg *LoadGen) doRequest(ctx context.Context) {
	id := fmt.Sprintf("r-%d", rand.Int63())
	slow := lg.durationMs.Load() > 0
	url := lg.targetURL
	if d := lg.durationMs.Load(); d > 0 {
		url = fmt.Sprintf("%s?wait=%d", url, d)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Request-ID", id)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dur := time.Since(start).Milliseconds()

	status := 0
	podName := ""
	if err == nil {
		status = resp.StatusCode
		if resp.Body != nil {
			var body struct {
				Pod string `json:"pod"`
			}
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			json.Unmarshal(data, &body)
			podName = body.Pod
		}
	}

	if podName != "" {
		lg.k8s.InFlightDec(podName)
	}

	ev := RequestEvent{
		Type:       EventRequest,
		ID:         id,
		PodName:    podName,
		Status:     status,
		DurationMs: dur,
		Slow:       slow,
	}
	lg.pub.Publish(ev)

	lg.mu2.Lock()
	lg.samples = append(lg.samples, sample{
		ts:         time.Now(),
		durationMs: dur,
		success:    status >= 200 && status < 300,
		podName:    podName,
	})
	// Keep only last 60 seconds of samples
	cutoff := time.Now().Add(-60 * time.Second)
	i := 0
	for i < len(lg.samples) && lg.samples[i].ts.Before(cutoff) {
		i++
	}
	lg.samples = lg.samples[i:]
	lg.mu2.Unlock()
}

func (lg *LoadGen) statsLoop(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			lg.publishStats()
		}
	}
}

func (lg *LoadGen) publishStats() {
	lg.mu2.Lock()
	window := time.Now().Add(-5 * time.Second)
	var recent []sample
	for _, s := range lg.samples {
		if s.ts.After(window) {
			recent = append(recent, s)
		}
	}
	lg.mu2.Unlock()

	if len(recent) == 0 {
		lg.pub.Publish(StatsEvent{Type: EventStats})
		return
	}

	success := 0
	durations := make([]int64, 0, len(recent))
	for _, s := range recent {
		if s.success {
			success++
		}
		durations = append(durations, s.durationMs)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	p50 := durations[len(durations)*50/100]
	p95 := durations[len(durations)*95/100]

	lg.pub.Publish(StatsEvent{
		Type:        EventStats,
		RPS:         float64(len(recent)) / 5.0,
		SuccessRate: float64(success) / float64(len(recent)),
		P50Ms:       p50,
		P95Ms:       p95,
	})
}
```

- [ ] **Step 4: Add missing import**

The `corev1` import is needed in the test file (for `*corev1.Pod`). Add to `loadgen_test.go`:
```go
import (
    ...
    corev1 "k8s.io/api/core/v1"
)
```

- [ ] **Step 5: Run test to confirm it passes**

```bash
go test ./cmd/demo-ctl/ -run TestLoadGenSendsRequests -v -timeout 10s
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/demo-ctl/loadgen.go cmd/demo-ctl/loadgen_test.go
git commit -m "feat(demo-ctl): HTTP load generator with RPS control and stats"
```

---

## Task 8: HTTP API server (server.go) [Agent B, after Task 7]

**Files:**
- Create: `cmd/demo-ctl/server.go`

**Interfaces:**
- Consumes: `*Hub`, `*Cluster`, `*K8s`, `*LoadGen`
- Produces: `*APIServer` with `Handler() http.Handler`

- [ ] **Step 1: Write server.go**

`cmd/demo-ctl/server.go`:
```go
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

//go:embed assets
var assetsFS embed.FS

// APIServer wires the HTTP API and static file serving.
type APIServer struct {
	hub     *Hub
	cluster *Cluster
	k8s     *K8s
	loadgen *LoadGen
}

func NewAPIServer(hub *Hub, cluster *Cluster, k8s *K8s, lg *LoadGen) *APIServer {
	return &APIServer{hub: hub, cluster: cluster, k8s: k8s, loadgen: lg}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// SSE stream
	mux.Handle("GET /api/stream", s.hub)

	// Pod control
	mux.HandleFunc("POST /api/pod/spawn", s.handleSpawn)
	mux.HandleFunc("DELETE /api/pod/{name}", s.handleDeletePod)
	mux.HandleFunc("PUT /api/pod/{name}/ready", s.handlePodReady)
	mux.HandleFunc("PUT /api/pod/{name}/delay", s.handlePodDelay)

	// Traffic control
	mux.HandleFunc("POST /api/traffic/start", s.handleTrafficStart)
	mux.HandleFunc("POST /api/traffic/stop", s.handleTrafficStop)
	mux.HandleFunc("PUT /api/traffic/rps", s.handleTrafficRPS)
	mux.HandleFunc("PUT /api/traffic/duration", s.handleTrafficDuration)

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

func (s *APIServer) handleSpawn(w http.ResponseWriter, r *http.Request) {
	name, err := s.k8s.SpawnPod(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"name": name})
}

func (s *APIServer) handleDeletePod(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.k8s.DeletePod(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
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

func (s *APIServer) handleClusterDestroy(w http.ResponseWriter, r *http.Request) {
	go func() {
		if err := s.cluster.Destroy(context.Background()); err != nil {
			log.Printf("destroy cluster: %v", err)
		}
	}()
	w.WriteHeader(http.StatusAccepted)
}
```

- [ ] **Step 2: Create placeholder assets so go:embed compiles**

```bash
mkdir -p cmd/demo-ctl/assets
touch cmd/demo-ctl/assets/.gitkeep
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/demo-ctl/server.go cmd/demo-ctl/assets/.gitkeep
git commit -m "feat(demo-ctl): HTTP API server with all endpoints and go:embed"
```

---

## Task 9: demo-ctl main.go (wiring) [Agent B, after Task 8]

**Files:**
- Replace: `cmd/demo-ctl/main.go`

- [ ] **Step 1: Write main.go**

`cmd/demo-ctl/main.go`:
```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	hub := NewHub()

	cluster := NewCluster(hub)
	if err := cluster.Bootstrap(ctx); err != nil {
		log.Fatalf("bootstrap cluster: %v", err)
	}

	k8s := NewK8s(hub, cluster.Clientset(), cluster.RESTConfig())
	go k8s.WatchPods(ctx)
	go k8s.WatchEndpoints(ctx)

	lg := NewLoadGen(hub, k8s, "http://localhost:8080/")

	srv := NewAPIServer(hub, cluster, k8s, lg)

	httpSrv := &http.Server{
		Addr:    ":9090",
		Handler: srv.Handler(),
	}

	// Open browser
	go openBrowser("http://localhost:9090")

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		lg.Stop()
		cluster.Destroy(context.Background())
		httpSrv.Shutdown(context.Background())
	}()

	log.Println("demo-ctl: listening on http://localhost:9090")
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func openBrowser(url string) {
	// macOS
	exec.Command("open", url).Start()
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/demo-ctl/main.go
git commit -m "feat(demo-ctl): wire all components in main.go"
```

---

## Task 10: Svelte frontend scaffolding + store [Agent C]

**Files:**
- Create: `web/package.json`
- Create: `web/vite.config.js`
- Create: `web/index.html`
- Create: `web/src/store.js`

**Interfaces:**
- Consumes: SSE event types from spec (events.go names map 1:1 to JSON `type` field values)
- Produces: reactive Svelte stores `pods`, `endpoints`, `stats`, `signals`, `clusterState`; `api` helper object

- [ ] **Step 1: Write package.json**

`web/package.json`:
```json
{
  "name": "k8s-readiness-demo",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "devDependencies": {
    "@sveltejs/vite-plugin-svelte": "^5.0.0",
    "svelte": "^5.0.0",
    "vite": "^6.0.0"
  }
}
```

- [ ] **Step 2: Write vite.config.js**

`web/vite.config.js`:
```js
import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: '../cmd/demo-ctl/assets',
    emptyOutDir: true,
  },
})
```

- [ ] **Step 3: Write index.html**

`web/index.html`:
```html
<!doctype html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark light">
  <title>K8s Readiness Demo</title>
  <link rel="icon" href="data:,">
</head>
<body>
  <div id="app"></div>
  <script type="module" src="/src/main.js"></script>
</body>
</html>
```

- [ ] **Step 4: Write web/src/main.js**

`web/src/main.js`:
```js
import App from './App.svelte'
import { mount } from 'svelte'

const app = mount(App, { target: document.getElementById('app') })
export default app
```

- [ ] **Step 5: Write web/src/store.js**

`web/src/store.js`:
```js
import { writable, derived } from 'svelte/store'

// ---- reactive state ----
export const clusterState = writable('creating') // 'creating' | 'ready' | 'destroying'
export const pods = writable({})         // name → PodEvent
export const endpoints = writable([])    // string[]
export const stats = writable(null)      // StatsEvent | null
export const signals = writable([])      // SignalEvent[] (last 50)

// ---- derived ----
export const podList = derived(pods, $pods => Object.values($pods))

// ---- SSE connection ----
let es = null

export function connectSSE() {
  if (es) return
  es = new EventSource('/api/stream')
  es.onmessage = (e) => {
    let ev
    try { ev = JSON.parse(e.data) } catch { return }
    switch (ev.type) {
      case 'cluster':
        clusterState.set(ev.state)
        break
      case 'pod':
        pods.update(p => ({ ...p, [ev.name]: ev }))
        break
      case 'endpoints':
        endpoints.set(ev.pods ?? [])
        break
      case 'stats':
        stats.set(ev)
        break
      case 'signal':
        signals.update(s => [ev, ...s].slice(0, 50))
        break
    }
  }
  es.onerror = () => {
    es.close()
    es = null
    setTimeout(connectSSE, 2000) // reconnect
  }
}

// ---- API helpers ----
async function apiPost(path, body) {
  const r = await fetch(path, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r
}
async function apiPut(path, body) {
  const r = await fetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r
}
async function apiDelete(path) {
  const r = await fetch(path, { method: 'DELETE' })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r
}

export const api = {
  spawnPod:          () => apiPost('/api/pod/spawn'),
  deletePod:         (name) => apiDelete(`/api/pod/${name}`),
  setPodReady:       (name, ready) => apiPut(`/api/pod/${name}/ready`, { ready }),
  setPodDelay:       (name, ms) => apiPut(`/api/pod/${name}/delay`, { ms }),
  startTraffic:      () => apiPost('/api/traffic/start'),
  stopTraffic:       () => apiPost('/api/traffic/stop'),
  setRPS:            (rps) => apiPut('/api/traffic/rps', { rps }),
  setDuration:       (ms) => apiPut('/api/traffic/duration', { ms }),
  destroyCluster:    () => apiPost('/api/cluster/destroy'),
}
```

- [ ] **Step 6: Install dependencies and verify build**

```bash
cd web && npm install
npm run build
```

Expected: `cmd/demo-ctl/assets/` is populated with `index.html`, JS bundles.

- [ ] **Step 7: Verify Go embed picks it up**

```bash
cd .. && go build ./cmd/demo-ctl/
```

Expected: compiles cleanly (the `.gitkeep` can be deleted now that assets has real content).

- [ ] **Step 8: Commit**

```bash
git add web/ cmd/demo-ctl/assets/
git commit -m "feat(web): Svelte scaffolding, SSE store, API helpers"
```

---

## Task 11: App.svelte + Sidebar.svelte [Agent C, after Task 10]

**Files:**
- Create: `web/src/App.svelte`
- Create: `web/src/Sidebar.svelte`

- [ ] **Step 1: Write App.svelte**

`web/src/App.svelte`:
```svelte
<script>
  import { onMount } from 'svelte'
  import { connectSSE, clusterState, stats } from './store.js'
  import SystemMap from './SystemMap.svelte'
  import Sidebar from './Sidebar.svelte'

  let theme = $state(localStorage.getItem('demo-theme') || 'dark')

  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark'
    localStorage.setItem('demo-theme', theme)
    document.documentElement.dataset.theme = theme
  }

  onMount(() => {
    document.documentElement.dataset.theme = theme
    connectSSE()
  })

  const stateColor = { creating: 'warn', ready: 'ok', destroying: 'err' }
</script>

<div class="shell">
  <header class="topbar">
    <h1>K8s Readiness <span>Demo</span></h1>
    <span class="pill {stateColor[$clusterState] ?? 'warn'}">{$clusterState}</span>
    {#if $stats}
      <div class="rates">
        <div><dt>req/s</dt><dd>{$stats.rps?.toFixed(1) ?? '–'}</dd></div>
        <div><dt>success</dt><dd>{$stats.successRate != null ? ($stats.successRate*100).toFixed(0)+'%' : '–'}</dd></div>
        <div><dt>p50</dt><dd>{$stats.p50Ms ?? '–'}ms</dd></div>
        <div><dt>p95</dt><dd>{$stats.p95Ms ?? '–'}ms</dd></div>
      </div>
    {/if}
    <button class="btn" onclick={toggleTheme} aria-label="Toggle theme">
      {theme === 'dark' ? '☀' : '☾'}
    </button>
  </header>

  <main class="body">
    <div class="map-area">
      <SystemMap />
    </div>
    <aside class="sidebar-area">
      <Sidebar />
    </aside>
  </main>
</div>

<style>
  :global(*) { box-sizing: border-box; margin: 0; padding: 0; }
  :global(:root) {
    --font-head: "Arial Black", system-ui, sans-serif;
    --font-body: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    --font-mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    --s1:4px; --s2:8px; --s3:12px; --s4:16px; --s5:24px;
    --radius:6px; --dur:180ms;
    --bg:#eef1f5; --surface:#fff; --surface-2:#f4f6f9; --surface-3:#e7ebf0;
    --border:#d3d9e1; --border-strong:#aab4c0;
    --text:#17202b; --muted:#4f5b69; --faint:#6a7583;
    --accent:#0a5fc2; --accent-ink:#fff;
    --add:#116329; --add-bg:#dcf3e3;
    --del:#b4141f; --del-bg:#fde0e2;
    --warn:#8a5300; --warn-bg:#fbeccb;
    --frame:#0f7a6c;
    --shadow:0 8px 28px rgba(18,30,45,.18);
  }
  :global([data-theme="dark"]) {
    --bg:#0a0e13; --surface:#111821; --surface-2:#161f2a; --surface-3:#1d2834;
    --border:#253242; --border-strong:#3a4a5d;
    --text:#e6edf4; --muted:#a3b0be; --faint:#8592a1;
    --accent:#5aa9ff; --accent-ink:#06121f;
    --add:#5fd47f; --add-bg:#12301d;
    --del:#ff7b80; --del-bg:#3a1519;
    --warn:#f0b54a; --warn-bg:#33270f;
    --frame:#3fd6c1;
    --shadow:0 10px 34px rgba(0,0,0,.55);
  }
  :global(body) {
    background: var(--bg); color: var(--text);
    font: 400 1rem/1.4 var(--font-body);
    font-variant-numeric: tabular-nums;
    height: 100vh; overflow: hidden; display: flex; flex-direction: column;
  }
  :global(button) { font: inherit; color: inherit; cursor: pointer; }
  .shell { display: flex; flex-direction: column; height: 100vh; }
  .topbar {
    display: flex; align-items: center; gap: var(--s3);
    padding: var(--s2) var(--s4); border-bottom: 1px solid var(--border);
    background: var(--surface); flex-shrink: 0;
  }
  h1 { font: 400 1.4rem var(--font-head); white-space: nowrap; }
  h1 span { color: var(--accent); }
  .pill { font: 700 .7rem var(--font-body); letter-spacing:.06em; text-transform:uppercase;
    padding: 3px 8px; border-radius: 999px; border: 1px solid currentColor; white-space: nowrap; }
  .pill.ok { color: var(--add); }
  .pill.warn { color: var(--warn); }
  .pill.err { color: var(--del); }
  .rates { display: flex; gap: var(--s4); margin-left: auto; }
  .rates div { display: flex; flex-direction: column; align-items: flex-end; }
  .rates dt { font-size: .65rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; }
  .rates dd { font: 700 1rem/1 var(--font-mono); }
  .btn { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 4px 10px; font-size: .9rem; }
  .body { display: grid; grid-template-columns: 1fr 300px; flex: 1; min-height: 0; }
  .map-area { min-height: 0; overflow: hidden; }
  .sidebar-area { border-left: 1px solid var(--border); overflow-y: auto; }
</style>
```

- [ ] **Step 2: Write Sidebar.svelte**

`web/src/Sidebar.svelte`:
```svelte
<script>
  import { pods, signals, api } from './store.js'

  let destroying = $state(false)
  let destroyConfirm = $state(false)
  let trafficRunning = $state(false)
  let rps = $state(5)
  let durationMs = $state(0)

  async function spawn() {
    await api.spawnPod()
  }
  async function deletePod(name) {
    await api.deletePod(name)
  }
  async function toggleReady(pod) {
    await api.setPodReady(pod.name, !pod.ready)
  }
  async function setDelay(name, ms) {
    await api.setPodDelay(name, ms)
  }
  async function toggleTraffic() {
    if (trafficRunning) {
      await api.stopTraffic()
    } else {
      await api.startTraffic()
    }
    trafficRunning = !trafficRunning
  }
  async function applyRPS() {
    await api.setRPS(Number(rps))
  }
  async function applyDuration() {
    await api.setDuration(Number(durationMs))
  }
  async function destroy() {
    destroying = true
    destroyConfirm = false
    await api.destroyCluster()
  }

  const signalColors = {
    PROBE_OK: 'add', PROBE_FAIL: 'warn',
    ENDPOINT_ADD: 'frame', ENDPOINT_REMOVE: 'warn',
    SIGTERM: 'del', DRAINED: 'frame',
  }
</script>

<div class="sidebar">
  <!-- Pods -->
  <section class="grp">
    <h3>Pods</h3>
    <button class="btn primary" onclick={spawn}>+ Spawn pod</button>
    {#each Object.values($pods) as pod (pod.name)}
      <div class="pod-row">
        <span class="pod-name mono">{pod.name}</span>
        <span class="badge {pod.terminating ? 'err' : pod.ready ? 'ok' : 'warn'}">
          {pod.terminating ? `term ${pod.graceRemaining}s` : pod.ready ? 'ready' : 'not ready'}
        </span>
        <div class="pod-actions">
          <button class="btn small" onclick={() => toggleReady(pod)}
            disabled={pod.terminating}>
            {pod.ready ? 'set not-ready' : 'set ready'}
          </button>
          <select class="sel" onchange={e => setDelay(pod.name, Number(e.target.value))}
            disabled={pod.terminating}>
            <option value="0">fast</option>
            <option value="500">500ms</option>
            <option value="2000">2s</option>
          </select>
          <button class="btn small danger" onclick={() => deletePod(pod.name)}
            disabled={pod.terminating}>delete</button>
        </div>
      </div>
    {/each}
    {#if Object.keys($pods).length === 0}
      <p class="empty">No pods. Spawn one to start.</p>
    {/if}
  </section>

  <!-- Traffic -->
  <section class="grp">
    <h3>Traffic</h3>
    <div class="row">
      <button class="btn {trafficRunning ? 'danger' : 'primary'}" onclick={toggleTraffic}>
        {trafficRunning ? 'Stop traffic' : 'Start traffic'}
      </button>
    </div>
    <div class="row">
      <label>req/s</label>
      <input type="number" min="1" max="50" bind:value={rps} onchange={applyRPS} class="input-num">
    </div>
    <div class="row">
      <label>duration</label>
      <select class="sel" bind:value={durationMs} onchange={applyDuration}>
        <option value={0}>fast</option>
        <option value={500}>500ms</option>
        <option value={2000}>2s</option>
      </select>
    </div>
  </section>

  <!-- Cluster -->
  <section class="grp">
    <h3>Cluster</h3>
    {#if !destroyConfirm}
      <button class="btn danger" onclick={() => destroyConfirm = true}
        disabled={destroying}>
        {destroying ? 'Destroying…' : 'Destroy cluster…'}
      </button>
    {:else}
      <p class="confirm-q">Delete the kind cluster?</p>
      <div class="row">
        <button class="btn danger" onclick={destroy}>Confirm</button>
        <button class="btn" onclick={() => destroyConfirm = false}>Cancel</button>
      </div>
    {/if}
  </section>

  <!-- K8s events -->
  <section class="grp events">
    <h3>K8s events</h3>
    <ol class="event-log">
      {#each $signals as sig (sig.message + sig.kind)}
        <li class="ev {signalColors[sig.kind] ?? 'muted'}">
          <span class="ev-kind mono">{sig.kind}</span>
          <span class="ev-msg">{sig.message}</span>
        </li>
      {/each}
    </ol>
  </section>
</div>

<style>
  .sidebar { padding: var(--s3); display: grid; gap: var(--s4); align-content: start; }
  .grp { display: grid; gap: var(--s2); }
  h3 { font: 400 .7rem var(--font-head); text-transform: uppercase; letter-spacing: .08em;
    color: var(--muted); }
  .btn {
    display: inline-flex; align-items: center; justify-content: center; gap: 6px;
    min-height: 30px; padding: 4px 10px; border-radius: var(--radius);
    border: 1px solid var(--border-strong); background: var(--surface-2);
    font-size: .82rem; font-weight: 600; white-space: nowrap;
    transition: background var(--dur);
  }
  .btn:hover:not([disabled]) { background: var(--surface-3); border-color: var(--accent); }
  .btn[disabled] { opacity: .5; cursor: not-allowed; }
  .btn.primary { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); }
  .btn.danger { color: var(--del); border-color: var(--del); }
  .btn.danger:hover:not([disabled]) { background: var(--del-bg); }
  .btn.small { font-size: .75rem; min-height: 24px; padding: 2px 7px; }
  .sel { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 3px 5px; font-size: .78rem; color: inherit; }
  .input-num { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 3px 5px; width: 60px; font: inherit; color: inherit; }
  .row { display: flex; gap: var(--s2); align-items: center; flex-wrap: wrap; }
  .row label { font-size: .75rem; color: var(--muted); width: 4.5rem; flex: none; }
  .pod-row { display: grid; gap: 4px; padding: var(--s2); background: var(--surface-2);
    border: 1px solid var(--border); border-radius: var(--radius); }
  .pod-name { font-size: .78rem; font-family: var(--font-mono); }
  .pod-actions { display: flex; gap: 4px; flex-wrap: wrap; }
  .badge { font: 700 .65rem var(--font-body); letter-spacing: .05em; text-transform: uppercase;
    padding: 2px 6px; border-radius: 3px; border: 1px solid currentColor; }
  .badge.ok { color: var(--add); }
  .badge.warn { color: var(--warn); }
  .badge.err { color: var(--del); }
  .empty { font-size: .78rem; color: var(--muted); }
  .confirm-q { font-size: .82rem; }
  .mono { font-family: var(--font-mono); }
  .events { border-top: 1px solid var(--border); padding-top: var(--s3); }
  .event-log { list-style: none; display: grid; gap: 3px; max-height: 300px; overflow-y: auto; }
  .ev { font-size: .75rem; padding: 3px 6px 3px 8px; border-left: 3px solid; 
    background: var(--surface-2); border-radius: 0 3px 3px 0; display: grid;
    grid-template-columns: auto 1fr; gap: 0 6px; }
  .ev.add { border-left-color: var(--add); }
  .ev.warn { border-left-color: var(--warn); }
  .ev.del { border-left-color: var(--del); }
  .ev.frame { border-left-color: var(--frame); }
  .ev.muted { border-left-color: var(--muted); }
  .ev-kind { font-weight: 700; color: var(--muted); font-family: var(--font-mono); font-size: .68rem; }
  .ev-msg { color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
```

- [ ] **Step 3: Build and verify**

```bash
cd web && npm run build && cd ..
go build ./cmd/demo-ctl/
```

Expected: both build without errors.

- [ ] **Step 4: Commit**

```bash
git add web/src/App.svelte web/src/Sidebar.svelte cmd/demo-ctl/assets/
git commit -m "feat(web): App shell with topbar stats and Sidebar controls"
```

---

## Task 12: SystemMap.svelte + PodNode.svelte [Agent C, after Task 11]

**Files:**
- Create: `web/src/SystemMap.svelte`
- Create: `web/src/PodNode.svelte`

- [ ] **Step 1: Write PodNode.svelte**

`web/src/PodNode.svelte`:
```svelte
<script>
  let { pod, x, y, w = 130, h = 50 } = $props()
  // pod: PodEvent shape — name, ready, terminating, graceRemaining, inFlight

  const borderClass = $derived(
    pod.terminating ? 'term' : pod.ready ? 'ready' : 'notready'
  )
  const label = $derived(
    pod.terminating
      ? (pod.inFlight > 0 ? `TERM · ${pod.inFlight} in-flight` : `TERM ${pod.graceRemaining}s`)
      : pod.ready ? 'ready' : 'NOT READY'
  )
</script>

<g transform="translate({x - w/2} {y - h/2})">
  <rect width={w} height={h} rx="6" class="pod-rect {borderClass}" />
  <text x={w/2} y={h/2 - 5} text-anchor="middle" class="t1">{pod.name}</text>
  <text x={w/2} y={h/2 + 11} text-anchor="middle" class="t2 {borderClass}">{label}</text>
</g>

<style>
  .pod-rect { fill: var(--surface-2); stroke-width: 1.8; }
  .pod-rect.ready { stroke: var(--add); }
  .pod-rect.notready { stroke: var(--warn); stroke-dasharray: 4 3; }
  .pod-rect.term { stroke: var(--del); stroke-dasharray: 4 3; }
  .t1 { font: 700 11px var(--font-mono); fill: var(--text); }
  .t2 { font: 500 10px var(--font-body); }
  .t2.ready { fill: var(--add); }
  .t2.notready { fill: var(--warn); }
  .t2.term { fill: var(--del); }
</style>
```

- [ ] **Step 2: Write SystemMap.svelte**

`web/src/SystemMap.svelte`:
```svelte
<script>
  import { onMount, onDestroy } from 'svelte'
  import { pods, endpoints } from './store.js'
  import PodNode from './PodNode.svelte'

  // ---- fixed layout ----
  const W = 1000, H = 400
  const GEN  = { x: 80,  y: 200, w: 100, h: 50, label: 'load gen',  sub: 'localhost:8080' }
  const ING  = { x: 280, y: 200, w: 130, h: 50, label: 'ingress',   sub: 'nginx' }
  const SVC  = { x: 490, y: 200, w: 130, h: 50, label: 'service',   sub: 'demo-svc' }
  const PODS_X = 730
  const POD_H  = 56
  const POD_W  = 130

  // Dynamic pod layout
  let podLayout = $derived.by(() => {
    const list = Object.values($pods)
    if (list.length === 0) return []
    const totalH = list.length * POD_H + (list.length - 1) * 8
    const startY = H / 2 - totalH / 2 + POD_H / 2
    return list.map((pod, i) => ({
      pod,
      x: PODS_X,
      y: startY + i * (POD_H + 8),
    }))
  })

  // SVG ref for getPointAtLength
  let svgEl = $state(null)

  // ---- dot animation ----
  const SPEED = 0.38 // viewBox units per ms
  let dots = []
  let raf = 0
  let dotPool = []

  function edgePath(a, b) {
    const x1 = a.x + a.w / 2, x2 = b.x - b.w / 2
    const y1 = a.y, y2 = b.y
    const mx = (x1 + x2) / 2
    return `M${x1} ${y1} C${mx} ${y1} ${mx} ${y2} ${x2} ${y2}`
  }

  // Edges between fixed nodes
  const fixedEdges = $derived([
    { id: 'gen-ing', d: edgePath(GEN, ING) },
    { id: 'ing-svc', d: edgePath(ING, SVC) },
  ])

  // Edges from SVC to each pod
  let podEdges = $derived(
    podLayout.map(({ pod, x, y }) => ({
      id: `svc-${pod.name}`,
      podName: pod.name,
      d: edgePath(SVC, { x, y, w: POD_W, h: POD_H }),
    }))
  )

  // All path elements indexed by edge id (populated by bind:this on <path>)
  let pathEls = {}

  export function spawnDot(podName, cls) {
    if (!svgEl) return
    const podEdge = podEdges.find(e => e.podName === podName)
    if (!podEdge) return

    const segIds = ['gen-ing', 'ing-svc', `svc-${podName}`]
    const segs = segIds.map(id => pathEls[id]).filter(Boolean).map(el => ({
      el,
      len: el.getTotalLength(),
    }))
    if (segs.some(s => s.len === 0)) return

    const c = dotPool.pop() || document.createElementNS('http://www.w3.org/2000/svg', 'circle')
    c.setAttribute('r', '4.5')
    c.setAttribute('class', `dot ${cls}`)
    c.setAttribute('cx', '-20')
    svgEl.querySelector('.dot-layer').appendChild(c)

    dots.push({
      c,
      segs,
      total: segs.reduce((a, s) => a + s.len, 0),
      start: performance.now(),
    })
    if (!raf) raf = requestAnimationFrame(tick)
  }

  function tick(now) {
    raf = 0
    for (let i = dots.length - 1; i >= 0; i--) {
      const d = dots[i]
      let dist = (now - d.start) * SPEED
      if (dist < 0) continue
      if (dist >= d.total) {
        d.c.remove()
        dotPool.push(d.c)
        dots.splice(i, 1)
        continue
      }
      let s = 0
      while (s < d.segs.length - 1 && dist > d.segs[s].len) {
        dist -= d.segs[s].len
        s++
      }
      const pt = d.segs[s].el.getPointAtLength(Math.min(dist, d.segs[s].len))
      d.c.setAttribute('cx', pt.x.toFixed(1))
      d.c.setAttribute('cy', pt.y.toFixed(1))
    }
    if (dots.length) raf = requestAnimationFrame(tick)
  }

  // Listen to request events from the SSE stream via a custom event
  function handleRequest(ev) {
    const { podName, status, slow } = ev.detail
    const cls = slow ? 'warn' : status >= 200 && status < 300 ? 'add' : 'del'
    spawnDot(podName, cls)
  }

  onMount(() => {
    window.addEventListener('demo:request', handleRequest)
  })
  onDestroy(() => {
    window.removeEventListener('demo:request', handleRequest)
    if (raf) cancelAnimationFrame(raf)
  })
</script>

<svg bind:this={svgEl} viewBox="0 0 {W} {H}" class="sysmap"
  role="img" aria-label="Traffic flow diagram">
  <!-- column labels -->
  <text x={GEN.x}   y="22" text-anchor="middle" class="col-label">LOAD GEN</text>
  <text x={ING.x}   y="22" text-anchor="middle" class="col-label">INGRESS</text>
  <text x={SVC.x}   y="22" text-anchor="middle" class="col-label">SERVICE</text>
  <text x={PODS_X}  y="22" text-anchor="middle" class="col-label">PODS</text>

  <!-- fixed edges -->
  {#each fixedEdges as edge (edge.id)}
    <path bind:this={pathEls[edge.id]} d={edge.d} class="edge" />
  {/each}

  <!-- pod edges -->
  {#each podEdges as edge (edge.id)}
    <path bind:this={pathEls[edge.id]} d={edge.d}
      class="edge {$endpoints.includes(edge.podName) ? 'active' : 'inactive'}" />
  {/each}

  <!-- fixed nodes -->
  {#each [GEN, ING, SVC] as node}
    <g>
      <rect x={node.x - node.w/2} y={node.y - node.h/2}
        width={node.w} height={node.h} rx="6" class="node-rect" />
      <text x={node.x} y={node.y - 5} text-anchor="middle" class="t1">{node.label}</text>
      <text x={node.x} y={node.y + 11} text-anchor="middle" class="t2">{node.sub}</text>
      {#if node === SVC && $endpoints.length > 0}
        <text x={node.x} y={node.y + 24} text-anchor="middle" class="t3">
          {$endpoints.length} endpoint{$endpoints.length !== 1 ? 's' : ''}
        </text>
      {/if}
    </g>
  {/each}

  <!-- dynamic pod nodes -->
  {#each podLayout as { pod, x, y } (pod.name)}
    <PodNode {pod} {x} {y} w={POD_W} h={POD_H} />
  {/each}

  <!-- empty state -->
  {#if podLayout.length === 0}
    <text x={PODS_X} y={H/2} text-anchor="middle" class="empty-hint">
      Spawn a pod →
    </text>
  {/if}

  <!-- dot layer (dots appended here programmatically) -->
  <g class="dot-layer" aria-hidden="true"></g>
</svg>

<style>
  .sysmap { width: 100%; height: 100%; display: block; color: var(--text); }
  .col-label { font: 400 11px var(--font-head); fill: var(--faint); letter-spacing: .1em; }
  .edge { fill: none; stroke: var(--border-strong); stroke-width: 1.6; }
  .edge.active { stroke: color-mix(in srgb, var(--add) 55%, var(--border-strong)); stroke-width: 2; }
  .edge.inactive { stroke: var(--border-strong); stroke-dasharray: 5 4; opacity: .6; }
  .node-rect { fill: var(--surface-2); stroke: var(--border-strong); stroke-width: 1.2; }
  .t1 { font: 700 13px var(--font-body); fill: var(--text); }
  .t2 { font: 500 10.5px var(--font-mono); fill: var(--muted); }
  .t3 { font: 500 9.5px var(--font-mono); fill: var(--faint); }
  .empty-hint { font: 400 13px var(--font-body); fill: var(--muted); }
  :global(.dot) { pointer-events: none; }
  :global(.dot.add)  { fill: var(--add); }
  :global(.dot.del)  { fill: var(--del); }
  :global(.dot.warn) { fill: var(--warn); }
</style>
```

- [ ] **Step 3: Wire RequestEvent to the SVG dot spawner**

The `store.js` SSE handler needs to dispatch a `demo:request` custom event for `SystemMap.svelte` to consume. In `web/src/store.js`, update the `request` case:

```js
case 'request':
  window.dispatchEvent(new CustomEvent('demo:request', { detail: ev }))
  break
```

Add this line inside the `switch` in the `es.onmessage` handler, replacing the empty `case 'request':` (which currently falls through).

- [ ] **Step 4: Build and verify**

```bash
cd web && npm run build && cd ..
go build ./cmd/demo-ctl/
```

Expected: both compile without errors.

- [ ] **Step 5: Commit**

```bash
git add web/src/SystemMap.svelte web/src/PodNode.svelte web/src/store.js cmd/demo-ctl/assets/
git commit -m "feat(web): SVG system map with animated request dots and dynamic pod nodes"
```

---

## Task 13: Makefile + integration [after Tasks 4, 9, 12]

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Write Makefile**

`Makefile`:
```makefile
.PHONY: demo build build-go build-web build-image clean

CLUSTER_NAME = k8s-readiness-demo
IMAGE        = demo-server:local

# Run the full demo: build everything, start demo-ctl (which bootstraps kind)
demo: build
	go run ./cmd/demo-ctl/

# Build all artefacts
build: build-image build-go

# Build the in-pod container image and load it into kind (if cluster exists)
build-image:
	docker build -f Dockerfile.demo-server -t $(IMAGE) .
	-kind load docker-image $(IMAGE) --name $(CLUSTER_NAME) 2>/dev/null || true

# Compile the Go binaries
build-go: build-web
	go build -o bin/demo-ctl  ./cmd/demo-ctl/
	go build -o bin/demo-server ./cmd/demo-server/

# Build the Svelte frontend
build-web:
	cd web && npm install --prefer-offline && npm run build

# Delete the kind cluster and clean build artefacts
clean:
	-kind delete cluster --name $(CLUSTER_NAME) 2>/dev/null || true
	rm -rf bin/ cmd/demo-ctl/assets/
	cd web && rm -rf dist node_modules

# Run Go tests
test:
	go test ./...
```

- [ ] **Step 2: Create bin/ directory placeholder**

```bash
mkdir -p bin && touch bin/.gitkeep
```

- [ ] **Step 3: Add .gitignore**

Create `.gitignore`:
```
bin/
cmd/demo-ctl/assets/
web/node_modules/
web/dist/
*.local
```

- [ ] **Step 4: Verify `make build` runs without a cluster**

```bash
make build
```

Expected: Docker image builds, Svelte compiles to `cmd/demo-ctl/assets/`, Go binaries compile to `bin/`.

- [ ] **Step 5: End-to-end smoke test (requires Docker running)**

```bash
# Start demo in background
make demo &
DEMO_PID=$!
sleep 30  # wait for kind cluster bootstrap (~20-30s)

# Verify demo-ctl is serving
curl -s http://localhost:9090/ | grep -q "K8s Readiness Demo" && echo "UI: OK"

# Verify SSE stream connects
curl -s --max-time 2 http://localhost:9090/api/stream | head -1 | grep -q "data:" && echo "SSE: OK"

# Spawn a pod via API
curl -s -X POST http://localhost:9090/api/pod/spawn | python3 -m json.tool

# Wait for pod to be ready
sleep 15

# Start traffic
curl -s -X POST http://localhost:9090/api/traffic/start && echo "traffic started"
sleep 5

# Confirm SSE emits request events
curl -s --max-time 3 http://localhost:9090/api/stream | grep '"type":"request"' | head -2

# Clean up
kill $DEMO_PID
make clean
```

Expected: all checks pass. The SSE stream emits `{"type":"request",...}` events with a `podName` field.

- [ ] **Step 6: Commit**

```bash
git add Makefile bin/.gitkeep .gitignore
git commit -m "feat: Makefile with make demo / build / clean targets"
```

---

## Self-review notes

**Spec coverage:**
- demo-server endpoints: ✓ Tasks 3–4
- Graceful SIGTERM drain: ✓ Task 3 (`shutdown()` method)
- kind cluster lifecycle: ✓ Task 5
- Pod readiness probe (2s period, failureThreshold 1): ✓ Task 6
- terminationGracePeriodSeconds 30: ✓ Task 6
- k8s API server pod proxy for per-pod admin: ✓ Task 6 (`podProxy`)
- EndpointSlice watcher + SignalEvent: ✓ Task 6
- Load generator with RPS + duration control: ✓ Task 7
- SSE event types (all 6): ✓ Task 1 + Task 7 + Task 6
- HTTP API all endpoints: ✓ Task 8
- Svelte store with SSE: ✓ Task 10
- Pod controls in sidebar: ✓ Task 11
- SVG map with dots: ✓ Task 12
- Dot colors (add/del/warn): ✓ Task 12
- Pod node states (ready/notready/terminating): ✓ Task 12
- make demo: ✓ Task 13

**Known implementation note:** In `loadgen.go`, `InFlightDec` is called after response returns. `InFlightInc` should be called before the request is sent in `doRequest()`. Add `k.k8s.InFlightInc("unknown")` before the HTTP call — the actual pod name won't be known until the response comes back, so the per-pod in-flight count in `PodEvent` will lag by one request latency. This is acceptable for the demo.
