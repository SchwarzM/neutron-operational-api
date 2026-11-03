# Cluster-Hosted Multi-Tenant Demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform demo-ctl from a single-session local binary into a cluster-hosted multi-tenant control plane where each browser visitor gets an isolated session namespace with demo-server pods and a loadgen pod.

**Architecture:** A `demo-system` namespace hosts a single always-on `demo-ctl` pod that serves the Svelte UI and HTTP API. Each session gets a `demo-{id}` namespace containing a demo-server Deployment, demo-svc Service, loadgen Deployment, and loadgen-svc Service. The loadgen pod POSTs per-request events to the control plane; the control plane multiplexes them to per-session SSE streams. HTTP routing is path-based: `/session/{id}/api/...`.

**Tech Stack:** Go 1.27, Kubernetes client-go (fake client for tests), Svelte/Vite (frontend), nginx ingress class

**Spec:** `docs/superpowers/specs/2026-11-03-cluster-hosted-demo-design.md`

## Global Constraints

- Go module: `github.com/schwarzm/neutron-operational-api` (check `go.mod` for exact value)
- Image: `ghcr.io/schwarzm/neutron-operational-api:edge` (both demo-server and loadgen entrypoints)
- Ingress class: `nginx`
- Session namespace format: `demo-{8-hex-id}`
- Control plane namespace: `demo-system`
- Session TTL: 30 minutes of inactivity; reaper runs every 5 minutes
- Session cookie name: `demo-session`, HttpOnly, SameSite=Lax, MaxAge=86400
- Loadgen control port: 8080 (same port as demo-server, different pods)
- All new Go files in `package main` under `cmd/demo-ctl/` or `cmd/loadgen/`
- Run `go test ./...` after each task to keep the build green

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `cmd/loadgen/main.go` | Create | Loadgen pod binary: sends traffic, POSTs events to control plane |
| `cmd/loadgen/main_test.go` | Create | Tests for loadgen request loop and control API |
| `cmd/demo-ctl/session.go` | Create | `createSessionNamespace` / `destroySession` — k8s objects per session |
| `cmd/demo-ctl/session_test.go` | Create | Tests for namespace creation / deletion |
| `cmd/demo-ctl/session_manager.go` | Create | `Session` struct, `SessionManager`, TTL reaper, cookie helpers |
| `cmd/demo-ctl/session_manager_test.go` | Create | Tests for create/get/destroy/reap |
| `cmd/demo-ctl/testhelpers_test.go` | Create | `capPublisher` shared across all `_test.go` files |
| `cmd/demo-ctl/cluster.go` | Delete | Replaced by `session.go` |
| `cmd/demo-ctl/cluster_test.go` | Delete | Replaced by `session_test.go` |
| `cmd/demo-ctl/loadgen.go` | Delete | Moved to `cmd/loadgen` |
| `cmd/demo-ctl/loadgen_test.go` | Delete | Moved to `cmd/loadgen/main_test.go` |
| `cmd/demo-ctl/apply.go` | Delete | Dead code since remote-cluster commit |
| `cmd/demo-ctl/apply_test.go` | Delete | Dead code since remote-cluster commit |
| `cmd/demo-ctl/k8s.go` | Modify | Add `namespace` field + `inFlightFn`; remove `InFlight*` methods |
| `cmd/demo-ctl/k8s_test.go` | Modify | Update `NewK8s` calls; replace `demoNamespace` literal |
| `cmd/demo-ctl/server.go` | Modify | Session-prefixed routing, internal event endpoint, loadgen HTTP proxy |
| `cmd/demo-ctl/main.go` | Modify | Wire `SessionManager`; remove single-session bootstrap; add `/healthz` |
| `web/src/store.js` | Modify | Session-prefixed API and SSE paths; redirect on destroy |
| `Dockerfile` | Modify | Multi-stage: node build → go build all 3 binaries |
| `Makefile.maker.yaml` | Modify | Add `loadgen` binary entry |
| `.goreleaser.yaml` | Modify | Add `loadgen` build |
| `deploy/demo-system.yaml` | Create | k8s manifests: Namespace, RBAC, Deployment, Service, Ingress |

---

## Task 1: loadgen binary (`cmd/loadgen`)

New standalone binary that runs as a pod in each session namespace. Sends HTTP traffic to `demo-svc`, POSTs `RequestEvent` and `StatsEvent` JSON to the control plane's internal endpoint. Exposes a tiny HTTP control API.

**Files:**
- Create: `cmd/loadgen/main.go`
- Create: `cmd/loadgen/main_test.go`

**Interfaces:**
- Produces: `POST /start`, `POST /stop`, `PUT /rps {"rps":N}`, `PUT /duration {"ms":N}` HTTP handlers on `:8080`
- Produces: POSTs to `$CONTROL_PLANE_URL/session/$SESSION_ID/internal/event` with JSON bodies matching `RequestEvent` and `StatsEvent` shapes from `cmd/demo-ctl/events.go`
- Env vars required: `TARGET_URL`, `SESSION_ID`, `CONTROL_PLANE_URL`

- [ ] **Step 1: Create `cmd/loadgen/main_test.go` with failing tests**

```go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadgenSendsRequestsToTarget(t *testing.T) {
	var hits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"pod": "test-pod"}) //nolint:errcheck
	}))
	defer target.Close()

	var mu sync.Mutex
	var received []map[string]any
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev map[string]any
		if json.NewDecoder(r.Body).Decode(&ev) == nil {
			mu.Lock()
			received = append(received, ev)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cp.Close()

	lg := &loadgen{targetURL: target.URL, sessionID: "test123", controlPlaneURL: cp.URL}
	lg.rps.Store(10)
	lg.Start()
	time.Sleep(400 * time.Millisecond)
	lg.Stop()

	if hits.Load() < 3 {
		t.Fatalf("expected >=3 hits at 10rps/400ms, got %d", hits.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, ev := range received {
		if ev["type"] == "request" && ev["podName"] == "test-pod" {
			return
		}
	}
	t.Fatal("no request event with podName=test-pod received by control plane")
}

func TestLoadgenControlAPIClampsRPS(t *testing.T) {
	lg := &loadgen{}
	lg.SetRPS(200)
	if lg.rps.Load() != maxRPS {
		t.Errorf("rps = %d, want %d (maxRPS)", lg.rps.Load(), maxRPS)
	}
	lg.SetRPS(0)
	if lg.rps.Load() != minRPS {
		t.Errorf("rps = %d, want %d (minRPS)", lg.rps.Load(), minRPS)
	}
}

func TestLoadgenSetDurationClampsNegative(t *testing.T) {
	lg := &loadgen{}
	lg.SetDurationMs(-100)
	if lg.durationMs.Load() != 0 {
		t.Errorf("durationMs = %d, want 0", lg.durationMs.Load())
	}
}

func TestLoadgenStartStopIdempotent(t *testing.T) {
	lg := &loadgen{targetURL: "http://127.0.0.1:0", sessionID: "x", controlPlaneURL: "http://127.0.0.1:0"}
	lg.rps.Store(1)
	lg.Start()
	lg.Start() // second Start must not panic or create extra goroutines
	lg.Stop()
	lg.Stop() // second Stop must not panic
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```
cd cmd/loadgen && go test ./... 2>&1 | head -20
```

Expected: `cannot find package` or `undefined: loadgen`

- [ ] **Step 3: Create `cmd/loadgen/main.go`**

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxRPS = 50
	minRPS = 1
)

type loadgen struct {
	targetURL       string
	sessionID       string
	controlPlaneURL string

	rps        atomic.Int64
	durationMs atomic.Int64
	reqID      atomic.Int64
	inFlight   atomic.Int64

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool

	mu2     sync.Mutex
	samples []lgSample
}

type lgSample struct {
	ts         time.Time
	durationMs int64
	success    bool
}

// These JSON shapes match cmd/demo-ctl/events.go exactly.
type requestEvent struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	PodName    string `json:"podName"`
	Status     int    `json:"status"`
	DurationMs int64  `json:"durationMs"`
	Slow       bool   `json:"slow"`
}

type statsEvent struct {
	Type        string  `json:"type"`
	RPS         float64 `json:"rps"`
	SuccessRate float64 `json:"successRate"`
	P50Ms       int64   `json:"p50Ms"`
	P95Ms       int64   `json:"p95Ms"`
	InFlight    int     `json:"inFlight"`
}

func (lg *loadgen) postEvent(v any) {
	body, err := json.Marshal(v)
	if err != nil {
		return
	}
	url := lg.controlPlaneURL + "/session/" + lg.sessionID + "/internal/event"
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		log.Printf("post event: %v", err)
		return
	}
	resp.Body.Close()
}

func (lg *loadgen) SetRPS(n int) {
	if n < minRPS {
		n = minRPS
	}
	if n > maxRPS {
		n = maxRPS
	}
	lg.rps.Store(int64(n))
}

func (lg *loadgen) SetDurationMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	lg.durationMs.Store(ms)
}

func (lg *loadgen) Start() {
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

func (lg *loadgen) Stop() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.running {
		return
	}
	lg.cancel()
	lg.running = false
}

func (lg *loadgen) run(ctx context.Context) {
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

func (lg *loadgen) doRequest(ctx context.Context) {
	id := fmt.Sprintf("r-%d", lg.reqID.Add(1))
	slow := lg.durationMs.Load() > 0
	url := lg.targetURL
	if d := lg.durationMs.Load(); d > 0 {
		url = fmt.Sprintf("%s?wait=%d", url, d)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return
	}
	req.Header.Set("X-Request-ID", id)

	lg.inFlight.Add(1)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dur := time.Since(start).Milliseconds()
	lg.inFlight.Add(-1)

	status := 0
	podName := ""
	if err == nil {
		status = resp.StatusCode
		if resp.Body != nil {
			var body struct {
				Pod string `json:"pod"`
			}
			data, rerr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if rerr == nil {
				json.Unmarshal(data, &body) //nolint:errcheck
				podName = body.Pod
			}
		}
	}

	if status == 0 || status >= 400 {
		log.Printf("request %s: status=%d err=%v dur=%dms", id, status, err, dur)
	}

	lg.postEvent(requestEvent{
		Type: "request", ID: id, PodName: podName,
		Status: status, DurationMs: dur, Slow: slow,
	})

	lg.mu2.Lock()
	lg.samples = append(lg.samples, lgSample{
		ts: time.Now(), durationMs: dur,
		success: status >= 200 && status < 300,
	})
	cutoff := time.Now().Add(-60 * time.Second)
	i := 0
	for i < len(lg.samples) && lg.samples[i].ts.Before(cutoff) {
		i++
	}
	lg.samples = lg.samples[i:]
	lg.mu2.Unlock()
}

func (lg *loadgen) statsLoop(ctx context.Context) {
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

func (lg *loadgen) publishStats() {
	lg.mu2.Lock()
	window := time.Now().Add(-5 * time.Second)
	var recent []lgSample
	for _, s := range lg.samples {
		if s.ts.After(window) {
			recent = append(recent, s)
		}
	}
	lg.mu2.Unlock()

	if len(recent) == 0 {
		lg.postEvent(statsEvent{Type: "stats", InFlight: int(lg.inFlight.Load())})
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
	slices.Sort(durations)

	lg.postEvent(statsEvent{
		Type:        "stats",
		RPS:         float64(len(recent)) / 5.0,
		SuccessRate: float64(success) / float64(len(recent)),
		P50Ms:       durations[len(durations)*50/100],
		P95Ms:       durations[len(durations)*95/100],
		InFlight:    int(lg.inFlight.Load()),
	})
}

func newHTTPServer(lg *loadgen) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, _ *http.Request) {
		lg.Start()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		lg.Stop()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("PUT /rps", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RPS int `json:"rps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lg.SetRPS(body.RPS)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("PUT /duration", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ms int64 `json:"ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lg.SetDurationMs(body.Ms)
		w.WriteHeader(http.StatusOK)
	})
	return &http.Server{Addr: ":8080", Handler: mux}
}

func main() {
	targetURL := os.Getenv("TARGET_URL")
	if targetURL == "" {
		log.Fatal("TARGET_URL env var required")
	}
	sessionID := os.Getenv("SESSION_ID")
	if sessionID == "" {
		log.Fatal("SESSION_ID env var required")
	}
	controlPlaneURL := os.Getenv("CONTROL_PLANE_URL")
	if controlPlaneURL == "" {
		log.Fatal("CONTROL_PLANE_URL env var required")
	}

	lg := &loadgen{targetURL: targetURL, sessionID: sessionID, controlPlaneURL: controlPlaneURL}
	lg.rps.Store(5)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := newHTTPServer(lg)
	go func() {
		<-ctx.Done()
		lg.Stop()
		srv.Shutdown(context.Background()) //nolint:errcheck
	}()

	log.Printf("loadgen: listening :8080 target=%s session=%s", targetURL, sessionID)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
```

- [ ] **Step 4: Run tests — expect PASS**

```
cd cmd/loadgen && go test ./... -v -count=1
```

Expected: all 4 tests pass

- [ ] **Step 5: Verify full build still compiles**

```
go build ./...
```

- [ ] **Step 6: Commit**

```
git add cmd/loadgen/
git commit -m "feat: add loadgen pod binary"
```

---

## Task 2: Replace `cluster.go` with `session.go`

Delete the `Cluster` struct and its namespace/ingress lifecycle. Create `session.go` with functions that create all k8s objects for a session namespace: demo-server Deployment + demo-svc Service + loadgen Deployment + loadgen-svc Service. Also delete dead `apply.go`/`apply_test.go`.

**Files:**
- Create: `cmd/demo-ctl/session.go`
- Create: `cmd/demo-ctl/session_test.go`
- Delete: `cmd/demo-ctl/cluster.go`
- Delete: `cmd/demo-ctl/cluster_test.go`
- Delete: `cmd/demo-ctl/apply.go`
- Delete: `cmd/demo-ctl/apply_test.go`

**Interfaces:**
- Produces: `sessionNamespace(id string) string` — returns `"demo-" + id`
- Produces: `createSessionNamespace(ctx, cs, id, controlPlaneURL string) error`
- Produces: `destroySession(ctx, cs, id string) error`
- Consumes: `terminationGracePeriod` constant from `k8s.go` (stays there)
- Consumes: `ghcrImage` constant defined in `session.go`

**Note on `demoNamespace`:** this constant is currently in `cluster.go` and referenced by `k8s.go` and `k8s_test.go`. After this task, `k8s.go` still compiles because Task 3 migrates it to a struct field — but for the build to stay green between tasks, keep `demoNamespace` temporarily in `session.go` (it will be removed in Task 3 when `k8s.go` is updated).

- [ ] **Step 1: Write `cmd/demo-ctl/session_test.go`**

```go
package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateSessionNamespace_CreatesAllObjects(t *testing.T) {
	cs := fake.NewSimpleClientset()
	if err := createSessionNamespace(context.Background(), cs, "abc123", "http://cp:9090"); err != nil {
		t.Fatalf("createSessionNamespace: %v", err)
	}

	ns := sessionNamespace("abc123")

	// namespace exists
	if _, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{}); err != nil {
		t.Errorf("namespace %q not found: %v", ns, err)
	}
	// demo-svc exists
	if _, err := cs.CoreV1().Services(ns).Get(context.Background(), "demo-svc", metav1.GetOptions{}); err != nil {
		t.Errorf("demo-svc not found: %v", err)
	}
	// demo deployment exists with 2 replicas
	dep, err := cs.AppsV1().Deployments(ns).Get(context.Background(), "demo", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("demo deployment not found: %v", err)
	}
	if *dep.Spec.Replicas != 2 {
		t.Errorf("demo replicas = %d, want 2", *dep.Spec.Replicas)
	}
	// POD_NAME downward API env var
	var podNameEnv *corev1.EnvVar
	for i := range dep.Spec.Template.Spec.Containers[0].Env {
		if dep.Spec.Template.Spec.Containers[0].Env[i].Name == "POD_NAME" {
			podNameEnv = &dep.Spec.Template.Spec.Containers[0].Env[i]
		}
	}
	if podNameEnv == nil || podNameEnv.ValueFrom == nil || podNameEnv.ValueFrom.FieldRef == nil {
		t.Fatal("POD_NAME downward API env var not configured correctly")
	}
	// loadgen-svc exists
	if _, err := cs.CoreV1().Services(ns).Get(context.Background(), "loadgen-svc", metav1.GetOptions{}); err != nil {
		t.Errorf("loadgen-svc not found: %v", err)
	}
	// loadgen deployment exists with correct env vars
	lgDep, err := cs.AppsV1().Deployments(ns).Get(context.Background(), "loadgen", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("loadgen deployment not found: %v", err)
	}
	envMap := map[string]string{}
	for _, e := range lgDep.Spec.Template.Spec.Containers[0].Env {
		envMap[e.Name] = e.Value
	}
	if envMap["SESSION_ID"] != "abc123" {
		t.Errorf("SESSION_ID = %q, want %q", envMap["SESSION_ID"], "abc123")
	}
	if envMap["CONTROL_PLANE_URL"] != "http://cp:9090" {
		t.Errorf("CONTROL_PLANE_URL = %q, want %q", envMap["CONTROL_PLANE_URL"], "http://cp:9090")
	}
}

func TestCreateSessionNamespace_Idempotent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	if err := createSessionNamespace(context.Background(), cs, "abc123", "http://cp"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := createSessionNamespace(context.Background(), cs, "abc123", "http://cp"); err != nil {
		t.Errorf("second call (idempotent) failed: %v", err)
	}
}

func TestDestroySession_DeletesNamespace(t *testing.T) {
	cs := fake.NewSimpleClientset()
	if err := createSessionNamespace(context.Background(), cs, "abc123", "http://cp"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := destroySession(context.Background(), cs, "abc123"); err != nil {
		t.Fatalf("destroySession: %v", err)
	}
	if _, err := cs.CoreV1().Namespaces().Get(
		context.Background(), sessionNamespace("abc123"), metav1.GetOptions{},
	); err == nil {
		t.Error("namespace still exists after destroy")
	}
}

func TestDestroySession_NotFoundIsNoop(t *testing.T) {
	cs := fake.NewSimpleClientset()
	if err := destroySession(context.Background(), cs, "nosuchid"); err != nil {
		t.Errorf("expected no error for missing session, got: %v", err)
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure** (cluster.go still exists, new functions don't yet)

```
go test ./cmd/demo-ctl/... 2>&1 | head -20
```

- [ ] **Step 3: Create `cmd/demo-ctl/session.go`**

```go
package main

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	ghcrImage = "ghcr.io/schwarzm/neutron-operational-api:edge"

	// demoNamespace is kept temporarily so k8s.go still compiles before Task 3
	// migrates it to a struct field. Remove when Task 3 is complete.
	demoNamespace = "demo"
)

func sessionNamespace(id string) string { return "demo-" + id }

// createSessionNamespace creates the demo-{id} namespace and all session-scoped
// k8s objects: demo-server Deployment, demo-svc, loadgen Deployment, loadgen-svc.
func createSessionNamespace(ctx context.Context, cs kubernetes.Interface, id, controlPlaneURL string) error {
	ns := sessionNamespace(id)
	if err := createNs(ctx, cs, ns); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	if err := createDemoSvc(ctx, cs, ns); err != nil {
		return fmt.Errorf("demo-svc: %w", err)
	}
	if err := createDemoDeployment(ctx, cs, ns); err != nil {
		return fmt.Errorf("demo deployment: %w", err)
	}
	if err := createLoadgenSvc(ctx, cs, ns); err != nil {
		return fmt.Errorf("loadgen-svc: %w", err)
	}
	if err := createLoadgenDeployment(ctx, cs, ns, id, controlPlaneURL); err != nil {
		return fmt.Errorf("loadgen deployment: %w", err)
	}
	return nil
}

// destroySession deletes the session namespace and everything in it.
func destroySession(ctx context.Context, cs kubernetes.Interface, id string) error {
	err := cs.CoreV1().Namespaces().Delete(ctx, sessionNamespace(id), metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

func createNs(ctx context.Context, cs kubernetes.Interface, ns string) error {
	_, err := cs.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createDemoSvc(ctx context.Context, cs kubernetes.Interface, ns string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-svc", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{{
				Port: 80, TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createDemoDeployment(ctx context.Context, cs kubernetes.Interface, ns string) error {
	replicas := int32(2)
	grace := int64(terminationGracePeriod)
	maxU := intstr.FromInt32(0)
	maxS := intstr.FromInt32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &maxU, MaxSurge: &maxS,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &grace,
					Containers: []corev1.Container{{
						Name:            "demo-server",
						Image:           ghcrImage,
						ImagePullPolicy: corev1.PullAlways,
						Env: []corev1.EnvVar{{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							},
						}},
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz", Port: intstr.FromInt32(8080),
								},
							},
							PeriodSeconds: 2, FailureThreshold: 1, SuccessThreshold: 1,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/livez", Port: intstr.FromInt32(8080),
								},
							},
							PeriodSeconds: 10, FailureThreshold: 3,
						},
					}},
				},
			},
		},
	}
	_, err := cs.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createLoadgenSvc(ctx context.Context, cs kubernetes.Interface, ns string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "loadgen-svc", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "loadgen"},
			Ports: []corev1.ServicePort{{
				Port: 8080, TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createLoadgenDeployment(ctx context.Context, cs kubernetes.Interface, ns, id, controlPlaneURL string) error {
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "loadgen", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "loadgen"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "loadgen"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            "loadgen",
						Image:           ghcrImage,
						ImagePullPolicy: corev1.PullAlways,
						Command:         []string{"/loadgen"},
						Env: []corev1.EnvVar{
							{Name: "TARGET_URL", Value: "http://demo-svc." + ns + ".svc.cluster.local/"},
							{Name: "SESSION_ID", Value: id},
							{Name: "CONTROL_PLANE_URL", Value: controlPlaneURL},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
					}},
				},
			},
		},
	}
	_, err := cs.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
```

- [ ] **Step 4: Delete `cluster.go`, `cluster_test.go`, `apply.go`, `apply_test.go`**

```
rm cmd/demo-ctl/cluster.go cmd/demo-ctl/cluster_test.go
rm cmd/demo-ctl/apply.go cmd/demo-ctl/apply_test.go
```

- [ ] **Step 5: Run tests — expect PASS**

```
go test ./cmd/demo-ctl/... -v -count=1 -run TestCreate
go test ./cmd/demo-ctl/... -v -count=1 -run TestDestroy
```

- [ ] **Step 6: Full build still compiles**

```
go build ./...
```

- [ ] **Step 7: Commit**

```
git add cmd/demo-ctl/session.go cmd/demo-ctl/session_test.go
git rm cmd/demo-ctl/cluster.go cmd/demo-ctl/cluster_test.go
git rm cmd/demo-ctl/apply.go cmd/demo-ctl/apply_test.go
git commit -m "feat: replace Cluster with per-session namespace management"
```

---

## Task 3: K8s struct — namespace field, remove InFlight

Change `K8s` to take `namespace string` and `inFlightFn func() int` at construction instead of using the hardcoded `demoNamespace` constant. Remove `InFlightInc`, `InFlightDec`, `TotalInFlight`. Delete `loadgen.go` and `loadgen_test.go` (moved to `cmd/loadgen`). Create `testhelpers_test.go` with `capPublisher` so all remaining tests compile.

**Files:**
- Modify: `cmd/demo-ctl/k8s.go`
- Modify: `cmd/demo-ctl/k8s_test.go`
- Create: `cmd/demo-ctl/testhelpers_test.go`
- Delete: `cmd/demo-ctl/loadgen.go`
- Delete: `cmd/demo-ctl/loadgen_test.go`
- Remove: `demoNamespace` constant from `session.go` (added as temp shim in Task 2)

**Interfaces:**
- Produces: `NewK8s(pub Publisher, cs kubernetes.Interface, rcfg *rest.Config, namespace string, inFlightFn func() int) *K8s`
- Consumes: `inFlightFn func() int` — called by `WatchPods` when publishing terminating pod events

- [ ] **Step 1: Create `cmd/demo-ctl/testhelpers_test.go`**

```go
package main

// capPublisher captures published events for test assertions.
type capPublisher struct {
	events []any
}

func (c *capPublisher) Publish(v any) { c.events = append(c.events, v) }
```

- [ ] **Step 2: Update `cmd/demo-ctl/k8s.go`**

Replace the `K8s` struct and `NewK8s`. Make these exact changes:

**Remove** the `inFlight map[string]int` field and the three methods `InFlightInc`, `InFlightDec`, `TotalInFlight`.

**Add** `namespace string` and `inFlightFn func() int` fields.

**Change** `NewK8s` signature to:
```go
func NewK8s(pub Publisher, cs kubernetes.Interface, rcfg *rest.Config, namespace string, inFlightFn func() int) *K8s {
	return &K8s{
		pub:        pub,
		cs:         cs,
		rcfg:       rcfg,
		namespace:  namespace,
		inFlightFn: inFlightFn,
		podSpecs:   make(map[string]*corev1.Pod),
	}
}
```

**Replace** every occurrence of `demoNamespace` in `k8s.go` with `k.namespace`.

**Replace** the two occurrences of `k.TotalInFlight()` in `WatchPods` with `k.inFlightFn()`.

The `inFlight` map is no longer needed in `WatchPods` — remove the `inFlight := k.getInFlight(pod.Name)` line and the `if terminating { inFlight = k.TotalInFlight() }` block; replace both with:
```go
inFlight := 0
if terminating {
    inFlight = k.inFlightFn()
}
```

Also remove the `k.mu.Lock()` / `delete(k.inFlight, ...)` / `k.mu.Unlock()` block in the `watch.Deleted` branch (only the `podSpecs` map cleanup remains).

- [ ] **Step 3: Update `cmd/demo-ctl/k8s_test.go`**

Replace `NewK8s(pub, cs, nil)` with `NewK8s(pub, cs, nil, "demo-test", func() int { return 0 })` in all three test functions.

Replace `demoNamespace` with `"demo-test"` in all test assertions.

- [ ] **Step 4: Delete `loadgen.go` and `loadgen_test.go`**

```
rm cmd/demo-ctl/loadgen.go cmd/demo-ctl/loadgen_test.go
```

- [ ] **Step 5: Remove temporary `demoNamespace` shim from `session.go`**

Delete the line `demoNamespace = "demo"` from the `const` block in `session.go`.

- [ ] **Step 6: Run all tests — expect PASS**

```
go test ./cmd/demo-ctl/... -v -count=1
```

Expected: `TestWatchPods*`, `TestStrategyForPreset`, `TestWatchDeployment*`, `TestCreateSession*`, `TestDestroySession*` all pass.

- [ ] **Step 7: Commit**

```
git add cmd/demo-ctl/k8s.go cmd/demo-ctl/k8s_test.go
git add cmd/demo-ctl/testhelpers_test.go cmd/demo-ctl/session.go
git rm cmd/demo-ctl/loadgen.go cmd/demo-ctl/loadgen_test.go
git commit -m "refactor: K8s namespace as field, remove in-flight tracking, delete loadgen"
```

---

## Task 4: `SessionManager` and `Session` structs

Introduce `Session` (hub + k8s + cancel + inFlight atomic) and `SessionManager` (map of sessions, Create/Get/Destroy, TTL reaper). The control plane URL is passed to `Create` so it can set the loadgen's env var.

**Files:**
- Create: `cmd/demo-ctl/session_manager.go`
- Create: `cmd/demo-ctl/session_manager_test.go`

**Interfaces:**
- Produces: `type Session struct { ID string; hub *Hub; k8s *K8s; cancel context.CancelFunc; inFlight *atomic.Int64 }`
- Produces: `NewSessionManager(cs, rcfg, controlPlaneURL string) *SessionManager`
- Produces: `(sm *SessionManager) Create(ctx) (*Session, error)`
- Produces: `(sm *SessionManager) Get(id) (*Session, bool)`
- Produces: `(sm *SessionManager) Touch(id)`
- Produces: `(sm *SessionManager) Destroy(ctx, id) error`

- [ ] **Step 1: Write `cmd/demo-ctl/session_manager_test.go`**

```go
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
	sess, _ := sm.Create(context.Background())

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
	id2, _ := generateSessionID()
	if len(id1) != 8 {
		t.Errorf("id length = %d, want 8", len(id1))
	}
	if id1 == id2 {
		t.Error("two IDs are equal — not random")
	}
}

func TestSessionReaperExpiresStaleSessions(t *testing.T) {
	sm := newTestSessionManager(t)
	sess, _ := sm.Create(context.Background())

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
		rcfg:            nil,
		controlPlaneURL: "http://localhost:9090",
	}
}
```

- [ ] **Step 2: Run — expect compile failure** (SessionManager not defined yet)

```
go test ./cmd/demo-ctl/... 2>&1 | head -10
```

- [ ] **Step 3: Create `cmd/demo-ctl/session_manager.go`**

```go
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
```

- [ ] **Step 4: Run tests — expect PASS**

```
go test ./cmd/demo-ctl/... -v -count=1 -run TestSession
go test ./cmd/demo-ctl/... -v -count=1 -run TestGenerate
```

- [ ] **Step 5: Commit**

```
git add cmd/demo-ctl/session_manager.go cmd/demo-ctl/session_manager_test.go
git commit -m "feat: add SessionManager with TTL reaper"
```

---

## Task 5: Server routing overhaul

Replace the single-session `APIServer` with a session-multiplexed server. All routes gain a `/session/{id}/` prefix. Add `handleRoot` (cookie check + redirect), `handleSessionUI` (serve Svelte index.html), `handleStream` (session-scoped SSE), `handleInternalEvent` (loadgen callback), and a `loadgenCall` helper that proxies traffic control to the loadgen ClusterIP service. Delete the `LoadGen` and `Cluster` references.

**Files:**
- Modify: `cmd/demo-ctl/server.go`

**Interfaces:**
- Consumes: `SessionManager.Get`, `SessionManager.Touch`, `SessionManager.Create`, `SessionManager.Destroy`
- Consumes: `Session.hub`, `Session.k8s`, `Session.inFlight`
- Produces: HTTP handler via `(s *APIServer) Handler() http.Handler`
- Produces: `NewAPIServer(sessions *SessionManager) *APIServer`

- [ ] **Step 1: Rewrite `cmd/demo-ctl/server.go`**

Replace the entire file content:

```go
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

	// Loadgen event callback (in-cluster only — not exposed via ingress path rules)
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
```

- [ ] **Step 2: Verify build**

```
go build ./cmd/demo-ctl/
```

Expected: no errors. (Tests may not compile yet if `main.go` still references the old `APIServer` constructor — that's fine, it will be fixed in Task 6.)

- [ ] **Step 3: Commit**

```
git add cmd/demo-ctl/server.go
git commit -m "feat: session-multiplexed HTTP routing with loadgen proxy"
```

---

## Task 6: Wire `main.go`

Replace the single-session bootstrap with `SessionManager` construction. Read `CONTROL_PLANE_URL` from env (default `http://localhost:9090` for local dev). Remove the old `openBrowser` call. The HTTP server stays on `:9090`.

**Files:**
- Modify: `cmd/demo-ctl/main.go`

**Interfaces:**
- Consumes: `NewSessionManager(cs, rcfg, controlPlaneURL)`, `NewAPIServer(sessions)`
- Consumes: `buildConfig()` — already in `k8s.go` or move here; check its current location after Task 3

**Note:** `buildConfig()` was in `cluster.go` (now deleted). It needs to move to `main.go` or a separate file. Add it to `main.go`.

- [ ] **Step 1: Rewrite `cmd/demo-ctl/main.go`**

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	rcfg, err := buildConfig()
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rcfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	controlPlaneURL := os.Getenv("CONTROL_PLANE_URL")
	if controlPlaneURL == "" {
		controlPlaneURL = "http://localhost:9090"
	}

	sessions := NewSessionManager(cs, rcfg, controlPlaneURL)
	srv := NewAPIServer(sessions)

	httpSrv := &http.Server{
		Addr:    ":9090",
		Handler: srv.Handler(),
	}

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		httpSrv.Shutdown(context.Background()) //nolint:errcheck
	}()

	log.Println("demo-ctl: listening on http://localhost:9090")
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// buildConfig returns an in-cluster config when running inside a pod,
// otherwise falls back to KUBECONFIG env / ~/.kube/config.
func buildConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}
```

- [ ] **Step 2: Run all tests and build**

```
go build ./...
go test ./...
```

Expected: all tests pass, binary builds.

- [ ] **Step 3: Commit**

```
git add cmd/demo-ctl/main.go
git commit -m "feat: wire SessionManager into main, add CONTROL_PLANE_URL env var"
```

---

## Task 7: Frontend — session-prefixed API paths

The Svelte store must extract the session ID from the URL path and prefix all API calls and the SSE URL with `/session/{id}`. The `destroyCluster` action must redirect to `/` after the API call so the browser leaves the expired session rather than retrying SSE.

**Files:**
- Modify: `web/src/store.js`

- [ ] **Step 1: Update `web/src/store.js`**

Replace the `connectSSE` function and the `api` export. The `const base` declaration must be at module scope so both `connectSSE` and `api` can use it.

Find the line:
```js
let es = null
```

Add above it:
```js
// Extract session ID from /session/{id}/... URL pattern.
const _parts = window.location.pathname.split('/')
const _sessionId = _parts[2] ?? ''
const base = _sessionId ? `/session/${_sessionId}` : ''
```

Replace the `connectSSE` function's SSE URL line:
```js
  es = new EventSource('/api/stream')
```
with:
```js
  es = new EventSource(`${base}/api/stream`)
```

Replace the entire `export const api = { ... }` block with:
```js
export const api = {
  setReplicas:    (n)   => apiPut(`${base}/api/deployment/replicas`, { replicas: n }),
  setStrategy:    (s)   => apiPut(`${base}/api/deployment/strategy`, { strategy: s }),
  setGrace:       (sec) => apiPut(`${base}/api/deployment/grace`,    { seconds: sec }),
  setPreStop:     (sec) => apiPut(`${base}/api/deployment/prestop`,  { seconds: sec }),
  triggerRollout: ()    => apiPost(`${base}/api/deployment/rollout`),
  setPodReady:    (name, ready) => apiPut(`${base}/api/pod/${name}/ready`,  { ready }),
  setPodDelay:    (name, ms)    => apiPut(`${base}/api/pod/${name}/delay`,  { ms }),
  startTraffic:   ()    => apiPost(`${base}/api/traffic/start`),
  stopTraffic:    ()    => apiPost(`${base}/api/traffic/stop`),
  setRPS:         (rps) => apiPut(`${base}/api/traffic/rps`,         { rps }),
  setDuration:    (ms)  => apiPut(`${base}/api/traffic/duration`,    { ms }),
  destroyCluster: async () => {
    await apiPost(`${base}/api/cluster/destroy`)
    window.location.href = '/'
  },
}
```

- [ ] **Step 2: Build the frontend**

```
cd web && npm run build
```

Expected: builds without errors; output written to `../cmd/demo-ctl/assets/`.

- [ ] **Step 3: Verify Go build still embeds correctly**

```
cd .. && go build ./cmd/demo-ctl/
```

- [ ] **Step 4: Commit**

```
git add web/src/store.js cmd/demo-ctl/assets/
git commit -m "feat: session-prefix all frontend API and SSE paths"
```

---

## Task 8: Dockerfile, build config, and k8s manifests

Update the Dockerfile for a three-binary multi-stage build (node → go → final). Add `loadgen` to `Makefile.maker.yaml` and `.goreleaser.yaml`. Create `deploy/demo-system.yaml` with all cluster resources for the control plane.

**Files:**
- Modify: `Dockerfile`
- Modify: `Makefile.maker.yaml`
- Modify: `.goreleaser.yaml`
- Create: `deploy/demo-system.yaml`

- [ ] **Step 1: Update `Dockerfile`**

Replace the entire file:

```dockerfile
# Stage 1: Build Svelte frontend
FROM node:lts-alpine AS web
WORKDIR /web
COPY web/package*.json ./
RUN npm install --prefer-offline
COPY web/ ./
RUN npm run build
# output lands in ../cmd/demo-ctl/assets relative to web/; capture it here
# vite.config.js sets outDir: '../cmd/demo-ctl/assets', so output is at /cmd/demo-ctl/assets
# We copy the web source alongside the Go source in the next stage instead.

# Stage 2: Build Go binaries
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Copy pre-built web assets from Stage 1 into the embed directory
COPY --from=web /web/../cmd/demo-ctl/assets ./cmd/demo-ctl/assets
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /demo-server  ./cmd/demo-server/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /demo-ctl     ./cmd/demo-ctl/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /loadgen      ./cmd/loadgen/

# Stage 3: Minimal runtime image
FROM alpine:3
COPY --from=build /demo-server /demo-server
COPY --from=build /demo-ctl    /demo-ctl
COPY --from=build /loadgen     /loadgen
ENTRYPOINT ["/demo-server"]
```

**Note on the Vite output path:** `vite.config.js` sets `outDir: '../cmd/demo-ctl/assets'` relative to `web/`. In the Docker context (WORKDIR `/web`), Vite writes to `/cmd/demo-ctl/assets`. The `COPY --from=web` line above uses that path. If the build fails with "path not found", check where Vite wrote the output inside the `web` stage with `RUN ls -la /cmd/demo-ctl/assets` and adjust accordingly.

- [ ] **Step 2: Update `Makefile.maker.yaml` — add loadgen binary**

In the `binaries:` list, add after the `demo-server` entry:

```yaml
  - name: loadgen
    fromPackage: ./cmd/loadgen
    installTo: bin/
```

- [ ] **Step 3: Update `.goreleaser.yaml` — add loadgen build**

In the `builds:` list, add a second entry after the `demo-ctl` entry:

```yaml
  - binary: 'loadgen'
    env:
      - CGO_ENABLED=0
    goos:
      - linux
    goarch:
      - amd64
      - arm64
    ldflags:
      - -s -w
    main: ./cmd/loadgen
    mod_timestamp: "{{ .CommitTimestamp }}"
```

- [ ] **Step 4: Create `deploy/demo-system.yaml`**

```yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: demo-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: demo-ctl
  namespace: demo-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: demo-ctl
rules:
  - apiGroups: [""]
    resources: [namespaces]
    verbs: [create, get, list, watch, delete]
  - apiGroups: ["apps"]
    resources: [deployments]
    verbs: [create, get, list, watch, patch, delete]
  - apiGroups: [""]
    resources: [services]
    verbs: [create, get, list, watch, delete]
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources: [pods/proxy]
    verbs: [get, post, put]
  - apiGroups: ["discovery.k8s.io"]
    resources: [endpointslices]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: demo-ctl
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: demo-ctl
subjects:
  - kind: ServiceAccount
    name: demo-ctl
    namespace: demo-system
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo-ctl
  namespace: demo-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: demo-ctl
  template:
    metadata:
      labels:
        app: demo-ctl
    spec:
      serviceAccountName: demo-ctl
      containers:
        - name: demo-ctl
          image: ghcr.io/schwarzm/neutron-operational-api:edge
          command: ["/demo-ctl"]
          env:
            - name: CONTROL_PLANE_URL
              value: "http://demo-ctl-svc.demo-system.svc.cluster.local:9090"
          ports:
            - containerPort: 9090
          readinessProbe:
            httpGet:
              path: /healthz
              port: 9090
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9090
            periodSeconds: 10
            failureThreshold: 3
---
apiVersion: v1
kind: Service
metadata:
  name: demo-ctl-svc
  namespace: demo-system
spec:
  selector:
    app: demo-ctl
  ports:
    - port: 9090
      targetPort: 9090
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: demo-ctl
  namespace: demo-system
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: demo-ctl-svc
                port:
                  number: 9090
```

- [ ] **Step 5: Run full test suite and build**

```
go test ./...
go build ./...
```

- [ ] **Step 6: Commit**

```
git add Dockerfile Makefile.maker.yaml .goreleaser.yaml deploy/demo-system.yaml
git commit -m "feat: multi-binary Dockerfile, loadgen in goreleaser, deploy manifests"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|-----------------|------|
| `demo-system` namespace control plane | Task 8 (deploy/demo-system.yaml) |
| Session auto-create on first visit (cookie) | Task 5 (handleRoot) |
| Session namespace `demo-{id}` | Task 2 (sessionNamespace) |
| demo-server Deployment + demo-svc | Task 2 (createDemoDeployment, createDemoSvc) |
| loadgen Deployment + loadgen-svc | Task 2 (createLoadgenDeployment, createLoadgenSvc) |
| No per-session ingress | Task 2 (confirmed absent) |
| Loadgen POSTs RequestEvent per request | Task 1 (doRequest → postEvent) |
| Loadgen POSTs StatsEvent every second | Task 1 (statsLoop → postEvent) |
| Control plane forwards events to hub | Task 5 (handleInternalEvent) |
| 30-min inactivity TTL + reaper | Task 4 (sessionTTL, reaper goroutine) |
| "End session" button destroys namespace | Task 5 (handleClusterDestroy) + Task 7 (redirect) |
| Path-based routing `/session/{id}/api/...` | Task 5 (Handler mux) |
| `inFlight` updated from loadgen StatsEvent | Task 5 (handleInternalEvent → sess.inFlight.Store) |
| In-cluster RBAC | Task 8 (ClusterRole) |
| `CONTROL_PLANE_URL` env var | Task 6 (main.go) + Task 8 (manifest env) |
| Frontend session-prefixed paths | Task 7 (store.js) |
| Multi-binary Docker image | Task 8 (Dockerfile) |
| `/healthz` endpoint | Task 5 (Handler) |

**Placeholder scan:** None found.

**Type consistency:**
- `NewK8s(pub, cs, rcfg, namespace, inFlightFn)` — defined Task 3, consumed Task 4 ✓
- `createSessionNamespace(ctx, cs, id, controlPlaneURL)` — defined Task 2, consumed Task 4 ✓
- `Session.hub *Hub` — defined Task 4, consumed Task 5 ✓
- `Session.inFlight *atomic.Int64` — defined Task 4, consumed Task 5 ✓
- `SessionManager.Create/Get/Touch/Destroy` — defined Task 4, consumed Task 5+6 ✓
- `sessionCookieName` constant — defined Task 4, consumed Task 5 ✓
- `loadgenCall(ctx, sessionID, method, path, body)` — defined Task 5, self-contained ✓
