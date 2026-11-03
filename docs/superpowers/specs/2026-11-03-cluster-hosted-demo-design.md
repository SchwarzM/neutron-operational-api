# Cluster-Hosted Multi-Tenant Demo

**Date:** 2026-11-03  
**Status:** Draft

## Goal

Move the demo entirely onto a remote Kubernetes cluster. A visitor hits the website, gets an isolated session namespace with demo-server pods and a loadgen pod, and interacts with the demo through the browser — no local binary required. Multiple concurrent sessions (target: ~20) each running up to 40 req/s are fully supported.

---

## Architecture

### Namespaces

```
demo-system           ← always-on control plane
  demo-ctl pod        ← serves UI + API, owns all session state
  demo-ctl-svc        ← ClusterIP
  demo-ingress        ← nginx, routes all traffic to demo-ctl-svc

demo-{8-hex-id}       ← one per live session
  demo-server         ← Deployment (2 replicas, existing ghcr image)
  demo-svc            ← ClusterIP (loadgen → here)
  loadgen             ← Deployment (1 replica, new binary)
  loadgen-svc         ← ClusterIP (control plane → here for control calls)
```

### Traffic flows

**Browser → demo:** browser connects to the cluster ingress, all requests routed to the `demo-ctl` pod in `demo-system`.

**Loadgen → demo-server:** `http://demo-svc.demo-{id}.svc.cluster.local/` — in-cluster ClusterIP, bypasses the ingress entirely. This is the traffic that tests readiness probe behavior.

**Loadgen → control plane (events):** loadgen POSTs one `RequestEvent` per completed request to `http://demo-ctl-svc.demo-system.svc.cluster.local/session/{id}/internal/event`. No batching — every request generates an event; this is what drives the real-time visualization.

**Control plane → loadgen (control):** the control plane calls `http://loadgen-svc.demo-{id}.svc.cluster.local/{start,stop,rps,duration}` directly via ClusterIP service. No k8s pod proxy needed.

**No per-session ingress.** The session namespace has no Ingress resource. The only ingress is in `demo-system`.

---

## Session lifecycle

1. **Creation:** visitor hits `/`. Control plane checks for `demo-session` cookie. If absent (or session not found), creates a new session: generate 8-char hex ID, create namespace, create k8s objects, set `demo-session` cookie (24h expiry), redirect to `/session/{id}/`.
2. **Activity tracking:** `lastSeen` updated on every API request and SSE connection heartbeat.
3. **Inactivity TTL:** reaper goroutine runs every 5 minutes, destroys any session idle for >30 minutes.
4. **Explicit end:** "End session" button calls `POST /session/{id}/api/cluster/destroy`. Control plane deletes the namespace (takes everything with it), removes session from map, closes the Hub (SSE clients disconnect).
5. **Cleanup:** deleting the namespace is sufficient — all pods, services, and the loadgen are owned by it.

---

## HTTP routing

All routes are served by the single `demo-ctl` pod.

| Path | Handler |
|------|---------|
| `GET /` | cookie check → redirect to existing or new session |
| `GET /session/{id}/` | serve Svelte SPA (embedded assets) |
| `GET /session/{id}/assets/*` | static assets |
| `GET /session/{id}/api/stream` | SSE (session hub) |
| `PUT/POST /session/{id}/api/*` | session API (pod/traffic/deployment/cluster) |
| `POST /session/{id}/internal/event` | loadgen callback (not behind auth, only reachable in-cluster) |
| `GET /healthz` | control plane health probe |

The SPA is served at `/session/{id}/` with a `<base href="/session/{id}/">` injected at serve time so relative asset paths resolve correctly.

---

## Go code changes

### `cmd/demo-ctl`

**`session_manager.go` (new)**

```go
type Session struct {
    ID       string
    hub      *Hub
    k8s      *K8s
    mu       sync.Mutex
    lastSeen time.Time
}

type SessionManager struct {
    mu       sync.RWMutex
    sessions map[string]*Session
    cs       kubernetes.Interface
    rcfg     *rest.Config
}
```

- `Create(ctx) (*Session, error)` — creates namespace + k8s objects, starts watchers, returns session
- `Get(id string) (*Session, bool)` — lookup
- `Destroy(ctx, id string) error` — delete namespace, close hub, remove from map
- `touch(id string)` — update lastSeen
- reaper goroutine: every 5 minutes, destroys sessions with `lastSeen > 30min ago`

**`session.go` (replaces `cluster.go`)**

Renamed concept: no more `Cluster` struct. Functions:
- `createSessionNamespace(ctx, cs, id)` — namespace + demo-server Deployment + demo-svc + loadgen Deployment + loadgen-svc
- `destroySession(ctx, cs, id)` — delete namespace

The loadgen Deployment spec sets env vars:
```
TARGET_URL=http://demo-svc.demo-{id}.svc.cluster.local/
SESSION_ID={id}
CONTROL_PLANE_URL=http://demo-ctl-svc.demo-system.svc.cluster.local
```

**`k8s.go`**

The hardcoded `demoNamespace = "demo"` constant is replaced with a `namespace` field on the `K8s` struct (passed at construction). All watch and patch calls use `k.namespace` instead of the constant. Otherwise unchanged.

The `K8s` struct loses `InFlightInc`/`InFlightDec`/`TotalInFlight` — in-flight tracking moves to the loadgen pod. The `K8s` struct gains a `statsLoop` that publishes `StatsEvent` every second by reading a ring buffer populated by incoming `RequestEvent` callbacks.

Note: in the current design, in-flight is already tracked globally (not per-pod) — `InFlightInc("")` always uses the empty string key. The loadgen pod tracks its own in-flight counter atomically and includes it in each `RequestEvent`. Per-pod routing attribution comes from the JSON response body (`{"pod": "..."}` returned by demo-server on every request), which the loadgen pod reads and forwards unchanged in `RequestEvent.podName`.

**`server.go`**

Routing restructured around session ID:
- Middleware extracts `{id}` from path, calls `sessionManager.Get(id)`, touches lastSeen, attaches session to request context.
- All existing handler methods (`handlePodReady`, `handleScaleReplicas`, etc.) are unchanged internally — they just get `k8s` and `hub` from the session in context instead of from the server struct.
- New handler: `handleInternalEvent` — decodes `RequestEvent` from loadgen POST, calls `session.hub.Publish(ev)`, appends to stats ring buffer.
- New handler: `handleTrafficStart/Stop/RPS/Duration` now proxies to loadgen-svc via HTTP instead of calling a local `LoadGen` struct.
- `handleClusterDestroy` calls `sessionManager.Destroy`.

**`loadgen.go` — deleted.** All load generation moves to `cmd/loadgen`.

**`main.go`**

- Removed: single-session `cluster.Bootstrap`, single `K8s` construction, single `LoadGen`.
- Added: `SessionManager` construction, in-cluster kubeconfig fallback, HTTP server on `:9090`.

### `cmd/loadgen` (new binary)

Simple Go binary, ~200 lines:

- Reads env: `TARGET_URL`, `SESSION_ID`, `CONTROL_PLANE_URL`, initial `RPS` (default 5).
- HTTP server on `:8080`:
  - `POST /start` — begin sending requests
  - `POST /stop` — halt
  - `PUT /rps` `{"rps": N}` — set rate
  - `PUT /duration` `{"ms": N}` — set wait param
- Request loop identical to existing `loadgen.go` goroutine logic.
- Per completed request: POST `RequestEvent` JSON to `$CONTROL_PLANE_URL/session/$SESSION_ID/internal/event`.
- In-flight counter tracked locally; included in each `RequestEvent`.
- Stats (`StatsEvent`) computed from the ring buffer every second and POSTed to the same internal event endpoint.

### `web/src/store.js`

The SSE URL and all API paths change from absolute (`/api/...`) to session-scoped:

```js
const sessionId = window.location.pathname.split('/')[2] // "/session/{id}/..."
const base = `/session/${sessionId}`

es = new EventSource(`${base}/api/stream`)

export const api = {
  setReplicas:    (n)   => apiPut(`${base}/api/deployment/replicas`, { replicas: n }),
  // ... all other calls prefixed with base
  destroyCluster: ()    => apiPost(`${base}/api/cluster/destroy`),
}
```

`connectSSE()` is called after the Svelte app mounts, at which point `window.location.pathname` has the session ID.

---

## Kubernetes manifests

New file: `deploy/demo-system.yaml`

Resources:
- `Namespace: demo-system`
- `ServiceAccount: demo-ctl` (in demo-system)
- `ClusterRole: demo-ctl` — verbs on: `namespaces` (create/get/delete/list/watch), `deployments`/`services`/`pods`/`endpointslices` (create/get/list/watch/patch/delete) — no namespace restriction in RBAC (k8s cannot filter by name prefix in a ClusterRole rule; acceptable for a demo tool)
- `ClusterRoleBinding: demo-ctl`
- `Deployment: demo-ctl` — image: `ghcr.io/schwarzm/neutron-operational-api:edge`, 1 replica, serviceAccount: demo-ctl, liveness/readiness on `/healthz`
- `Service: demo-ctl-svc` (ClusterIP, port 9090)
- `Ingress: demo-ctl` — nginx class, routes all paths (`/`) to demo-ctl-svc

### Goreleaser / image

The `demo-ctl` binary and the new `loadgen` binary are both included in the Docker image (multi-binary). The loadgen Deployment in session namespaces runs the same image with entrypoint overridden to `/loadgen`.

---

## Spec self-review

- No TBDs or incomplete sections.
- Per-pod routing attribution: works unchanged — demo-server returns `{"pod": "..."}` in every response body; the loadgen pod reads and forwards it in `RequestEvent.podName`. In-flight tracking was already global (not per-pod) in the original design; the loadgen pod tracks its own atomic counter and includes it in each event.
- Stats ring buffer: the control plane computes StatsEvent from the stream of RequestEvents it receives via `/internal/event`, same algorithm as existing `statsLoop`. This is equivalent behavior.
- Session ID collision: 8 hex chars = 4 billion combinations; for 20 concurrent sessions the probability is negligible.
- The `internal/event` endpoint is not exposed through the cluster ingress (it has no path rule) — it is only reachable at the ClusterIP address, which is in-cluster only.
