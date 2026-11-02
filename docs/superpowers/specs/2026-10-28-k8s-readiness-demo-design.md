# K8s Readiness Demo — Design Spec

**Date:** 2026-10-28  
**Context:** First deliverable of the Neutron Operational API epic (internal issue #559).  
A KT tool that teaches zero-request-missed maintenance on Kubernetes by making readiness probes, endpoint slice updates, and graceful shutdown viscerally visible through live traffic animation.

---

## Goal

A self-contained demo (`make demo`) that:
- Spins up a local kind cluster with nginx-ingress
- Lets the user spawn and delete individual HTTP server pods from a browser UI
- Sends configurable HTTP load through the ingress → service → pods
- Animates every request as a colored dot traveling the traffic path in real time
- Shows k8s signals (probe fail, endpoint remove, SIGTERM, grace period countdown) as they happen
- Demonstrates the contrast: 1-pod deployment has downtime during maintenance; N-pod deployment with readiness probes has zero dropped requests

---

## Components

### `demo-server` (in-pod binary)

Tiny Go HTTP server. Runs inside each spawned pod.

**Endpoints:**

| Method | Path | Behaviour |
|--------|------|-----------|
| `GET` | `/` | Returns JSON after configurable delay: `{"pod":"demo-abc","ready":true,"delayMs":0,"requestsServed":142}`. The `requestsServed` counter lets the load generator attribute each response to a specific pod. |
| `GET` | `/readyz` | `200 OK` or `503 Service Unavailable` — toggled via `/admin/ready`. This is what the kubelet readiness probe calls. |
| `GET` | `/livez` | Always `200 OK`. |
| `POST` | `/admin/ready` | Body: `{"ready":true\|false}`. Toggles the `/readyz` response. |
| `POST` | `/admin/delay` | Body: `{"ms":500}`. Sets response delay on `/`. |

**Graceful shutdown:** On SIGTERM the server stops accepting new connections on `/` (new requests get 503) but keeps the HTTP server alive until all in-flight requests complete or the process is SIGKILL'd at `terminationGracePeriodSeconds`. It does **not** touch `/readyz` on SIGTERM — by that point k8s has already updated the endpoint slice based on prior readiness state. The grace period is purely for draining in-flight work.

**Container image:** Scratch-based (~5 MB), built and `kind load`ed at cluster bootstrap time by `demo-ctl`.

---

### `demo-ctl` (host orchestrator binary)

Go binary running on the host. Single process that owns: kind cluster lifecycle, k8s object management, load generation, SSE streaming, and static file serving.

#### Cluster lifecycle

1. `kind create cluster` with embedded config (extraPortMappings: host `8080→80` on control-plane node, ingress-ready node label)
2. Apply nginx-ingress manifest (kind-flavored upstream manifest)
3. Wait for ingress-controller pod Ready
4. Create `demo` namespace
5. Create `demo-svc` Service (ClusterIP, selector: `app=demo`)
6. Create Ingress rule routing `/` → `demo-svc:80`
7. Open `localhost:9090` in browser

On exit (or "Destroy cluster" button): `kind delete cluster`.

#### Pod spec (per spawned pod)

- `terminationGracePeriodSeconds: 30`
- `readinessProbe`: GET `/readyz`, `periodSeconds: 2`, `failureThreshold: 1` (fast feedback for demo)
- `livenessProbe`: GET `/livez`, `periodSeconds: 10`
- Label: `app=demo` (picked up by Service selector)
- Name: `demo-{6-char random}`

Pods are created individually (not via Deployment) for independent per-pod control.

#### Per-pod admin control

Uses the k8s API server pod proxy to reach each pod's admin endpoints without direct pod IP access (works on Mac/Docker):

```
/api/v1/namespaces/demo/pods/{name}/proxy/admin/ready
/api/v1/namespaces/demo/pods/{name}/proxy/admin/delay
```

#### Load generator

Goroutine pool inside `demo-ctl`. Each goroutine sends HTTP GET to `http://localhost:8080/` (kind NodePort → nginx-ingress → service → pod), reads the JSON response, extracts `pod` name and `requestsServed`, records status code and duration, then emits a `request` SSE event. Rate and request duration are configurable at runtime. Slow requests use a `?wait=N` query param (demo-server sleeps for N ms before responding).

#### k8s watchers

Two informers running continuously:
- **Pod informer** (namespace `demo`, label `app=demo`): emits `pod` SSE events on any state change (phase, ready condition, `deletionTimestamp`)
- **EndpointSlice informer** (namespace `demo`, service `demo-svc`): emits `endpoints` SSE events when the endpoint set changes

Grace period countdown: when a pod has `deletionTimestamp` set, `demo-ctl` computes `graceRemaining = deletionTimestamp + terminationGracePeriodSeconds - now` and includes it in the `pod` event every tick.

#### HTTP API

```
POST   /api/pod/spawn              → create pod (name auto-generated)
DELETE /api/pod/{name}             → delete pod (graceful)
PUT    /api/pod/{name}/ready       → {"ready":false}  → pod proxy
PUT    /api/pod/{name}/delay       → {"ms":500}       → pod proxy
POST   /api/traffic/start
POST   /api/traffic/stop
PUT    /api/traffic/rps            → {"rps":10}
PUT    /api/traffic/duration       → {"ms":0}  (0 = fast)
POST   /api/cluster/destroy
GET    /api/stream                 → SSE
GET    /                           → embedded Svelte app
```

#### SSE event types

```json
{"type":"cluster",    "state":"creating|ready|destroying"}
{"type":"pod",        "name":"demo-abc","phase":"Running","ready":true,"terminating":false,"graceRemaining":0,"inFlight":2}
{"type":"endpoints",  "pods":["demo-abc","demo-xyz"]}
{"type":"request",    "id":"r-1","podName":"demo-abc","status":200,"durationMs":52,"slow":false}
{"type":"signal",     "kind":"PROBE_FAIL|ENDPOINT_REMOVE|SIGTERM|DRAINED","podName":"demo-abc","message":"..."}
{"type":"stats",      "rps":12.3,"successRate":0.98,"p50Ms":45,"p95Ms":201,"inFlight":3}
```

---

### Frontend (Svelte + Vite)

Compiled to static assets, embedded in `demo-ctl` via `go:embed`.

#### Visual style

Identical to pheme demo: dark/light CSS custom property theme, Archivo Black headings, system-sans body, monospace for values and pod names. Same color tokens: `--add` (green), `--del` (red), `--warn` (amber), `--accent` (blue).

#### Layout

Full-viewport, three regions:

- **Top bar**: title, cluster status pill, live stats (req/s · success rate · p50 · p95), theme toggle
- **Main area** (left ~75%): SVG system map fills full height
- **Sidebar** (right ~25%): controls + k8s event log

#### SVG system map

1000×400 viewBox, four columns:

```
LOAD GEN     INGRESS      SERVICE       PODS
[gen]    →   [nginx]  →   [svc]    →   [demo-abc]
                                        [demo-xyz]
                                        [demo-...] (dynamic)
```

- Pod node positions are computed dynamically, stacked evenly in the PODS column
- Service node subtitle shows the current endpoint list (pod names in the slice)
- Edges are cubic bezier paths; dots travel via `getPointAtLength` + `requestAnimationFrame` (same as pheme)

**Dot colors:**
- Green (`--add`): 2xx response
- Red (`--del`): 5xx or timeout
- Amber (`--warn`): slow request — dot parks at the pod node and pulses until the response arrives, then resolves to green or red

**Pod node states** (driven by SSE `pod` events):
- Ready: standard node, green border
- NotReady: amber border + "NOT READY" badge
- Terminating: red dashed border + "TERMINATING N s" badge (countdown from `graceRemaining`)
- Terminating + in-flight: badge shows "TERMINATING · N in-flight"

#### Svelte components

```
App.svelte          — SSE connection, global store, layout shell
SystemMap.svelte    — SVG map, dot animation loop, node/edge rendering
PodNode.svelte      — individual pod SVG group (state-driven styles)
Sidebar.svelte      — controls + event log
store.js            — SSE-fed reactive state (pods, endpoints, stats, events)
```

#### Sidebar controls

Fieldset groups (pheme style):

- **Pods**: "Spawn pod" button; per-pod row with name, ready toggle, delay selector (fast / 500 ms / 2 s), delete button
- **Traffic**: start/stop; req/s input (1–50); request duration (fast / 500 ms / 2 s)
- **Cluster**: "Destroy cluster" button with confirm step
- **K8s events**: scrolling log, left-bordered by event type (green=probe ok, amber=probe fail/endpoint remove, red=SIGTERM, teal=drained)

---

## Project structure

```
neutron-operational-api/
├── cmd/
│   ├── demo-ctl/
│   │   ├── main.go
│   │   ├── cluster.go      # kind cluster lifecycle
│   │   ├── k8s.go          # pod/service/endpoint watch + control
│   │   ├── loadgen.go      # HTTP load generator
│   │   ├── stream.go       # SSE hub
│   │   └── server.go       # HTTP API + static file serving
│   └── demo-server/
│       └── main.go
├── web/
│   ├── src/
│   │   ├── App.svelte
│   │   ├── SystemMap.svelte
│   │   ├── PodNode.svelte
│   │   ├── Sidebar.svelte
│   │   └── store.js
│   ├── package.json
│   └── vite.config.js
├── Makefile
├── go.mod
└── go.sum
```

`make demo`: build Go binaries + `npm run build`, start `demo-ctl`, open `localhost:9090`.  
`make clean`: `kind delete cluster`, remove build artifacts.  
`demo-ctl` embeds `web/dist/` via `go:embed` — single self-contained binary.

---

## Key teaching moments the demo enables

1. **1 pod → rolling restart → gap in traffic**: spawn 1 pod, start traffic (all dots go to it), delete the pod. Red dots appear immediately as the pod terminates. Shows why 1 pod = downtime.

2. **N pods → readiness gate → zero drops**: spawn 3 pods, start traffic (dots spread across all 3), toggle one pod to not-ready. Within ~2s the endpoint slice updates and dots stop reaching that pod. No red dots. Delete it — dots never go there again.

3. **Grace period**: set request duration to 2 s, send traffic, then delete a pod. The pod shows TERMINATING with countdown. Amber dots (slow, in-flight) are still traveling to it and completing. No new dots go there. Green dots resolve as requests finish. Shows that graceful shutdown + readiness = zero dropped requests even with long-running requests.

4. **Liveness vs readiness**: toggle readiness off (not-ready, removed from endpoints). Toggle it back on — pod re-enters endpoints, dots resume. Contrast: liveness probe failure would restart the pod entirely (not shown in the demo, but explained in the event log).
