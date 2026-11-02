# Deployment Rollout Demo — Design Spec

## Goal

Replace the manual pod-spawn/delete workflow with a k8s Deployment that owns all demo pods. Add controls for replica count, grace period, and rollout strategy presets. Trigger a rollout by patching the pod template annotation, and visualise the rolling update through the existing signal-pulse and traffic-dot system.

## Architecture

Bootstrap creates a `Deployment` in the `demo` namespace with 2 initial replicas using the same pod spec currently used by `SpawnPod`. The Deployment is watched alongside pods; its status is streamed to the browser as a new `DeploymentEvent` SSE event type. The sidebar replaces spawn/delete buttons with replica ±, plus a Deployment control section (grace period, strategy, rollout trigger). Per-pod controls (ready/delay) stay because they work on deployment-managed pods.

`WatchPods` requires no changes — deployment-managed pods carry the same `app=demo` label the watcher already selects.

## Tech Stack

Go 1.27 · k8s.io/client-go v0.37.2 · Svelte 5 · Vite 6

---

## Global Constraints

- Module path: `github.com/schwarzm/neutron-operational-api`
- Go version: 1.27
- No new dependencies beyond what is already in go.mod
- Rollout is triggered by patching annotation `demo/rolledAt: <RFC3339 timestamp>` on the pod template; no image change needed
- Deployment name: `demo` (same namespace: `demo`)
- Initial replicas on Bootstrap: 2
- Default grace period: 30 s (unchanged from current `terminationGracePeriod` const)
- All existing per-pod API routes (`PUT /api/pod/{name}/ready`, `PUT /api/pod/{name}/delay`) must continue to work unchanged
- `WatchPods`, `WatchEndpoints`, signal pulses, and traffic dots require no structural changes

---

## Rollout Strategy Presets

| UI label | k8s strategy type | maxUnavailable | maxSurge |
|---|---|---|---|
| `rolling` | RollingUpdate | 1 | 0 |
| `surge` | RollingUpdate | 0 | 1 |
| `recreate` | Recreate | — | — |

Default on Bootstrap: `surge`.

---

## New Event Type

### DeploymentEvent (`type: "deployment"`)

```go
type DeploymentEvent struct {
    Type              EventType `json:"type"`
    Replicas          int32     `json:"replicas"`           // desired
    ReadyReplicas     int32     `json:"readyReplicas"`
    UpdatedReplicas   int32     `json:"updatedReplicas"`
    AvailableReplicas int32     `json:"availableReplicas"`
    RolloutInProgress bool      `json:"rolloutInProgress"`  // updatedReplicas < replicas || availableReplicas < replicas
    Strategy          string    `json:"strategy"`           // "rolling" | "surge" | "recreate"
    GracePeriod       int32     `json:"gracePeriod"`        // seconds
}
```

---

## Backend File Changes

### `cmd/demo-ctl/events.go`
- Add `EventDeployment EventType = "deployment"`
- Add `DeploymentEvent` struct (see above)

### `cmd/demo-ctl/k8s.go`
- **Remove** `SpawnPod`, `DeletePod`
- **Add** methods:
  - `ScaleDeployment(ctx, replicas int32) error` — patches `spec.replicas`
  - `SetStrategy(ctx, preset string) error` — patches `spec.strategy` from preset name
  - `SetGracePeriod(ctx, seconds int32) error` — patches `spec.template.spec.terminationGracePeriodSeconds`
  - `TriggerRollout(ctx) error` — patches `spec.template.metadata.annotations["demo/rolledAt"]` with current RFC3339 time
  - `WatchDeployment(ctx)` — watch the `demo` Deployment; on any change publish `DeploymentEvent`
- Preset-to-strategy mapping (unexported helper):
  ```go
  func strategyForPreset(preset string) appsv1.DeploymentStrategy {
      switch preset {
      case "rolling":
          maxU := intstr.FromInt32(1); maxS := intstr.FromInt32(0)
          return appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
              RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: &maxU, MaxSurge: &maxS}}
      case "surge":
          maxU := intstr.FromInt32(0); maxS := intstr.FromInt32(1)
          return appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
              RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: &maxU, MaxSurge: &maxS}}
      default: // "recreate"
          return appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
      }
  }
  ```

### `cmd/demo-ctl/cluster.go`
- After `createIngress`, call `createDeployment(ctx)`:
  ```go
  func (c *Cluster) createDeployment(ctx context.Context) error {
      // appsv1.Deployment, name "demo", namespace demoNamespace
      // spec.replicas = 2
      // spec.selector = {app: demo}
      // spec.strategy = surge preset (maxUnavailable:0, maxSurge:1)
      // spec.template = same pod spec as former SpawnPod, with:
      //   - name omitted (generated); POD_NAME env from fieldRef metadata.name
      //   - terminationGracePeriodSeconds: 30
      //   - same readiness/liveness probes
      // IsAlreadyExists → return nil (idempotent)
  }
  ```
- `POD_NAME` must use a k8s downward-API `fieldRef`:
  ```go
  corev1.EnvVar{
      Name: "POD_NAME",
      ValueFrom: &corev1.EnvVarSource{
          FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
      },
  }
  ```

### `cmd/demo-ctl/server.go`
- **Remove** handlers: `handleSpawn`, `handleDeletePod`
- **Remove** routes: `POST /api/pod/spawn`, `DELETE /api/pod/{name}`
- **Add** routes and handlers:
  - `PUT /api/deployment/replicas` → body `{"replicas": N}` → `k8s.ScaleDeployment`
  - `PUT /api/deployment/strategy` → body `{"strategy": "surge"|"rolling"|"recreate"}` → `k8s.SetStrategy`
  - `PUT /api/deployment/grace` → body `{"seconds": N}` → `k8s.SetGracePeriod`
  - `POST /api/deployment/rollout` → `k8s.TriggerRollout`

### `cmd/demo-ctl/main.go`
- Add `go k8s.WatchDeployment(ctx)` after `WatchEndpoints`

---

## Frontend File Changes

### `web/src/store.js`
- Add `export const deployment = writable(null)` — holds latest `DeploymentEvent`
- In SSE handler, add:
  ```js
  case 'deployment':
    deployment.set(ev)
    break
  ```
- Remove `api.spawnPod` and `api.deletePod`
- Add:
  ```js
  setReplicas:   (n)       => apiPut('/api/deployment/replicas', { replicas: n }),
  setStrategy:   (s)       => apiPut('/api/deployment/strategy', { strategy: s }),
  setGrace:      (seconds) => apiPut('/api/deployment/grace',    { seconds }),
  triggerRollout:()        => apiPost('/api/deployment/rollout'),
  ```

### `web/src/Sidebar.svelte`
Replace the Pods section entirely. New layout:

```
── Deployment ──────────────────────────
  [−]  2  [+]     [Trigger rollout]
  
  Grace    [5s ▾]
  Strategy [surge ▾]
  
  Status: 2/2 ready  ← or "rolling out: 1/2 updated" during rollout

── Pods ────────────────────────────────
  demo-abc123  [ready]
    [set not-ready]  [fast ▾]
  
  demo-xyz456  [ready]
    [set not-ready]  [fast ▾]
```

- Remove delete button from pod rows
- `deployment` store drives the status line and the replica count display
- Strategy and grace selects call their API immediately on change (no separate save button)
- Rollout button is disabled while `deployment.rolloutInProgress`

### `web/src/SystemMap.svelte`
No changes required. Deployment-managed pods appear automatically via `WatchPods`. Signal pulses and traffic dots work unchanged.

---

## Self-Review

**Spec coverage:**
- Bootstrap auto-creates Deployment with 2 replicas ✓
- Replica ± replaces spawn button ✓
- Per-pod ready/delay controls preserved ✓
- Grace period select (5s / 15s / 30s / 60s) ✓
- Rollout strategy presets (rolling / surge / recreate) ✓
- Trigger rollout via annotation patch ✓
- DeploymentEvent streamed over SSE ✓
- Rollout-in-progress status in sidebar ✓
- Delete button removed from pod rows ✓
- No changes to WatchPods, signal pulses, traffic dots ✓

**No placeholders** — all types, signatures, and field names specified above.

**Type consistency** — `strategyForPreset` used in both `SetStrategy` and `createDeployment`; preset string literals `"rolling"`, `"surge"`, `"recreate"` used consistently across server, k8s, and frontend.
