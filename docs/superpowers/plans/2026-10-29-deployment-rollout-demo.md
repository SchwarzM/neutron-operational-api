# Deployment Rollout Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace manual pod-spawn/delete with a k8s Deployment; add replica scaling, grace period, rollout strategy presets, and a rollout trigger button; visualise the rolling update through the existing signal-pulse + traffic-dot system.

**Architecture:** Bootstrap creates a `Deployment` named `demo` in namespace `demo` with 2 replicas and the surge strategy. A new `WatchDeployment` goroutine streams `DeploymentEvent` over SSE. The sidebar replaces spawn/delete buttons with replica ± and a deployment control section.

**Tech Stack:** Go 1.27 · k8s.io/client-go v0.37.2 · k8s.io/api/apps/v1 · Svelte 5 · Vite 6

**Spec:** docs/superpowers/specs/2026-10-29-deployment-rollout-demo.md

## Global Constraints

- Module path: `github.com/schwarzm/neutron-operational-api`
- Go 1.27; no new go.mod dependencies (appsv1 and apimachinery/pkg/types are already transitive deps)
- Deployment name: `"demo"`, namespace: `"demo"` (const `demoNamespace`)
- Initial replicas on Bootstrap: 2; default grace: 30 s (const `terminationGracePeriod`); default strategy: `"surge"`
- Rollout trigger: patch `spec.template.metadata.annotations["demo/rolledAt"]` with RFC3339 timestamp
- Preset names (exact strings, used in JSON and UI): `"rolling"`, `"surge"`, `"recreate"`
- All existing per-pod routes (`PUT /api/pod/{name}/ready`, `PUT /api/pod/{name}/delay`) must remain unchanged
- `WatchPods`, `WatchEndpoints`, signal pulses, traffic dots — no structural changes
- `POD_NAME` env var must use k8s downward API `fieldRef: metadata.name` (not a hardcoded string)

---

### Task 1: DeploymentEvent type + Deployment Bootstrap

**Files:**
- Modify: `cmd/demo-ctl/events.go`
- Modify: `cmd/demo-ctl/cluster.go`
- Create: `cmd/demo-ctl/cluster_test.go`

**Interfaces:**
- Produces: `DeploymentEvent` struct (consumed by Task 2's `WatchDeployment`)
- Produces: `Cluster.createDeployment(ctx)` called from `Bootstrap`
- Produces: `Cluster.Clientset() kubernetes.Interface` (return type change; main.go already passes this to `NewK8s` which takes `kubernetes.Interface`)

- [ ] **Step 1: Add `EventDeployment` and `DeploymentEvent` to events.go**

In `cmd/demo-ctl/events.go`, add to the `const` block:
```go
EventDeployment EventType = "deployment"
```

Add the struct after `StatsEvent`:
```go
// DeploymentEvent is emitted whenever the demo Deployment's status changes.
type DeploymentEvent struct {
    Type              EventType `json:"type"`
    Replicas          int32     `json:"replicas"`
    ReadyReplicas     int32     `json:"readyReplicas"`
    UpdatedReplicas   int32     `json:"updatedReplicas"`
    AvailableReplicas int32     `json:"availableReplicas"`
    RolloutInProgress bool      `json:"rolloutInProgress"`
    Strategy          string    `json:"strategy"`  // "rolling" | "surge" | "recreate"
    GracePeriod       int32     `json:"gracePeriod"`
}
```

- [ ] **Step 2: Change `Cluster.cs` type and `Clientset()` return type**

In `cmd/demo-ctl/cluster.go`:

Change the struct field:
```go
// before:
cs   *kubernetes.Clientset
// after:
cs   kubernetes.Interface
```

Change the method:
```go
// before:
func (c *Cluster) Clientset() *kubernetes.Clientset { return c.cs }
// after:
func (c *Cluster) Clientset() kubernetes.Interface { return c.cs }
```

The assignment `c.cs, err = kubernetes.NewForConfig(c.rcfg)` still compiles because `*kubernetes.Clientset` implements `kubernetes.Interface`.

- [ ] **Step 3: Add `createDeployment` to cluster.go**

Add import `appsv1 "k8s.io/api/apps/v1"` to cluster.go's import block.

Add the method:
```go
func (c *Cluster) createDeployment(ctx context.Context) error {
    replicas := int32(2)
    grace    := int64(terminationGracePeriod)
    maxU     := intstr.FromInt32(0)
    maxS     := intstr.FromInt32(1)
    dep := &appsv1.Deployment{
        ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: demoNamespace},
        Spec: appsv1.DeploymentSpec{
            Replicas: &replicas,
            Selector: &metav1.LabelSelector{
                MatchLabels: map[string]string{"app": "demo"},
            },
            Strategy: appsv1.DeploymentStrategy{
                Type: appsv1.RollingUpdateDeploymentStrategyType,
                RollingUpdate: &appsv1.RollingUpdateDeployment{
                    MaxUnavailable: &maxU,
                    MaxSurge:       &maxS,
                },
            },
            Template: corev1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{
                    Labels: map[string]string{"app": "demo"},
                },
                Spec: corev1.PodSpec{
                    TerminationGracePeriodSeconds: &grace,
                    Containers: []corev1.Container{{
                        Name:            "demo-server",
                        Image:           demoImageName,
                        ImagePullPolicy: corev1.PullNever,
                        Env: []corev1.EnvVar{{
                            Name: "POD_NAME",
                            ValueFrom: &corev1.EnvVarSource{
                                FieldRef: &corev1.ObjectFieldSelector{
                                    FieldPath: "metadata.name",
                                },
                            },
                        }},
                        Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
                        ReadinessProbe: &corev1.Probe{
                            ProbeHandler: corev1.ProbeHandler{
                                HTTPGet: &corev1.HTTPGetAction{
                                    Path: "/readyz",
                                    Port: intstr.FromInt32(8080),
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
                                    Port: intstr.FromInt32(8080),
                                },
                            },
                            PeriodSeconds:    10,
                            FailureThreshold: 3,
                        },
                    }},
                },
            },
        },
    }
    _, err := c.cs.AppsV1().Deployments(demoNamespace).Create(ctx, dep, metav1.CreateOptions{})
    if k8serrors.IsAlreadyExists(err) {
        return nil
    }
    return err
}
```

- [ ] **Step 4: Call `createDeployment` from Bootstrap**

In `Bootstrap`, after the `createIngress` call:
```go
if err := c.createDeployment(ctx); err != nil {
    return err
}
```

- [ ] **Step 5: Write the failing test**

Create `cmd/demo-ctl/cluster_test.go`:
```go
package main

import (
    "context"
    "testing"

    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes/fake"
)

func TestCreateDeploymentSpec(t *testing.T) {
    cs := fake.NewSimpleClientset()
    cluster := &Cluster{pub: &capPublisher{}, cs: cs}
    if err := cluster.createDeployment(context.Background()); err != nil {
        t.Fatalf("createDeployment: %v", err)
    }

    dep, err := cs.AppsV1().Deployments(demoNamespace).Get(
        context.Background(), "demo", metav1.GetOptions{},
    )
    if err != nil {
        t.Fatalf("get deployment: %v", err)
    }
    if *dep.Spec.Replicas != 2 {
        t.Errorf("replicas = %d, want 2", *dep.Spec.Replicas)
    }
    container := dep.Spec.Template.Spec.Containers[0]
    var podNameEnv *corev1.EnvVar
    for i := range container.Env {
        if container.Env[i].Name == "POD_NAME" {
            podNameEnv = &container.Env[i]
        }
    }
    if podNameEnv == nil {
        t.Fatal("POD_NAME env var not found")
    }
    if podNameEnv.ValueFrom == nil || podNameEnv.ValueFrom.FieldRef == nil {
        t.Fatal("POD_NAME must use downward API fieldRef")
    }
    if podNameEnv.ValueFrom.FieldRef.FieldPath != "metadata.name" {
        t.Errorf("fieldPath = %q, want metadata.name", podNameEnv.ValueFrom.FieldRef.FieldPath)
    }
}
```

- [ ] **Step 6: Run test — expect it to fail**

```
go test ./cmd/demo-ctl/ -run TestCreateDeploymentSpec -v
```

Expected: FAIL (createDeployment not yet implemented).

- [ ] **Step 7: Run test after implementation — expect it to pass**

```
go test ./cmd/demo-ctl/ -run TestCreateDeploymentSpec -v
```

Expected: PASS.

- [ ] **Step 8: Full build check**

```
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 9: Commit**

```
git add cmd/demo-ctl/events.go cmd/demo-ctl/cluster.go cmd/demo-ctl/cluster_test.go
git commit -m "feat: DeploymentEvent type + Bootstrap creates Deployment"
```

---

### Task 2: Deployment management methods in k8s.go

**Files:**
- Modify: `cmd/demo-ctl/k8s.go`
- Modify: `cmd/demo-ctl/k8s_test.go`

**Interfaces:**
- Consumes: `DeploymentEvent` (from Task 1)
- Removes: `SpawnPod(ctx) (string, error)`, `DeletePod(ctx, name string) error`
- Produces: `strategyForPreset(preset string) appsv1.DeploymentStrategy`
- Produces: `presetForStrategy(s appsv1.DeploymentStrategy) string`
- Produces: `(k *K8s) ScaleDeployment(ctx, replicas int32) error`
- Produces: `(k *K8s) SetStrategy(ctx, preset string) error`
- Produces: `(k *K8s) SetGracePeriod(ctx, seconds int32) error`
- Produces: `(k *K8s) TriggerRollout(ctx) error`
- Produces: `(k *K8s) WatchDeployment(ctx)`

- [ ] **Step 1: Write the failing tests**

Add to `cmd/demo-ctl/k8s_test.go`:

```go
import (
    // existing imports ...
    appsv1 "k8s.io/api/apps/v1"
    // k8stesting already imported
)

func int32ptr(n int32) *int32 { return &n }

func TestStrategyForPreset(t *testing.T) {
    cases := []struct {
        preset  string
        wantType appsv1.DeploymentStrategyType
        maxU, maxS int32
    }{
        {"surge",    appsv1.RollingUpdateDeploymentStrategyType, 0, 1},
        {"rolling",  appsv1.RollingUpdateDeploymentStrategyType, 1, 0},
        {"recreate", appsv1.RecreateDeploymentStrategyType, 0, 0},
    }
    for _, tc := range cases {
        s := strategyForPreset(tc.preset)
        if s.Type != tc.wantType {
            t.Errorf("preset %q: type = %q, want %q", tc.preset, s.Type, tc.wantType)
        }
        if s.Type == appsv1.RollingUpdateDeploymentStrategyType {
            if s.RollingUpdate == nil {
                t.Fatalf("preset %q: RollingUpdate is nil", tc.preset)
            }
            if s.RollingUpdate.MaxUnavailable.IntValue() != int(tc.maxU) {
                t.Errorf("preset %q: maxUnavailable = %d, want %d",
                    tc.preset, s.RollingUpdate.MaxUnavailable.IntValue(), tc.maxU)
            }
            if s.RollingUpdate.MaxSurge.IntValue() != int(tc.maxS) {
                t.Errorf("preset %q: maxSurge = %d, want %d",
                    tc.preset, s.RollingUpdate.MaxSurge.IntValue(), tc.maxS)
            }
        }
    }
}

func TestWatchDeploymentPublishesEvent(t *testing.T) {
    cs := fake.NewSimpleClientset()
    fakeWatcher := watch.NewFake()
    cs.PrependWatchReactor("deployments", func(_ k8stesting.Action) (bool, watch.Interface, error) {
        return true, fakeWatcher, nil
    })
    pub := &capPublisher{}
    k := NewK8s(pub, cs, nil)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go k.WatchDeployment(ctx)
    time.Sleep(50 * time.Millisecond)

    dep := &appsv1.Deployment{
        ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: demoNamespace},
        Spec: appsv1.DeploymentSpec{
            Replicas: int32ptr(3),
            Strategy: appsv1.DeploymentStrategy{
                Type: appsv1.RollingUpdateDeploymentStrategyType,
                RollingUpdate: &appsv1.RollingUpdateDeployment{
                    MaxUnavailable: func() *intstr.IntOrString { v := intstr.FromInt32(0); return &v }(),
                    MaxSurge:       func() *intstr.IntOrString { v := intstr.FromInt32(1); return &v }(),
                },
            },
        },
        Status: appsv1.DeploymentStatus{
            Replicas: 3, ReadyReplicas: 2, UpdatedReplicas: 1, AvailableReplicas: 2,
        },
    }
    fakeWatcher.Modify(dep)
    time.Sleep(50 * time.Millisecond)

    for _, ev := range pub.events {
        if de, ok := ev.(DeploymentEvent); ok && de.Replicas == 3 && de.Strategy == "surge" {
            if !de.RolloutInProgress {
                t.Error("expected RolloutInProgress=true (updatedReplicas < replicas)")
            }
            return
        }
    }
    t.Fatal("expected DeploymentEvent{Replicas:3, Strategy:\"surge\"} but did not find it")
}
```

Note: `intstr` must be imported in k8s_test.go: `"k8s.io/apimachinery/pkg/util/intstr"`.

- [ ] **Step 2: Run tests — expect them to fail**

```
go test ./cmd/demo-ctl/ -run "TestStrategyForPreset|TestWatchDeployment" -v
```

Expected: compilation error or FAIL.

- [ ] **Step 3: Add imports to k8s.go**

Add to the import block:
```go
appsv1 "k8s.io/api/apps/v1"
"k8s.io/apimachinery/pkg/types"
```

- [ ] **Step 4: Remove `SpawnPod` and `DeletePod` from k8s.go**

Delete both functions entirely (lines starting with `// SpawnPod` through the closing `}`, and `// DeletePod` through its closing `}`).

- [ ] **Step 5: Add `strategyForPreset` and `presetForStrategy`**

```go
func strategyForPreset(preset string) appsv1.DeploymentStrategy {
    switch preset {
    case "rolling":
        maxU := intstr.FromInt32(1)
        maxS := intstr.FromInt32(0)
        return appsv1.DeploymentStrategy{
            Type: appsv1.RollingUpdateDeploymentStrategyType,
            RollingUpdate: &appsv1.RollingUpdateDeployment{
                MaxUnavailable: &maxU, MaxSurge: &maxS,
            },
        }
    case "surge":
        maxU := intstr.FromInt32(0)
        maxS := intstr.FromInt32(1)
        return appsv1.DeploymentStrategy{
            Type: appsv1.RollingUpdateDeploymentStrategyType,
            RollingUpdate: &appsv1.RollingUpdateDeployment{
                MaxUnavailable: &maxU, MaxSurge: &maxS,
            },
        }
    default: // "recreate"
        return appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
    }
}

func presetForStrategy(s appsv1.DeploymentStrategy) string {
    if s.Type == appsv1.RecreateDeploymentStrategyType {
        return "recreate"
    }
    if s.RollingUpdate != nil && s.RollingUpdate.MaxUnavailable != nil &&
        s.RollingUpdate.MaxUnavailable.IntValue() == 0 {
        return "surge"
    }
    return "rolling"
}
```

- [ ] **Step 6: Add `ScaleDeployment`, `SetStrategy`, `SetGracePeriod`, `TriggerRollout`**

```go
func (k *K8s) ScaleDeployment(ctx context.Context, replicas int32) error {
    patch, _ := json.Marshal(map[string]any{
        "spec": map[string]any{"replicas": replicas},
    })
    _, err := k.cs.AppsV1().Deployments(demoNamespace).Patch(
        ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
    )
    return err
}

func (k *K8s) SetStrategy(ctx context.Context, preset string) error {
    strat := strategyForPreset(preset)
    patch, _ := json.Marshal(map[string]any{
        "spec": map[string]any{"strategy": strat},
    })
    _, err := k.cs.AppsV1().Deployments(demoNamespace).Patch(
        ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
    )
    return err
}

func (k *K8s) SetGracePeriod(ctx context.Context, seconds int32) error {
    patch, _ := json.Marshal(map[string]any{
        "spec": map[string]any{
            "template": map[string]any{
                "spec": map[string]any{
                    "terminationGracePeriodSeconds": seconds,
                },
            },
        },
    })
    _, err := k.cs.AppsV1().Deployments(demoNamespace).Patch(
        ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
    )
    return err
}

func (k *K8s) TriggerRollout(ctx context.Context) error {
    patch, _ := json.Marshal(map[string]any{
        "spec": map[string]any{
            "template": map[string]any{
                "metadata": map[string]any{
                    "annotations": map[string]string{
                        "demo/rolledAt": time.Now().UTC().Format(time.RFC3339),
                    },
                },
            },
        },
    })
    _, err := k.cs.AppsV1().Deployments(demoNamespace).Patch(
        ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
    )
    return err
}
```

- [ ] **Step 7: Add `WatchDeployment`**

```go
// WatchDeployment watches the demo Deployment and publishes DeploymentEvent on changes.
func (k *K8s) WatchDeployment(ctx context.Context) {
    for {
        if ctx.Err() != nil {
            return
        }
        w, err := k.cs.AppsV1().Deployments(demoNamespace).Watch(ctx, metav1.ListOptions{
            FieldSelector: "metadata.name=demo",
        })
        if err != nil {
            time.Sleep(2 * time.Second)
            continue
        }
        for ev := range w.ResultChan() {
            dep, ok := ev.Object.(*appsv1.Deployment)
            if !ok {
                continue
            }
            desired := int32(0)
            if dep.Spec.Replicas != nil {
                desired = *dep.Spec.Replicas
            }
            grace := int32(terminationGracePeriod)
            if dep.Spec.Template.Spec.TerminationGracePeriodSeconds != nil {
                grace = int32(*dep.Spec.Template.Spec.TerminationGracePeriodSeconds)
            }
            k.pub.Publish(DeploymentEvent{
                Type:              EventDeployment,
                Replicas:          desired,
                ReadyReplicas:     dep.Status.ReadyReplicas,
                UpdatedReplicas:   dep.Status.UpdatedReplicas,
                AvailableReplicas: dep.Status.AvailableReplicas,
                RolloutInProgress: dep.Status.UpdatedReplicas < desired ||
                    dep.Status.AvailableReplicas < desired,
                Strategy:    presetForStrategy(dep.Spec.Strategy),
                GracePeriod: grace,
            })
        }
        time.Sleep(2 * time.Second)
    }
}
```

- [ ] **Step 8: Run tests — expect them to pass**

```
go test ./cmd/demo-ctl/ -run "TestStrategyForPreset|TestWatchDeployment" -v
```

Expected: PASS.

- [ ] **Step 9: Run full test suite**

```
go test ./cmd/demo-ctl/
```

Expected: all pass.

- [ ] **Step 10: Build check**

```
go build ./cmd/demo-ctl/
```

- [ ] **Step 11: Commit**

```
git add cmd/demo-ctl/k8s.go cmd/demo-ctl/k8s_test.go
git commit -m "feat: deployment management methods + WatchDeployment"
```

---

### Task 3: Server routes + main wiring

**Files:**
- Modify: `cmd/demo-ctl/server.go`
- Modify: `cmd/demo-ctl/main.go`

**Interfaces:**
- Consumes: `K8s.ScaleDeployment`, `K8s.SetStrategy`, `K8s.SetGracePeriod`, `K8s.TriggerRollout`, `K8s.WatchDeployment` (all from Task 2)
- Removes routes: `POST /api/pod/spawn`, `DELETE /api/pod/{name}`
- Adds routes: `PUT /api/deployment/replicas`, `PUT /api/deployment/strategy`, `PUT /api/deployment/grace`, `POST /api/deployment/rollout`

- [ ] **Step 1: Remove spawn and delete from server.go**

In `Handler()`, remove these two lines:
```go
mux.HandleFunc("POST /api/pod/spawn", s.handleSpawn)
mux.HandleFunc("DELETE /api/pod/{name}", s.handleDeletePod)
```

Delete `handleSpawn` and `handleDeletePod` functions entirely.

- [ ] **Step 2: Add deployment routes to `Handler()`**

After the traffic control routes, add:
```go
// Deployment control
mux.HandleFunc("PUT /api/deployment/replicas",  s.handleScaleReplicas)
mux.HandleFunc("PUT /api/deployment/strategy",  s.handleSetStrategy)
mux.HandleFunc("PUT /api/deployment/grace",     s.handleSetGrace)
mux.HandleFunc("POST /api/deployment/rollout",  s.handleRollout)
```

- [ ] **Step 3: Add handler functions to server.go**

```go
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

func (s *APIServer) handleRollout(w http.ResponseWriter, _ *http.Request) {
    if err := s.k8s.TriggerRollout(r.Context()); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusAccepted)
}
```

**Important:** `handleRollout` uses `r *http.Request` in the signature but the parameter is named `_` — fix: use `r *http.Request` and call `r.Context()`:

```go
func (s *APIServer) handleRollout(w http.ResponseWriter, r *http.Request) {
    if err := s.k8s.TriggerRollout(r.Context()); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusAccepted)
}
```

- [ ] **Step 4: Wire `WatchDeployment` in main.go**

After `go k8s.WatchEndpoints(ctx)`, add:
```go
go k8s.WatchDeployment(ctx)
```

- [ ] **Step 5: Build check**

```
go build ./cmd/demo-ctl/
```

Expected: no errors.

- [ ] **Step 6: Run full test suite**

```
go test ./cmd/demo-ctl/
```

- [ ] **Step 7: Commit**

```
git add cmd/demo-ctl/server.go cmd/demo-ctl/main.go
git commit -m "feat: deployment API routes + WatchDeployment goroutine"
```

---

### Task 4: Frontend — store.js + Sidebar.svelte

**Files:**
- Modify: `web/src/store.js`
- Modify: `web/src/Sidebar.svelte`

**Interfaces:**
- Consumes: `PUT /api/deployment/replicas`, `PUT /api/deployment/strategy`, `PUT /api/deployment/grace`, `POST /api/deployment/rollout` (from Task 3)
- Consumes: SSE event `type: "deployment"` (DeploymentEvent shape from Task 1)
- Removes: `api.spawnPod`, `api.deletePod`
- Produces: `deployment` Svelte store (writable, DeploymentEvent | null)

- [ ] **Step 1: Update store.js**

Add `deployment` store export (near the top with other stores):
```js
export const deployment = writable(null) // DeploymentEvent | null
```

In the SSE `switch`, add after `case 'signal'`:
```js
case 'deployment':
  deployment.set(ev)
  break
```

In `api`, remove `spawnPod` and `deletePod`. Add:
```js
setReplicas:    (n)   => apiPut('/api/deployment/replicas', { replicas: n }),
setStrategy:    (s)   => apiPut('/api/deployment/strategy', { strategy: s }),
setGrace:       (sec) => apiPut('/api/deployment/grace',    { seconds: sec }),
triggerRollout: ()    => apiPost('/api/deployment/rollout'),
```

- [ ] **Step 2: Rewrite Sidebar.svelte script section**

Replace the entire `<script>` block with:

```js
<script>
  import { pods, signals, deployment, api } from './store.js'

  // Deployment controls — synced from backend once it arrives
  let strategy = $state('surge')
  let grace    = $state(30)

  $effect(() => {
    if ($deployment) {
      strategy = $deployment.strategy
      grace    = $deployment.gracePeriod
    }
  })

  async function scaleUp() {
    await api.setReplicas(($deployment?.replicas ?? 2) + 1)
  }
  async function scaleDown() {
    const cur = $deployment?.replicas ?? 2
    if (cur > 1) await api.setReplicas(cur - 1)
  }
  async function applyStrategy() {
    await api.setStrategy(strategy)
  }
  async function applyGrace() {
    await api.setGrace(Number(grace))
  }
  async function rollout() {
    await api.triggerRollout()
  }

  // Per-pod
  async function toggleReady(pod) {
    await api.setPodReady(pod.name, !pod.ready)
  }
  async function setDelay(name, ms) {
    await api.setPodDelay(name, ms)
  }

  // Traffic
  let trafficRunning = $state(false)
  let rps            = $state(5)
  let durationMs     = $state(0)

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

  // Cluster destroy
  let destroying     = $state(false)
  let destroyConfirm = $state(false)
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
```

- [ ] **Step 3: Rewrite Sidebar.svelte template section**

Replace everything between `<div class="sidebar">` and its closing `</div>` with:

```html
<div class="sidebar">
  <!-- Deployment -->
  <section class="grp">
    <h3>Deployment</h3>
    <div class="row">
      <button class="btn small" onclick={scaleDown}
        disabled={!$deployment || $deployment.replicas <= 1}>−</button>
      <span class="replica-count mono">{$deployment?.replicas ?? '…'}</span>
      <button class="btn small" onclick={scaleUp}
        disabled={!$deployment}>+</button>
      <button class="btn primary" onclick={rollout}
        disabled={!$deployment || $deployment.rolloutInProgress}>
        {$deployment?.rolloutInProgress ? 'rolling…' : 'Trigger rollout'}
      </button>
    </div>
    <div class="row">
      <label>Grace</label>
      <select class="sel" bind:value={grace} onchange={applyGrace} disabled={!$deployment}>
        <option value={5}>5s</option>
        <option value={15}>15s</option>
        <option value={30}>30s</option>
        <option value={60}>60s</option>
      </select>
    </div>
    <div class="row">
      <label>Strategy</label>
      <select class="sel" bind:value={strategy} onchange={applyStrategy} disabled={!$deployment}>
        <option value="surge">surge — zero downtime</option>
        <option value="rolling">rolling — one at a time</option>
        <option value="recreate">recreate — full restart</option>
      </select>
    </div>
    {#if $deployment}
      <p class="dep-status">
        {#if $deployment.rolloutInProgress}
          rolling out: {$deployment.updatedReplicas}/{$deployment.replicas} updated
        {:else}
          {$deployment.readyReplicas}/{$deployment.replicas} ready
        {/if}
      </p>
    {/if}
  </section>

  <!-- Pods -->
  <section class="grp">
    <h3>Pods</h3>
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
        </div>
      </div>
    {/each}
    {#if Object.keys($pods).length === 0}
      <p class="empty">No pods yet.</p>
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
      {#each $signals as sig, i (i)}
        <li class="ev {signalColors[sig.kind] ?? 'muted'}">
          <span class="ev-kind mono">{sig.kind}</span>
          <span class="ev-msg">{sig.message}</span>
        </li>
      {/each}
    </ol>
  </section>
</div>
```

- [ ] **Step 4: Add CSS for new elements to Sidebar.svelte**

Add inside `<style>` (keep all existing rules, add these):
```css
.replica-count { font: 700 1rem var(--font-mono); min-width: 2rem; text-align: center; }
.dep-status { font-size: .75rem; color: var(--muted); margin: 0; }
```

- [ ] **Step 5: Build Svelte**

```
cd web && npm run build
```

Expected: build succeeds, no errors (warnings about label accessibility are pre-existing and acceptable).

- [ ] **Step 6: Build Go with new embed**

```
cd .. && go build ./cmd/demo-ctl/
```

- [ ] **Step 7: Run full test suite**

```
go test ./...
```

- [ ] **Step 8: Commit**

```
git add web/src/store.js web/src/Sidebar.svelte cmd/demo-ctl/assets/
git commit -m "feat: deployment controls in sidebar — replicas, strategy, grace, rollout"
```

- [ ] **Step 9: Final push**

```
git push
```

---

## Self-Review

**Spec coverage:**
- Bootstrap auto-creates Deployment with 2 replicas, surge strategy, 30s grace ✓
- `cs kubernetes.Interface` on `Cluster` (enables fake in tests) ✓
- `POD_NAME` via downward API `fieldRef: metadata.name` ✓
- `ScaleDeployment`, `SetStrategy`, `SetGracePeriod`, `TriggerRollout`, `WatchDeployment` ✓
- `DeploymentEvent` with all required fields ✓
- Preset presets: "surge" (maxU=0, maxS=1), "rolling" (maxU=1, maxS=0), "recreate" ✓
- Grace period options: 5s, 15s, 30s, 60s ✓
- Rollout trigger via `demo/rolledAt` annotation ✓
- Remove `POST /api/pod/spawn`, `DELETE /api/pod/{name}` ✓
- Add 4 deployment routes ✓
- Frontend: replica ±, strategy select, grace select, rollout button, status line ✓
- Delete button removed from pod rows ✓
- Per-pod ready/delay controls preserved ✓
- `WatchDeployment` goroutine in main.go ✓
- No changes to WatchPods, WatchEndpoints, signal pulses, traffic dots ✓

**Placeholder scan:** none — all code is complete.

**Type consistency:**
- Preset strings `"surge"`, `"rolling"`, `"recreate"` used verbatim in `strategyForPreset`, `presetForStrategy`, frontend selects, and tests
- `DeploymentEvent` fields match JSON keys used in store.js (`replicas`, `readyReplicas`, `updatedReplicas`, `availableReplicas`, `rolloutInProgress`, `strategy`, `gracePeriod`)
- `int32ptr` helper defined in k8s_test.go for use in `TestWatchDeploymentPublishesEvent`
