package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	cs   kubernetes.Interface
	rcfg *rest.Config

	mu       sync.Mutex
	inFlight map[string]int
	podSpecs map[string]*corev1.Pod
}

func NewK8s(pub Publisher, cs kubernetes.Interface, rcfg *rest.Config) *K8s {
	return &K8s{
		pub:      pub,
		cs:       cs,
		rcfg:     rcfg,
		inFlight: make(map[string]int),
		podSpecs: make(map[string]*corev1.Pod),
	}
}

// SetReady calls POST /admin/ready on the pod via the API server pod proxy.
func (k *K8s) SetReady(ctx context.Context, name string, ready bool) error {
	body, err := json.Marshal(map[string]bool{"ready": ready})
	if err != nil {
		return fmt.Errorf("marshal ready: %w", err)
	}
	return k.podProxy(ctx, name, "POST", "admin/ready", body)
}

// SetDelay calls POST /admin/delay on the pod via the API server pod proxy.
func (k *K8s) SetDelay(ctx context.Context, name string, ms int64) error {
	body, err := json.Marshal(map[string]int64{"ms": ms})
	if err != nil {
		return fmt.Errorf("marshal delay: %w", err)
	}
	return k.podProxy(ctx, name, "POST", "admin/delay", body)
}

// InFlightInc increments the in-flight counter for a pod (called by loadgen).
func (k *K8s) InFlightInc(podName string) {
	k.mu.Lock()
	k.inFlight[podName]++
	k.mu.Unlock()
}

// InFlightDec decrements the in-flight counter for a pod (called by loadgen).
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

// TotalInFlight returns the sum of all in-flight counters across all pods.
func (k *K8s) TotalInFlight() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	total := 0
	for _, v := range k.inFlight {
		total += v
	}
	return total
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
			if terminating {
				if rem := time.Until(pod.DeletionTimestamp.Time); rem > 0 {
					graceRemaining = int(rem.Seconds())
				}
			}

			inFlight := k.getInFlight(pod.Name)
			if terminating {
				inFlight = k.TotalInFlight()
			}
			k.pub.Publish(PodEvent{
				Type:           EventPod,
				Name:           pod.Name,
				Phase:          string(pod.Status.Phase),
				Ready:          ready,
				Terminating:    terminating,
				GraceRemaining: graceRemaining,
				GracePeriod:    int32(podGracePeriod(pod)), //nolint:gosec // G115: grace period in seconds, always within int32 range
				PreStopSleep:   podPreStopSleep(pod),
				InFlight:       inFlight,
			})

			if ev.Type == watch.Modified {
				if was, seen := prevReady[pod.Name]; seen && was != ready {
					if ready {
						k.pub.Publish(SignalEvent{
							Type:    EventSignal,
							Kind:    SignalProbeOK,
							PodName: pod.Name,
							Message: pod.Name + ": readiness probe passing",
						})
					} else {
						k.pub.Publish(SignalEvent{
							Type:    EventSignal,
							Kind:    SignalProbeFail,
							PodName: pod.Name,
							Message: pod.Name + ": readiness probe failed",
						})
					}
				}
				if terminating {
					k.mu.Lock()
					prev, alreadySeen := k.podSpecs[pod.Name]
					wasTerm := alreadySeen && prev.DeletionTimestamp != nil
					k.mu.Unlock()
					if alreadySeen && !wasTerm {
						k.pub.Publish(SignalEvent{
							Type:    EventSignal,
							Kind:    SignalSIGTERM,
							PodName: pod.Name,
							Message: fmt.Sprintf("%s: SIGTERM sent (grace: %ds)", pod.Name, podGracePeriod(pod)),
						})
						// Tick every second to keep graceRemaining counting down.
						go func(podName string) {
							ticker := time.NewTicker(time.Second)
							defer ticker.Stop()
							for {
								select {
								case <-ticker.C:
									k.mu.Lock()
									p, ok := k.podSpecs[podName]
									k.mu.Unlock()
									if !ok || p.DeletionTimestamp == nil {
										return
									}
									remaining := int(time.Until(p.DeletionTimestamp.Time).Seconds())
									if remaining <= 0 {
										return
									}
									k.pub.Publish(PodEvent{
										Type:           EventPod,
										Name:           podName,
										Phase:          string(p.Status.Phase),
										Ready:          isPodReady(p),
										Terminating:    true,
										GraceRemaining: remaining,
										GracePeriod:    int32(podGracePeriod(p)), //nolint:gosec // G115: grace period in seconds, always within int32 range
										PreStopSleep:   podPreStopSleep(p),
										InFlight:       k.TotalInFlight(),
									})
								case <-ctx.Done():
									return
								}
							}
						}(pod.Name)
					}
				}
			}

			if ev.Type == watch.Deleted {
				k.pub.Publish(SignalEvent{
					Type:    EventSignal,
					Kind:    SignalDrained,
					PodName: pod.Name,
					Message: pod.Name + ": pod exited",
				})
				k.pub.Publish(PodEvent{
					Type:    EventPod,
					Name:    pod.Name,
					Deleted: true,
				})
				k.mu.Lock()
				delete(k.podSpecs, pod.Name)
				delete(k.inFlight, pod.Name)
				k.mu.Unlock()
			} else {
				k.mu.Lock()
				k.podSpecs[pod.Name] = pod.DeepCopy()
				k.mu.Unlock()
			}

			prevReady[pod.Name] = ready
		}
		time.Sleep(2 * time.Second)
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
					if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" {
						currentPods[ep.TargetRef.Name] = true
					}
				}
			}

			names := make([]string, 0, len(currentPods))
			for n := range currentPods {
				names = append(names, n)
			}
			k.pub.Publish(EndpointsEvent{Type: EventEndpoints, Pods: names})

			for name := range currentPods {
				if !prevPods[name] {
					k.pub.Publish(SignalEvent{
						Type:    EventSignal,
						Kind:    SignalEndpointAdd,
						PodName: name,
						Message: name + ": added to endpoints",
					})
				}
			}
			for name := range prevPods {
				if !currentPods[name] {
					k.pub.Publish(SignalEvent{
						Type:    EventSignal,
						Kind:    SignalEndpointRemove,
						PodName: name,
						Message: name + ": removed from endpoints",
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
	result := k.cs.CoreV1().RESTClient().Verb(method).
		Namespace(demoNamespace).
		Resource("pods").
		Name(podName).
		SubResource("proxy").
		Suffix(path).
		Body(bytes.NewReader(body)).
		Do(ctx)
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

func podGracePeriod(pod *corev1.Pod) int64 {
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		return *pod.Spec.TerminationGracePeriodSeconds
	}
	return terminationGracePeriod
}

func podPreStopSleep(pod *corev1.Pod) int32 {
	for _, c := range pod.Spec.Containers {
		if c.Name != "demo-server" {
			continue
		}
		if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
			return 0
		}
		cmd := c.Lifecycle.PreStop.Exec.Command
		if len(cmd) == 2 && cmd[0] == "sleep" {
			if n, err := strconv.ParseInt(cmd[1], 10, 32); err == nil {
				return int32(n)
			}
		}
		return 0
	}
	return 0
}

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

func (k *K8s) ScaleDeployment(ctx context.Context, replicas int32) error {
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{"replicas": replicas},
	})
	if err != nil {
		return fmt.Errorf("marshal scale patch: %w", err)
	}
	_, err = k.cs.AppsV1().Deployments(demoNamespace).Patch(
		ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

func (k *K8s) SetStrategy(ctx context.Context, preset string) error {
	// JSON merge patch must explicitly null rollingUpdate when switching to
	// Recreate; omitting it leaves the existing rollingUpdate object in place,
	// which k8s rejects as forbidden when type=Recreate.
	var stratPatch any
	if preset == "recreate" {
		stratPatch = map[string]any{"type": "Recreate", "rollingUpdate": nil}
	} else {
		s := strategyForPreset(preset)
		stratPatch = s
	}
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{"strategy": stratPatch},
	})
	if err != nil {
		return fmt.Errorf("marshal strategy patch: %w", err)
	}
	_, err = k.cs.AppsV1().Deployments(demoNamespace).Patch(
		ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

func (k *K8s) SetGracePeriod(ctx context.Context, seconds int32) error {
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"terminationGracePeriodSeconds": seconds,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal grace period patch: %w", err)
	}
	_, err = k.cs.AppsV1().Deployments(demoNamespace).Patch(
		ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

func (k *K8s) TriggerRollout(ctx context.Context) error {
	patch, err := json.Marshal(map[string]any{
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
	if err != nil {
		return fmt.Errorf("marshal rollout patch: %w", err)
	}
	_, err = k.cs.AppsV1().Deployments(demoNamespace).Patch(
		ctx, "demo", types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

// SetPreStopSleep patches the demo-server container's preStop exec hook to
// sleep for the given number of seconds. seconds=0 removes the hook.
// Uses strategic merge patch so only the named container is updated.
func (k *K8s) SetPreStopSleep(ctx context.Context, seconds int32) error {
	var lifecycle any
	if seconds > 0 {
		lifecycle = map[string]any{
			"preStop": map[string]any{
				"exec": map[string]any{
					"command": []string{"sleep", strconv.Itoa(int(seconds))},
				},
			},
		}
	}
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{{
						"name":      "demo-server",
						"lifecycle": lifecycle,
					}},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal prestop patch: %w", err)
	}
	_, err = k.cs.AppsV1().Deployments(demoNamespace).Patch(
		ctx, "demo", types.StrategicMergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

// preStopSleep reads the sleep duration from the demo-server container's
// preStop exec hook. Returns 0 if no hook is set.
func preStopSleep(dep *appsv1.Deployment) int32 {
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "demo-server" {
			continue
		}
		if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
			return 0
		}
		cmd := c.Lifecycle.PreStop.Exec.Command
		if len(cmd) == 2 && cmd[0] == "sleep" {
			if n, err := strconv.ParseInt(cmd[1], 10, 32); err == nil {
				return int32(n)
			}
		}
		return 0
	}
	return 0
}

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
				grace = int32(*dep.Spec.Template.Spec.TerminationGracePeriodSeconds) //nolint:gosec // G115: grace period in seconds, always within int32 range
			}
			k.pub.Publish(DeploymentEvent{
				Type:              EventDeployment,
				Replicas:          desired,
				ReadyReplicas:     dep.Status.ReadyReplicas,
				UpdatedReplicas:   dep.Status.UpdatedReplicas,
				AvailableReplicas: dep.Status.AvailableReplicas,
				RolloutInProgress: dep.Status.UpdatedReplicas < desired ||
					dep.Status.AvailableReplicas < desired,
				Strategy:     presetForStrategy(dep.Spec.Strategy),
				GracePeriod:  grace,
				PreStopSleep: preStopSleep(dep),
			})
		}
		time.Sleep(2 * time.Second)
	}
}
