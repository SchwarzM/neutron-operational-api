package main

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestWatchPodsPublishesDeletedEvent verifies that when a pod is deleted from
// k8s, WatchPods publishes a PodEvent{Deleted:true} so the frontend can remove
// it from the UI.
func TestWatchPodsPublishesDeletedEvent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	fakeWatcher := watch.NewFake()
	cs.PrependWatchReactor("pods", func(_ k8stesting.Action) (bool, watch.Interface, error) {
		return true, fakeWatcher, nil
	})

	pub := &capPublisher{}
	k := NewK8s(pub, cs, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go k.WatchPods(ctx)
	time.Sleep(50 * time.Millisecond) // let watch loop start

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-abc123",
			Namespace: demoNamespace,
		},
	}
	fakeWatcher.Delete(pod)
	time.Sleep(50 * time.Millisecond) // let event propagate

	for _, ev := range pub.events {
		if pe, ok := ev.(PodEvent); ok && pe.Name == "demo-abc123" && pe.Deleted {
			return // test passes
		}
	}
	t.Fatal("expected PodEvent{Deleted:true, Name:\"demo-abc123\"} but did not find it in published events")
}

func int32ptr(n int32) *int32 { return &n }

func TestStrategyForPreset(t *testing.T) {
	cases := []struct {
		preset     string
		wantType   appsv1.DeploymentStrategyType
		maxU, maxS int32
	}{
		{"surge", appsv1.RollingUpdateDeploymentStrategyType, 0, 1},
		{"rolling", appsv1.RollingUpdateDeploymentStrategyType, 1, 0},
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
