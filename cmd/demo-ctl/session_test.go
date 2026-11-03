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
