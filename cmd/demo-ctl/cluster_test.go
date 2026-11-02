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
