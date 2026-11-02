package main

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
)

// fakeMapper is a minimal restMapper for tests.
type fakeMapper struct {
	m map[schema.GroupKind]apimeta.RESTMapping
}

func (f *fakeMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*apimeta.RESTMapping, error) {
	if m, ok := f.m[gk]; ok {
		return &m, nil
	}
	return nil, fmt.Errorf("no mapping for %v", gk)
}

type rootScope struct{}

func (rootScope) Name() apimeta.RESTScopeName { return apimeta.RESTScopeNameRoot }

type nsScope struct{}

func (nsScope) Name() apimeta.RESTScopeName { return apimeta.RESTScopeNameNamespace }

var (
	nsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	cmGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	baseMapper = &fakeMapper{m: map[schema.GroupKind]apimeta.RESTMapping{
		{Group: "", Kind: "Namespace"}: {
			Resource:         nsGVR,
			GroupVersionKind: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"},
			Scope:            rootScope{},
		},
		{Group: "", Kind: "ConfigMap"}: {
			Resource:         cmGVR,
			GroupVersionKind: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			Scope:            nsScope{},
		},
	}}
)

func newFakeDC(t *testing.T) *dynfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	return dynfake.NewSimpleDynamicClient(scheme)
}

func TestApplyManifests_CreatesClusterScopedResource(t *testing.T) {
	dc := newFakeDC(t)
	manifest := []byte(`apiVersion: v1
kind: Namespace
metadata:
  name: test-ns
`)
	if err := applyManifests(context.Background(), dc, baseMapper, manifest); err != nil {
		t.Fatalf("applyManifests: %v", err)
	}
	if _, err := dc.Resource(nsGVR).Get(context.Background(), "test-ns", metav1.GetOptions{}); err != nil {
		t.Errorf("namespace not found: %v", err)
	}
}

func TestApplyManifests_CreatesNamespacedResource(t *testing.T) {
	dc := newFakeDC(t)
	manifest := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: test-cm
  namespace: kube-system
`)
	if err := applyManifests(context.Background(), dc, baseMapper, manifest); err != nil {
		t.Fatalf("applyManifests: %v", err)
	}
	if _, err := dc.Resource(cmGVR).Namespace("kube-system").Get(context.Background(), "test-cm", metav1.GetOptions{}); err != nil {
		t.Errorf("configmap not found: %v", err)
	}
}

func TestApplyManifests_MultiDoc(t *testing.T) {
	dc := newFakeDC(t)
	manifest := []byte(`apiVersion: v1
kind: Namespace
metadata:
  name: ns-a
---
apiVersion: v1
kind: Namespace
metadata:
  name: ns-b
`)
	if err := applyManifests(context.Background(), dc, baseMapper, manifest); err != nil {
		t.Fatalf("applyManifests: %v", err)
	}
	for _, name := range []string{"ns-a", "ns-b"} {
		if _, err := dc.Resource(nsGVR).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
			t.Errorf("namespace %s not found: %v", name, err)
		}
	}
}

func TestApplyManifests_IdempotentOnAlreadyExists(t *testing.T) {
	dc := newFakeDC(t)
	manifest := []byte(`apiVersion: v1
kind: Namespace
metadata:
  name: test-ns
`)
	if err := applyManifests(context.Background(), dc, baseMapper, manifest); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := applyManifests(context.Background(), dc, baseMapper, manifest); err != nil {
		t.Errorf("second apply should be idempotent: %v", err)
	}
}
