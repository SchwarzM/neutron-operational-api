package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

type restMapper interface {
	RESTMapping(gk schema.GroupKind, versions ...string) (*apimeta.RESTMapping, error)
}

// applyManifests applies a multi-doc YAML manifest using create semantics.
// Already-existing resources are silently skipped (idempotent for fresh clusters).
func applyManifests(ctx context.Context, dc dynamic.Interface, mapper restMapper, manifest []byte) error {
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		var raw runtime.RawExtension
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("decode manifest: %w", err)
		}
		if len(raw.Raw) == 0 {
			continue
		}

		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(raw.Raw); err != nil {
			return fmt.Errorf("unmarshal object: %w", err)
		}

		gvk := obj.GroupVersionKind()
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return fmt.Errorf("rest mapping for %s: %w", gvk.Kind, err)
		}

		var dr dynamic.ResourceInterface
		if mapping.Scope.Name() == apimeta.RESTScopeNameNamespace {
			ns := obj.GetNamespace()
			if ns == "" {
				ns = "default"
			}
			dr = dc.Resource(mapping.Resource).Namespace(ns)
		} else {
			dr = dc.Resource(mapping.Resource)
		}

		if _, err = dr.Create(ctx, obj, metav1.CreateOptions{}); err != nil && !k8serrors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s %s: %w", gvk.Kind, obj.GetName(), err)
		}
	}
}

func applyManifestsFromConfig(ctx context.Context, cfg *rest.Config, manifest []byte) error {
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disc))
	return applyManifests(ctx, dc, mapper, manifest)
}
