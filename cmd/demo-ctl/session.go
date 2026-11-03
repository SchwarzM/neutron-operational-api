package main

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	ghcrImage = "ghcr.io/schwarzm/neutron-operational-api:edge"
)

func sessionNamespace(id string) string { return "demo-" + id }

// createSessionNamespace creates the demo-{id} namespace and all session-scoped
// k8s objects: demo-server Deployment, demo-svc, loadgen Deployment, loadgen-svc.
func createSessionNamespace(ctx context.Context, cs kubernetes.Interface, id, controlPlaneURL string) error {
	ns := sessionNamespace(id)
	if err := createNs(ctx, cs, ns); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	if err := createDemoSvc(ctx, cs, ns); err != nil {
		return fmt.Errorf("demo-svc: %w", err)
	}
	if err := createDemoDeployment(ctx, cs, ns); err != nil {
		return fmt.Errorf("demo deployment: %w", err)
	}
	if err := createLoadgenSvc(ctx, cs, ns); err != nil {
		return fmt.Errorf("loadgen-svc: %w", err)
	}
	if err := createLoadgenDeployment(ctx, cs, ns, id, controlPlaneURL); err != nil {
		return fmt.Errorf("loadgen deployment: %w", err)
	}
	return nil
}

// destroySession deletes the session namespace and everything in it.
func destroySession(ctx context.Context, cs kubernetes.Interface, id string) error {
	err := cs.CoreV1().Namespaces().Delete(ctx, sessionNamespace(id), metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

func createNs(ctx context.Context, cs kubernetes.Interface, ns string) error {
	_, err := cs.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, //nolint:modernize
		metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createDemoSvc(ctx context.Context, cs kubernetes.Interface, ns string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-svc", Namespace: ns}, //nolint:modernize
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{{
				Port: 80, TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createDemoDeployment(ctx context.Context, cs kubernetes.Interface, ns string) error {
	replicas := int32(2)
	grace := int64(terminationGracePeriod)
	maxU := intstr.FromInt32(0)
	maxS := intstr.FromInt32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns}, //nolint:modernize
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &maxU, MaxSurge: &maxS,
				},
			},
			Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &grace,
					Containers: []corev1.Container{{
						Name:            "demo-server",
						Image:           ghcrImage,
						ImagePullPolicy: corev1.PullAlways,
						Env: []corev1.EnvVar{{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							},
						}},
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{ //nolint:modernize
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz", Port: intstr.FromInt32(8080),
								},
							},
							PeriodSeconds: 2, FailureThreshold: 1, SuccessThreshold: 1,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{ //nolint:modernize
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/livez", Port: intstr.FromInt32(8080),
								},
							},
							PeriodSeconds: 10, FailureThreshold: 3,
						},
					}},
				},
			},
		},
	}
	_, err := cs.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createLoadgenSvc(ctx context.Context, cs kubernetes.Interface, ns string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "loadgen-svc", Namespace: ns}, //nolint:modernize
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "loadgen"},
			Ports: []corev1.ServicePort{{
				Port: 8080, TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func createLoadgenDeployment(ctx context.Context, cs kubernetes.Interface, ns, id, controlPlaneURL string) error {
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "loadgen", Namespace: ns}, //nolint:modernize
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "loadgen"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "loadgen"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            "loadgen",
						Image:           ghcrImage,
						ImagePullPolicy: corev1.PullAlways,
						Command:         []string{"/loadgen"},
						Env: []corev1.EnvVar{
							{Name: "TARGET_URL", Value: "http://demo-svc." + ns + ".svc.cluster.local/"},
							{Name: "SESSION_ID", Value: id},
							{Name: "CONTROL_PLANE_URL", Value: controlPlaneURL},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
					}},
				},
			},
		},
	}
	_, err := cs.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
