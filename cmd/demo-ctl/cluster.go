package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "embed"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

//go:embed kind-config.yaml
var kindConfigYAML []byte

//go:embed nginx-ingress-kind.yaml
var nginxIngressYAML []byte

const (
	clusterName   = "k8s-readiness-demo"
	demoNamespace = "demo"
	ghcrImage     = "ghcr.io/schwarzm/neutron-operational-api:edge"
)

// Cluster owns the kind cluster lifecycle and bootstraps all k8s objects.
type Cluster struct {
	pub  Publisher
	cs   kubernetes.Interface
	rcfg *rest.Config
}

func NewCluster(pub Publisher) *Cluster {
	return &Cluster{pub: pub}
}

// Bootstrap creates the kind cluster, installs nginx-ingress, and creates
// the demo namespace, Service, and Ingress. Blocks until the cluster is ready.
func (c *Cluster) Bootstrap(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "creating"})

	cfgFile, err := os.CreateTemp("", "kind-config-*.yaml")
	if err != nil {
		return fmt.Errorf("write kind config: %w", err)
	}
	defer os.Remove(cfgFile.Name())
	if _, err = cfgFile.Write(kindConfigYAML); err != nil {
		return fmt.Errorf("write kind config content: %w", err)
	}
	cfgFile.Close()

	// Always start clean: delete any existing cluster first.
	existOut, err := exec.CommandContext(ctx, "kind", "get", "clusters").Output()
	if err != nil {
		existOut = nil // kind not available or failed; proceed, subsequent calls will report errors
	}
	if bytes.Contains(existOut, []byte(clusterName)) {
		if err := kindRun(ctx, "delete", "cluster", "--name", clusterName); err != nil {
			return fmt.Errorf("kind delete cluster: %w", err)
		}
	}
	if err := kindRun(ctx, "create", "cluster", "--name", clusterName, "--config", cfgFile.Name()); err != nil {
		return fmt.Errorf("kind create cluster: %w", err)
	}

	kubeconfig, err := kindOutput(ctx, "get", "kubeconfig", "--name", clusterName)
	if err != nil {
		return fmt.Errorf("get kubeconfig: %w", err)
	}

	c.rcfg, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("parse kubeconfig: %w", err)
	}
	c.cs, err = kubernetes.NewForConfig(c.rcfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	if err := applyManifestsFromConfig(ctx, c.rcfg, nginxIngressYAML); err != nil {
		return fmt.Errorf("apply nginx-ingress: %w", err)
	}

	if err := c.waitForIngressController(ctx); err != nil {
		return fmt.Errorf("wait for ingress-controller ready: %w", err)
	}

	if err := c.createNamespace(ctx); err != nil {
		return err
	}
	if err := c.createService(ctx); err != nil {
		return err
	}
	// Retry until the admission webhook endpoint is accepting connections.
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			err := c.createIngress(ctx)
			if err == nil {
				return true, nil
			}
			if strings.Contains(err.Error(), "webhook") {
				return false, nil // webhook not ready yet — retry
			}
			return false, err
		}); err != nil {
		return fmt.Errorf("create ingress: %w", err)
	}
	if err := c.createDeployment(ctx); err != nil {
		return err
	}

	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "ready"})
	return nil
}

// Destroy deletes the kind cluster.
func (c *Cluster) Destroy(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "destroying"})
	return kindRun(ctx, "delete", "cluster", "--name", clusterName)
}

// Clientset returns the k8s client (valid after Bootstrap).
func (c *Cluster) Clientset() kubernetes.Interface { return c.cs }

// RESTConfig returns the REST config (valid after Bootstrap).
func (c *Cluster) RESTConfig() *rest.Config { return c.rcfg }

func (c *Cluster) waitForIngressController(ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, 3*time.Second, 4*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			pods, err := c.cs.CoreV1().Pods("ingress-nginx").List(ctx, metav1.ListOptions{
				LabelSelector: "app.kubernetes.io/component=controller",
			})
			if err != nil {
				return false, err
			}
			if len(pods.Items) == 0 {
				return false, nil
			}
			for _, p := range pods.Items {
				for _, cond := range p.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						return true, nil
					}
				}
			}
			return false, nil
		})
}

func (c *Cluster) createNamespace(ctx context.Context) error {
	ns := &corev1.Namespace{Name: demoNamespace}
	_, err := c.cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *Cluster) createService(ctx context.Context) error {
	svc := &corev1.Service{
		Name: "demo-svc", Namespace: demoNamespace,
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{{
				Port:       80,
				TargetPort: intstr.FromInt32(8080),
			}},
		},
	}
	_, err := c.cs.CoreV1().Services(demoNamespace).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *Cluster) createIngress(ctx context.Context) error {
	pathType := networkingv1.PathTypePrefix
	className := "nginx"
	ing := &networkingv1.Ingress{
		Name:      "demo",
		Namespace: demoNamespace,
		Spec: networkingv1.IngressSpec{
			IngressClassName: &className,
			Rules: []networkingv1.IngressRule{{
				HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: "demo-svc",
								Port: networkingv1.ServiceBackendPort{Number: 80},
							},
						},
					}},
				},
			}},
		},
	}
	_, err := c.cs.NetworkingV1().Ingresses(demoNamespace).Create(ctx, ing, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (c *Cluster) createDeployment(ctx context.Context) error {
	replicas := int32(2)
	grace := int64(terminationGracePeriod)
	maxU := intstr.FromInt32(0)
	maxS := intstr.FromInt32(1)
	dep := &appsv1.Deployment{
		Name: "demo", Namespace: demoNamespace,
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
						Image:           ghcrImage,
						ImagePullPolicy: corev1.PullAlways,
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
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/readyz",
								Port: intstr.FromInt32(8080),
							},
							PeriodSeconds:    2,
							FailureThreshold: 1,
							SuccessThreshold: 1,
						},
						LivenessProbe: &corev1.Probe{
							HTTPGet: &corev1.HTTPGetAction{
								Path: "/livez",
								Port: intstr.FromInt32(8080),
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

func kindRun(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "kind", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func kindOutput(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "kind", args...).Output()
}
