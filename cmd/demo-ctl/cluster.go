package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

const (
	demoNamespace    = "demo"
	ghcrImage        = "ghcr.io/schwarzm/neutron-operational-api:edge"
	ingressClassName = "nginx"
)

// Cluster bootstraps demo k8s objects in an existing cluster.
type Cluster struct {
	pub        Publisher
	cs         kubernetes.Interface
	rcfg       *rest.Config
	ingressURL string
}

func NewCluster(pub Publisher) *Cluster {
	return &Cluster{pub: pub}
}

// Bootstrap connects to an existing cluster and creates the demo namespace,
// Deployment, Service, and Ingress. Blocks until the Ingress has an external address.
func (c *Cluster) Bootstrap(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "creating"})

	var err error
	c.rcfg, err = buildConfig()
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	c.cs, err = kubernetes.NewForConfig(c.rcfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	if err := c.createNamespace(ctx); err != nil {
		return err
	}
	if err := c.createService(ctx); err != nil {
		return err
	}
	if err := c.createIngress(ctx); err != nil {
		return fmt.Errorf("create ingress: %w", err)
	}
	if err := c.createDeployment(ctx); err != nil {
		return err
	}

	addr, err := c.waitForIngressAddress(ctx)
	if err != nil {
		return fmt.Errorf("wait for ingress address: %w", err)
	}
	c.ingressURL = "http://" + addr + "/"

	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "ready"})
	return nil
}

// Destroy removes the demo namespace and everything in it.
func (c *Cluster) Destroy(ctx context.Context) error {
	c.pub.Publish(ClusterEvent{Type: EventCluster, State: "destroying"})
	err := c.cs.CoreV1().Namespaces().Delete(ctx, demoNamespace, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

// IngressURL returns the external URL of the demo ingress (valid after Bootstrap).
func (c *Cluster) IngressURL() string { return c.ingressURL }

// Clientset returns the k8s client (valid after Bootstrap).
func (c *Cluster) Clientset() kubernetes.Interface { return c.cs }

// RESTConfig returns the REST config (valid after Bootstrap).
func (c *Cluster) RESTConfig() *rest.Config { return c.rcfg }

// buildConfig returns an in-cluster config when running inside a pod,
// otherwise falls back to KUBECONFIG env / ~/.kube/config.
func buildConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func (c *Cluster) waitForIngressAddress(ctx context.Context) (string, error) {
	var addr string
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			ing, err := c.cs.NetworkingV1().Ingresses(demoNamespace).Get(ctx, "demo", metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			for _, lb := range ing.Status.LoadBalancer.Ingress {
				if lb.Hostname != "" {
					addr = lb.Hostname
					return true, nil
				}
				if lb.IP != "" {
					addr = lb.IP
					return true, nil
				}
			}
			return false, nil
		})
	return addr, err
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
	className := ingressClassName
	ing := &networkingv1.Ingress{
		Name: "demo", Namespace: demoNamespace,
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
