package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	rcfg, err := buildConfig()
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rcfg)
	if err != nil {
		return fmt.Errorf("build clientset: %w", err)
	}

	controlPlaneURL := os.Getenv("CONTROL_PLANE_URL")
	if controlPlaneURL == "" {
		controlPlaneURL = "http://localhost:9090"
	}

	sessions := NewSessionManager(cs, controlPlaneURL)
	srv := NewAPIServer(sessions)

	httpSrv := &http.Server{
		Addr:    ":9090",
		Handler: srv.Handler(),
	}

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		httpSrv.Shutdown(context.Background()) //nolint:errcheck
	}()

	log.Println("demo-ctl: listening on http://localhost:9090")
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

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
