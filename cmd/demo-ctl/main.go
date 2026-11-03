package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	hub := NewHub()

	cluster := NewCluster(hub)
	if err := cluster.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap cluster: %w", err)
	}

	k8s := NewK8s(hub, cluster.Clientset(), cluster.RESTConfig(), "", func() int { return 0 })
	go k8s.WatchPods(ctx)
	go k8s.WatchEndpoints(ctx)
	go k8s.WatchDeployment(ctx)

	lg := NewLoadGen(hub, k8s, cluster.IngressURL())

	srv := NewAPIServer(hub, cluster, k8s, lg, stop)

	httpSrv := &http.Server{
		Addr:    ":9090",
		Handler: srv.Handler(),
	}

	// Open browser
	go openBrowser("http://localhost:9090")

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		lg.Stop()
		if err := cluster.Destroy(context.Background()); err != nil {
			log.Printf("destroy cluster: %v", err)
		}
		if err := httpSrv.Shutdown(context.Background()); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	log.Println("demo-ctl: listening on http://localhost:9090")
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func openBrowser(url string) {
	// macOS
	exec.Command("open", url).Start() //nolint:errcheck // best-effort browser open, ignore failure
}
