package main

import (
	"context"
	"log"
	"net/http"
	"os/exec"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	hub := NewHub()

	cluster := NewCluster(hub)
	if err := cluster.Bootstrap(ctx); err != nil {
		log.Fatalf("bootstrap cluster: %v", err)
	}

	k8s := NewK8s(hub, cluster.Clientset(), cluster.RESTConfig())
	go k8s.WatchPods(ctx)
	go k8s.WatchEndpoints(ctx)
	go k8s.WatchDeployment(ctx)

	lg := NewLoadGen(hub, k8s, "http://localhost:8080/")

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
		cluster.Destroy(context.Background())
		httpSrv.Shutdown(context.Background())
	}()

	log.Println("demo-ctl: listening on http://localhost:9090")
	if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func openBrowser(url string) {
	// macOS
	exec.Command("open", url).Start()
}
