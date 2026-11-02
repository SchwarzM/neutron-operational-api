# Kubernetes Readiness Demo

An interactive demo showing how Kubernetes readiness probes, rolling deployments, and connection draining work together. A local kind cluster runs the demo; a browser UI lets you trigger rolling updates, toggle readiness, and watch live traffic routing.

## Prerequisites

- [Go](https://go.dev/dl/) 1.21+
- [Docker](https://docs.docker.com/get-docker/)
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)

## Run

```sh
make demo
```

This builds the frontend, compiles the Go binaries, creates a local kind cluster, loads the demo image, and opens the UI at http://localhost:9090.

Press Ctrl-C to stop. The kind cluster is deleted on exit.

## Clean up

```sh
make clean
```
