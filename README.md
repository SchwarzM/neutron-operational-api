# Kubernetes Readiness Demo

An interactive demo showing how Kubernetes readiness probes, rolling deployments, and connection draining work together. A local kind cluster runs the demo; a browser UI lets you trigger rolling updates, toggle readiness, and watch live traffic routing.

## Quick start (no Go required)

Prerequisites: [Docker](https://docs.docker.com/get-docker/) and [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)

1. Download `demo-ctl` for your platform from the [latest release](https://github.com/SchwarzM/neutron-operational-api/releases/latest)
2. Run the demo:

```sh
make demo-quick
```

or without make:

```sh
docker pull ghcr.io/schwarzm/neutron-operational-api:edge
docker tag ghcr.io/schwarzm/neutron-operational-api:edge demo-server:local
./demo-ctl
```

The demo opens at http://localhost:9090. Press Ctrl-C to stop — the kind cluster is deleted on exit.

## Build from source

Prerequisites: [Go](https://go.dev/dl/) 1.27+, [Docker](https://docs.docker.com/get-docker/), [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)

```sh
make demo
```

This builds the frontend, compiles the Go binaries, creates a local kind cluster, loads the demo image, and opens the UI at http://localhost:9090.

## Clean up

```sh
make clean
```
