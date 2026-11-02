.PHONY: demo build build-go build-web build-image clean test check-prereqs

CLUSTER_NAME = k8s-readiness-demo
IMAGE        = demo-server:local

check-prereqs:
	@for tool in go docker kind; do \
		command -v $$tool >/dev/null 2>&1 || { \
			echo "missing prerequisite: $$tool"; \
			case $$tool in \
				go)     echo "  install: https://go.dev/dl/";; \
				docker) echo "  install: https://docs.docker.com/get-docker/";; \
				kind)   echo "  install: https://kind.sigs.k8s.io/docs/user/quick-start/#installation";; \
			esac; \
			exit 1; \
		}; \
	done

# Run the full demo: build everything, start demo-ctl (which bootstraps kind)
demo: check-prereqs build
	go run ./cmd/demo-ctl/

# Build all artefacts
build: build-image build-go

# Build the in-pod container image and load it into kind (if cluster exists)
build-image:
	docker build -f Dockerfile.demo-server -t $(IMAGE) .
	-kind load docker-image $(IMAGE) --name $(CLUSTER_NAME) 2>/dev/null || true

# Compile the Go binaries
build-go: build-web
	go build -o bin/demo-ctl  ./cmd/demo-ctl/
	go build -o bin/demo-server ./cmd/demo-server/

# Build the Svelte frontend inside a Docker container (no local node/npm needed)
build-web:
	docker run --rm \
		-v "$(CURDIR)/web:/app" \
		-w /app \
		node:lts-alpine \
		sh -c "npm install --prefer-offline && npm run build"

# Delete the kind cluster and clean build artefacts
clean:
	-kind delete cluster --name $(CLUSTER_NAME) 2>/dev/null || true
	rm -rf bin/ cmd/demo-ctl/assets/
	cd web && rm -rf dist node_modules

# Run Go tests
test:
	go test ./...
