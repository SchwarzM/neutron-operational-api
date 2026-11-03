# Stage 1: Build Svelte frontend
FROM node:lts-alpine AS web
WORKDIR /src/web
COPY web/ .
RUN npm install --prefer-offline
RUN npm run build
# outDir: '../cmd/demo-ctl/assets' relative to web/ → /src/cmd/demo-ctl/assets

# Stage 2: Build Go binaries
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/cmd/demo-ctl/assets ./cmd/demo-ctl/assets
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /demo-server  ./cmd/demo-server/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /demo-ctl     ./cmd/demo-ctl/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /loadgen      ./cmd/loadgen/

# Stage 3: Minimal runtime image
FROM alpine:3
COPY --from=build /demo-server /demo-server
COPY --from=build /demo-ctl    /demo-ctl
COPY --from=build /loadgen     /loadgen
ENTRYPOINT ["/demo-server"]
