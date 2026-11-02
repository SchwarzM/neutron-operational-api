FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/demo-server/ ./cmd/demo-server/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /demo-server ./cmd/demo-server/

FROM alpine:3
COPY --from=build /demo-server /demo-server
ENTRYPOINT ["/demo-server"]
