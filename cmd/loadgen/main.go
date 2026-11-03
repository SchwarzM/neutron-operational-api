package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxRPS = 50
	minRPS = 1
)

type loadgen struct {
	targetURL       string
	sessionID       string
	controlPlaneURL string

	rps        atomic.Int64
	durationMs atomic.Int64
	reqID      atomic.Int64
	inFlight   atomic.Int64

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool

	mu2     sync.Mutex
	samples []lgSample
}

type lgSample struct {
	ts         time.Time
	durationMs int64
	success    bool
}

// These JSON shapes match cmd/demo-ctl/events.go exactly.
type requestEvent struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	PodName    string `json:"podName"`
	Status     int    `json:"status"`
	DurationMs int64  `json:"durationMs"`
	Slow       bool   `json:"slow"`
}

type statsEvent struct {
	Type        string  `json:"type"`
	RPS         float64 `json:"rps"`
	SuccessRate float64 `json:"successRate"`
	P50Ms       int64   `json:"p50Ms"`
	P95Ms       int64   `json:"p95Ms"`
	InFlight    int     `json:"inFlight"`
}

func (lg *loadgen) postEvent(v any) {
	body, err := json.Marshal(v)
	if err != nil {
		return
	}
	url := lg.controlPlaneURL + "/session/" + lg.sessionID + "/internal/event"
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		log.Printf("post event: %v", err)
		return
	}
	resp.Body.Close()
}

func (lg *loadgen) SetRPS(n int) {
	if n < minRPS {
		n = minRPS
	}
	if n > maxRPS {
		n = maxRPS
	}
	lg.rps.Store(int64(n))
}

func (lg *loadgen) SetDurationMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	lg.durationMs.Store(ms)
}

func (lg *loadgen) Start() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if lg.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	lg.cancel = cancel
	lg.running = true
	go lg.run(ctx)
	go lg.statsLoop(ctx)
}

func (lg *loadgen) Stop() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.running {
		return
	}
	lg.cancel()
	lg.running = false
}

func (lg *loadgen) run(ctx context.Context) {
	for {
		rps := lg.rps.Load()
		interval := time.Duration(float64(time.Second) / float64(rps))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			go lg.doRequest(ctx)
		}
	}
}

func (lg *loadgen) doRequest(ctx context.Context) {
	id := fmt.Sprintf("r-%d", lg.reqID.Add(1))
	slow := lg.durationMs.Load() > 0
	url := lg.targetURL
	if d := lg.durationMs.Load(); d > 0 {
		url = fmt.Sprintf("%s?wait=%d", url, d)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return
	}
	req.Header.Set("X-Request-ID", id)

	lg.inFlight.Add(1)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dur := time.Since(start).Milliseconds()
	lg.inFlight.Add(-1)

	status := 0
	podName := ""
	if err == nil {
		status = resp.StatusCode
		if resp.Body != nil {
			var body struct {
				Pod string `json:"pod"`
			}
			data, rerr := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if rerr == nil {
				json.Unmarshal(data, &body) //nolint:errcheck
				podName = body.Pod
			}
		}
	}

	if status == 0 || status >= 400 {
		log.Printf("request %s: status=%d err=%v dur=%dms", id, status, err, dur)
	}

	lg.postEvent(requestEvent{
		Type: "request", ID: id, PodName: podName,
		Status: status, DurationMs: dur, Slow: slow,
	})

	lg.mu2.Lock()
	lg.samples = append(lg.samples, lgSample{
		ts: time.Now(), durationMs: dur,
		success: status >= 200 && status < 300,
	})
	cutoff := time.Now().Add(-60 * time.Second)
	i := 0
	for i < len(lg.samples) && lg.samples[i].ts.Before(cutoff) {
		i++
	}
	lg.samples = lg.samples[i:]
	lg.mu2.Unlock()
}

func (lg *loadgen) statsLoop(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			lg.publishStats()
		}
	}
}

func (lg *loadgen) publishStats() {
	lg.mu2.Lock()
	window := time.Now().Add(-5 * time.Second)
	var recent []lgSample
	for _, s := range lg.samples {
		if s.ts.After(window) {
			recent = append(recent, s)
		}
	}
	lg.mu2.Unlock()

	if len(recent) == 0 {
		lg.postEvent(statsEvent{Type: "stats", InFlight: int(lg.inFlight.Load())})
		return
	}

	success := 0
	durations := make([]int64, 0, len(recent))
	for _, s := range recent {
		if s.success {
			success++
		}
		durations = append(durations, s.durationMs)
	}
	slices.Sort(durations)

	lg.postEvent(statsEvent{
		Type:        "stats",
		RPS:         float64(len(recent)) / 5.0,
		SuccessRate: float64(success) / float64(len(recent)),
		P50Ms:       durations[len(durations)*50/100],
		P95Ms:       durations[len(durations)*95/100],
		InFlight:    int(lg.inFlight.Load()),
	})
}

func newHTTPServer(lg *loadgen) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, _ *http.Request) {
		lg.Start()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, _ *http.Request) {
		lg.Stop()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("PUT /rps", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RPS int `json:"rps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lg.SetRPS(body.RPS)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("PUT /duration", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ms int64 `json:"ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		lg.SetDurationMs(body.Ms)
		w.WriteHeader(http.StatusOK)
	})
	return &http.Server{Addr: ":8080", Handler: mux}
}

func main() {
	targetURL := os.Getenv("TARGET_URL")
	if targetURL == "" {
		log.Fatal("TARGET_URL env var required")
	}
	sessionID := os.Getenv("SESSION_ID")
	if sessionID == "" {
		log.Fatal("SESSION_ID env var required")
	}
	controlPlaneURL := os.Getenv("CONTROL_PLANE_URL")
	if controlPlaneURL == "" {
		log.Fatal("CONTROL_PLANE_URL env var required")
	}

	lg := &loadgen{targetURL: targetURL, sessionID: sessionID, controlPlaneURL: controlPlaneURL}
	lg.rps.Store(5)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := newHTTPServer(lg)
	go func() {
		<-ctx.Done()
		lg.Stop()
		srv.Shutdown(context.Background()) //nolint:errcheck
	}()

	log.Printf("loadgen: listening :8080 target=%s session=%s", targetURL, sessionID)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
