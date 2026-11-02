package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LoadGen sends HTTP traffic to the ingress and publishes RequestEvent and StatsEvent.
type LoadGen struct {
	pub        Publisher
	k8s        *K8s
	targetURL  string
	rps        atomic.Int64
	durationMs atomic.Int64
	reqID      atomic.Int64

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool

	// metrics ring buffer (last 60 samples)
	mu2     sync.Mutex
	samples []sample
}

type sample struct {
	ts         time.Time
	durationMs int64
	success    bool
	podName    string
}

func NewLoadGen(pub Publisher, k8s *K8s, targetURL string) *LoadGen {
	lg := &LoadGen{pub: pub, k8s: k8s, targetURL: targetURL}
	lg.rps.Store(5)
	return lg
}

// SetRPS sets the target requests per second (1–50).
func (lg *LoadGen) SetRPS(n int) {
	if n < 1 {
		n = 1
	}
	if n > 50 {
		n = 50
	}
	lg.rps.Store(int64(n))
}

// SetDurationMs sets the ?wait= parameter sent to demo-server (0 = fast).
func (lg *LoadGen) SetDurationMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	lg.durationMs.Store(ms)
}

// Start begins load generation. Safe to call multiple times.
func (lg *LoadGen) Start() {
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

// Stop halts load generation.
func (lg *LoadGen) Stop() {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if !lg.running {
		return
	}
	lg.cancel()
	lg.running = false
}

func (lg *LoadGen) run(ctx context.Context) {
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

func (lg *LoadGen) doRequest(ctx context.Context) {
	id := fmt.Sprintf("r-%d", lg.reqID.Add(1))
	slow := lg.durationMs.Load() > 0
	url := lg.targetURL
	if d := lg.durationMs.Load(); d > 0 {
		url = fmt.Sprintf("%s?wait=%d", url, d)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Request-ID", id)

	lg.k8s.InFlightInc("")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dur := time.Since(start).Milliseconds()

	status := 0
	podName := ""
	if err == nil {
		status = resp.StatusCode
		if resp.Body != nil {
			var body struct {
				Pod string `json:"pod"`
			}
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			json.Unmarshal(data, &body)
			podName = body.Pod
		}
	}
	lg.k8s.InFlightDec("")

	if status == 0 || status >= 400 {
		log.Printf("request %s failed: status=%d err=%v dur=%dms", id, status, err, dur)
	}

	ev := RequestEvent{
		Type:       EventRequest,
		ID:         id,
		PodName:    podName,
		Status:     status,
		DurationMs: dur,
		Slow:       slow,
	}
	lg.pub.Publish(ev)

	lg.mu2.Lock()
	lg.samples = append(lg.samples, sample{
		ts:         time.Now(),
		durationMs: dur,
		success:    status >= 200 && status < 300,
		podName:    podName,
	})
	// Keep only last 60 seconds of samples
	cutoff := time.Now().Add(-60 * time.Second)
	i := 0
	for i < len(lg.samples) && lg.samples[i].ts.Before(cutoff) {
		i++
	}
	lg.samples = lg.samples[i:]
	lg.mu2.Unlock()
}

func (lg *LoadGen) statsLoop(ctx context.Context) {
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

func (lg *LoadGen) publishStats() {
	lg.mu2.Lock()
	window := time.Now().Add(-5 * time.Second)
	var recent []sample
	for _, s := range lg.samples {
		if s.ts.After(window) {
			recent = append(recent, s)
		}
	}
	lg.mu2.Unlock()

	if len(recent) == 0 {
		lg.pub.Publish(StatsEvent{Type: EventStats})
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
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	p50 := durations[len(durations)*50/100]
	p95 := durations[len(durations)*95/100]

	lg.pub.Publish(StatsEvent{
		Type:        EventStats,
		RPS:         float64(len(recent)) / 5.0,
		SuccessRate: float64(success) / float64(len(recent)),
		P50Ms:       p50,
		P95Ms:       p95,
		InFlight:    lg.k8s.TotalInFlight(),
	})
}
