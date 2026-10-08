package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
)

// TaintRecord tracks reputation metadata for a tainted exit IP.
type TaintRecord struct {
	ExitIP       string    `json:"exit_ip"`
	TaintedUntil time.Time `json:"tainted_until"`
	Reason       string    `json:"reason"`
	FailCount    int       `json:"fail_count"`
	CreatedAt    time.Time `json:"created_at"`
}

// ExitReputationTracker maintains an in-memory thread-safe cache of tainted exit IPs with expiration.
type ExitReputationTracker struct {
	mu      sync.RWMutex
	records map[string]*TaintRecord
}

// NewExitReputationTracker constructs a new ExitReputationTracker.
func NewExitReputationTracker() *ExitReputationTracker {
	return &ExitReputationTracker{
		records: make(map[string]*TaintRecord),
	}
}

// MarkTainted records an exit IP as tainted for a specified duration (default 10m if <=0).
func (r *ExitReputationTracker) MarkTainted(exitIP string, duration time.Duration, reason string) {
	if exitIP == "" {
		return
	}
	if duration <= 0 {
		duration = 10 * time.Minute
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, exists := r.records[exitIP]
	if !exists {
		rec = &TaintRecord{
			ExitIP:    exitIP,
			CreatedAt: time.Now(),
		}
		r.records[exitIP] = rec
	}
	rec.TaintedUntil = time.Now().Add(duration)
	rec.Reason = reason
	rec.FailCount++
}

// IsTainted returns true if the exit IP is currently tainted and unexpired.
func (r *ExitReputationTracker) IsTainted(exitIP string) bool {
	if exitIP == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	rec, exists := r.records[exitIP]
	if !exists {
		return false
	}
	return time.Now().Before(rec.TaintedUntil)
}

// ClearTaint explicitly removes the taint for an exit IP.
func (r *ExitReputationTracker) ClearTaint(exitIP string) {
	if exitIP == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, exitIP)
}

// Prune removes all expired records and returns the number of pruned entries.
func (r *ExitReputationTracker) Prune() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	pruned := 0
	for ip, rec := range r.records {
		if now.After(rec.TaintedUntil) {
			delete(r.records, ip)
			pruned++
		}
	}
	return pruned
}

// TaintedCount returns the number of currently active tainted IPs.
func (r *ExitReputationTracker) TaintedCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	count := 0
	for _, rec := range r.records {
		if now.Before(rec.TaintedUntil) {
			count++
		}
	}
	return count
}

// GetRecord returns a copy of the taint record if present.
func (r *ExitReputationTracker) GetRecord(exitIP string) (TaintRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, exists := r.records[exitIP]
	if !exists {
		return TaintRecord{}, false
	}
	return *rec, true
}

// AllRecords returns a snapshot copy of all taint records.
func (r *ExitReputationTracker) AllRecords() []TaintRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]TaintRecord, 0, len(r.records))
	for _, rec := range r.records {
		result = append(result, *rec)
	}
	return result
}

// HealthDaemon periodically audits circuits in a TorCircuitPool.
type HealthDaemon struct {
	pool      *TorCircuitPool
	interval  time.Duration
	stopCh    chan struct{}
	closeOnce sync.Once
}

// StartHealthDaemon starts the background health daemon for the pool.
func StartHealthDaemon(ctx context.Context, pool *TorCircuitPool, interval time.Duration) *HealthDaemon {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	d := &HealthDaemon{
		pool:     pool,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
	go d.run(ctx)
	return d
}

// StartHealthDaemon method on TorCircuitPool.
func (p *TorCircuitPool) StartHealthDaemon(ctx context.Context, interval time.Duration) *HealthDaemon {
	return StartHealthDaemon(ctx, p, interval)
}

// Stop cleanly terminates the health daemon.
func (d *HealthDaemon) Stop() {
	d.closeOnce.Do(func() {
		close(d.stopCh)
	})
}

// run is the health daemon's background loop.
func (d *HealthDaemon) run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	pruneTicker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-pruneTicker.C:
			if d.pool != nil && d.pool.reputation != nil {
				d.pool.reputation.Prune()
			}
		case <-ticker.C:
			d.sweep(ctx)
		}
	}
}

// sweep inspects idle circuits, expiring stale ones and probing health.
func (d *HealthDaemon) sweep(ctx context.Context) {
	if d.pool == nil {
		return
	}

	d.pool.mu.RLock()
	if d.pool.isClosed {
		d.pool.mu.RUnlock()
		return
	}
	circuits := make([]*PreWarmedCircuit, 0, len(d.pool.circuits))
	for _, c := range d.pool.circuits {
		circuits = append(circuits, c)
	}
	d.pool.mu.RUnlock()

	for _, circuit := range circuits {
		state := circuit.GetState()
		if state != CircuitStateReady {
			continue // Skip in-use, initializing, or already closed circuits
		}

		// Evict circuits exceeding maxCircuitAge
		if time.Since(circuit.CreatedAt) > d.pool.maxCircuitAge {
			d.pool.EvictIfReady(circuit, "max_age_exceeded", false)
			continue
		}

		// Non-blocking health probe per circuit
		go func(c *PreWarmedCircuit) {
			probeCtx, cancel := context.WithTimeout(ctx, d.pool.probeTimeout)
			defer cancel()

			exitIP, latency, err := d.pool.probeFunc(probeCtx, c.Client)

			// Mid-flight checkout check: if circuit was acquired by client (CircuitStateInUse) or closed mid-probe,
			// abort immediately without evicting or mutating.
			if c.GetState() != CircuitStateReady {
				return
			}

			if err != nil || latency > 3500 {
				// Evict dead, throttled, or slow circuits before client traffic touches them
				d.pool.EvictIfReady(c, "health_probe_failed", true)
				return
			}

			// If exit IP became tainted while idle
			if exitIP != "" && d.pool.reputation.IsTainted(exitIP) {
				d.pool.EvictIfReady(c, "exit_ip_tainted", true)
				return
			}

			c.mu.Lock()
			if c.GetState() != CircuitStateReady {
				c.mu.Unlock()
				return
			}
			if exitIP != "" {
				c.ExitIP = exitIP
			}
			c.LatencyMs = latency
			c.LastHealthyAt = time.Now()
			c.LastTestedAt = time.Now()
			c.IsHealthy = true
			c.mu.Unlock()
		}(circuit)
	}
}

// defaultProbeCircuit performs dual-tier probing:
// Tier 1: cloudflare.com/cdn-cgi/trace
// Tier 2: check.torproject.org/api/ip
func defaultProbeCircuit(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
	if client == nil {
		return "", 0, errors.New("nil http client")
	}

	start := time.Now()

	// Tier 1: Cloudflare trace
	req, err := fhttp.NewRequestWithContext(ctx, "GET", "https://cloudflare.com/cdn-cgi/trace", nil)
	if err == nil {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "text/plain")

		resp, err := client.Do(req)
		if err == nil && resp != nil {
			latency := time.Since(start).Milliseconds()
			var bodyBytes []byte
			var readErr error
			if resp.StatusCode == http.StatusOK {
				bodyBytes, readErr = io.ReadAll(io.LimitReader(resp.Body, 4096))
			}
			// Drain and close Tier 1 response body immediately before return or Tier 2 fallthrough
			if resp.Body != nil {
				_, _ = io.CopyN(io.Discard, resp.Body, 1024)
				_ = resp.Body.Close()
			}
			if resp.StatusCode == http.StatusOK && readErr == nil {
				exitIP := parseTraceExitIP(string(bodyBytes))
				if exitIP != "" {
					return exitIP, latency, nil
				}
			}
		}
	}

	// Tier 2: Fallback to check.torproject.org/api/ip
	req2, err2 := fhttp.NewRequestWithContext(ctx, "GET", "https://check.torproject.org/api/ip", nil)
	if err2 != nil {
		return "", time.Since(start).Milliseconds(), err2
	}
	req2.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req2.Header.Set("Accept", "application/json")

	resp2, err2 := client.Do(req2)
	if err2 != nil {
		return "", time.Since(start).Milliseconds(), err2
	}
	defer func() {
		if resp2.Body != nil {
			_, _ = io.CopyN(io.Discard, resp2.Body, 1024)
			_ = resp2.Body.Close()
		}
	}()

	latency2 := time.Since(start).Milliseconds()
	if resp2.StatusCode != http.StatusOK {
		return "", latency2, fmt.Errorf("probe failed with status %d", resp2.StatusCode)
	}

	var torResp struct {
		IP    string `json:"IP"`
		IsTor bool   `json:"IsTor"`
	}
	if err := json.NewDecoder(io.LimitReader(resp2.Body, 1024)).Decode(&torResp); err == nil && torResp.IP != "" {
		return torResp.IP, latency2, nil
	}

	return "", latency2, errors.New("unable to determine exit ip from probes")
}

// parseTraceExitIP extracts the exit IP from Cloudflare trace plaintext response.
func parseTraceExitIP(body string) string {
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ip=") {
			return strings.TrimPrefix(line, "ip=")
		}
	}
	return ""
}
