package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
)

// syntheticMockClient implements tls_client.HttpClient for fine-grained adversarial testing.
type syntheticMockClient struct {
	tls_client.HttpClient
	circuitID       string
	proxyURL        string
	exitIP          string
	doFunc          func(req *fhttp.Request) (*fhttp.Response, error)
	closeIdleCalled int32
}

func (s *syntheticMockClient) Do(req *fhttp.Request) (*fhttp.Response, error) {
	if s.doFunc != nil {
		return s.doFunc(req)
	}
	return &fhttp.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(`{"status":"ok"}`)),
	}, nil
}

func (s *syntheticMockClient) CloseIdleConnections() {
	atomic.AddInt32(&s.closeIdleCalled, 1)
	if s.HttpClient != nil {
		s.HttpClient.CloseIdleConnections()
	}
}

// syntheticClientManager tracks all created mock clients across the pool.
type syntheticClientManager struct {
	mu      sync.Mutex
	clients map[string]*syntheticMockClient
}

func newSyntheticClientManager() *syntheticClientManager {
	return &syntheticClientManager{
		clients: make(map[string]*syntheticMockClient),
	}
}

func (m *syntheticClientManager) register(proxyURL string, client *syntheticMockClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[proxyURL] = client
}

func (m *syntheticClientManager) allClients() []*syntheticMockClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*syntheticMockClient, 0, len(m.clients))
	for _, c := range m.clients {
		res = append(res, c)
	}
	return res
}

// buildAdversarialPool sets up a test pool with mock probe and client factory.
func buildAdversarialPool(
	minReady, maxCapacity int,
	mgr *syntheticClientManager,
	doHandler func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error),
) (*TorCircuitPool, *int64) {
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", minReady, maxCapacity)
	pool.SetWarmupTimeout(50 * time.Millisecond)
	pool.SetProbeTimeout(50 * time.Millisecond)

	var ipCounter int64
	pool.SetProbeFunc(func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		idx := atomic.AddInt64(&ipCounter, 1)
		ip := fmt.Sprintf("198.51.100.%d", idx)
		if syn, ok := client.(*syntheticMockClient); ok {
			syn.exitIP = ip
		}
		return ip, 5, nil
	})

	pool.SetClientFactory(func(proxyURL string) (tls_client.HttpClient, error) {
		base, _ := tls_client.NewHttpClient(tls_client.NewNoopLogger())
		mock := &syntheticMockClient{
			HttpClient: base,
			proxyURL:   proxyURL,
		}
		mock.doFunc = func(req *fhttp.Request) (*fhttp.Response, error) {
			if doHandler != nil {
				return doHandler(mock, req)
			}
			return &fhttp.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`ok`)),
			}, nil
		}
		if mgr != nil {
			mgr.register(proxyURL, mock)
		}
		return mock, nil
	})

	return pool, &ipCounter
}

// waitForPoolReady polls until the pool has reached the desired ready circuit count.
func waitForPoolReady(t *testing.T, pool *TorCircuitPool, targetReady int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pool.ReadyCount() >= targetReady {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for pool to reach %d ready circuits; current ready: %d", targetReady, pool.ReadyCount())
}

// -----------------------------------------------------------------------------
// CHALLENGE 1 & 2: Consecutive 503 & 429 in R1 and R2 -> Success on R3 with Taint Checks
// -----------------------------------------------------------------------------

func TestAdversarial_MultiRoundRetry_Consecutive503And429_SucceedsOnRound3(t *testing.T) {
	mgr := newSyntheticClientManager()

	var reqCounter int64
	var taintedExitIPs sync.Map
	var round3ExitIP atomic.Value

	pool, _ := buildAdversarialPool(6, 12, mgr, func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error) {
		reqNum := atomic.AddInt64(&reqCounter, 1)
		switch reqNum {
		case 1:
			// Round 1, Probe 1: HTTP 503
			taintedExitIPs.Store(client.exitIP, "status_503")
			return &fhttp.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error":"high load round 1 probe 1"}`)),
			}, nil
		case 2:
			// Round 1, Probe 2: HTTP 429
			taintedExitIPs.Store(client.exitIP, "status_429")
			return &fhttp.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error":"rate limited round 1 probe 2"}`)),
			}, nil
		case 3:
			// Round 2, Probe 1: HTTP 429
			taintedExitIPs.Store(client.exitIP, "status_429")
			return &fhttp.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error":"rate limited round 2 probe 1"}`)),
			}, nil
		case 4:
			// Round 2, Probe 2: HTTP 503
			taintedExitIPs.Store(client.exitIP, "status_503")
			return &fhttp.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error":"high load round 2 probe 2"}`)),
			}, nil
		default:
			// Round 3+: Fresh circuit succeeds with 200 OK
			round3ExitIP.Store(client.exitIP)
			return &fhttp.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`data: {"model":"muse-spark-1.3-contributor-free","content":"resilience verified"}`)),
			}, nil
		}
	})
	defer pool.Close()

	waitForPoolReady(t, pool, 6, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://opencode.internal/responses", bytes.NewBufferString(`{"model":"muse-spark-1.3-contributor-free"}`))
	}

	result, err := ExecuteResilientHedgedRace(ctx, pool, reqBuilder, 20*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("ExecuteResilientHedgedRace failed unexpectedly: %v", err)
	}
	if result == nil || result.Response == nil {
		t.Fatalf("expected non-nil result and response, got nil")
	}
	defer result.Response.Body.Close()
	defer pool.Release(result.Circuit)

	// 1. Verify response is HTTP 200 OK
	if result.Response.StatusCode != http.StatusOK {
		t.Errorf("expected HTTP 200 OK on Round 3, got %d", result.Response.StatusCode)
	}

	bodyBytes, err := io.ReadAll(result.Response.Body)
	if err != nil {
		t.Fatalf("failed reading response body: %v", err)
	}
	if !strings.Contains(string(bodyBytes), "resilience verified") {
		t.Errorf("response body does not match expected payload: %s", string(bodyBytes))
	}

	// 2. Verify total requests made: at least 5 (2 in R1, 2 in R2, 1 in R3)
	totalReqs := atomic.LoadInt64(&reqCounter)
	if totalReqs < 5 {
		t.Errorf("expected at least 5 requests executed across 3 rounds, got %d", totalReqs)
	}

	// 3. Verify Round 3 used a fresh circuit whose exit IP was NOT tainted in R1 or R2
	r3IP, _ := round3ExitIP.Load().(string)
	if r3IP == "" {
		t.Errorf("expected round 3 exit IP to be recorded")
	}
	if _, wasTainted := taintedExitIPs.Load(r3IP); wasTainted {
		t.Errorf("FATAL: Round 3 reused a tainted exit IP from Round 1/2: %s", r3IP)
	}

	// 4. Verify that faulted exit IPs from R1 and R2 are actually tainted in ExitReputationTracker
	tracker := pool.GetReputationTracker()
	if tracker == nil {
		t.Fatalf("reputation tracker is nil")
	}

	taintedCount := tracker.TaintedCount()
	if taintedCount < 4 {
		t.Errorf("expected at least 4 tainted exit IPs in tracker, got %d", taintedCount)
	}

	taintedExitIPs.Range(func(key, val interface{}) bool {
		ip := key.(string)
		expectedReason := val.(string)
		if !tracker.IsTainted(ip) {
			t.Errorf("expected faulted IP %s to be tainted in tracker", ip)
		}
		rec, found := tracker.GetRecord(ip)
		if !found {
			t.Errorf("expected record for tainted IP %s", ip)
		} else {
			if !strings.Contains(rec.Reason, expectedReason) {
				t.Errorf("IP %s reason mismatch: expected %s, got %s", ip, expectedReason, rec.Reason)
			}
			if rec.FailCount < 1 {
				t.Errorf("IP %s fail count expected >= 1, got %d", ip, rec.FailCount)
			}
			if time.Now().After(rec.TaintedUntil) {
				t.Errorf("IP %s taint expiration %v is already in the past", ip, rec.TaintedUntil)
			}
		}
		return true
	})
}

// -----------------------------------------------------------------------------
// CHALLENGE 3: Non-Retryable Client Errors (400, 401, 404, 422) Fail Fast
// -----------------------------------------------------------------------------

func TestAdversarial_ClientErrors_FailFastWithoutExhaustingPool(t *testing.T) {
	clientErrorCodes := []struct {
		statusCode int
		name       string
	}{
		{http.StatusBadRequest, "HTTP 400 Bad Request"},
		{http.StatusUnauthorized, "HTTP 401 Unauthorized"},
		{http.StatusNotFound, "HTTP 404 Not Found"},
		{http.StatusUnprocessableEntity, "HTTP 422 Unprocessable Entity"},
	}

	for _, tc := range clientErrorCodes {
		t.Run(tc.name, func(t *testing.T) {
			mgr := newSyntheticClientManager()
			var reqCounter int64

			pool, _ := buildAdversarialPool(6, 12, mgr, func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error) {
				atomic.AddInt64(&reqCounter, 1)
				return &fhttp.Response{
					StatusCode: tc.statusCode,
					Body:       io.NopCloser(bytes.NewBufferString(fmt.Sprintf(`{"error":"client_error_%d"}`, tc.statusCode))),
				}, nil
			})
			defer pool.Close()

			waitForPoolReady(t, pool, 6, 2*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
				return fhttp.NewRequestWithContext(ctx, "POST", "http://opencode.internal/responses", bytes.NewBufferString(`{"bad":"payload"}`))
			}

			start := time.Now()
			result, err := ExecuteResilientHedgedRace(ctx, pool, reqBuilder, 20*time.Millisecond, 3)
			elapsed := time.Since(start)

			// 1. Must return an error
			if err == nil {
				t.Fatalf("expected error for %s, got success with result: %v", tc.name, result)
			}

			// 2. Error message must reflect the client status code
			expectedStatusSubstr := fmt.Sprintf("status_%d", tc.statusCode)
			if !strings.Contains(err.Error(), expectedStatusSubstr) {
				t.Errorf("expected error string to contain %q, got: %v", expectedStatusSubstr, err)
			}

			// 3. Must fail fast on Round 1: probe 1 fails, probe 2 fails via earlyFailCh => total 2 requests
			totalReqs := atomic.LoadInt64(&reqCounter)
			if totalReqs > 2 {
				t.Fatalf("CRITICAL REGRESSION: %s did NOT fail fast! Ran %d requests (retried across rounds) instead of failing on Round 1", tc.name, totalReqs)
			}
			t.Logf("[%s] failed fast in %v with %d requests (0 retries across rounds)", tc.name, elapsed, totalReqs)

			// 4. Must NOT taint any exit IPs for client errors! Client errors are not Tor faults!
			tracker := pool.GetReputationTracker()
			if tracker.TaintedCount() != 0 {
				t.Errorf("client error %s must NOT taint exit IPs in tracker, got %d tainted IPs", tc.name, tracker.TaintedCount())
			}

			// 5. Verify pool is NOT exhausted: pool can immediately satisfy next Acquire without stalling
			acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer acquireCancel()
			circ, errAcquire := pool.Acquire(acquireCtx)
			if errAcquire != nil {
				t.Errorf("failed to acquire circuit after client error: %v (pool was starved/exhausted)", errAcquire)
			} else {
				pool.Release(circ)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// CHALLENGE 4: All Rounds Exhausted Returns Wrapped Error & Taints All Faulted Circuits
// -----------------------------------------------------------------------------

func TestAdversarial_AllRoundsExhausted_ReturnsWrappedError(t *testing.T) {
	mgr := newSyntheticClientManager()
	var reqCounter int64

	pool, _ := buildAdversarialPool(6, 12, mgr, func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error) {
		atomic.AddInt64(&reqCounter, 1)
		return &fhttp.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":"upstream down"}`)),
		}, nil
	})
	defer pool.Close()

	waitForPoolReady(t, pool, 6, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://opencode.internal/responses", bytes.NewBufferString(`{}`))
	}

	result, err := ExecuteResilientHedgedRace(ctx, pool, reqBuilder, 15*time.Millisecond, 3)
	if err == nil {
		t.Fatalf("expected error when all rounds fail, got result: %v", result)
	}

	if !strings.Contains(err.Error(), "all 3 circuit retry attempts failed") {
		t.Errorf("expected error message to indicate all 3 attempts failed, got: %v", err)
	}

	totalReqs := atomic.LoadInt64(&reqCounter)
	if totalReqs < 6 {
		t.Errorf("expected 6 requests across 3 failed hedged rounds (2 per round), got %d", totalReqs)
	}

	// Verify all faulted circuits were tainted
	tracker := pool.GetReputationTracker()
	if tracker.TaintedCount() < 6 {
		t.Errorf("expected at least 6 tainted exit IPs, got %d", tracker.TaintedCount())
	}
}

// -----------------------------------------------------------------------------
// CHALLENGE 5: Pool Replenishment Under Concurrent Evictions (-race, no deadlocks, no leaks)
// -----------------------------------------------------------------------------

func TestAdversarial_PoolReplenishment_ConcurrentEvictions_ZeroDeadlocks_ZeroLeaks(t *testing.T) {
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	mgr := newSyntheticClientManager()
	pool, _ := buildAdversarialPool(6, 12, mgr, func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error) {
		// Randomly simulate 503, 429, or 200
		nano := time.Now().UnixNano()
		if nano%3 == 0 {
			return &fhttp.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(bytes.NewBufferString(`503`)),
			}, nil
		} else if nano%3 == 1 {
			return &fhttp.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(bytes.NewBufferString(`429`)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`200 ok`)),
		}, nil
	})

	waitForPoolReady(t, pool, 6, 2*time.Second)

	// Start health daemon alongside pool
	daemon := pool.StartHealthDaemon(context.Background(), 20*time.Millisecond)

	const (
		numWorkers   = 30
		opsPerWorker = 25
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startSignal

			for op := 0; op < opsPerWorker; op++ {
				opType := (workerID + op) % 5

				switch opType {
				case 0:
					// Hedged race execution with 2 retries
					ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
					reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
						return fhttp.NewRequestWithContext(ctx, "POST", "http://opencode.internal/test", bytes.NewBufferString(`{}`))
					}
					res, err := ExecuteResilientHedgedRace(ctx, pool, reqBuilder, 10*time.Millisecond, 2)
					cancel()
					if err == nil && res != nil {
						res.Response.Body.Close()
						pool.Release(res.Circuit)
					}

				case 1:
					// Single acquire and immediate eviction with taint
					ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					c, err := pool.Acquire(ctx)
					cancel()
					if err == nil && c != nil {
						pool.Evict(c, "concurrent_stress_taint", true)
					}

				case 2:
					// Single acquire and normal release
					ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					c, err := pool.Acquire(ctx)
					cancel()
					if err == nil && c != nil {
						time.Sleep(1 * time.Millisecond)
						pool.Release(c)
					}

				case 3:
					// Pair acquire, evict one, release other
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					c1, c2, err := pool.AcquirePair(ctx)
					cancel()
					if err == nil && c1 != nil && c2 != nil {
						pool.EvictOnHTTPError(c1, 503)
						pool.Release(c2)
					}

				case 4:
					// Acquire and EvictIfReady test
					ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					c, err := pool.Acquire(ctx)
					cancel()
					if err == nil && c != nil {
						// EvictIfReady should safely return false because c is IN_USE
						pool.EvictIfReady(c, "in_use_check", true)
						pool.Release(c)
					}
				}
			}
		}(w)
	}

	// Unleash all workers simultaneously
	close(startSignal)

	// Deadlock check: must finish cleanly within 15 seconds
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		t.Log("Concurrent stress workers completed with ZERO deadlocks!")
	case <-time.After(15 * time.Second):
		t.Fatalf("DEADLOCK DETECTED: concurrent workers failed to complete within 15 seconds")
	}

	daemon.Stop()

	// Wait briefly for background replenishment to settle, then verify pool stats
	time.Sleep(100 * time.Millisecond)
	stats := pool.Stats()
	t.Logf("Pool stats after stress: Acquisitions=%d, Evictions=%d, Warmups=%d, Releases=%d, TaintedIPs=%d",
		stats.TotalAcquisitions, stats.TotalEvictions, stats.TotalWarmups, stats.TotalReleases, stats.TaintedExitIPs)

	if stats.TotalAcquisitions == 0 {
		t.Errorf("expected > 0 acquisitions during stress test")
	}
	if stats.TotalEvictions == 0 {
		t.Errorf("expected > 0 evictions during stress test")
	}

	// Close pool to retire all circuits
	if err := pool.Close(); err != nil {
		t.Errorf("pool.Close() returned error: %v", err)
	}

	// Allow asynchronous retireCircuit routines to finalize
	time.Sleep(150 * time.Millisecond)

	// 1. Socket leak check: Verify all synthetic clients have CloseIdleConnections called
	allClients := mgr.allClients()
	socketLeaks := 0
	for _, c := range allClients {
		if atomic.LoadInt32(&c.closeIdleCalled) == 0 {
			socketLeaks++
		}
	}
	if socketLeaks > 0 {
		t.Errorf("SOCKET LEAK DETECTED: %d / %d clients did not have CloseIdleConnections called", socketLeaks, len(allClients))
	} else {
		t.Logf("ZERO SOCKET LEAKS: all %d created clients had CloseIdleConnections invoked", len(allClients))
	}

	// 2. Goroutine leak check: Goroutine count should return to near-baseline
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()
	leakedGoroutines := finalGoroutines - baselineGoroutines
	t.Logf("Goroutines baseline=%d, final=%d, diff=%d", baselineGoroutines, finalGoroutines, leakedGoroutines)

	// Allow small delta (<= 4) for test runner / runtime GC routines
	if leakedGoroutines > 4 {
		t.Errorf("GOROUTINE LEAK DETECTED: %d leaked goroutines remaining after pool closure", leakedGoroutines)
	} else {
		t.Log("ZERO GOROUTINE LEAKS: goroutine count returned to baseline")
	}
}

// -----------------------------------------------------------------------------
// CHALLENGE 6: Tainted Exit IP Rejection During Warmup & Mid-Queue Eviction
// -----------------------------------------------------------------------------

func TestAdversarial_TaintedExitIP_RejectedDuringWarmupAndQueueAcquisition(t *testing.T) {
	mgr := newSyntheticClientManager()
	taintedIP := "198.51.100.999"

	pool, _ := buildAdversarialPool(3, 6, mgr, nil)
	defer pool.Close()

	// 1. Manually mark an IP as tainted before warmup
	tracker := pool.GetReputationTracker()
	tracker.MarkTainted(taintedIP, 10*time.Minute, "proactive_taint")

	if !tracker.IsTainted(taintedIP) {
		t.Fatalf("expected IP %s to be tainted", taintedIP)
	}

	// 2. Override probe func to return the tainted IP on next warmup attempt
	var returnTainted atomic.Bool
	returnTainted.Store(true)
	var dynamicIPCounter int64

	pool.SetProbeFunc(func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		if returnTainted.Load() {
			return taintedIP, 10, nil
		}
		n := atomic.AddInt64(&dynamicIPCounter, 1)
		return fmt.Sprintf("198.51.100.%d", 200+n), 10, nil
	})

	// Trigger replenishment: warmNewCircuit must reject the circuit because exitIP is tainted
	pool.notifyReplenish()
	time.Sleep(100 * time.Millisecond)

	// Verify no circuits in pool have the tainted IP
	pool.mu.RLock()
	for _, c := range pool.circuits {
		if c.ExitIP == taintedIP {
			t.Errorf("SECURITY DEFECT: Pool contains circuit with tainted exit IP %s!", taintedIP)
		}
	}
	pool.mu.RUnlock()

	// 3. Test Mid-Queue Eviction: prime a circuit, taint its IP, then Acquire
	returnTainted.Store(false)
	waitForPoolReady(t, pool, 1, 2*time.Second)

	// Taint one of the circuits currently sitting ready in the queue
	pool.mu.RLock()
	var victimIP string
	for _, c := range pool.circuits {
		if c.GetState() == CircuitStateReady {
			victimIP = c.ExitIP
			break
		}
	}
	pool.mu.RUnlock()

	if victimIP == "" {
		t.Fatalf("no ready circuit found to taint")
	}

	tracker.MarkTainted(victimIP, 10*time.Minute, "mid_queue_taint")

	// Now Acquire: Acquire MUST detect tainted == true, evict victim, and return a clean circuit
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	acquired, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer pool.Release(acquired)

	if acquired.ExitIP == victimIP {
		t.Fatalf("CRITICAL DEFECT: Acquire returned tainted circuit %s!", victimIP)
	}
	t.Logf("Acquire successfully bypassed tainted circuit %s and returned clean circuit %s", victimIP, acquired.ExitIP)
}

// -----------------------------------------------------------------------------
// CHALLENGE 7: Context Cancellation During Hedged Race Retries
// -----------------------------------------------------------------------------

func TestAdversarial_ContextCancellation_DuringHedgedRaceRetries(t *testing.T) {
	mgr := newSyntheticClientManager()

	pool, _ := buildAdversarialPool(4, 8, mgr, func(client *syntheticMockClient, req *fhttp.Request) (*fhttp.Response, error) {
		// Hang until context is canceled
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return &fhttp.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`late`)),
			}, nil
		}
	})
	defer pool.Close()

	waitForPoolReady(t, pool, 4, 2*time.Second)

	// Cancel context quickly (50ms) during probe execution
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://opencode.internal/test", bytes.NewBufferString(`{}`))
	}

	res, err := ExecuteResilientHedgedRace(ctx, pool, reqBuilder, 20*time.Millisecond, 3)
	if res != nil {
		t.Errorf("expected nil result on context cancellation, got %v", res)
	}
	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context error, got: %v", err)
	}

	// Verify no circuits are left permanently locked in CircuitStateInUse
	time.Sleep(100 * time.Millisecond)
	pool.mu.RLock()
	inUseCount := 0
	for _, c := range pool.circuits {
		if c.GetState() == CircuitStateInUse {
			inUseCount++
		}
	}
	pool.mu.RUnlock()

	if inUseCount > 0 {
		t.Errorf("DEFECT: %d circuits remained stuck in IN_USE state after canceled race!", inUseCount)
	} else {
		t.Log("All circuits cleanly released or closed after context cancellation")
	}
}

// -----------------------------------------------------------------------------
// CHALLENGE 8: Dining Philosophers Hold-and-Wait Starvation on AcquirePair
// -----------------------------------------------------------------------------

func TestAdversarial_DiningPhilosophers_PairAcquisitionUnderStarvation(t *testing.T) {
	mgr := newSyntheticClientManager()
	// Extreme constraint: minReady=2, maxCapacity=2
	pool, _ := buildAdversarialPool(2, 2, mgr, nil)
	defer pool.Close()

	waitForPoolReady(t, pool, 2, 2*time.Second)

	const concurrency = 20
	var wg sync.WaitGroup
	var completedPairs int64

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
			defer cancel()

			c1, c2, err := pool.AcquirePair(ctx)
			if err != nil {
				// Acceptable under extreme starvation timeout
				return
			}
			if c1 != nil && c2 != nil {
				atomic.AddInt64(&completedPairs, 1)
				// Hold briefly
				time.Sleep(5 * time.Millisecond)
				pool.Release(c1)
				pool.Release(c2)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Dining philosophers starvation test completed cleanly with %d pair acquisitions", atomic.LoadInt64(&completedPairs))
	case <-time.After(8 * time.Second):
		t.Fatalf("DEADLOCK: Dining philosophers hold-and-wait deadlocked during AcquirePair!")
	}
}

