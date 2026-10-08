package main

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tls_client "github.com/bogdanfinn/tls-client"
)

// spyHttpClient wraps a tls_client.HttpClient to track CloseIdleConnections invocations.
type spyHttpClient struct {
	tls_client.HttpClient
	closeIdleCalled int32
}

func (s *spyHttpClient) CloseIdleConnections() {
	atomic.AddInt32(&s.closeIdleCalled, 1)
	if s.HttpClient != nil {
		s.HttpClient.CloseIdleConnections()
	}
}

// spyClientTracker provides thread-safe access to created spy clients.
type spyClientTracker struct {
	mu    sync.Mutex
	spies map[string]*spyHttpClient
}

func newSpyTracker() *spyClientTracker {
	return &spyClientTracker{spies: make(map[string]*spyHttpClient)}
}

func (s *spyClientTracker) set(proxyURL string, spy *spyHttpClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spies[proxyURL] = spy
}

func (s *spyClientTracker) get(proxyURL string) (*spyHttpClient, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spy, ok := s.spies[proxyURL]
	return spy, ok
}

// createTestMockClientFactory creates a factory returning real HttpClient instances with spy support.
func createTestMockClientFactory(tracker *spyClientTracker) HttpClientFactoryFunc {
	return func(proxyURL string) (tls_client.HttpClient, error) {
		baseClient, err := tls_client.NewHttpClient(tls_client.NewNoopLogger())
		if err != nil {
			return nil, err
		}
		spy := &spyHttpClient{HttpClient: baseClient}
		if tracker != nil {
			tracker.set(proxyURL, spy)
		}
		return spy, nil
	}
}

// createMockProbe creates a deterministic probe function returning synthetic exit IPs.
func createMockProbe(ipCounter *int32, latencyMs int64) CircuitProbeFunc {
	return func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		n := atomic.AddInt32(ipCounter, 1)
		ip := fmt.Sprintf("198.51.100.%d", n)
		if latencyMs > 0 {
			time.Sleep(time.Duration(latencyMs) * time.Millisecond)
		}
		return ip, latencyMs, nil
	}
}

// TestTorPool_AcquireRelease verifies channel depth, acquisition in <5ms, circuit state transitions, and use count.
func TestTorPool_AcquireRelease(t *testing.T) {
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 4, 8)
	pool.SetClientFactory(createTestMockClientFactory(nil))
	// 50ms probe so background replenishment doesn't race during the immediate acquire check
	pool.SetProbeFunc(createMockProbe(&ipCounter, 50))
	defer pool.Close()

	// Wait until pool reaches steady state (minReady=4 and activeWarmups=0)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	for {
		if pool.ReadyCount() == 4 && pool.ActiveWarmups() == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ready circuits, ready=%d, active=%d", pool.ReadyCount(), pool.ActiveWarmups())
		case <-time.After(20 * time.Millisecond):
		}
	}

	initialReady := pool.ReadyCount() // 4

	// 1. Measure acquisition speed (must be < 5ms, target < 1ms)
	acquireStart := time.Now()
	circ, err := pool.Acquire(context.Background())
	acquireDuration := time.Since(acquireStart)

	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if circ == nil {
		t.Fatalf("acquired circuit is nil")
	}

	if acquireDuration > 5*time.Millisecond {
		t.Errorf("Acquire took %v, expected < 5ms", acquireDuration)
	} else {
		t.Logf("Acquire succeeded in %v (well under 5ms target)", acquireDuration)
	}

	// Verify state and channel depth reduction immediately before new warmup completes
	if circ.GetState() != CircuitStateInUse {
		t.Errorf("expected circuit state IN_USE, got %v", circ.GetState())
	}
	if circ.ID == "" || !strings.HasPrefix(circ.ID, "tor_pool_") {
		t.Errorf("unexpected circuit ID format: %q", circ.ID)
	}
	if circ.SOCKSPass != "isolate" {
		t.Errorf("expected SOCKSPass 'isolate', got %q", circ.SOCKSPass)
	}

	depthAfterAcquire := pool.ReadyCount()
	if depthAfterAcquire != initialReady-1 {
		t.Errorf("expected ready count %d immediately after acquire, got %d", initialReady-1, depthAfterAcquire)
	}

	// 2. Release circuit back to pool
	pool.Release(circ)

	if circ.GetState() != CircuitStateReady {
		t.Errorf("expected circuit state READY after release, got %v", circ.GetState())
	}
	if circ.UseCount != 1 {
		t.Errorf("expected circuit UseCount 1, got %d", circ.UseCount)
	}

	// 3. Test Max Uses Recycling
	pool.SetMaxUsesPerCircuit(2)

	circ2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	// Simulate circ2 already used once
	circ2.UseCount = 1
	pool.Release(circ2)

	// Since UseCount is now 2 (>= maxUsesPerCircuit 2), circ2 should be retired, not returned
	if circ2.GetState() != CircuitStateClosed {
		t.Errorf("expected circuit exceeding max uses to be CLOSED, got %v", circ2.GetState())
	}
}

// TestTorPool_EvictAndTaint verifies eviction, taint marking in ExitReputationTracker, and socket cleanup.
func TestTorPool_EvictAndTaint(t *testing.T) {
	var ipCounter int32
	trackerSpy := newSpyTracker()
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 2, 4)
	pool.SetClientFactory(createTestMockClientFactory(trackerSpy))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	defer pool.Close()

	// Wait for 1 ready circuit
	for i := 0; i < 50; i++ {
		if pool.ReadyCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	circ, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	exitIP := circ.ExitIP
	if exitIP == "" {
		t.Fatalf("acquired circuit has empty ExitIP")
	}

	// Get spy
	spy, hasSpy := trackerSpy.get(circ.ProxyURL)
	if !hasSpy {
		baseClient, _ := tls_client.NewHttpClient(tls_client.NewNoopLogger())
		spy = &spyHttpClient{HttpClient: baseClient}
		circ.Client = spy
	}

	// 1. Evict with taint marking and measure duration (<0.1ms target)
	evictStart := time.Now()
	pool.Evict(circ, "upstream_429", true)
	evictDuration := time.Since(evictStart)

	t.Logf("Evict took %v", evictDuration)
	if evictDuration > 1*time.Millisecond {
		t.Errorf("Evict took %v, expected < 1ms", evictDuration)
	}

	// Verify circuit state
	if circ.GetState() != CircuitStateClosed {
		t.Errorf("expected circuit state CLOSED, got %v", circ.GetState())
	}

	// Verify reputation tracker marked exit IP
	tracker := pool.GetReputationTracker()
	if !tracker.IsTainted(exitIP) {
		t.Errorf("expected exit IP %s to be tainted in reputation tracker", exitIP)
	}
	record, found := tracker.GetRecord(exitIP)
	if !found || record.Reason != "upstream_429" {
		t.Errorf("expected record reason 'upstream_429', got %v (found=%v)", record.Reason, found)
	}

	// Verify socket cleanup: CloseIdleConnections was called asynchronously to prevent CLOSE-WAIT socket leaks
	var closed bool
	for i := 0; i < 50; i++ {
		if atomic.LoadInt32(&spy.closeIdleCalled) > 0 {
			closed = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !closed {
		t.Errorf("expected CloseIdleConnections to be called on evicted circuit client")
	}

	// 2. Test non-tainting eviction
	for i := 0; i < 50; i++ {
		if pool.ReadyCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	circ2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	exitIP2 := circ2.ExitIP

	pool.Evict(circ2, "normal_rotation", false)
	if tracker.IsTainted(exitIP2) {
		t.Errorf("expected exit IP %s NOT to be tainted when markTainted=false", exitIP2)
	}

	// 3. Test EvictOnHTTPError helper
	for i := 0; i < 50; i++ {
		if pool.ReadyCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	circ3, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 3 failed: %v", err)
	}
	exitIP3 := circ3.ExitIP
	pool.EvictOnHTTPError(circ3, 503)
	if !tracker.IsTainted(exitIP3) {
		t.Errorf("expected exit IP %s to be tainted on HTTP 503", exitIP3)
	}
}

// TestTorPool_Replenishment verifies background worker continuously replenishes readyQueue.
func TestTorPool_Replenishment(t *testing.T) {
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 3, 6)
	pool.SetClientFactory(createTestMockClientFactory(nil))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	defer pool.Close()

	// Wait for pool to reach target minReady (3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for {
		if pool.ReadyCount() >= 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for pool to reach minReady=3, current=%d", pool.ReadyCount())
		case <-time.After(20 * time.Millisecond):
		}
	}

	if pool.ReadyCount() != 3 {
		t.Errorf("expected exactly 3 ready circuits, got %d", pool.ReadyCount())
	}

	// Acquire 2 circuits simultaneously
	c1, err1 := pool.Acquire(context.Background())
	c2, err2 := pool.Acquire(context.Background())
	if err1 != nil || err2 != nil {
		t.Fatalf("failed acquiring circuits: err1=%v, err2=%v", err1, err2)
	}

	// Background worker should automatically replenish back to 3
	replenishCtx, replenishCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer replenishCancel()

	for {
		if pool.ReadyCount() >= 3 {
			break
		}
		select {
		case <-replenishCtx.Done():
			t.Fatalf("timed out waiting for background replenishment to restore minReady=3, current=%d", pool.ReadyCount())
		case <-time.After(25 * time.Millisecond):
		}
	}

	t.Logf("Background replenishment successfully restored readyQueue to %d circuits", pool.ReadyCount())

	// Verify credential format
	credentialPattern := regexp.MustCompile(`^tor_pool_\d+_[0-9a-f]+$`)
	if !credentialPattern.MatchString(c1.SOCKSUser) {
		t.Errorf("circuit 1 SOCKSUser %q did not match expected pattern", c1.SOCKSUser)
	}
	if !credentialPattern.MatchString(c2.SOCKSUser) {
		t.Errorf("circuit 2 SOCKSUser %q did not match expected pattern", c2.SOCKSUser)
	}
	if c1.SOCKSUser == c2.SOCKSUser {
		t.Errorf("expected unique credentials between circuits, got identical %q", c1.SOCKSUser)
	}

	// Verify Stats
	stats := pool.Stats()
	if stats.TotalAcquisitions < 2 {
		t.Errorf("expected TotalAcquisitions >= 2, got %d", stats.TotalAcquisitions)
	}
	if stats.TotalWarmups < 5 {
		t.Errorf("expected TotalWarmups >= 5, got %d", stats.TotalWarmups)
	}
}

// TestExitReputationTracker verifies taint marking, expiry, query, pruning, and clearing.
func TestExitReputationTracker(t *testing.T) {
	tracker := NewExitReputationTracker()

	ip := "192.0.2.100"

	// 1. Initial query must be false
	if tracker.IsTainted(ip) {
		t.Errorf("expected untainted initially for %s", ip)
	}
	if tracker.TaintedCount() != 0 {
		t.Errorf("expected 0 tainted count initially, got %d", tracker.TaintedCount())
	}

	// 2. Mark tainted with 80ms duration
	tracker.MarkTainted(ip, 80*time.Millisecond, "upstream_429")

	if !tracker.IsTainted(ip) {
		t.Errorf("expected %s to be tainted", ip)
	}
	if tracker.TaintedCount() != 1 {
		t.Errorf("expected tainted count 1, got %d", tracker.TaintedCount())
	}

	rec, found := tracker.GetRecord(ip)
	if !found {
		t.Fatalf("expected record for %s", ip)
	}
	if rec.Reason != "upstream_429" || rec.FailCount != 1 {
		t.Errorf("unexpected record metadata: reason=%s, failCount=%d", rec.Reason, rec.FailCount)
	}

	// 3. Mark again to test FailCount increment
	tracker.MarkTainted(ip, 80*time.Millisecond, "upstream_503")
	rec2, _ := tracker.GetRecord(ip)
	if rec2.FailCount != 2 || rec2.Reason != "upstream_503" {
		t.Errorf("expected FailCount=2 and updated reason, got %d and %s", rec2.FailCount, rec2.Reason)
	}

	// 4. Test TTL expiry
	time.Sleep(100 * time.Millisecond)

	if tracker.IsTainted(ip) {
		t.Errorf("expected %s taint to have expired after 100ms", ip)
	}
	if tracker.TaintedCount() != 0 {
		t.Errorf("expected 0 active tainted count after expiry, got %d", tracker.TaintedCount())
	}

	// 5. Test Prune
	pruned := tracker.Prune()
	if pruned != 1 {
		t.Errorf("expected 1 record pruned, got %d", pruned)
	}
	if len(tracker.AllRecords()) != 0 {
		t.Errorf("expected 0 records remaining after prune, got %d", len(tracker.AllRecords()))
	}

	// 6. Test ClearTaint
	ip2 := "192.0.2.200"
	tracker.MarkTainted(ip2, 10*time.Minute, "manual")
	if !tracker.IsTainted(ip2) {
		t.Errorf("expected %s to be tainted", ip2)
	}
	tracker.ClearTaint(ip2)
	if tracker.IsTainted(ip2) {
		t.Errorf("expected %s to be untainted after ClearTaint", ip2)
	}
}

// TestTorPool_AcquirePair verifies acquiring two distinct circuits with path diversity.
func TestTorPool_AcquirePair(t *testing.T) {
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 4, 8)
	pool.SetClientFactory(createTestMockClientFactory(nil))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	defer pool.Close()

	// Wait for pool
	for i := 0; i < 50; i++ {
		if pool.ReadyCount() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	c1, c2, err := pool.AcquirePair(context.Background())
	if err != nil {
		t.Fatalf("AcquirePair failed: %v", err)
	}
	defer pool.Release(c1)
	defer pool.Release(c2)

	if c1.ID == c2.ID {
		t.Errorf("expected distinct circuit IDs, got %s for both", c1.ID)
	}
	if c1.SOCKSUser == c2.SOCKSUser {
		t.Errorf("expected distinct SOCKS usernames, got %s for both", c1.SOCKSUser)
	}
	if c1.GetState() != CircuitStateInUse || c2.GetState() != CircuitStateInUse {
		t.Errorf("expected both circuits to be in IN_USE state")
	}
}

// TestTorPool_HealthDaemon verifies background daemon sweeps idle circuits and evicts slow/failing ones.
func TestTorPool_HealthDaemon(t *testing.T) {
	var probeFail atomic.Bool
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 2, 4)
	pool.SetClientFactory(createTestMockClientFactory(nil))

	pool.SetProbeFunc(func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		if probeFail.Load() {
			return "", 4000, fmt.Errorf("simulated high latency / timeout")
		}
		return "198.51.100.77", 20, nil
	})
	defer pool.Close()

	// Wait for 2 ready circuits
	for i := 0; i < 50; i++ {
		if pool.ReadyCount() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	daemon := pool.StartHealthDaemon(context.Background(), 50*time.Millisecond)
	defer daemon.Stop()

	// Trigger simulated failure in probe
	probeFail.Store(true)

	// Daemon sweep runs every 50ms and evicts failing circuits
	evictedCtx, evictedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer evictedCancel()

	for {
		stats := pool.Stats()
		if stats.TotalEvictions >= 1 {
			t.Logf("Health daemon successfully evicted failing circuit (total evictions: %d)", stats.TotalEvictions)
			break
		}
		select {
		case <-evictedCtx.Done():
			t.Fatalf("timed out waiting for health daemon to evict degraded circuits")
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// TestTorPool_LiveTorIntegration tests connecting to actual local Tor if port 9050 is open.
func TestTorPool_LiveTorIntegration(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9050", 300*time.Millisecond)
	if err != nil {
		t.Skip("local Tor 127.0.0.1:9050 not reachable, skipping live Tor test")
		return
	}
	_ = conn.Close()

	t.Log("Local Tor 127.0.0.1:9050 detected, running genuine live Tor integration probe")

	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 1, 2)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	circ, err := pool.Acquire(ctx)
	if err != nil {
		t.Logf("Live Tor Acquire encountered network/bootstrap delay: %v (tolerable in test env)", err)
		return
	}
	defer pool.Release(circ)

	t.Logf("Live Tor circuit acquired successfully! ID: %s, ExitIP: %s, Latency: %dms", circ.ID, circ.ExitIP, circ.LatencyMs)
	if circ.ExitIP == "" {
		t.Errorf("expected live Tor circuit to discover an exit IP")
	}
}

// TestTorPool_HealthDaemon_MidFlightCheckoutPreventsEviction verifies that when a background health probe
// fails on a circuit that was acquired mid-flight by an active client request, the circuit is NOT evicted
// or marked closed out from under the active user request.
func TestTorPool_HealthDaemon_MidFlightCheckoutPreventsEviction(t *testing.T) {
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 1, 2)
	pool.SetClientFactory(createTestMockClientFactory(nil))

	probeStarted := make(chan struct{})
	probeRelease := make(chan struct{})
	var (
		probeInFlight atomic.Bool
		daemonProbing atomic.Bool
	)

	pool.SetProbeFunc(func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		if daemonProbing.Load() {
			if probeInFlight.CompareAndSwap(false, true) {
				close(probeStarted)
				<-probeRelease
				return "", 4000, fmt.Errorf("simulated probe failure after checkout")
			}
		}
		return "198.51.100.99", 10, nil
	})
	defer pool.Close()

	// Wait for pool prime (1 circuit ready)
	primeCtx, primeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer primeCancel()
	for pool.ReadyCount() < 1 {
		select {
		case <-primeCtx.Done():
			t.Fatalf("prime timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Enable blocking probe for daemon and start health daemon
	daemonProbing.Store(true)
	daemon := pool.StartHealthDaemon(context.Background(), 10*time.Millisecond)
	defer daemon.Stop()

	// Wait for daemon probe to initiate on the circuit
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for health daemon probe to start")
	}

	// While probe is in-flight, active client request acquires the circuit
	clientCirc, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("client Acquire failed: %v", err)
	}
	if clientCirc.GetState() != CircuitStateInUse {
		t.Fatalf("expected acquired circuit to be IN_USE, got %v", clientCirc.GetState())
	}

	// Now release the probe so it finishes and attempts eviction on failure
	close(probeRelease)
	time.Sleep(50 * time.Millisecond)

	// Verify invariant: circuit MUST NOT be evicted or set to CLOSED while in use by client
	state := clientCirc.GetState()
	if state == CircuitStateClosed {
		t.Fatalf("DEFECT: HealthDaemon evicted circuit out from under active client! State: %v", state)
	}
	if state != CircuitStateInUse {
		t.Errorf("expected circuit state to remain IN_USE, got %v", state)
	}

	// Verify circuit is still tracked in pool.circuits
	pool.mu.RLock()
	tracked := pool.circuits[clientCirc.ID]
	pool.mu.RUnlock()
	if tracked == nil {
		t.Errorf("expected circuit to still be tracked in pool.circuits")
	}

	// Client cleanly finishes request and releases circuit
	pool.Release(clientCirc)
	if clientCirc.GetState() != CircuitStateReady {
		t.Errorf("expected circuit state READY after client release, got %v", clientCirc.GetState())
	}
}

// TestTorPool_AcquirePair_ConcurrentNoDeadlock verifies that AcquirePair does not encounter
// hold-and-wait deadlocks or convoy starvation when multiple goroutines compete for pairs.
func TestTorPool_AcquirePair_ConcurrentNoDeadlock(t *testing.T) {
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 6, 12)
	pool.SetClientFactory(createTestMockClientFactory(nil))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	defer pool.Close()

	// Wait for pool prime (6 ready circuits)
	primeCtx, primeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer primeCancel()
	for pool.ReadyCount() < 6 {
		select {
		case <-primeCtx.Done():
			t.Fatalf("prime timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}

	const (
		concurrency = 25
		opsPerG     = 20
	)
	var wg sync.WaitGroup

	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				c1, c2, err := pool.AcquirePair(ctx)
				cancel()

				if err != nil {
					continue
				}

				if c1 == nil || c2 == nil {
					t.Errorf("AcquirePair returned nil: c1=%v, c2=%v", c1, c2)
					return
				}
				if c1 == c2 || c1.ID == c2.ID {
					t.Errorf("AcquirePair returned identical pair: %s == %s", c1.ID, c2.ID)
					return
				}

				// Simulate brief hedged work
				time.Sleep(20 * time.Microsecond)

				pool.Release(c1)
				pool.Release(c2)
			}
		}(g)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		t.Log("AcquirePair concurrency completed cleanly with zero deadlocks or pair collisions")
	case <-time.After(10 * time.Second):
		t.Fatalf("DEADLOCK during concurrent AcquirePair")
	}
}

// TestTorPool_ReplenishmentWithExpiredCircuitsInQueue verifies that checkAndReplenish
// calculates ready depth from live, unexpired circuits rather than phantom queue depth,
// ensuring background replenishment primes fresh circuits even if expired circuits sit in queue.
func TestTorPool_ReplenishmentWithExpiredCircuitsInQueue(t *testing.T) {
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 4, 8)
	pool.SetClientFactory(createTestMockClientFactory(nil))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	// Set 50ms TTL so primed circuits expire quickly
	pool.SetMaxCircuitAge(50 * time.Millisecond)
	defer pool.Close()

	// Wait for pool prime to 4 circuits
	primeCtx, primeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer primeCancel()
	for pool.ReadyCount() < 4 {
		select {
		case <-primeCtx.Done():
			t.Fatalf("prime timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Sleep for 70ms to allow all circuits in queue to expire
	time.Sleep(70 * time.Millisecond)

	// ReadyCount should accurately report 0 ready unexpired circuits
	if ready := pool.ReadyCount(); ready != 0 {
		t.Errorf("expected ReadyCount=0 after expiry, got %d", ready)
	}

	// Trigger replenishment to recover
	pool.notifyReplenish()

	// Background replenishment must restore pool to minReady=4 with fresh circuits
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer recoverCancel()
	for pool.ReadyCount() < 4 {
		select {
		case <-recoverCtx.Done():
			t.Fatalf("timed out waiting for background replenishment to restore expired pool; ReadyCount=%d", pool.ReadyCount())
		case <-time.After(15 * time.Millisecond):
		}
	}

	// Acquire a circuit: client must receive a fresh, unexpired circuit without a cold build delay
	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer acquireCancel()

	start := time.Now()
	circ, err := pool.Acquire(acquireCtx)
	acquireDuration := time.Since(start)

	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer pool.Release(circ)

	if acquireDuration > 50*time.Millisecond {
		t.Errorf("Acquire took %v, expected fast acquisition from pre-warmed pool", acquireDuration)
	}
	if time.Since(circ.CreatedAt) > pool.maxCircuitAge {
		t.Errorf("Acquire returned expired circuit: age=%v > max=%v", time.Since(circ.CreatedAt), pool.maxCircuitAge)
	}
	if circ.GetState() != CircuitStateInUse {
		t.Errorf("expected circuit state IN_USE, got %v", circ.GetState())
	}
}

// TestTorPool_EvictIfReady_AtomicTOCTOU verifies that EvictIfReady atomically
// evicts circuits in CircuitStateReady, but safely returns false and does NOT
// close sockets or mutate state when a circuit is checked out in CircuitStateInUse.
func TestTorPool_EvictIfReady_AtomicTOCTOU(t *testing.T) {
	tracker := newSpyTracker()
	var ipCounter int32
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 4, 8)
	pool.SetClientFactory(createTestMockClientFactory(tracker))
	pool.SetProbeFunc(createMockProbe(&ipCounter, 5))
	defer pool.Close()

	// Wait for pool prime to 4 ready circuits
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for pool.ReadyCount() < 4 {
		select {
		case <-ctx.Done():
			t.Fatalf("prime timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// 1. Ready Circuit Eviction: EvictIfReady must return true, transition to CLOSED,
	// delete from pool.circuits, increment totalEvictions, and retire sockets.
	readyCirc, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	// Release back to readyQueue so it's in CircuitStateReady
	pool.Release(readyCirc)
	if state := readyCirc.GetState(); state != CircuitStateReady {
		t.Fatalf("expected circuit to be in READY state, got %v", state)
	}

	spyReady, ok := tracker.get(readyCirc.ProxyURL)
	if !ok || spyReady == nil {
		t.Fatalf("spy client not found for proxy URL %s", readyCirc.ProxyURL)
	}

	initialEvictions := atomic.LoadUint64(&pool.totalEvictions)
	evicted := pool.EvictIfReady(readyCirc, "test_ready_eviction", true)
	if !evicted {
		t.Fatalf("expected EvictIfReady to return true for ready circuit, got false")
	}

	// State must be CircuitStateClosed
	if state := readyCirc.GetState(); state != CircuitStateClosed {
		t.Errorf("expected circuit state to be CLOSED, got %v", state)
	}

	// Total evictions metric must increment
	if currentEvictions := atomic.LoadUint64(&pool.totalEvictions); currentEvictions != initialEvictions+1 {
		t.Errorf("expected totalEvictions to be %d, got %d", initialEvictions+1, currentEvictions)
	}

	// Circuit must be deleted from pool map
	pool.mu.RLock()
	_, existsInPool := pool.circuits[readyCirc.ID]
	pool.mu.RUnlock()
	if existsInPool {
		t.Errorf("expected circuit %s to be deleted from pool.circuits", readyCirc.ID)
	}

	// Retire circuit is asynchronous: wait briefly and check socket was closed
	retired := false
	for i := 0; i < 20; i++ {
		if atomic.LoadInt32(&spyReady.closeIdleCalled) > 0 {
			retired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !retired {
		t.Errorf("expected CloseIdleConnections to be called on evicted ready circuit")
	}

	// 2. In-Use Circuit Protection: EvictIfReady MUST return false and MUST NOT close sockets
	inUseCirc, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer pool.Release(inUseCirc)

	if state := inUseCirc.GetState(); state != CircuitStateInUse {
		t.Fatalf("expected acquired circuit to be IN_USE, got %v", state)
	}

	spyInUse, ok := tracker.get(inUseCirc.ProxyURL)
	if !ok || spyInUse == nil {
		t.Fatalf("spy client not found for proxy URL %s", inUseCirc.ProxyURL)
	}
	closeCallsBefore := atomic.LoadInt32(&spyInUse.closeIdleCalled)
	evictionsBefore := atomic.LoadUint64(&pool.totalEvictions)

	// Attempt EvictIfReady while in use
	evicted = pool.EvictIfReady(inUseCirc, "test_race_eviction", true)
	if evicted {
		t.Fatalf("CRITICAL TOCTOU VIOLATION: EvictIfReady returned true for circuit in IN_USE state!")
	}

	// Circuit MUST retain IN_USE state
	if state := inUseCirc.GetState(); state != CircuitStateInUse {
		t.Errorf("circuit state mutated unexpectedly: expected IN_USE, got %v", state)
	}

	// Circuit MUST NOT have CloseIdleConnections invoked
	time.Sleep(20 * time.Millisecond)
	if closeCallsAfter := atomic.LoadInt32(&spyInUse.closeIdleCalled); closeCallsAfter != closeCallsBefore {
		t.Errorf("sockets were closed on in-use circuit: close calls went from %d to %d", closeCallsBefore, closeCallsAfter)
	}

	// totalEvictions must not change
	if evictionsAfter := atomic.LoadUint64(&pool.totalEvictions); evictionsAfter != evictionsBefore {
		t.Errorf("totalEvictions mutated unexpectedly: was %d, now %d", evictionsBefore, evictionsAfter)
	}

	// Circuit MUST remain in pool.circuits
	pool.mu.RLock()
	_, existsInPool = pool.circuits[inUseCirc.ID]
	pool.mu.RUnlock()
	if !existsInPool {
		t.Errorf("in-use circuit %s was unexpectedly removed from pool.circuits", inUseCirc.ID)
	}

	// 3. Already-Closed Circuit: EvictIfReady MUST return false
	if pool.EvictIfReady(readyCirc, "already_closed", false) {
		t.Errorf("expected EvictIfReady to return false for already closed circuit")
	}

	// 4. Nil circuit: EvictIfReady MUST return false
	if pool.EvictIfReady(nil, "nil_circuit", false) {
		t.Errorf("expected EvictIfReady to return false for nil circuit")
	}
}
