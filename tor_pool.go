package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

// CircuitState represents the lifecycle status of a circuit in the pool.
type CircuitState int32

const (
	CircuitStateInitializing CircuitState = iota // SOCKS credentials created, background probe running
	CircuitStateReady                            // Primed, verified healthy, waiting in readyQueue
	CircuitStateInUse                            // Checked out by an active proxy request
	CircuitStateTainted                          // Encountered 429/503/timeout; scheduled for eviction
	CircuitStateClosed                           // Sockets closed, resources freed
)

func (s CircuitState) String() string {
	switch s {
	case CircuitStateInitializing:
		return "INITIALIZING"
	case CircuitStateReady:
		return "READY"
	case CircuitStateInUse:
		return "IN_USE"
	case CircuitStateTainted:
		return "TAINTED"
	case CircuitStateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// PreWarmedCircuit encapsulates a single isolated Tor circuit and its dedicated HTTP client.
type PreWarmedCircuit struct {
	ID            string                // Unique circuit identifier (e.g. tor_pool_<nano>_<hex>)
	ProxyURL      string                // SOCKS5 URL with isolated credentials
	SOCKSUser     string                // SOCKS5 username
	SOCKSPass     string                // SOCKS5 password ("isolate")
	Client        tls_client.HttpClient // Pre-configured Chrome_131 client with persistent transport
	ExitIP        string                // Discovered public exit relay IP
	IsHealthy     bool                  // Liveness status
	CreatedAt     time.Time             // Creation timestamp
	LastUsedAt    time.Time             // Last checkout timestamp
	LastTestedAt  time.Time             // Last probe timestamp
	LastHealthyAt time.Time             // Last healthy probe timestamp
	LatencyMs     int64                 // Latency (ms) recorded during latest probe
	UseCount      int64                 // Number of requests processed
	FailureCount  int64                 // Consecutive error count
	State         CircuitState          // Atomic lifecycle state
	mu            sync.Mutex            // Guards circuit metadata updates
}

// GetState returns the current lifecycle state atomically.
func (c *PreWarmedCircuit) GetState() CircuitState {
	return CircuitState(atomic.LoadInt32((*int32)(&c.State)))
}

// SetState updates the lifecycle state atomically.
func (c *PreWarmedCircuit) SetState(s CircuitState) {
	atomic.StoreInt32((*int32)(&c.State), int32(s))
}

// CloseIdleConnections closes all idle sockets for the circuit's HTTP client.
func (c *PreWarmedCircuit) CloseIdleConnections() {
	if c.Client != nil {
		c.Client.CloseIdleConnections()
	}
}

// CircuitProbeFunc is the signature for probing circuit health and discovering exit IP.
type CircuitProbeFunc func(ctx context.Context, client tls_client.HttpClient) (exitIP string, latencyMs int64, err error)

// HttpClientFactoryFunc is the signature for creating a tls_client.HttpClient with a given proxy URL.
type HttpClientFactoryFunc func(proxyURL string) (tls_client.HttpClient, error)

// TorCircuitPool manages pre-warmed Tor circuits, background replenishment, and lifecycle.
type TorCircuitPool struct {
	socksProxyAddr    string
	minReady          int
	maxCapacity       int
	maxCircuitAge     time.Duration
	maxUsesPerCircuit int64
	warmupTimeout     time.Duration
	probeTimeout      time.Duration

	mu            sync.RWMutex
	circuits      map[string]*PreWarmedCircuit
	readyQueue    chan *PreWarmedCircuit
	replenishCh   chan struct{}
	stopCh        chan struct{}
	closeOnce     sync.Once
	isClosed      bool
	activeWarmups int32

	reputation    *ExitReputationTracker
	probeFunc     CircuitProbeFunc
	clientFactory HttpClientFactoryFunc

	pairAcquireSem chan struct{}

	// Metrics
	totalAcquisitions uint64
	totalEvictions    uint64
	totalWarmups      uint64
	totalReleases     uint64
	poolHits          uint64
	poolMisses        uint64
}

// PoolStats holds structured pool metrics for observability and health endpoints.
type PoolStats struct {
	ReadyCircuits     int    `json:"ready_circuits"`
	InUseCircuits     int    `json:"in_use_circuits"`
	TotalCircuits     int    `json:"total_circuits"`
	ActiveWarmups     int    `json:"active_warmups"`
	TotalAcquisitions uint64 `json:"total_acquisitions"`
	TotalEvictions    uint64 `json:"total_evictions"`
	TotalWarmups      uint64 `json:"total_warmups"`
	TotalReleases     uint64 `json:"total_releases"`
	PoolHits          uint64 `json:"pool_hits"`
	PoolMisses        uint64 `json:"pool_misses"`
	TaintedExitIPs    int    `json:"tainted_exit_ips"`
}

// NewTorCircuitPool constructs and starts a pre-warmed Tor circuit pool.
func NewTorCircuitPool(socksProxyAddr string, minReady int, maxCapacity int) *TorCircuitPool {
	if socksProxyAddr == "" {
		socksProxyAddr = os.Getenv("TOR_PROXY_URL")
		if socksProxyAddr == "" {
			socksProxyAddr = os.Getenv("PROXY_URL")
		}
		if socksProxyAddr == "" {
			socksProxyAddr = "socks5://127.0.0.1:9050"
		}
	}
	if !strings.HasPrefix(socksProxyAddr, "socks5://") &&
		!strings.HasPrefix(socksProxyAddr, "http://") &&
		!strings.HasPrefix(socksProxyAddr, "https://") {
		socksProxyAddr = "socks5://" + socksProxyAddr
	}

	if minReady <= 0 {
		minReady = 6
	}
	if maxCapacity <= 0 {
		maxCapacity = 12
	}
	if maxCapacity < minReady {
		maxCapacity = minReady * 2
	}

	p := &TorCircuitPool{
		socksProxyAddr:    socksProxyAddr,
		minReady:          minReady,
		maxCapacity:       maxCapacity,
		maxCircuitAge:     5 * time.Minute,
		maxUsesPerCircuit: 25,
		warmupTimeout:     15 * time.Second,
		probeTimeout:      3500 * time.Millisecond,
		circuits:          make(map[string]*PreWarmedCircuit),
		readyQueue:        make(chan *PreWarmedCircuit, maxCapacity),
		replenishCh:       make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
		reputation:        NewExitReputationTracker(),
		probeFunc:         defaultProbeCircuit,
		clientFactory:     defaultClientFactory,
		pairAcquireSem:    make(chan struct{}, 1),
	}

	go p.replenishmentWorker()

	// Initial trigger to prime pool
	p.notifyReplenish()

	return p
}

// SetProbeFunc overrides the probe function (useful for unit tests).
func (p *TorCircuitPool) SetProbeFunc(fn CircuitProbeFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probeFunc = fn
}

// SetClientFactory overrides the client factory (useful for unit tests).
func (p *TorCircuitPool) SetClientFactory(fn HttpClientFactoryFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clientFactory = fn
}

// SetMaxCircuitAge sets the maximum age before an idle circuit is retired.
func (p *TorCircuitPool) SetMaxCircuitAge(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxCircuitAge = d
}

// SetMaxUsesPerCircuit sets the maximum number of requests processed by a circuit.
func (p *TorCircuitPool) SetMaxUsesPerCircuit(uses int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxUsesPerCircuit = uses
}

// SetWarmupTimeout sets the timeout for priming a single circuit.
func (p *TorCircuitPool) SetWarmupTimeout(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.warmupTimeout = d
}

// SetProbeTimeout sets the timeout for circuit health probes.
func (p *TorCircuitPool) SetProbeTimeout(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probeTimeout = d
}

// GetReputationTracker returns the exit reputation tracker used by the pool.
func (p *TorCircuitPool) GetReputationTracker() *ExitReputationTracker {
	return p.reputation
}

// SetReputationTracker sets a custom reputation tracker.
func (p *TorCircuitPool) SetReputationTracker(r *ExitReputationTracker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reputation = r
}

// Acquire retrieves an immediately available pre-warmed circuit in < 1ms.
func (p *TorCircuitPool) Acquire(ctx context.Context) (*PreWarmedCircuit, error) {
	p.notifyReplenish()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-p.stopCh:
			return nil, errors.New("tor circuit pool closed")

		case circuit, ok := <-p.readyQueue:
			if !ok {
				return nil, errors.New("tor circuit pool queue closed")
			}

			circuit.mu.Lock()
			state := atomic.LoadInt32((*int32)(&circuit.State))
			expired := time.Since(circuit.CreatedAt) > p.maxCircuitAge
			tainted := circuit.ExitIP != "" && p.reputation.IsTainted(circuit.ExitIP)

			if state != int32(CircuitStateReady) || expired || tainted {
				circuit.mu.Unlock()
				// Stale or tainted in queue; evict and pull next
				p.Evict(circuit, "stale_or_tainted_in_queue", tainted)
				continue
			}

			atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateInUse))
			circuit.LastUsedAt = time.Now()
			circuit.mu.Unlock()

			atomic.AddUint64(&p.totalAcquisitions, 1)
			atomic.AddUint64(&p.poolHits, 1)
			return circuit, nil

		default:
			// Queue currently empty: trigger replenishment and wait on select
			p.notifyReplenish()
			atomic.AddUint64(&p.poolMisses, 1)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()

			case <-p.stopCh:
				return nil, errors.New("tor circuit pool closed")

			case circuit, ok := <-p.readyQueue:
				if !ok {
					return nil, errors.New("tor circuit pool queue closed")
				}

				circuit.mu.Lock()
				state := atomic.LoadInt32((*int32)(&circuit.State))
				expired := time.Since(circuit.CreatedAt) > p.maxCircuitAge
				tainted := circuit.ExitIP != "" && p.reputation.IsTainted(circuit.ExitIP)

				if state != int32(CircuitStateReady) || expired || tainted {
					circuit.mu.Unlock()
					p.Evict(circuit, "stale_or_tainted_in_queue", tainted)
					continue
				}

				atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateInUse))
				circuit.LastUsedAt = time.Now()
				circuit.mu.Unlock()

				atomic.AddUint64(&p.totalAcquisitions, 1)
				atomic.AddUint64(&p.poolHits, 1)
				return circuit, nil
			}
		}
	}
}

// AcquirePair retrieves two distinct pre-warmed circuits for hedged racing.
// To prevent the Dining Philosophers hold-and-wait convoy / deadlock under concurrent load,
// it uses pairAcquireSem and bounds the acquisition of c2. If c2 cannot be acquired
// (or times out), c1 is immediately released back to the pool to prevent starvation,
// followed by jittered backoff before retry.
func (p *TorCircuitPool) AcquirePair(ctx context.Context) (*PreWarmedCircuit, *PreWarmedCircuit, error) {
	p.mu.RLock()
	if p.isClosed {
		p.mu.RUnlock()
		return nil, nil, errors.New("tor circuit pool closed")
	}
	if p.maxCapacity < 2 {
		p.mu.RUnlock()
		return nil, nil, errors.New("tor circuit pool capacity insufficient for pairs")
	}
	p.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-p.stopCh:
			return nil, nil, errors.New("tor circuit pool closed")
		case p.pairAcquireSem <- struct{}{}:
		}

		c1, err := p.Acquire(ctx)
		if err != nil {
			<-p.pairAcquireSem
			return nil, nil, err
		}

		// Bound c2 acquisition to break hold-and-wait if pool capacity is exhausted
		c2Timeout := 250 * time.Millisecond
		if d, ok := ctx.Deadline(); ok {
			rem := time.Until(d)
			if rem < c2Timeout {
				c2Timeout = rem
			}
		}

		c2Ctx, c2Cancel := context.WithTimeout(ctx, c2Timeout)
		c2, err := p.Acquire(c2Ctx)
		c2Cancel()

		if err != nil {
			// Immediately release c1 to avoid starving other concurrent goroutines
			p.Release(c1)
			<-p.pairAcquireSem

			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-p.stopCh:
				return nil, nil, errors.New("tor circuit pool closed")
			default:
			}

			// Jitter backoff to yield and break synchronization
			jitter := time.Duration(100+(time.Now().UnixNano()%400)) * time.Microsecond
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-p.stopCh:
				return nil, nil, errors.New("tor circuit pool closed")
			case <-time.After(jitter):
				continue
			}
		}

		// Successfully acquired pair; release semaphore immediately
		<-p.pairAcquireSem

		// Prevent duplicate instance collision
		if c1 == c2 || c1.ID == c2.ID {
			p.Release(c2)
			p.Release(c1)
			continue
		}

		// Path Diversity Check: if both share the exact same exit relay, attempt to swap c2
		if c1.ExitIP != "" && c1.ExitIP == c2.ExitIP {
			select {
			case c3, ok := <-p.readyQueue:
				if ok {
					c3.mu.Lock()
					state := atomic.LoadInt32((*int32)(&c3.State))
					expired := time.Since(c3.CreatedAt) > p.maxCircuitAge
					tainted := c3.ExitIP != "" && p.reputation.IsTainted(c3.ExitIP)
					if state == int32(CircuitStateReady) && !expired && !tainted && c3.ID != c1.ID && c3.ID != c2.ID {
						atomic.StoreInt32((*int32)(&c3.State), int32(CircuitStateInUse))
						c3.LastUsedAt = time.Now()
						c3.mu.Unlock()
						p.Release(c2)
						c2 = c3
					} else {
						c3.mu.Unlock()
						p.Evict(c3, "stale_in_queue", tainted)
					}
				}
			default:
				// Keep c2 as is: distinct SOCKS5 credential guarantees isolated Tor stream
			}
		}

		return c1, c2, nil
	}
}

// Release marks a circuit idle and returns it to the pool or retires it if max uses reached.
func (p *TorCircuitPool) Release(circuit *PreWarmedCircuit) {
	if circuit == nil {
		return
	}

	circuit.mu.Lock()
	defer circuit.mu.Unlock()

	state := atomic.LoadInt32((*int32)(&circuit.State))
	if state != int32(CircuitStateInUse) {
		return
	}

	atomic.AddUint64(&p.totalReleases, 1)
	circuit.UseCount++

	// Check retirement conditions
	expired := time.Since(circuit.CreatedAt) > p.maxCircuitAge
	maxUsesReached := circuit.UseCount >= p.maxUsesPerCircuit
	tainted := circuit.ExitIP != "" && p.reputation.IsTainted(circuit.ExitIP)

	if expired || maxUsesReached || tainted {
		atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
		circuit.IsHealthy = false

		p.mu.Lock()
		delete(p.circuits, circuit.ID)
		p.mu.Unlock()

		go p.retireCircuit(circuit)
		p.notifyReplenish()
		return
	}

	atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateReady))
	circuit.FailureCount = 0

	select {
	case p.readyQueue <- circuit:
		// Returned to readyQueue successfully
	default:
		// Ready queue full; retire circuit cleanly
		atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
		circuit.IsHealthy = false
		p.mu.Lock()
		delete(p.circuits, circuit.ID)
		p.mu.Unlock()
		go p.retireCircuit(circuit)
	}
}

// Evict permanently removes a circuit, marks exit IP tainted if requested, and cleans sockets.
func (p *TorCircuitPool) Evict(circuit *PreWarmedCircuit, reason string, markTainted bool) {
	if circuit == nil || circuit.GetState() == CircuitStateClosed {
		return
	}

	circuit.mu.Lock()
	if circuit.GetState() == CircuitStateClosed {
		circuit.mu.Unlock()
		return
	}
	atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
	circuit.IsHealthy = false
	exitIP := circuit.ExitIP
	circuit.mu.Unlock()

	if markTainted && exitIP != "" {
		p.reputation.MarkTainted(exitIP, 10*time.Minute, reason)
	}

	p.mu.Lock()
	delete(p.circuits, circuit.ID)
	p.mu.Unlock()

	atomic.AddUint64(&p.totalEvictions, 1)

	// Explicitly close idle connections asynchronously to prevent socket leaks and avoid blocking
	go p.retireCircuit(circuit)

	p.notifyReplenish()
}

// EvictIfReady evicts a circuit ONLY if it is currently in CircuitStateReady.
// Under circuit.mu.Lock(), it verifies circuit.State == CircuitStateReady.
// If circuit.State != CircuitStateReady (e.g. acquired by a concurrent client as CircuitStateInUse or already closed),
// it releases the lock and returns false without evicting or closing connections.
// If it is CircuitStateReady, it atomically transitions circuit.State = CircuitStateClosed,
// releases circuit.mu.Unlock(), removes it from the pool, marks exit IP tainted if requested,
// increments totalEvictions, launches retireCircuit, triggers replenishment, and returns true.
func (p *TorCircuitPool) EvictIfReady(circuit *PreWarmedCircuit, reason string, markTainted bool) bool {
	if circuit == nil {
		return false
	}

	circuit.mu.Lock()
	if circuit.State != CircuitStateReady {
		circuit.mu.Unlock()
		return false
	}
	atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
	circuit.IsHealthy = false
	exitIP := circuit.ExitIP
	circuit.mu.Unlock()

	if markTainted && exitIP != "" && p.reputation != nil {
		p.reputation.MarkTainted(exitIP, 10*time.Minute, reason)
	}

	p.mu.Lock()
	delete(p.circuits, circuit.ID)
	p.mu.Unlock()

	atomic.AddUint64(&p.totalEvictions, 1)

	// Explicitly close idle connections asynchronously to prevent socket leaks and avoid blocking
	go p.retireCircuit(circuit)

	p.notifyReplenish()
	return true
}

// EvictDefault evicts a circuit with default parameters.
func (p *TorCircuitPool) EvictDefault(circuit *PreWarmedCircuit) {
	p.Evict(circuit, "default_evict", true)
}

// EvictOnHTTPError provides instantaneous (<0.1ms) eviction when 429/503 errors occur.
func (p *TorCircuitPool) EvictOnHTTPError(circuit *PreWarmedCircuit, statusCode int) {
	if circuit == nil {
		return
	}
	reason := fmt.Sprintf("http_status_%d", statusCode)
	markTainted := (statusCode == 429 || statusCode == 503)
	p.Evict(circuit, reason, markTainted)
}

// retireCircuit closes idle HTTP connections to prevent CLOSE-WAIT socket leaks.
func (p *TorCircuitPool) retireCircuit(circuit *PreWarmedCircuit) {
	if circuit == nil {
		return
	}
	circuit.CloseIdleConnections()
}

// notifyReplenish triggers the background replenishment worker.
func (p *TorCircuitPool) notifyReplenish() {
	select {
	case p.replenishCh <- struct{}{}:
	default:
	}
}

// replenishmentWorker monitors pool depth and warms new circuits.
func (p *TorCircuitPool) replenishmentWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.replenishCh:
			p.checkAndReplenish()
		case <-ticker.C:
			p.checkAndReplenish()
		}
	}
}

// checkAndReplenish spawns warmup routines to maintain minReady circuits.
func (p *TorCircuitPool) checkAndReplenish() {
	p.mu.RLock()
	if p.isClosed {
		p.mu.RUnlock()
		return
	}
	now := time.Now()
	readyCount := 0
	for _, c := range p.circuits {
		if c.GetState() == CircuitStateReady && now.Sub(c.CreatedAt) <= p.maxCircuitAge {
			readyCount++
		}
	}
	totalCount := len(p.circuits)
	p.mu.RUnlock()

	active := int(atomic.LoadInt32(&p.activeWarmups))
	needed := p.minReady - readyCount - active

	if needed <= 0 {
		return
	}

	if totalCount+active >= p.maxCapacity {
		return
	}

	maxToSpawn := p.maxCapacity - (totalCount + active)
	if needed > maxToSpawn {
		needed = maxToSpawn
	}

	for i := 0; i < needed; i++ {
		atomic.AddInt32(&p.activeWarmups, 1)
		go func() {
			defer atomic.AddInt32(&p.activeWarmups, -1)
			p.warmNewCircuit()
		}()
	}
}

// warmNewCircuit creates and verifies a fresh isolated circuit.
func (p *TorCircuitPool) warmNewCircuit() {
	p.mu.RLock()
	if p.isClosed {
		p.mu.RUnlock()
		return
	}
	factory := p.clientFactory
	probe := p.probeFunc
	timeout := p.warmupTimeout
	p.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	user, pass := generateCircuitID()
	proxyURL := p.formatIsolatedProxyURL(user, pass)

	client, err := factory(proxyURL)
	if err != nil {
		return
	}

	circuit := &PreWarmedCircuit{
		ID:        user,
		ProxyURL:  proxyURL,
		SOCKSUser: user,
		SOCKSPass: pass,
		Client:    client,
		CreatedAt: time.Now(),
		State:     CircuitStateInitializing,
	}

	exitIP, latency, err := probe(ctx, client)
	if err != nil {
		p.retireCircuit(circuit)
		return
	}

	if exitIP != "" && p.reputation.IsTainted(exitIP) {
		p.retireCircuit(circuit)
		return
	}

	circuit.mu.Lock()
	circuit.ExitIP = exitIP
	circuit.LatencyMs = latency
	circuit.IsHealthy = true
	circuit.LastHealthyAt = time.Now()
	circuit.LastTestedAt = time.Now()
	atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateReady))
	circuit.mu.Unlock()

	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		p.retireCircuit(circuit)
		return
	}
	p.circuits[circuit.ID] = circuit
	p.mu.Unlock()

	select {
	case p.readyQueue <- circuit:
		atomic.AddUint64(&p.totalWarmups, 1)
		p.mu.RLock()
		if len(p.readyQueue) < p.minReady {
			p.mu.RUnlock()
			p.notifyReplenish()
		} else {
			p.mu.RUnlock()
		}
	default:
		// Ready queue was full. Check if a stale/closed/expired circuit sits in queue
		// that should be popped to make room for this fresh circuit.
		select {
		case stale := <-p.readyQueue:
			if stale.GetState() != CircuitStateReady || time.Since(stale.CreatedAt) > p.maxCircuitAge {
				p.Evict(stale, "stale_in_full_queue", false)
				select {
				case p.readyQueue <- circuit:
					atomic.AddUint64(&p.totalWarmups, 1)
					return
				default:
				}
			} else {
				// Re-insert valid circuit
				select {
				case p.readyQueue <- stale:
				default:
				}
			}
		default:
		}

		// Ready queue was full and could not enqueue; clean up
		p.mu.Lock()
		delete(p.circuits, circuit.ID)
		p.mu.Unlock()
		p.retireCircuit(circuit)
	}
}

// formatIsolatedProxyURL embeds isolated SOCKS5 credentials into proxy URL.
func (p *TorCircuitPool) formatIsolatedProxyURL(user, pass string) string {
	base := p.socksProxyAddr
	if base == "" {
		base = "socks5://127.0.0.1:9050"
	}
	if !strings.HasPrefix(base, "socks5://") &&
		!strings.HasPrefix(base, "http://") &&
		!strings.HasPrefix(base, "https://") {
		base = "socks5://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	u.User = url.UserPassword(user, pass)
	return u.String()
}

// generateCircuitID generates unique tor_pool_<timestamp>_<rand>:isolate credentials.
func generateCircuitID() (string, string) {
	randBytes := make([]byte, 6)
	_, _ = rand.Read(randBytes)
	user := fmt.Sprintf("tor_pool_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBytes))
	return user, "isolate"
}

// defaultClientFactory constructs a Chrome_131 tls-client instance with proxy URL.
func defaultClientFactory(proxyURL string) (tls_client.HttpClient, error) {
	opts := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(300),
		tls_client.WithClientProfile(profiles.Chrome_131),
		tls_client.WithProxyUrl(proxyURL),
	}
	return tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
}

// ReadyCount returns the number of ready, unexpired circuits currently available.
func (p *TorCircuitPool) ReadyCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	ready := 0
	for _, c := range p.circuits {
		if c.GetState() == CircuitStateReady && now.Sub(c.CreatedAt) <= p.maxCircuitAge {
			ready++
		}
	}
	return ready
}

// TotalCount returns the total number of tracked circuits.
func (p *TorCircuitPool) TotalCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.circuits)
}

// ActiveWarmups returns the number of currently active circuit warming routines.
func (p *TorCircuitPool) ActiveWarmups() int {
	return int(atomic.LoadInt32(&p.activeWarmups))
}

// Stats returns structured metrics for observability.
func (p *TorCircuitPool) Stats() PoolStats {
	p.mu.RLock()
	now := time.Now()
	ready := 0
	total := len(p.circuits)
	inUse := 0
	for _, c := range p.circuits {
		if c.GetState() == CircuitStateInUse {
			inUse++
		} else if c.GetState() == CircuitStateReady && now.Sub(c.CreatedAt) <= p.maxCircuitAge {
			ready++
		}
	}
	p.mu.RUnlock()

	tainted := 0
	if p.reputation != nil {
		tainted = p.reputation.TaintedCount()
	}

	return PoolStats{
		ReadyCircuits:     ready,
		InUseCircuits:     inUse,
		TotalCircuits:     total,
		ActiveWarmups:     int(atomic.LoadInt32(&p.activeWarmups)),
		TotalAcquisitions: atomic.LoadUint64(&p.totalAcquisitions),
		TotalEvictions:    atomic.LoadUint64(&p.totalEvictions),
		TotalWarmups:      atomic.LoadUint64(&p.totalWarmups),
		TotalReleases:     atomic.LoadUint64(&p.totalReleases),
		PoolHits:          atomic.LoadUint64(&p.poolHits),
		PoolMisses:        atomic.LoadUint64(&p.poolMisses),
		TaintedExitIPs:    tainted,
	}
}

// Close gracefully closes the pool, terminates workers, and releases all connections.
func (p *TorCircuitPool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.isClosed = true
		close(p.stopCh)

		for id, circuit := range p.circuits {
			atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
			p.retireCircuit(circuit)
			delete(p.circuits, id)
		}
		p.mu.Unlock()

		// Drain readyQueue
		for {
			select {
			case circuit := <-p.readyQueue:
				atomic.StoreInt32((*int32)(&circuit.State), int32(CircuitStateClosed))
				p.retireCircuit(circuit)
			default:
				return
			}
		}
	})
	return nil
}

// Global Tor Circuit Pool singleton support
var (
	globalTorPool     *TorCircuitPool
	globalTorPoolLock sync.RWMutex
)

// InitGlobalTorPool initializes the singleton Tor circuit pool.
func InitGlobalTorPool(socksProxyAddr string, minReady int, maxCapacity int) *TorCircuitPool {
	globalTorPoolLock.Lock()
	defer globalTorPoolLock.Unlock()
	if globalTorPool != nil {
		_ = globalTorPool.Close()
	}
	globalTorPool = NewTorCircuitPool(socksProxyAddr, minReady, maxCapacity)
	return globalTorPool
}

// GetGlobalTorPool returns the singleton Tor circuit pool (auto-initializing if needed).
func GetGlobalTorPool() *TorCircuitPool {
	globalTorPoolLock.RLock()
	p := globalTorPool
	globalTorPoolLock.RUnlock()
	if p != nil {
		return p
	}

	globalTorPoolLock.Lock()
	defer globalTorPoolLock.Unlock()
	if globalTorPool != nil {
		return globalTorPool
	}

	minReady := 6
	maxCapacity := 12
	if mrStr := os.Getenv("TOR_POOL_MIN_READY"); mrStr != "" {
		if mr, err := strconv.Atoi(mrStr); err == nil && mr > 0 {
			minReady = mr
		}
	}
	if mcStr := os.Getenv("TOR_POOL_MAX_SIZE"); mcStr != "" {
		if mc, err := strconv.Atoi(mcStr); err == nil && mc > 0 {
			maxCapacity = mc
		}
	}
	globalTorPool = NewTorCircuitPool("", minReady, maxCapacity)
	return globalTorPool
}
