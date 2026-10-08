package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// HedgedRaceResult represents the winning response and associated circuit resources.
type HedgedRaceResult struct {
	Response   *fhttp.Response
	Circuit    *PreWarmedCircuit
	CancelFunc context.CancelFunc
	ProbeID    int
	Latency    time.Duration
}

// RequestBuilderFunc builds a fresh clone of the fhttp.Request for each probe.
type RequestBuilderFunc func(ctx context.Context) (*fhttp.Request, error)

// DefaultMaxCircuitRetries defines the maximum retry attempts across candidate Tor circuits.
const DefaultMaxCircuitRetries = 3

// ExecuteHedgedRace executes concurrent hedged racing with automatic multi-circuit retries on 503/429.
func ExecuteHedgedRace(
	ctx context.Context,
	pool *TorCircuitPool,
	requestBuilder RequestBuilderFunc,
	staggerDelay time.Duration,
) (*HedgedRaceResult, error) {
	return ExecuteResilientHedgedRace(ctx, pool, requestBuilder, staggerDelay, DefaultMaxCircuitRetries)
}

// ExecuteResilientHedgedRace attempts hedged racing across candidate Tor circuits up to maxAttempts times.
// When a circuit encounters 503 or 429, it evicts/taints the faulted exit IP and immediately rotates
// to fresh pre-warmed circuits from the pool.
func ExecuteResilientHedgedRace(
	ctx context.Context,
	pool *TorCircuitPool,
	requestBuilder RequestBuilderFunc,
	staggerDelay time.Duration,
	maxAttempts int,
) (*HedgedRaceResult, error) {
	if pool == nil {
		return nil, errors.New("tor circuit pool is nil")
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxCircuitRetries
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		res, err := executeSingleHedgedRaceRound(ctx, pool, requestBuilder, staggerDelay)
		if err == nil && res != nil && res.Response != nil && res.Response.StatusCode == http.StatusOK {
			return res, nil
		}

		lastErr = err

		// If error is non-retryable (client error 400, 401, 403, 404, 422), do not retry across more circuits
		if err != nil && isNonRetryableStatus(err.Error()) {
			return nil, err
		}

		// If more retry attempts remain, briefly yield/backoff to allow replenishment worker
		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt*50) * time.Millisecond):
			}
		}
	}

	return nil, fmt.Errorf("all %d circuit retry attempts failed: %w", maxAttempts, lastErr)
}

func isNonRetryableStatus(errMsg string) bool {
	return strings.Contains(errMsg, "status_400") ||
		strings.Contains(errMsg, "status_401") ||
		strings.Contains(errMsg, "status_403") ||
		strings.Contains(errMsg, "status_404") ||
		strings.Contains(errMsg, "status_422")
}

// executeSingleHedgedRaceRound executes a single round of hedged racing across candidate Tor circuits.
func executeSingleHedgedRaceRound(
	ctx context.Context,
	pool *TorCircuitPool,
	requestBuilder RequestBuilderFunc,
	staggerDelay time.Duration,
) (*HedgedRaceResult, error) {
	if pool == nil {
		return nil, errors.New("tor circuit pool is nil")
	}
	if staggerDelay <= 0 {
		staggerDelay = 250 * time.Millisecond
	}

	// 1. Attempt to acquire a diverse pair of pre-warmed circuits.
	c1, c2, errPair := pool.AcquirePair(ctx)
	if errPair != nil {
		// Fallback to single circuit if pair cannot be acquired
		cSingle, errSingle := pool.Acquire(ctx)
		if errSingle != nil {
			return nil, fmt.Errorf("failed to acquire circuit from pool: %w (pair error: %v)", errSingle, errPair)
		}
		return executeSingleProbe(ctx, pool, cSingle, requestBuilder)
	}

	type probeOutcome struct {
		result *HedgedRaceResult
		err    error
		probe  int
	}

	outcomeCh := make(chan probeOutcome, 2)
	earlyFailCh := make(chan struct{}, 1)

	var winnerChosen int32 // 0 = none, 1 = probe1, 2 = probe2
	var cancelProbe1 context.CancelFunc
	var cancelProbe2 context.CancelFunc

	ctx1, c1Cancel := context.WithCancel(ctx)
	cancelProbe1 = c1Cancel

	ctx2, c2Cancel := context.WithCancel(ctx)
	cancelProbe2 = c2Cancel

	var wg sync.WaitGroup
	startTotal := time.Now()

	// Probe 1 Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		startP1 := time.Now()

		req1, errReq := requestBuilder(ctx1)
		if errReq != nil {
			select {
			case earlyFailCh <- struct{}{}:
			default:
			}
			pool.Release(c1)
			outcomeCh <- probeOutcome{err: errReq, probe: 1}
			return
		}
		if req1 != nil && req1.Header != nil {
			req1.Header.Set("X-Hedge-Probe", "1")
		}

		resp1, errDo := c1.Client.Do(req1)
		if errDo != nil || resp1 == nil || resp1.StatusCode != http.StatusOK {
			// Probe 1 failed
			select {
			case earlyFailCh <- struct{}{}:
			default:
			}

			errMsg := "unknown probe failure"
			if errDo != nil {
				errMsg = errDo.Error()
			} else if resp1 != nil {
				errMsg = fmt.Sprintf("status_%d", resp1.StatusCode)
				resp1.Body.Close()
			}

			// Taint exit if rate limited or server overloaded
			isTainted := (resp1 != nil && (resp1.StatusCode == http.StatusTooManyRequests || resp1.StatusCode == http.StatusServiceUnavailable || resp1.StatusCode == http.StatusBadGateway))
			pool.Evict(c1, errMsg, isTainted)

			outcomeCh <- probeOutcome{err: fmt.Errorf("probe 1 failed: %s", errMsg), probe: 1}
			return
		}

		// Probe 1 succeeded: attempt to claim victory
		if atomic.CompareAndSwapInt32(&winnerChosen, 0, 1) {
			cancelProbe2()
			outcomeCh <- probeOutcome{
				result: &HedgedRaceResult{
					Response:   resp1,
					Circuit:    c1,
					CancelFunc: cancelProbe1,
					ProbeID:    1,
					Latency:    time.Since(startP1),
				},
				probe: 1,
			}
		} else {
			// Lost race; close body and release
			resp1.Body.Close()
			cancelProbe1()
			pool.Release(c1)
		}
	}()

	// Probe 2 Worker (Staggered or triggered on early failure)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Staggered delay or early failure trigger
		timer := time.NewTimer(staggerDelay)
		defer timer.Stop()

		select {
		case <-ctx2.Done():
			// Cancelled before launch
			pool.Release(c2)
			return
		case <-earlyFailCh:
			// Probe 1 failed early, launch immediately
		case <-timer.C:
			// Normal stagger window elapsed
		}

		if atomic.LoadInt32(&winnerChosen) != 0 {
			// Probe 1 already won
			pool.Release(c2)
			return
		}

		startP2 := time.Now()
		req2, errReq := requestBuilder(ctx2)
		if errReq != nil {
			pool.Release(c2)
			outcomeCh <- probeOutcome{err: errReq, probe: 2}
			return
		}
		if req2 != nil && req2.Header != nil {
			req2.Header.Set("X-Hedge-Probe", "2")
		}

		resp2, errDo := c2.Client.Do(req2)
		if errDo != nil || resp2 == nil || resp2.StatusCode != http.StatusOK {
			errMsg := "unknown probe failure"
			if errDo != nil {
				errMsg = errDo.Error()
			} else if resp2 != nil {
				errMsg = fmt.Sprintf("status_%d", resp2.StatusCode)
				resp2.Body.Close()
			}

			isTainted := (resp2 != nil && (resp2.StatusCode == http.StatusTooManyRequests || resp2.StatusCode == http.StatusServiceUnavailable || resp2.StatusCode == http.StatusBadGateway))
			pool.Evict(c2, errMsg, isTainted)

			outcomeCh <- probeOutcome{err: fmt.Errorf("probe 2 failed: %s", errMsg), probe: 2}
			return
		}

		// Probe 2 succeeded: attempt to claim victory
		if atomic.CompareAndSwapInt32(&winnerChosen, 0, 2) {
			cancelProbe1()
			outcomeCh <- probeOutcome{
				result: &HedgedRaceResult{
					Response:   resp2,
					Circuit:    c2,
					CancelFunc: cancelProbe2,
					ProbeID:    2,
					Latency:    time.Since(startP2),
				},
				probe: 2,
			}
		} else {
			// Lost race; close body and release
			resp2.Body.Close()
			cancelProbe2()
			pool.Release(c2)
		}
	}()

	// Wait for a winner or both to fail
	var firstErr error
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			cancelProbe1()
			cancelProbe2()
			return nil, ctx.Err()

		case outcome := <-outcomeCh:
			if outcome.result != nil {
				// Winner found! Ensure cleanup of loser goroutine in background
				go func() {
					wg.Wait()
					if outcome.result.ProbeID == 1 {
						pool.Release(c2)
					} else {
						pool.Release(c1)
					}
				}()
				outcome.result.Latency = time.Since(startTotal)
				return outcome.result, nil
			}
			if firstErr == nil {
				firstErr = outcome.err
			}
		}
	}

	wg.Wait()
	return nil, fmt.Errorf("all hedged racing probes failed: %w", firstErr)
}

// executeSingleProbe handles circuit dispatch when only a single circuit was acquired.
func executeSingleProbe(
	ctx context.Context,
	pool *TorCircuitPool,
	circuit *PreWarmedCircuit,
	requestBuilder RequestBuilderFunc,
) (*HedgedRaceResult, error) {
	start := time.Now()
	probeCtx, cancel := context.WithCancel(ctx)

	req, errReq := requestBuilder(probeCtx)
	if errReq != nil {
		cancel()
		pool.Release(circuit)
		return nil, errReq
	}

	resp, errDo := circuit.Client.Do(req)
	if errDo != nil || resp == nil || resp.StatusCode != http.StatusOK {
		cancel()
		errMsg := "unknown single probe failure"
		if errDo != nil {
			errMsg = errDo.Error()
		} else if resp != nil {
			errMsg = fmt.Sprintf("status_%d", resp.StatusCode)
			resp.Body.Close()
		}

		isTainted := (resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusBadGateway))
		pool.Evict(circuit, errMsg, isTainted)
		return nil, errors.New(errMsg)
	}

	return &HedgedRaceResult{
		Response:   resp,
		Circuit:    circuit,
		CancelFunc: cancel,
		ProbeID:    1,
		Latency:    time.Since(start),
	}, nil
}
