package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
)

type mockRaceHttpClient struct {
	tls_client.HttpClient
	doFunc func(req *fhttp.Request) (*fhttp.Response, error)
}

func (m *mockRaceHttpClient) Do(req *fhttp.Request) (*fhttp.Response, error) {
	if m.doFunc != nil {
		return m.doFunc(req)
	}
	return &fhttp.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString("ok")),
	}, nil
}

func (m *mockRaceHttpClient) CloseIdleConnections() {
	if m.HttpClient != nil {
		m.HttpClient.CloseIdleConnections()
	}
}

func createMockRacePool(p1Delay, p2Delay time.Duration, p1Status, p2Status int) *TorCircuitPool {
	pool := NewTorCircuitPool("socks5://127.0.0.1:9050", 4, 8)
	pool.SetWarmupTimeout(50 * time.Millisecond)
	pool.SetProbeTimeout(50 * time.Millisecond)

	var probeCounter int64
	pool.SetProbeFunc(func(ctx context.Context, client tls_client.HttpClient) (string, int64, error) {
		pIdx := atomic.AddInt64(&probeCounter, 1)
		return fmt.Sprintf("192.0.2.%d", pIdx), 10, nil
	})

	pool.SetClientFactory(func(proxyURL string) (tls_client.HttpClient, error) {
		baseClient, _ := tls_client.NewHttpClient(tls_client.NewNoopLogger())
		return &mockRaceHttpClient{
			HttpClient: baseClient,
			doFunc: func(req *fhttp.Request) (*fhttp.Response, error) {
				var delay time.Duration
				var status int
				if req.Header.Get("X-Hedge-Probe") == "2" {
					delay = p2Delay
					status = p2Status
				} else {
					delay = p1Delay
					status = p1Status
				}

				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				case <-time.After(delay):
				}

				if status == 0 {
					return nil, errors.New("network failure")
				}

				return &fhttp.Response{
					StatusCode: status,
					Body:       io.NopCloser(bytes.NewBufferString("chunk_data")),
				}, nil
			},
		}, nil
	})

	// Wait for pool to prime
	time.Sleep(100 * time.Millisecond)
	return pool
}

func TestExecuteHedgedRace_Probe1Wins(t *testing.T) {
	pool := createMockRacePool(20*time.Millisecond, 200*time.Millisecond, http.StatusOK, http.StatusOK)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://example.com", bytes.NewBufferString("{}"))
	}

	res, err := ExecuteHedgedRace(ctx, pool, reqBuilder, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected hedged race success, got: %v", err)
	}
	defer res.Response.Body.Close()
	defer pool.Release(res.Circuit)

	if res.ProbeID != 1 {
		t.Errorf("expected Probe 1 to win, got Probe %d", res.ProbeID)
	}
	if res.Response.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", res.Response.StatusCode)
	}
}

func TestExecuteHedgedRace_Probe1FailsFast_Probe2Wins(t *testing.T) {
	// Probe 1 fails immediately (HTTP 429), Probe 2 succeeds after 30ms
	pool := createMockRacePool(5*time.Millisecond, 30*time.Millisecond, http.StatusTooManyRequests, http.StatusOK)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://example.com", bytes.NewBufferString("{}"))
	}

	start := time.Now()
	res, err := ExecuteHedgedRace(ctx, pool, reqBuilder, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("expected Probe 2 to win, got error: %v", err)
	}
	defer res.Response.Body.Close()
	defer pool.Release(res.Circuit)

	elapsed := time.Since(start)
	if res.ProbeID != 2 {
		t.Errorf("expected Probe 2 to win, got Probe %d", res.ProbeID)
	}
	// Early fail trigger should launch Probe 2 without waiting the full 200ms stagger!
	if elapsed > 150*time.Millisecond {
		t.Errorf("expected early fail trigger to fast-path probe 2, took %v", elapsed)
	}
}

func TestExecuteHedgedRace_Probe1Slow_Probe2Overtakes(t *testing.T) {
	// Probe 1 hangs for 300ms, Probe 2 launches at 50ms and finishes in 20ms (total ~70ms)
	pool := createMockRacePool(300*time.Millisecond, 20*time.Millisecond, http.StatusOK, http.StatusOK)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reqBuilder := func(ctx context.Context) (*fhttp.Request, error) {
		return fhttp.NewRequestWithContext(ctx, "POST", "http://example.com", bytes.NewBufferString("{}"))
	}

	res, err := ExecuteHedgedRace(ctx, pool, reqBuilder, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected race success, got error: %v", err)
	}
	defer res.Response.Body.Close()
	defer pool.Release(res.Circuit)

	if res.ProbeID != 2 {
		t.Errorf("expected faster Probe 2 to overtake Probe 1, got Probe %d", res.ProbeID)
	}
}
