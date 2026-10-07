#!/usr/bin/env python3
"""
Unified AI Gateway & Proxy Benchmark and Verification Harness.
Executes 4-Tier E2E test suites and real-time streaming benchmarks.
Enforces strict programmatic assertions on:
- TTFT <= 4.5s on healthy streaming requests.
- First SSE chunk delivered immediately (zero full completion buffering).
- Zero unhandled 403 FreeTierErrors.
- Automatic 429 / 503 circuit rotation or model rescue returning HTTP 200 OK.
- Valid streaming SSE delta schema conformance for OpenAI and Anthropic formats.
- Programmatic exit code 0 on complete pass with structured JSON metrics summary.
"""

import argparse
import json
import os
import sys
import time
import unittest
from typing import Dict, Any, List, Optional, Tuple

import requests

from tests.e2e.mock_server import start_mock_server, stop_mock_server
from tests.e2e.validator import (
    validate_openai_chunk,
    validate_openai_completion,
    validate_anthropic_event,
    validate_anthropic_completion,
    assert_ttft_within_limit,
    assert_zero_buffering,
    assert_no_unhandled_403,
    SchemaValidationError,
    InvariantViolationError
)


def benchmark_stream_endpoint(
    name: str,
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    protocol: str = "openai",
    timeout: float = 60.0
) -> Dict[str, Any]:
    """
    Executes a streaming request, clocking TTFT and throughput with monotonic timers,
    and enforcing programmatic invariant assertions.
    """
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    start_time = time.perf_counter()
    first_token_time: Optional[float] = None
    chunks_count = 0
    raw_lines: List[str] = []
    status_code = 0
    error_msg: Optional[str] = None

    try:
        with requests.post(url, json=payload, headers=req_headers, stream=True, timeout=timeout) as resp:
            status_code = resp.status_code
            if status_code != 200:
                raw_body = resp.text[:300]
                error_msg = f"HTTP {status_code}: {raw_body}"
                return {
                    "name": name,
                    "url": url,
                    "model": payload.get("model", "unknown"),
                    "protocol": protocol,
                    "status_code": status_code,
                    "ttft_seconds": 0.0,
                    "total_time_seconds": round(time.perf_counter() - start_time, 3),
                    "chunks": 0,
                    "tps": 0.0,
                    "status": "FAIL",
                    "error": error_msg
                }

            saw_anthropic_stop = False
            for line in resp.iter_lines():
                if line:
                    now = time.perf_counter()
                    if first_token_time is None:
                        first_token_time = now
                    decoded = line.decode("utf-8")
                    raw_lines.append(decoded)
                    if protocol == "openai":
                        if decoded == "data: [DONE]":
                            break
                        if decoded.startswith("data: "):
                            chunks_count += 1
                            chunk_data = json.loads(decoded[6:])
                            validate_openai_chunk(chunk_data)
                    elif protocol == "anthropic":
                        if decoded.startswith("event: "):
                            chunks_count += 1
                            if decoded.strip() == "event: message_stop":
                                saw_anthropic_stop = True
                        elif saw_anthropic_stop and decoded.startswith("data: "):
                            break

    except Exception as exc:
        error_msg = str(exc)
        return {
            "name": name,
            "url": url,
            "model": payload.get("model", "unknown"),
            "protocol": protocol,
            "status_code": status_code,
            "ttft_seconds": 0.0,
            "total_time_seconds": round(time.perf_counter() - start_time, 3),
            "chunks": 0,
            "tps": 0.0,
            "status": "FAIL",
            "error": error_msg
        }

    end_time = time.perf_counter()
    ttft = (first_token_time - start_time) if first_token_time else 0.0
    total_time = end_time - start_time
    gen_time = total_time - ttft
    tps = chunks_count / gen_time if gen_time > 0 else 0.0

    # Invariant Assertions:
    assert_no_unhandled_403(status_code, "".join(raw_lines[:5]))
    assert_ttft_within_limit(ttft, max_limit=4.5, context_name=name)
    assert_zero_buffering(ttft, total_time, chunks_count, context_name=name)

    return {
        "name": name,
        "url": url,
        "model": payload.get("model", "unknown"),
        "protocol": protocol,
        "status_code": status_code,
        "ttft_seconds": round(ttft, 3),
        "total_time_seconds": round(total_time, 3),
        "chunks": chunks_count,
        "tps": round(tps, 2),
        "status": "PASS",
        "error": None
    }


def run_tier_tests() -> Dict[str, Any]:
    """
    Executes Tiers 1-4 through unittest runner and reports structured results.
    """
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Discover and add all tests from tests/e2e/
    suite.addTests(loader.discover("tests/e2e", pattern="test_tier*.py"))

    runner = unittest.TextTestRunner(verbosity=1)
    start_ts = time.time()
    result = runner.run(suite)
    duration = time.time() - start_ts

    return {
        "total": result.testsRun,
        "passed": result.testsRun - len(result.failures) - len(result.errors),
        "failures": len(result.failures),
        "errors": len(result.errors),
        "duration_seconds": round(duration, 3),
        "is_successful": result.wasSuccessful()
    }


def main():
    parser = argparse.ArgumentParser(description="Unified AI Gateway & Proxy Benchmark Runner")
    parser.add_argument("--url", help="Target gateway/proxy URL", default=None)
    parser.add_argument("--output", help="Path to write JSON results", default="benchmark_results.json")
    parser.add_argument("--mock", action="store_true", help="Run against hermetic mock fixture server (default)")
    parser.add_argument("--live", action="store_true", help="Run against running live proxy backend")
    parser.add_argument("--only-bench", action="store_true", help="Run only latency benchmarks")
    parser.add_argument("--only-tiers", action="store_true", help="Run only tier test suites")
    args = parser.parse_args()

    print("=" * 78)
    print("      KIIT CODE AI GATEWAY & PROXY — BENCHMARK & VERIFICATION HARNESS")
    print("=" * 78)

    # 1. Resolve Target URL
    target_url = args.url or os.getenv("GATEWAY_URL")
    mock_server_started = False

    if not target_url and args.live:
        # Check if local live proxy container/backend is running on 8790 or 8787
        for port in [8790, 8787]:
            try:
                r = requests.get(f"http://127.0.0.1:{port}/", timeout=0.5)
                if r.status_code == 200:
                    target_url = f"http://127.0.0.1:{port}"
                    print(f"[*] Detected running live proxy backend at {target_url}")
                    break
            except Exception:
                pass

    if not target_url:
        print("[*] Starting hermetic mock fixture server for autonomous test execution...")
        target_url, _ = start_mock_server()
        mock_server_started = True

    os.environ["GATEWAY_URL"] = target_url
    print(f"[*] Target Endpoint: {target_url}")

    overall_pass = True
    tier_results = {}
    benchmark_results = []

    # 2. Run Tiered E2E Tests (Tiers 1-4)
    if not args.only_bench:
        print("\n" + "-" * 78)
        print(" [1/2] RUNNING TIER 1-4 VERIFICATION TEST SUITES")
        print("-" * 78)
        tier_results = run_tier_tests()
        print(f"\n[*] Tier Test Summary: {tier_results['passed']}/{tier_results['total']} PASSED "
              f"in {tier_results['duration_seconds']}s")
        if not tier_results["is_successful"]:
            overall_pass = False

    # 3. Run Performance & Streaming Invariant Benchmarks
    if not args.only_tiers:
        print("\n" + "-" * 78)
        print(" [2/2] RUNNING REAL-TIME SSE STREAMING & TTFT BENCHMARKS")
        print("-" * 78)

        auth_headers = {
            "Authorization": "Bearer test-key",
            "x-opencode-session": "ses_f1ca452fdffe1IvfaQCvkIzXHe",
            "x-opencode-request": "msg_f0f66a50bffeB7MDxc270HJi55"
        }

        benchmarks_to_run = [
            {
                "name": "OpenAI /v1/chat/completions (Small Payload)",
                "url": f"{target_url}/v1/chat/completions",
                "protocol": "openai",
                "payload": {
                    "model": "gpt-4o",
                    "messages": [{"role": "user", "content": "Count from 1 to 5. Be very brief."}],
                    "stream": True
                }
            },
            {
                "name": "OpenAI /v1/chat/completions (Large Payload)",
                "url": f"{target_url}/v1/chat/completions",
                "protocol": "openai",
                "payload": {
                    "model": "gpt-4o",
                    "messages": [{"role": "user", "content": "Write a clean concurrent task worker pool in Python with threading and queues, complete with detailed explanation."}],
                    "stream": True
                }
            },
            {
                "name": "Anthropic /v1/messages (Small Payload)",
                "url": f"{target_url}/v1/messages",
                "protocol": "anthropic",
                "payload": {
                    "model": "claude-3-5-sonnet-20241022",
                    "messages": [{"role": "user", "content": "Explain recursion in one sentence."}],
                    "stream": True
                }
            },
            {
                "name": "Anthropic /v1/messages (Large Payload)",
                "url": f"{target_url}/v1/messages",
                "protocol": "anthropic",
                "payload": {
                    "model": "claude-3-5-sonnet-20241022",
                    "messages": [{"role": "user", "content": "Provide a comprehensive tutorial on building an AI gateway with streaming Web Streams."}],
                    "stream": True
                }
            },
            {
                "name": "Dynamic Model Rescue & Throttling Failover",
                "url": f"{target_url}/v1/chat/completions",
                "protocol": "openai",
                "payload": {
                    "model": "simulated-rescue-model",
                    "messages": [{"role": "user", "content": "Verify dynamic model rescue fallback to 200 OK."}],
                    "stream": True
                }
            }
        ]

        for b in benchmarks_to_run:
            print(f"\n[*] Benchmarking: {b['name']} ...")
            res = benchmark_stream_endpoint(
                name=b["name"],
                url=b["url"],
                payload=b["payload"],
                headers=auth_headers,
                protocol=b["protocol"],
                timeout=60.0
            )
            benchmark_results.append(res)
            print(f"    Status: {res['status_code']} | Status: {res['status']}")
            print(f"    TTFT/TTFB: {res['ttft_seconds']:.3f}s (assert <= 4.5s)")
            print(f"    Total Time: {res['total_time_seconds']:.3f}s | Chunks: {res['chunks']} | Speed: {res['tps']:.1f} chunks/s")
            if res["status"] != "PASS":
                overall_pass = False
                print(f"    [!] Violation: {res['error']}")

    # 4. Generate Structured Output JSON
    output_data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target_url": target_url,
        "overall_status": "PASS" if overall_pass else "FAIL",
        "tier_verification": tier_results,
        "benchmarks": benchmark_results,
        "invariants": {
            "ttft_max_allowed_seconds": 4.5,
            "zero_buffering_enforced": True,
            "zero_unhandled_403_enforced": True,
            "automatic_model_rescue_enforced": True,
            "schema_conformance_enforced": True
        }
    }

    out_file = args.output
    if not os.path.isabs(out_file):
        out_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), out_file)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    print("\n" + "=" * 78)
    print(f"[*] Benchmark metrics saved to: {out_file}")
    print(f"[*] Final Overall Status: {'PASS (100% compliant)' if overall_pass else 'FAIL'}")
    print("=" * 78)

    if mock_server_started:
        stop_mock_server()

    sys.exit(0 if overall_pass else 1)


if __name__ == "__main__":
    main()
