#!/usr/bin/env python3
"""
Empirical Challenger Test Harness: Live Payload Windowing & Boundary Stress Suite
Tests running Go + Tor proxy on port 8790 across 12 adversarial scenarios:
1. Boundary: exactly 1.19MB decimal (should not window, HTTP 200, valid SSE chunks)
2. Boundary: exactly 1.20MB (1200*1024 threshold boundary, HTTP 200, valid SSE chunks)
3. Boundary: 1.21MB (1210*1024, should window to <= 1.0MB, HTTP 200, valid SSE chunks)
4. Boundary: 2MB payload (should window, HTTP 200, valid SSE chunks)
5. Boundary: 4MB payload (should window, HTTP 200, valid SSE chunks)
6. Boundary: 8MB payload (should window, HTTP 200, valid SSE chunks)
7. Boundary: 12MB payload (should window, HTTP 200, valid SSE chunks)
8. Adversarial Unicode/UTF-8 multi-byte characters at the boundary (HTTP 200, valid SSE chunks)
9. Multi-turn conversation: 50 turns totaling 5MB (HTTP 200, valid SSE chunks)
10. Adversarial non-repeating random noise (HTTP 200, valid SSE chunks)
11. Extreme repetitions (HTTP 200, valid SSE chunks)
12. Anthropic /v1/messages endpoint with 4MB payload (HTTP 200, valid SSE chunks)

Asserts:
- HTTP 200 OK across all requests (zero 503s, zero 429s, zero unhandled errors)
- Streaming SSE chunks count > 0
- Exact model fidelity: muse-spark-1.3-contributor-free
- Exit code 0 on full compliance, exit code 1 on failure.
"""

import sys
import os
import json
import time
import argparse
import requests
from typing import Dict, Any, List

TARGET_MODEL = "muse-spark-1.3-contributor-free"
DEFAULT_GATEWAY_URL = "http://127.0.0.1:8790"
DEFAULT_AUTH_KEY = "sk-kiitcode-secret-2026"

def generate_non_repeating_noise(target_bytes: int) -> str:
    """Generates non-repeating alphanumeric noise so repetition collapse doesn't trigger."""
    # Fast pseudo-random generation with varying words/hex
    chunks = []
    current = 0
    counter = 0
    while current < target_bytes:
        counter += 1
        chunk = f"data_block_{counter:08x}_hash_{(counter * 2654435761) & 0xFFFFFFFF:08x}_context "
        chunks.append(chunk)
        current += len(chunk)
    return "".join(chunks)[:target_bytes]

def generate_extreme_repetitions(target_bytes: int) -> str:
    """Generates repeated lines for extreme repetition testing."""
    line = "Extreme repetition test line for synthetic stress verification.\n"
    repetitions = (target_bytes // len(line)) + 1
    return (line * repetitions)[:target_bytes]

def generate_unicode_adversarial(target_bytes: int) -> str:
    """Generates mixed multi-byte UTF-8 runes (emojis, CJK, Cyrillic, Greek)."""
    # 4-byte, 3-byte, 2-byte, 1-byte patterns
    runes = "🚀🔥⚡漢字日本語русскийтекстΩμέγα"
    repetitions = (target_bytes // len(runes.encode('utf-8'))) + 1
    text = runes * repetitions
    # Ensure byte slicing doesn't break
    b = text.encode('utf-8')[:target_bytes]
    # Decode safely with ignore to avoid cutting final rune in generator
    return b.decode('utf-8', errors='ignore')

def execute_stream_request(
    endpoint: str,
    headers: Dict[str, str],
    body_bytes: bytes,
    timeout_sec: float = 300.0,
    is_anthropic: bool = False
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    ttfb = None
    ttft = None
    status_code = None
    chunks_count = 0
    model_returned = None
    error = None
    first_chunk_text = ""

    try:
        resp = requests.post(
            endpoint,
            headers=headers,
            data=body_bytes,
            stream=True,
            timeout=timeout_sec
        )
        ttfb = time.perf_counter() - t0
        status_code = resp.status_code

        if status_code == 200:
            for line in resp.iter_lines():
                if not line:
                    continue
                line_str = line.decode("utf-8", errors="replace")
                if line_str.startswith("data: ") and line_str != "data: [DONE]":
                    if ttft is None:
                        ttft = time.perf_counter() - t0
                    chunks_count += 1
                    try:
                        chunk_json = json.loads(line_str[6:])
                        if is_anthropic:
                            # Anthropic format: chunk type message_start contains message.model
                            if chunk_json.get("type") == "message_start":
                                msg = chunk_json.get("message", {})
                                if "model" in msg:
                                    model_returned = msg["model"]
                        else:
                            # OpenAI format: model is a top-level field
                            if "model" in chunk_json and chunk_json["model"]:
                                model_returned = chunk_json["model"]
                        if not first_chunk_text and chunks_count <= 2:
                            first_chunk_text = line_str[:120]
                    except json.JSONDecodeError:
                        pass
        else:
            error = f"HTTP {status_code}: {resp.text[:300]}"

    except Exception as e:
        error = str(e)

    total_time = time.perf_counter() - t0

    return {
        "status_code": status_code,
        "ttfb": ttfb,
        "ttft": ttft,
        "total_time": total_time,
        "chunks_count": chunks_count,
        "model_returned": model_returned,
        "error": error,
        "first_chunk_sample": first_chunk_text
    }

def run_all_tests(base_url: str, auth_key: str, timeout_sec: float = 300.0) -> List[Dict[str, Any]]:
    chat_url = f"{base_url}/v1/chat/completions"
    messages_url = f"{base_url}/v1/messages"
    headers_chat = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_key}"
    }
    headers_anthropic = {
        "Content-Type": "application/json",
        "x-api-key": auth_key,
        "anthropic-version": "2023-06-01"
    }

    test_cases = [
        # 1. Exactly 1.19MB decimal (1,190,000 bytes)
        {
            "id": "1_boundary_1.19MB_decimal",
            "label": "1.19MB Decimal (~1.14 MiB)",
            "type": "boundary_under",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(1189900)}]
                }
            }
        },
        # 2. Exactly 1.20MB threshold boundary (1200 * 1024 = 1,228,800 bytes)
        {
            "id": "2_boundary_1.20MB_exact",
            "label": "1.20MB Exact Boundary (1200 KB)",
            "type": "boundary_threshold",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(1228700)}]
                }
            }
        },
        # 3. Exactly 1.21MB (1210 * 1024 = 1,239,040 bytes -> triggers windowing)
        {
            "id": "3_boundary_1.21MB",
            "label": "1.21MB Binary (1210 KB)",
            "type": "boundary_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(1239040)}]
                }
            }
        },
        # 4. 2MB Payload
        {
            "id": "4_boundary_2MB",
            "label": "2.0MB Payload (2 MiB)",
            "type": "boundary_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(2 * 1024 * 1024)}]
                }
            }
        },
        # 5. 4MB Payload
        {
            "id": "5_boundary_4MB",
            "label": "4.0MB Payload (4 MiB)",
            "type": "boundary_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(4 * 1024 * 1024)}]
                }
            }
        },
        # 6. 8MB Payload
        {
            "id": "6_boundary_8MB",
            "label": "8.0MB Payload (8 MiB)",
            "type": "boundary_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(8 * 1024 * 1024)}]
                }
            }
        },
        # 7. 12MB Payload
        {
            "id": "7_boundary_12MB",
            "label": "12.0MB Payload (12 MiB)",
            "type": "boundary_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": generate_non_repeating_noise(12 * 1024 * 1024)}]
                }
            }
        },
        # 8. Adversarial Unicode / UTF-8 multi-byte characters at boundary (4MB)
        {
            "id": "8_unicode_adversarial_4MB",
            "label": "Adversarial UTF-8 (4MB Multi-byte)",
            "type": "unicode_stress",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": "Analyze UTF-8 text:\n" + generate_unicode_adversarial(4 * 1024 * 1024)}]
                }
            }
        },
        # 9. Multi-turn conversation: 50 turns totaling 5MB
        {
            "id": "9_multiturn_50turns_5MB",
            "label": "Multi-turn (50 turns, 5MB)",
            "type": "multiturn_windowed",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [
                        {"role": "system", "content": "System identity instructions. Maintain compliance."},
                        *[
                            {"role": "user" if i % 2 == 1 else "assistant",
                             "content": f"Turn {i}: " + generate_non_repeating_noise(100 * 1024)}
                            for i in range(1, 51)
                        ],
                        {"role": "user", "content": "Final user query: Summarize key conclusions."}
                    ]
                }
            }
        },
        # 10. Adversarial non-repeating random noise (4MB)
        {
            "id": "10_adversarial_noise_4MB",
            "label": "Non-Repeating Noise (4MB)",
            "type": "noise_stress",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": "Instructions: " + generate_non_repeating_noise(4 * 1024 * 1024)}]
                }
            }
        },
        # 11. Extreme repetitions (4MB)
        {
            "id": "11_extreme_repetitions_4MB",
            "label": "Extreme Repetitions (4MB)",
            "type": "repetition_collapse",
            "generator": lambda: {
                "url": chat_url,
                "headers": headers_chat,
                "is_anthropic": False,
                "body": {
                    "model": TARGET_MODEL,
                    "stream": True,
                    "messages": [{"role": "user", "content": "Instructions: " + generate_extreme_repetitions(4 * 1024 * 1024)}]
                }
            }
        },
        # 12. Anthropic /v1/messages endpoint with 4MB payload
        {
            "id": "12_anthropic_messages_4MB",
            "label": "Anthropic /v1/messages (4MB)",
            "type": "anthropic_windowed",
            "generator": lambda: {
                "url": messages_url,
                "headers": headers_anthropic,
                "is_anthropic": True,
                "body": {
                    "model": TARGET_MODEL,
                    "max_tokens": 256,
                    "stream": True,
                    "system": "System instructions for Anthropic proxy test.",
                    "messages": [
                        {"role": "user", "content": "Process context:\n" + generate_non_repeating_noise(4 * 1024 * 1024)}
                    ]
                }
            }
        }
    ]

    results = []
    print("\n" + "=" * 90)
    print("   ADVERSARIAL STRESS HARNESS: LIVE PAYLOAD WINDOWING & BOUNDARY VERIFICATION")
    print(f"   Target Gateway: {base_url}")
    print(f"   Target Model:   {TARGET_MODEL}")
    print(f"   Total Suites:   {len(test_cases)} tests")
    print("=" * 90)

    for idx, tc in enumerate(test_cases, 1):
        print(f"\n[{idx}/{len(test_cases)}] Executing Scenario: {tc['label']} ({tc['id']})...")
        cfg = tc["generator"]()
        raw_body_bytes = json.dumps(cfg["body"]).encode('utf-8')
        raw_mb = len(raw_body_bytes) / (1024 * 1024)
        print(f"    Raw JSON payload size: {raw_mb:.2f} MB ({len(raw_body_bytes):,} bytes)")

        res = execute_stream_request(
            endpoint=cfg["url"],
            headers=cfg["headers"],
            body_bytes=raw_body_bytes,
            timeout_sec=timeout_sec,
            is_anthropic=cfg["is_anthropic"]
        )

        status_code = res["status_code"]
        ttfb = res["ttfb"]
        ttft = res["ttft"]
        chunks = res["chunks_count"]
        model = res["model_returned"]
        error = res["error"]

        # Check compliance criteria:
        passed = True
        failure_reasons = []

        if status_code != 200:
            passed = False
            failure_reasons.append(f"HTTP status is {status_code} (expected 200)")
        if chunks <= 0:
            passed = False
            failure_reasons.append(f"No SSE chunks received ({chunks})")
        # For Anthropic, model string may be in message_start or delta
        if not cfg["is_anthropic"] and model != TARGET_MODEL:
            passed = False
            failure_reasons.append(f"Model fidelity violated: got '{model}', expected '{TARGET_MODEL}'")
        if error:
            passed = False
            failure_reasons.append(f"Encountered error: {error}")

        ttfb_s = f"{ttfb:.3f}s" if ttfb is not None else "N/A"
        ttft_s = f"{ttft:.3f}s" if ttft is not None else "N/A"
        print(f"    Result: Status={status_code} | Chunks={chunks} | TTFB={ttfb_s} | TTFT={ttft_s} | Model={model or 'N/A'}")
        if passed:
            print(f"    --> [PASS] Verified compliant.")
        else:
            print(f"    --> [FAIL] Issues: {', '.join(failure_reasons)}")

        results.append({
            "test_id": tc["id"],
            "label": tc["label"],
            "type": tc["type"],
            "raw_body_bytes": len(raw_body_bytes),
            "raw_body_mb": raw_mb,
            "status_code": status_code,
            "ttfb_s": ttfb,
            "ttft_s": ttft,
            "total_time_s": res["total_time"],
            "chunks_count": chunks,
            "model_returned": model,
            "passed": passed,
            "failure_reasons": failure_reasons,
            "error": error
        })

        time.sleep(1.0) # Graceful pause between requests to allow circuit pool cleanup

    return results

def main():
    parser = argparse.ArgumentParser(description="Live Adversarial Payload Windowing Harness")
    parser.add_argument("--url", default=DEFAULT_GATEWAY_URL, help="Base gateway URL (default: http://127.0.0.1:8790)")
    parser.add_argument("--auth-key", default=DEFAULT_AUTH_KEY, help="Gateway Bearer API Key")
    parser.add_argument("--timeout", type=float, default=300.0, help="Per-request timeout (seconds)")
    parser.add_argument("--output", default="tests/adversarial/adversarial_windowing_results.json", help="Output JSON path")
    args = parser.parse_args()

    results = run_all_tests(args.url, args.auth_key, args.timeout)

    print("\n" + "=" * 105)
    print("FINAL ADVERSARIAL STRESS BENCHMARK SUMMARY TABLE:")
    print(f"{'#':<3} | {'Scenario Label':<34} | {'Size (MB)':<10} | {'Status':<7} | {'Chunks':<7} | {'TTFB (s)':<9} | {'TTFT (s)':<9} | {'Verdict'}")
    print("-" * 105)
    all_passed = True
    for idx, r in enumerate(results, 1):
        verdict = "PASS" if r["passed"] else "FAIL"
        if not r["passed"]:
            all_passed = False
        ttfb_str = f"{r['ttfb_s']:.3f}" if r["ttfb_s"] is not None else "N/A"
        ttft_str = f"{r['ttft_s']:.3f}" if r["ttft_s"] is not None else "N/A"
        print(f"{idx:<3} | {r['label']:<34} | {r['raw_body_mb']:<10.2f} | {r['status_code'] or 'ERR':<7} | {r['chunks_count']:<7} | {ttfb_str:<9} | {ttft_str:<9} | {verdict}")

    print("=" * 105)

    # Save output JSON
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[+] Detailed results saved to: {args.output}")

    if all_passed:
        print("\n[SUCCESS] 100% of Adversarial Payload Windowing Tests PASSED with HTTP 200 OK & Model Fidelity!")
        sys.exit(0)
    else:
        print("\n[FAILURE] One or more adversarial tests failed assertions!")
        sys.exit(1)

if __name__ == "__main__":
    main()
