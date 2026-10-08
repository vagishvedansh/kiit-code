#!/usr/bin/env python3
"""
Multi-Payload Size Latency & TTFB/TTFT Benchmark for Go + Tor Proxy.
Tests:
- Small Payload (~100 Bytes)
- Medium Payload (~50 KB)
- Large Payload (~1 MB)
- Extra-Large Payload (~8 MB)

Measures:
- TTFB (Time To First Byte / HTTP headers received)
- TTFT (Time To First Token in streaming)
- Total execution time
- Upstream status code & Model fidelity check (ensuring muse-spark-1.3 is preserved)
"""

import sys
import json
import time
import requests
from typing import Dict, Any

GATEWAY_URL = "http://127.0.0.1:8790/v1/chat/completions"
AUTH_KEY = "sk-kiitcode-secret-2026"
MODEL_NAME = "muse-spark-1.3-contributor-free"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_KEY}"
}

def generate_payload(text_size_bytes: int, prompt_instruction: str = "Acknowledge receipt and summarize in one sentence: ") -> str:
    """Generates synthetic context to reach the target byte size."""
    base_text = "The quick brown fox jumps over the lazy dog. 0123456789. AI proxy low-latency test. "
    repetitions = text_size_bytes // len(base_text)
    if repetitions <= 0:
        return prompt_instruction
    padding = base_text * repetitions
    return prompt_instruction + padding[:text_size_bytes]

def run_payload_test(label: str, target_bytes: int, stream: bool = True) -> Dict[str, Any]:
    print(f"\n[+] Testing {label} (Payload ~{target_bytes / 1024:.1f} KB, Stream={stream})...")
    content = generate_payload(target_bytes)
    actual_payload_size = len(content.encode('utf-8'))
    
    body = {
        "model": MODEL_NAME,
        "stream": stream,
        "messages": [
            {"role": "user", "content": content}
        ]
    }
    
    json_bytes = json.dumps(body).encode('utf-8')
    payload_mb = len(json_bytes) / (1024 * 1024)
    print(f"    Raw JSON body size: {payload_mb:.2f} MB ({len(json_bytes)} bytes)")

    t0 = time.perf_counter()
    ttfb = None
    ttft = None
    status_code = None
    error = None
    chunks_count = 0
    full_response = ""
    model_returned = None

    try:
        # Stream response connection to measure TTFB immediately
        resp = requests.post(
            GATEWAY_URL,
            headers=HEADERS,
            data=json_bytes,
            stream=True,
            timeout=120.0
        )
        ttfb = time.perf_counter() - t0
        status_code = resp.status_code
        print(f"    TTFB: {ttfb:.3f}s | HTTP Status: {status_code}")

        if status_code == 200:
            if stream:
                for line in resp.iter_lines():
                    if not line:
                        continue
                    line_str = line.decode("utf-8", errors="replace")
                    if line_str.startswith("data: ") and line_str != "data: [DONE]":
                        if ttft is None:
                            ttft = time.perf_counter() - t0
                        chunks_count += 1
                        try:
                            chunk_data = json.loads(line_str[6:])
                            if "model" in chunk_data:
                                model_returned = chunk_data["model"]
                            delta = chunk_data.get("choices", [{}])[0].get("delta", {})
                            if "content" in delta and delta["content"]:
                                full_response += delta["content"]
                        except Exception:
                            pass
            else:
                data = resp.json()
                model_returned = data.get("model")
                choices = data.get("choices", [])
                if choices:
                    full_response = choices[0].get("message", {}).get("content", "")
        else:
            err_peek = resp.text[:300]
            error = f"HTTP {status_code}: {err_peek}"
            print(f"    Error: {error}")

    except Exception as e:
        error = str(e)
        print(f"    Exception: {error}")

    t_end = time.perf_counter()
    total_time = t_end - t0

    result = {
        "label": label,
        "target_bytes": target_bytes,
        "actual_body_bytes": len(json_bytes),
        "stream": stream,
        "status_code": status_code,
        "ttfb_s": round(ttfb, 3) if ttfb is not None else None,
        "ttft_s": round(ttft, 3) if ttft is not None else None,
        "total_time_s": round(total_time, 3),
        "chunks": chunks_count,
        "model_returned": model_returned,
        "response_preview": full_response[:100].strip(),
        "error": error
    }
    
    print(f"    Done -> TTFB: {result['ttfb_s']}s | TTFT: {result['ttft_s']}s | Total: {result['total_time_s']}s | Model: {result['model_returned']}")
    return result

def main():
    print("=" * 80)
    print("    MULTI-PAYLOAD SIZE LATENCY BENCHMARK (GO + PRE-WARMED TOR POOL)")
    print("    Gateway: " + GATEWAY_URL)
    print("    Target Model: " + MODEL_NAME)
    print("=" * 80)

    # 1. Warm-up check
    try:
        pool_status = requests.get("http://127.0.0.1:8790/health/pool", timeout=5).json()
        print(f"Initial Pool State: Ready={pool_status.get('ready_circuits')} | InUse={pool_status.get('in_use_circuits')}")
    except Exception as e:
        print(f"Pool check warning: {e}")

    test_plan = [
        ("Small (~100B)", 100),
        ("Medium (~50KB)", 50 * 1024),
        ("Large (~1MB)", 1024 * 1024),
        ("Extra-Large (~4MB)", 4 * 1024 * 1024),
        ("Max Payload (~8MB)", 8 * 1024 * 1024),
    ]

    results = []
    for label, byte_size in test_plan:
        res = run_payload_test(label, byte_size, stream=True)
        results.append(res)
        time.sleep(1.0) # Graceful pause between requests

    print("\n" + "=" * 80)
    print("FINAL SUMMARY TABLE:")
    print(f"{'Payload Label':<20} | {'Body Size':<10} | {'Status':<7} | {'TTFB (s)':<9} | {'TTFT (s)':<9} | {'Total (s)':<9} | {'Model Verified'}")
    print("-" * 80)
    for r in results:
        size_str = f"{r['actual_body_bytes'] / 1024:.1f} KB" if r['actual_body_bytes'] < 1024*1024 else f"{r['actual_body_bytes'] / (1024*1024):.2f} MB"
        ttfb_str = str(r['ttfb_s']) if r['ttfb_s'] is not None else "N/A"
        ttft_str = str(r['ttft_s']) if r['ttft_s'] is not None else "N/A"
        model_ok = "YES" if (r['model_returned'] and "muse-spark" in r['model_returned']) else ("NO" if r['model_returned'] else "ERR")
        print(f"{r['label']:<20} | {size_str:<10} | {r['status_code'] or 'ERR':<7} | {ttfb_str:<9} | {ttft_str:<9} | {r['total_time_s']:<9} | {model_ok}")

    print("\nComplete Results JSON:")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
