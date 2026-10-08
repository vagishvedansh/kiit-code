#!/usr/bin/env python3
"""
Tor & Proxy Architecture Latency and TTFB/TTFT Benchmark.
Compares:
1. Python proxy with Tor + obfs4 (localhost:8767)
2. Go proxy on Render with Tor delay (kiitcode.onrender.com)
3. Go proxy Local Direct/Tor (localhost:8790)

Target Model: muse-spark-1.3-contributor-free / muse-spark-1.3
"""

import json
import time
import requests
from typing import Dict, Any

TARGETS = [
    {
        "name": "Python Proxy (Tor + obfs4)",
        "url": "http://127.0.0.1:8767/v1/chat/completions",
        "headers": {"Content-Type": "application/json"},
        "model": "muse-spark-1.3-contributor-free",
        "timeout": 45.0,
    },
    {
        "name": "Go Proxy on Render (Render Network + Tor)",
        "url": "https://kiitcode.onrender.com/v1/chat/completions",
        "headers": {
            "Content-Type": "application/json",
            "Authorization": "Bearer test-key"
        },
        "model": "muse-spark",
        "timeout": 45.0,
    },
    {
        "name": "Go Proxy Local (Direct-First + Tor)",
        "url": "http://127.0.0.1:8790/v1/chat/completions",
        "headers": {
            "Content-Type": "application/json",
            "Authorization": "Bearer sk-kiitcode-secret-2026"
        },
        "model": "muse-spark-1.3-contributor-free",
        "timeout": 45.0,
    }
]

def test_streaming(target: Dict[str, Any], prompt: str = "Explain quantum computing in one sentence."):
    payload = {
        "model": target["model"],
        "stream": True,
        "messages": [{"role": "user", "content": prompt}]
    }
    
    t0 = time.perf_counter()
    first_token_time = None
    chunks_count = 0
    full_content = ""
    status_code = None
    error = None
    model_returned = target["model"]

    try:
        resp = requests.post(
            target["url"],
            headers=target["headers"],
            json=payload,
            stream=True,
            timeout=target["timeout"]
        )
        status_code = resp.status_code
        
        if status_code != 200:
            err_text = resp.text[:200]
            error = f"HTTP {status_code}: {err_text}"
        else:
            for line in resp.iter_lines():
                if not line:
                    continue
                line_str = line.decode("utf-8", errors="replace")
                if line_str.startswith("data: ") and line_str != "data: [DONE]":
                    if first_token_time is None:
                        first_token_time = time.perf_counter()
                    chunks_count += 1
                    try:
                        chunk_json = json.loads(line_str[6:])
                        if "model" in chunk_json:
                            model_returned = chunk_json["model"]
                        delta = chunk_json.get("choices", [{}])[0].get("delta", {})
                        if "content" in delta and delta["content"]:
                            full_content += delta["content"]
                    except Exception:
                        pass
    except Exception as e:
        error = str(e)
    
    t_end = time.perf_counter()
    ttft = (first_token_time - t0) if first_token_time else (t_end - t0)
    total_time = t_end - t0

    return {
        "name": target["name"],
        "mode": "streaming",
        "status_code": status_code,
        "ttft_s": round(ttft, 3) if first_token_time else None,
        "total_time_s": round(total_time, 3),
        "chunks": chunks_count,
        "content_length": len(full_content),
        "model_returned": model_returned,
        "error": error
    }

def test_non_streaming(target: Dict[str, Any], prompt: str = "Respond with CONFIRMED"):
    payload = {
        "model": target["model"],
        "stream": False,
        "messages": [{"role": "user", "content": prompt}]
    }

    t0 = time.perf_counter()
    status_code = None
    error = None
    model_returned = target["model"]
    content = ""

    try:
        resp = requests.post(
            target["url"],
            headers=target["headers"],
            json=payload,
            timeout=target["timeout"]
        )
        status_code = resp.status_code
        if status_code != 200:
            err_text = resp.text[:200]
            error = f"HTTP {status_code}: {err_text}"
        else:
            data = resp.json()
            model_returned = data.get("model", target["model"])
            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
    except Exception as e:
        error = str(e)

    total_time = time.perf_counter() - t0

    return {
        "name": target["name"],
        "mode": "non-streaming",
        "status_code": status_code,
        "ttfb_s": round(total_time, 3),
        "total_time_s": round(total_time, 3),
        "content": content[:60],
        "model_returned": model_returned,
        "error": error
    }

def main():
    print("=" * 80)
    print("    BENCHMARK: PYTHON (TOR+OBFS4) vs RENDER (TOR DELAY) vs GO LOCAL")
    print("=" * 80)

    results = []

    print("\n--- [1] NON-STREAMING ROUND ---")
    for t in TARGETS:
        print(f"Testing {t['name']} (non-streaming)...")
        r = test_non_streaming(t)
        results.append(r)
        print(f"  -> Status: {r['status_code']} | Latency: {r['total_time_s']}s | Model: {r['model_returned']} | Error: {r['error']}")

    print("\n--- [2] STREAMING ROUND ---")
    for t in TARGETS:
        print(f"Testing {t['name']} (streaming)...")
        r = test_streaming(t)
        results.append(r)
        print(f"  -> Status: {r['status_code']} | TTFT: {r['ttft_s']}s | Total: {r['total_time_s']}s | Chunks: {r['chunks']} | Model: {r['model_returned']} | Error: {r['error']}")

    print("\n" + "=" * 80)
    print("SUMMARY RESULTS JSON:")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
