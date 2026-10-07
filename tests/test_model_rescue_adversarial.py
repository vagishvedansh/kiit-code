#!/usr/bin/env python3
"""
Adversarial Model Rescue Test Suite for Challenger M3-2.
Tests claude-3-5-sonnet, gpt-4o, gpt-4o-mini, and unmapped models across both
OpenAI (/v1/chat/completions) and Anthropic (/v1/messages) endpoints.
Asserts that requests rescue cleanly to operational models without producing 503 or 403 FreeTierErrors.
"""

import sys
import json
import time
import requests

SERVER_URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8794"

AUTH_HEADERS = {
    "Authorization": "Bearer test-key",
    "anthropic-version": "2023-06-01",
    "x-opencode-session": "ses_f1ca452fdffe1IvfaQCvkIzXHe",
    "x-opencode-request": "msg_f0f66a50bffeB7MDxc270HJi55",
}

MODELS_TO_TEST = [
    # Explicitly mandated models
    ("claude-3-5-sonnet", True),
    ("claude-3-5-sonnet-20241022", True),
    ("gpt-4o", True),
    ("gpt-4o-mini", True),
    ("simulated-rescue-model", True),
    # Unmapped models with standard prefixes (should rescue to muse-spark-1.3-contributor-free)
    ("claude-future-unmapped-model", True),
    ("gpt-future-unmapped-model", True),
    ("deepseek-unmapped-special", True),
    ("qwen-unmapped-variant", True),
    # Completely unknown model without prefix (should return clean 404, NEVER 503 or 403)
    ("nonexistent-unsupported-model-9999", False),
]

def test_openai_chat(model: str, stream: bool, should_succeed: bool):
    url = f"{SERVER_URL}/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with only the word OK."}],
        "stream": stream,
        "max_tokens": 10,
    }
    t0 = time.time()
    try:
        resp = requests.post(url, json=payload, headers=AUTH_HEADERS, stream=stream, timeout=45)
    except Exception as e:
        return False, f"Connection failed: {e}", 0

    dur = time.time() - t0
    status = resp.status_code
    body = ""

    if stream:
        chunks = []
        for line in resp.iter_lines():
            if line:
                decoded = line.decode("utf-8")
                chunks.append(decoded)
        body = "\n".join(chunks[:10])
    else:
        body = resp.text

    # Assert no 403 FreeTierError
    if status == 403 or "FreeTierError" in body:
        return False, f"FAILED: Received HTTP {status} / FreeTierError: {body[:200]}", dur

    # Assert no 503 Overloaded
    if status == 503:
        return False, f"FAILED: Received HTTP 503 Service Unavailable / Overloaded: {body[:200]}", dur

    if should_succeed:
        if status != 200:
            return False, f"FAILED: Expected HTTP 200 OK, got {status}: {body[:200]}", dur
        if stream:
            has_data = any(c.startswith("data: ") for c in chunks if c != "data: [DONE]")
            has_done = any(c == "data: [DONE]" for c in chunks)
            if not has_data or not has_done:
                return False, f"FAILED: Incomplete stream frames: {body[:200]}", dur
        else:
            try:
                data = json.loads(body)
                if not data.get("choices") or len(data["choices"]) == 0:
                    return False, f"FAILED: Missing choices in JSON response: {body[:200]}", dur
            except Exception as e:
                return False, f"FAILED: Invalid JSON response: {e}: {body[:200]}", dur
        return True, f"HTTP {status} OK (TTFT/Total: {dur:.3f}s)", dur
    else:
        # For unsupported models, expect clean 404
        if status == 404:
            return True, f"HTTP 404 Not Found as expected for unknown model: {body[:100]}", dur
        else:
            return False, f"FAILED: Expected 404 for unknown model, got HTTP {status}: {body[:200]}", dur

def test_anthropic_messages(model: str, stream: bool, should_succeed: bool):
    url = f"{SERVER_URL}/v1/messages"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with only the word OK."}],
        "stream": stream,
        "max_tokens": 10,
    }
    t0 = time.time()
    try:
        resp = requests.post(url, json=payload, headers=AUTH_HEADERS, stream=stream, timeout=45)
    except Exception as e:
        return False, f"Connection failed: {e}", 0

    dur = time.time() - t0
    status = resp.status_code
    body = ""

    if stream:
        events = []
        for line in resp.iter_lines():
            if line:
                decoded = line.decode("utf-8")
                events.append(decoded)
        body = "\n".join(events[:10])
    else:
        body = resp.text

    # Assert no 403 FreeTierError
    if status == 403 or "FreeTierError" in body:
        return False, f"FAILED: Received HTTP 403 / FreeTierError: {body[:200]}", dur

    # Assert no 503 Overloaded
    if status == 503:
        return False, f"FAILED: Received HTTP 503 Service Unavailable / Overloaded: {body[:200]}", dur

    if should_succeed:
        if status != 200:
            return False, f"FAILED: Expected HTTP 200 OK, got {status}: {body[:200]}", dur
        if stream:
            has_start = any("message_start" in e for e in events)
            has_delta = any("content_block_delta" in e for e in events)
            has_stop = any("message_stop" in e for e in events)
            if not (has_start and has_stop):
                return False, f"FAILED: Incomplete Anthropic SSE lifecycle: {body[:200]}", dur
        else:
            try:
                data = json.loads(body)
                if data.get("type") != "message" or not data.get("content"):
                    return False, f"FAILED: Malformed Anthropic JSON message: {body[:200]}", dur
            except Exception as e:
                return False, f"FAILED: Invalid Anthropic JSON: {e}: {body[:200]}", dur
        return True, f"HTTP {status} OK (TTFT/Total: {dur:.3f}s)", dur
    else:
        if status == 404:
            return True, f"HTTP 404 Not Found as expected for unknown model: {body[:100]}", dur
        else:
            return False, f"FAILED: Expected 404 for unknown model, got HTTP {status}: {body[:200]}", dur

def run_all_tests():
    print(f"=== Adversarial Model Rescue & Fallback Verification against {SERVER_URL} ===")
    all_passed = True
    total_tests = 0
    passed_tests = 0

    for model, should_succeed in MODELS_TO_TEST:
        print(f"\n--- Testing model: {model} (Expect 200: {should_succeed}) ---")
        
        # 1. OpenAI Chat streaming
        total_tests += 1
        ok, msg, dur = test_openai_chat(model, stream=True, should_succeed=should_succeed)
        print(f"  [OpenAI Stream]     {'PASS' if ok else 'FAIL'}: {msg}")
        if ok: passed_tests += 1
        else: all_passed = False

        # 2. OpenAI Chat non-streaming
        total_tests += 1
        ok, msg, dur = test_openai_chat(model, stream=False, should_succeed=should_succeed)
        print(f"  [OpenAI Non-Stream] {'PASS' if ok else 'FAIL'}: {msg}")
        if ok: passed_tests += 1
        else: all_passed = False

        # 3. Anthropic Messages streaming
        total_tests += 1
        ok, msg, dur = test_anthropic_messages(model, stream=True, should_succeed=should_succeed)
        print(f"  [Anthropic Stream]  {'PASS' if ok else 'FAIL'}: {msg}")
        if ok: passed_tests += 1
        else: all_passed = False

        # 4. Anthropic Messages non-streaming
        total_tests += 1
        ok, msg, dur = test_anthropic_messages(model, stream=False, should_succeed=should_succeed)
        print(f"  [Anthropic Non-Str] {'PASS' if ok else 'FAIL'}: {msg}")
        if ok: passed_tests += 1
        else: all_passed = False

    print("\n" + "=" * 60)
    print(f"Model Rescue Test Summary: {passed_tests}/{total_tests} PASSED")
    print("=" * 60)
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
