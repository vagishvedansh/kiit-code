#!/usr/bin/env python3
"""
Empirical Adversarial Challenge Suite for Milestone M3 (Protocol & Format Parity).
Targets running server on port 8793 (or configured PORT).
"""

import json
import sys
import time
import requests

PORT = 8793
BASE_URL = f"http://127.0.0.1:{PORT}"

AUTH_HEADERS = {
    "Authorization": "Bearer test-key",
    "x-opencode-session": "ses_f1ca452fdffe1IvfaQCvkIzXHe",
    "x-opencode-request": "msg_f0f66a50bffeB7MDxc270HJi55",
    "Content-Type": "application/json"
}

def log(msg):
    print(f"[CHALLENGER] {msg}")

def test_malformed_payloads():
    log("=== Testing Malformed & Empty Payloads ===")
    
    test_cases = [
        ("Empty body", b""),
        ("Malformed JSON", b"{ invalid_json "),
        ("Missing messages field", b"{\"model\": \"gpt-4o\"}"),
        ("Empty messages array", b"{\"model\": \"gpt-4o\", \"messages\": []}"),
        ("Messages is a string", b"{\"model\": \"gpt-4o\", \"messages\": \"not an array\"}"),
        ("Messages is an integer", b"{\"model\": \"gpt-4o\", \"messages\": 123}"),
        ("Messages is null", b"{\"model\": \"gpt-4o\", \"messages\": null}"),
    ]
    
    endpoints = [
        ("/v1/chat/completions", {"Authorization": "Bearer test-key", "Content-Type": "application/json"}),
        ("/v1/messages", {"x-api-key": "test-key", "anthropic-version": "2023-06-01", "Content-Type": "application/json"}),
        ("/v1/v1/messages", {"x-api-key": "test-key", "anthropic-version": "2023-06-01", "Content-Type": "application/json"}),
    ]
    
    for path, headers in endpoints:
        for name, body in test_cases:
            url = f"{BASE_URL}{path}"
            resp = requests.post(url, data=body, headers=headers)
            assert resp.status_code == 400, f"FAIL: {path} with {name} returned status {resp.status_code}, expected 400"
            log(f"PASS: {path} - {name} -> 400 Bad Request")
                
    log("All malformed and empty payload tests PASSED (HTTP 400 asserted).\n")

def test_openai_streaming_latency_and_zero_buffering():
    log("=== Testing OpenAI Streaming Latency & Zero Buffering ===")
    url = f"{BASE_URL}/v1/chat/completions"
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Count from 1 to 5. Be very brief."}],
        "stream": True
    }
    
    start_time = time.perf_counter()
    resp = requests.post(url, json=payload, headers=AUTH_HEADERS, stream=True, timeout=60)
    assert resp.status_code == 200, f"Expected 200 OK, got {resp.status_code}: {resp.text}"
    
    first_chunk_time = None
    chunk_times = []
    chunk_deltas = []
    done_count = 0
    full_text = ""
    
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8")
        now = time.perf_counter()
        
        if line == "data: [DONE]":
            done_count += 1
            continue
            
        if line.startswith("data: "):
            if first_chunk_time is None:
                first_chunk_time = now
            chunk_times.append(now)
            chunk_json = json.loads(line[6:])
            
            # Assert OpenAI chunk schema
            assert chunk_json.get("object") == "chat.completion.chunk", f"Invalid object: {chunk_json}"
            assert "choices" in chunk_json, f"Missing choices: {chunk_json}"
            choices = chunk_json["choices"]
            assert len(choices) > 0, f"Empty choices: {chunk_json}"
            delta = choices[0].get("delta", {})
            
            # Assert no reasoning content leak
            assert "reasoning_content" not in delta, f"Leaked reasoning_content in delta: {delta}"
            assert "reasoning_details" not in delta, f"Leaked reasoning_details in delta: {delta}"
            assert "reasoning_content" not in choices[0], f"Leaked reasoning_content in choice: {choices[0]}"
            
            content = delta.get("content", "")
            if content:
                chunk_deltas.append(content)
                full_text += content
                assert "<think>" not in content, f"Leaked <think> in delta: {content}"
                assert "</think>" not in content, f"Leaked </think> in delta: {content}"
    
    total_time = time.perf_counter() - start_time
    ttft = first_chunk_time - start_time if first_chunk_time else total_time
    
    log(f"OpenAI Stream: TTFT = {ttft:.3f}s (Invariant <= 4.5s)")
    log(f"OpenAI Stream: Total Time = {total_time:.3f}s")
    log(f"OpenAI Stream: Total Chunks = {len(chunk_times)}")
    log(f"OpenAI Stream: DONE count = {done_count}")
    log(f"OpenAI Stream: Full Text = {repr(full_text)}")
    
    assert ttft <= 4.5, f"TTFT {ttft:.3f}s exceeded 4.5s threshold!"
    assert done_count == 1, f"Expected exactly one [DONE] event, got {done_count}"
    assert len(chunk_times) >= 2, f"Expected >= 2 chunks, got {len(chunk_times)}"
    
    # Assert zero buffering: chunks arrived incrementally
    if len(chunk_times) >= 3 and total_time > 0.5:
        assert ttft < total_time * 0.98, "Buffering detected: TTFT is >= 98% of total stream duration"
        
    log("PASS: OpenAI streaming latency <= 4.5s, zero buffering verified, exactly one [DONE], schema clean.\n")
    return ttft

def test_anthropic_streaming_latency_and_zero_buffering():
    log("=== Testing Anthropic Streaming Latency & Zero Buffering ===")
    url = f"{BASE_URL}/v1/messages"
    headers = dict(AUTH_HEADERS)
    headers["x-api-key"] = "test-key"
    headers["anthropic-version"] = "2023-06-01"
    payload = {
        "model": "claude-3-5-sonnet",
        "messages": [{"role": "user", "content": "Count from 1 to 5. Be very brief."}],
        "stream": True,
        "max_tokens": 100
    }
    
    start_time = time.perf_counter()
    resp = requests.post(url, json=payload, headers=headers, stream=True, timeout=60)
    assert resp.status_code == 200, f"Expected 200 OK, got {resp.status_code}: {resp.text}"
    
    first_chunk_time = None
    event_times = []
    events = []
    current_event_type = None
    full_text = ""
    
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8")
        now = time.perf_counter()
        
        if line.startswith("event: "):
            current_event_type = line[7:].strip()
            events.append(current_event_type)
            if first_chunk_time is None:
                first_chunk_time = now
            event_times.append(now)
        elif line.startswith("data: "):
            data = json.loads(line[6:])
            if current_event_type == "content_block_delta":
                delta = data.get("delta", {})
                text = delta.get("text", "")
                full_text += text
                assert "<think>" not in text, f"Leaked <think> in Anthropic delta: {text}"
                assert "</think>" not in text, f"Leaked </think> in Anthropic delta: {text}"
    
    total_time = time.perf_counter() - start_time
    ttft = first_chunk_time - start_time if first_chunk_time else total_time
    
    log(f"Anthropic Stream: TTFT = {ttft:.3f}s (Invariant <= 4.5s)")
    log(f"Anthropic Stream: Total Time = {total_time:.3f}s")
    log(f"Anthropic Stream: Total Events = {len(events)}")
    log(f"Anthropic Stream: Lifecycle Events Seen = {set(events)}")
    log(f"Anthropic Stream: Full Text = {repr(full_text)}")
    
    assert ttft <= 4.5, f"Anthropic TTFT {ttft:.3f}s exceeded 4.5s threshold!"
    assert "message_start" in events, "Missing message_start event"
    assert "content_block_start" in events, "Missing content_block_start event"
    assert "content_block_delta" in events, "Missing content_block_delta event"
    assert "content_block_stop" in events, "Missing content_block_stop event"
    assert "message_delta" in events, "Missing message_delta event"
    assert "message_stop" in events, "Missing message_stop event"
    
    # Assert zero buffering
    delta_times = [t for i, t in enumerate(event_times) if events[i] == "content_block_delta"]
    assert len(delta_times) >= 2, f"Expected >= 2 content_block_delta events, got {len(delta_times)}"
    if len(delta_times) >= 3 and total_time > 0.5:
        assert ttft < total_time * 0.98, "Buffering detected: TTFT is >= 98% of total stream duration"
        
    log("PASS: Anthropic streaming latency <= 4.5s, zero buffering verified, full lifecycle intact.\n")
    return ttft

def test_model_rescue_streaming():
    log("=== Testing Dynamic Model Rescue with Streaming ===")
    url = f"{BASE_URL}/v1/chat/completions"
    payload = {
        "model": "simulated-rescue-model",
        "messages": [{"role": "user", "content": "Verify dynamic model rescue fallback to 200 OK."}],
        "stream": True,
        "max_tokens": 50
    }
    
    start_time = time.perf_counter()
    resp = requests.post(url, json=payload, headers=AUTH_HEADERS, stream=True, timeout=60)
    assert resp.status_code == 200, f"Model rescue failed, got status {resp.status_code}: {resp.text}"
    
    chunks = 0
    first_time = None
    for raw_line in resp.iter_lines():
        if raw_line:
            line = raw_line.decode("utf-8")
            if first_time is None:
                first_time = time.perf_counter()
            if line.startswith("data: ") and line != "data: [DONE]":
                chunks += 1
                
    ttft = first_time - start_time if first_time else 0.0
    log(f"Model Rescue: Status = 200 OK | TTFT = {ttft:.3f}s | Chunks = {chunks}")
    assert ttft <= 4.5, f"Rescued model TTFT {ttft:.3f}s exceeded 4.5s"
    assert chunks >= 1, "Rescued model returned zero chunks"
    log("PASS: Dynamic model rescue stream succeeded with 200 OK.\n")
    return ttft

if __name__ == "__main__":
    try:
        test_malformed_payloads()
        ttft_openai = test_openai_streaming_latency_and_zero_buffering()
        ttft_anthropic = test_anthropic_streaming_latency_and_zero_buffering()
        ttft_rescue = test_model_rescue_streaming()
        log(f"SUMMARY: TTFT OpenAI = {ttft_openai:.3f}s | TTFT Anthropic = {ttft_anthropic:.3f}s | TTFT Rescue = {ttft_rescue:.3f}s")
        log("ALL ADVERSARIAL HTTP INTEGRATION TESTS PASSED!")
    except Exception as e:
        log(f"TEST FAILED WITH EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
