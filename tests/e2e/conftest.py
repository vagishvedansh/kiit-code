"""
E2E Test Configuration and Fixtures.
Provides client helpers, URL detection (Live vs Mock), and SSE stream readers.
"""

import json
import os
import time
import requests
from typing import Dict, Any, List, Tuple, Optional
from tests.e2e.mock_server import start_mock_server, stop_mock_server


def get_base_url() -> str:
    """
    Returns the target test URL. Checks env var GATEWAY_URL or GO_BACKEND_URL.
    If none provided or unreachable, starts the hermetic mock fixture server.
    """
    env_url = os.getenv("GATEWAY_URL") or os.getenv("GO_BACKEND_URL")
    if env_url:
        return env_url.rstrip("/")

    # If port 8790 or 8787 is alive, check if caller wants live target
    if os.getenv("TARGET_LIVE") == "1":
        for live_port in [8787, 8790]:
            try:
                r = requests.get(f"http://127.0.0.1:{live_port}/", timeout=1.0)
                if r.status_code == 200:
                    return f"http://127.0.0.1:{live_port}"
            except Exception:
                pass

    # Fallback to hermetic mock server for autonomous testing
    url, _ = start_mock_server()
    return url


def execute_streaming_request(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 60.0
) -> Tuple[int, List[str], float, float]:
    """
    Executes a streaming POST request.
    Returns: (status_code, lines_received, ttft, total_duration)
    """
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    start_time = time.perf_counter()
    first_token_time: Optional[float] = None
    lines: List[str] = []

    try:
        with requests.post(url, json=payload, headers=req_headers, stream=True, timeout=timeout) as resp:
            status_code = resp.status_code
            if status_code != 200:
                return status_code, [resp.text], 0.0, time.perf_counter() - start_time

            saw_anthropic_stop = False
            for raw_line in resp.iter_lines():
                if raw_line:
                    now = time.perf_counter()
                    if first_token_time is None:
                        first_token_time = now
                    decoded = raw_line.decode("utf-8")
                    lines.append(decoded)
                    if decoded == "data: [DONE]":
                        break
                    if decoded.strip() == "event: message_stop":
                        saw_anthropic_stop = True
                    elif saw_anthropic_stop and decoded.startswith("data: "):
                        break
    except Exception as exc:
        duration = time.perf_counter() - start_time
        return 0, [str(exc)], 0.0, duration

    end_time = time.perf_counter()
    ttft = (first_token_time - start_time) if first_token_time else 0.0
    total_time = end_time - start_time
    return status_code, lines, ttft, total_time


def execute_json_request(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 30.0
) -> Tuple[int, Dict[str, Any], requests.structures.CaseInsensitiveDict]:
    """
    Executes a non-streaming POST request.
    Returns: (status_code, response_json_dict, headers)
    """
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    resp = requests.post(url, json=payload, headers=req_headers, timeout=timeout)
    try:
        body = resp.json()
    except Exception:
        body = {"raw_text": resp.text}
    return resp.status_code, body, resp.headers
