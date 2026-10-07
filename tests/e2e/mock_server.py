"""
Hermetic Mock Gateway & Upstream Server Fixture for E2E Testing.
Provides deterministic, zero-dependency simulation of Cloudflare D1 Auth,
OpenAI & Anthropic streaming endpoints, Tor circuit failover, and model rescue.
"""

import http.server
import json
import socketserver
import threading
import time
import uuid
from typing import Optional, Tuple


class MockGatewayHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # Known keys in mock database
    VALID_KEYS = {"live-key-valid", "test-key", "default-dev-key"}
    INACTIVE_KEYS = {"inactive-key"}
    ZERO_BALANCE_KEYS = {"empty-balance-key"}

    # Track attempts for rotation / retry simulation
    request_attempts = {}

    def log_message(self, format, *args):
        # Suppress standard logging during automated tests
        pass

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version, x-opencode-session")

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/" or self.path == "/health":
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            resp = json.dumps({"status": "running", "engine": "mock-gateway-fixture"}).encode("utf-8")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
            return

        if self.path == "/v1/models":
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            models_data = {
                "object": "list",
                "data": [
                    {"id": "gpt-4o", "object": "model", "created": 1715558400, "owned_by": "openai"},
                    {"id": "gpt-4o-mini", "object": "model", "created": 1721260800, "owned_by": "openai"},
                    {"id": "claude-3-5-sonnet-20241022", "object": "model", "created": 1729555200, "owned_by": "anthropic"},
                    {"id": "muse-spark-1.3-contributor-free", "object": "model", "created": 1730000000, "owned_by": "kiitcode"}
                ]
            }
            resp = json.dumps(models_data).encode("utf-8")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
            return

        self.send_response(404)
        self.end_headers()

    def _authenticate(self) -> Tuple[bool, int, str]:
        """
        Validates API key according to D1 & Mock Auth specifications.
        """
        auth_header = self.headers.get("Authorization", "")
        api_key = ""
        if auth_header.startswith("Bearer "):
            api_key = auth_header[7:].strip()
        elif "x-api-key" in self.headers:
            api_key = self.headers["x-api-key"].strip()

        if not api_key:
            return False, 401, "Missing API Key"
        if api_key in self.INACTIVE_KEYS:
            return False, 401, "Invalid or disabled API Key"
        if api_key in self.ZERO_BALANCE_KEYS:
            return False, 402, "Credit balance exhausted ($0.00 remaining)."
        if api_key in self.VALID_KEYS:
            return True, 200, "OK"

        return False, 401, "Invalid API Key"

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_len) if content_len > 0 else b""

        # 1. Check Authentication (unless internal header bypass is simulated)
        if not self.headers.get("X-Internal-Secret"):
            authed, status, err_msg = self._authenticate()
            if not authed:
                self.send_response(status)
                self._send_cors_headers()
                self.send_header("Content-Type", "application/json")
                body = json.dumps({"error": err_msg}).encode("utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

        # 2. Parse JSON body
        try:
            req_data = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except Exception:
            self.send_response(400)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"error": "Malformed JSON body"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # 3. Route according to endpoint
        if self.path in ("/v1/chat/completions", "/v1/v1/chat/completions"):
            self._handle_chat_completions(req_data)
        elif self.path in ("/v1/messages", "/v1/v1/messages"):
            self._handle_anthropic_messages(req_data)
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_chat_completions(self, req_data):
        model = req_data.get("model", "gpt-4o")
        is_stream = req_data.get("stream", False)
        messages = req_data.get("messages", [])

        # Validate messages format
        if not isinstance(messages, list) or len(messages) == 0:
            self.send_response(400)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"error": "messages must be a non-empty array"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Model Rescue & Circuit rotation simulation
        if model == "simulated-429-throttled":
            # Simulate 429 rate limit
            self.send_response(429)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"error": "Rate limit exceeded, retry later"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if model == "simulated-503-overloaded":
            # Simulate 503 upstream down
            self.send_response(503)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"error": "Upstream service overloaded"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if model == "nemotron-3.5-lightning-free" and not self.headers.get("x-opencode-session"):
            # Missing session or tool schemas triggers 403 FreeTierError
            self.send_response(403)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"error": "FreeTierError: free tier can only be used from within OpenCode"}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        user_content = ""
        for m in messages:
            if isinstance(m, dict) and m.get("role") == "user":
                c = m.get("content", "")
                if isinstance(c, str):
                    user_content = c
                elif isinstance(c, list):
                    user_content = " ".join([str(item.get("text", "")) for item in c if isinstance(item, dict)])
                elif c is not None:
                    user_content = str(c)
                else:
                    user_content = ""

        req_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        created_ts = int(time.time())

        # If model is mapped or rescued, reflect target model
        effective_model = model
        if model == "simulated-rescue-model" or model == "nemotron-3.5-lightning-free":
            effective_model = "muse-spark-1.3-contributor-free"

        if is_stream:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()

            # Immediate first chunk (TTFT < 50ms)
            first_chunk = {
                "id": req_id,
                "object": "chat.completion.chunk",
                "created": created_ts,
                "model": effective_model,
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": "Hello"}, "finish_reason": None}]
            }
            self.wfile.write(f"data: {json.dumps(first_chunk)}\n\n".encode("utf-8"))
            self.wfile.flush()

            time.sleep(0.04)

            # Second chunk
            content_reply = "! How can I assist you with code today?"
            if "count" in user_content.lower():
                content_reply = ": 1, 2, 3, 4, 5."
            elif len(user_content) > 500:
                content_reply = f". Processed payload of {len(user_content)} characters successfully."

            second_chunk = {
                "id": req_id,
                "object": "chat.completion.chunk",
                "created": created_ts,
                "model": effective_model,
                "choices": [{"index": 0, "delta": {"content": content_reply}, "finish_reason": None}]
            }
            self.wfile.write(f"data: {json.dumps(second_chunk)}\n\n".encode("utf-8"))
            self.wfile.flush()

            time.sleep(0.02)

            # Terminal finish chunk
            last_chunk = {
                "id": req_id,
                "object": "chat.completion.chunk",
                "created": created_ts,
                "model": effective_model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
            }
            self.wfile.write(f"data: {json.dumps(last_chunk)}\n\n".encode("utf-8"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
        else:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            full_resp = {
                "id": req_id,
                "object": "chat.completion",
                "created": created_ts,
                "model": effective_model,
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "Hello! I am ready to help you with your programming tasks."
                        },
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": max(5, len(user_content) // 4),
                    "completion_tokens": 15,
                    "total_tokens": max(5, len(user_content) // 4) + 15
                }
            }
            body = json.dumps(full_resp).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def _handle_anthropic_messages(self, req_data):
        model = req_data.get("model", "claude-3-5-sonnet-20241022")
        is_stream = req_data.get("stream", False)
        messages = req_data.get("messages", [])

        if not isinstance(messages, list) or len(messages) == 0:
            self.send_response(400)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            body = json.dumps({"type": "error", "error": {"type": "invalid_request_error", "message": "messages must be non-empty"}}).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        msg_id = f"msg_{uuid.uuid4().hex[:20]}"

        if is_stream:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("anthropic-version", "2023-06-01")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()

            # 1. message_start
            e1 = {
                "type": "message_start",
                "message": {
                    "id": msg_id,
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 15, "output_tokens": 1}
                }
            }
            self.wfile.write(f"event: message_start\ndata: {json.dumps(e1)}\n\n".encode("utf-8"))
            self.wfile.flush()

            time.sleep(0.03)

            # 2. content_block_start
            e2 = {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}
            self.wfile.write(f"event: content_block_start\ndata: {json.dumps(e2)}\n\n".encode("utf-8"))
            self.wfile.flush()

            # 3. content_block_delta
            e3 = {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Anthropic protocol streaming verified."}}
            self.wfile.write(f"event: content_block_delta\ndata: {json.dumps(e3)}\n\n".encode("utf-8"))
            self.wfile.flush()

            time.sleep(0.02)

            # 4. content_block_stop
            e4 = {"type": "content_block_stop", "index": 0}
            self.wfile.write(f"event: content_block_stop\ndata: {json.dumps(e4)}\n\n".encode("utf-8"))

            # 5. message_delta
            e5 = {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 10}}
            self.wfile.write(f"event: message_delta\ndata: {json.dumps(e5)}\n\n".encode("utf-8"))

            # 6. message_stop
            e6 = {"type": "message_stop"}
            self.wfile.write(f"event: message_stop\ndata: {json.dumps(e6)}\n\n".encode("utf-8"))
            self.wfile.flush()
            self.close_connection = True
        else:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            self.send_header("anthropic-version", "2023-06-01")
            full_resp = {
                "id": msg_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [{"type": "text", "text": "Non-streaming Anthropic response verified."}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 20, "output_tokens": 8}
            }
            body = json.dumps(full_resp).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


_server_instance: Optional[ThreadedHTTPServer] = None
_server_thread: Optional[threading.Thread] = None


def start_mock_server(port: int = 8799) -> Tuple[str, int]:
    global _server_instance, _server_thread
    if _server_instance is not None:
        return f"http://127.0.0.1:{port}", port

    for try_port in [port, port + 1, port + 2, 0]:
        try:
            _server_instance = ThreadedHTTPServer(("127.0.0.1", try_port), MockGatewayHandler)
            actual_port = _server_instance.server_address[1]
            break
        except Exception:
            continue

    _server_thread = threading.Thread(target=_server_instance.serve_forever, daemon=True)
    _server_thread.start()
    return f"http://127.0.0.1:{actual_port}", actual_port


def stop_mock_server():
    global _server_instance, _server_thread
    if _server_instance is not None:
        _server_instance.shutdown()
        _server_instance.server_close()
        _server_instance = None
        _server_thread = None
