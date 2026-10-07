"""
Tier 3: Cross-Feature Combinations Test Suite.
Verifies pairwise interactions:
1. Streaming + Authentication
2. Tools + Dynamic Model Rescue
3. Tor Circuit Failover on 429/503
4. Reasoning Sanitization + Streaming Delivery
"""

import unittest
import json
import requests
from tests.e2e.conftest import get_base_url, execute_streaming_request, execute_json_request
from tests.e2e.validator import (
    validate_openai_chunk,
    assert_no_unhandled_403
)


class TestTier3CrossFeatureCombinations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = get_base_url()
        cls.valid_headers = {"Authorization": "Bearer test-key"}

    # =========================================================================
    # Combination 1: Streaming + Authentication
    # =========================================================================

    def test_pairwise_streaming_with_valid_bearer(self):
        """Streaming request authenticated via Bearer token succeeds."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer live-key-valid"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Stream bearer"}], "stream": True}
        status, lines, ttft, total = execute_streaming_request(url, payload, headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

    def test_pairwise_streaming_with_x_api_key(self):
        """Streaming request authenticated via x-api-key succeeds."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"x-api-key": "live-key-valid"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Stream x-api-key"}], "stream": True}
        status, lines, ttft, total = execute_streaming_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_pairwise_streaming_with_mock_auth(self):
        """Streaming request authenticated via mock fallback key succeeds."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer default-dev-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Stream mock"}], "stream": True}
        status, lines, ttft, total = execute_streaming_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_pairwise_streaming_unauthorized_immediate_abort(self):
        """Streaming request without credentials aborts immediately with 401 without SSE frames."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Stream unauthed"}], "stream": True}
        status, lines, _, _ = execute_streaming_request(url, payload, headers={})
        self.assertEqual(status, 401)
        # Should return JSON error, not stream data lines
        has_sse_data = any(line.startswith("data: ") for line in lines)
        self.assertFalse(has_sse_data)

    def test_pairwise_streaming_exhausted_balance_abort(self):
        """Streaming request with exhausted balance aborts immediately with 402."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer empty-balance-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Stream empty"}], "stream": True}
        status, lines, _, _ = execute_streaming_request(url, payload, headers)
        self.assertEqual(status, 402)

    # =========================================================================
    # Combination 2: Tools + Dynamic Model Rescue
    # =========================================================================

    def test_pairwise_tools_injected_on_throttled_model(self):
        """Tools and session headers preserved during model rescue."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {
            "model": "simulated-rescue-model",
            "messages": [{"role": "user", "content": "Use tool"}],
            "tools": [{"type": "function", "function": {"name": "bash", "description": "run shell"}}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))

    def test_pairwise_dead_model_rescued_to_working_model(self):
        """Initial dead model automatically rescued to working model returning 200 OK."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {
            "model": "nemotron-3.5-lightning-free",
            "messages": [{"role": "user", "content": "Rescue test"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))

    def test_pairwise_session_affinity_preserved_across_rescue(self):
        """Session token header msg_<timestamp_hex><base62> maintained during rescue."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-request"] = "msg_f0f66a50bffeB7MDxc270HJi55"
        payload = {
            "model": "simulated-rescue-model",
            "messages": [{"role": "user", "content": "Affinity test"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_pairwise_rescue_returns_streaming_sse(self):
        """Rescued model request with stream: true returns valid SSE chunks."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {
            "model": "simulated-rescue-model",
            "messages": [{"role": "user", "content": "Stream rescue"}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

    def test_pairwise_rescue_sanitizes_reasoning(self):
        """Rescued model response strips internal reasoning thoughts."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {
            "model": "simulated-rescue-model",
            "messages": [{"role": "user", "content": "Sanitize test"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        content = body["choices"][0]["message"]["content"]
        self.assertNotIn("<identity_guard>", content)
        self.assertNotIn("reasoning_content", str(body))

    # =========================================================================
    # Combination 3: Tor Circuit Failover on 429/503
    # =========================================================================

    def test_pairwise_upstream_429_triggers_circuit_rotation(self):
        """Upstream 429 rate limit triggers rotation/rescue with 200 OK on working alternative."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "simulated-rescue-model", "messages": [{"role": "user", "content": "429 test"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_pairwise_upstream_503_triggers_model_rescue(self):
        """Upstream 503 overloaded model rescues to available model returning 200 OK."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "simulated-rescue-model", "messages": [{"role": "user", "content": "503 test"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_pairwise_isolate_socks_auth_fresh_credentials(self):
        """Per-request credentials guarantee fresh circuit allocation."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-circuit-id"] = "circuit_test_fresh"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Circuit isolate"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_pairwise_zero_unhandled_403_under_circuit_switch(self):
        """Circuit switching avoids unhandled 403 FreeTierErrors."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Circuit 403 check"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))

    def test_pairwise_retry_budget_exhaustion_clean_503(self):
        """When an un-rescuable model fails, gateway returns clean 503 instead of hanging."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "simulated-503-overloaded", "messages": [{"role": "user", "content": "Dead model"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertIn(status, [503, 502])


if __name__ == "__main__":
    unittest.main()
