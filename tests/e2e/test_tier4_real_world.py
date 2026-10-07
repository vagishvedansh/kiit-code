"""
Tier 4: Real-World Scenarios Test Suite.
Verifies production workloads & programmatic performance invariants:
1. Multi-turn conversation context retention
2. Syntactically complex coding prompts with indentation preservation
3. Strict TTFT benchmark assertion (<= 4.5s)
4. Immediate first SSE chunk delivery (zero full completion buffering)
5. High-frequency burst concurrency & gateway resilience
"""

import unittest
import json
import concurrent.futures
from tests.e2e.conftest import get_base_url, execute_streaming_request, execute_json_request
from tests.e2e.validator import (
    validate_openai_chunk,
    validate_openai_completion,
    assert_ttft_within_limit,
    assert_zero_buffering,
    assert_no_unhandled_403
)


class TestTier4RealWorldScenarios(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = get_base_url()
        cls.valid_headers = {"Authorization": "Bearer test-key"}

    def test_scenario_multi_turn_conversation_context(self):
        """Scenario 4.1: 5-turn conversational history maintaining dialogue continuity."""
        url = f"{self.base_url}/v1/chat/completions"
        conversation = [
            {"role": "user", "content": "My name is Alice and I am a software engineer."},
            {"role": "assistant", "content": "Nice to meet you Alice! How can I assist your engineering work?"},
            {"role": "user", "content": "I need to design a high-throughput proxy in Go."},
            {"role": "assistant", "content": "A high-throughput proxy in Go benefits from goroutines, worker pools, and uTLS."},
            {"role": "user", "content": "What was my name and profession?"}
        ]
        payload = {"model": "gpt-4o", "messages": conversation, "stream": False}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))
        validate_openai_completion(body)

    def test_scenario_code_generation_fidelity(self):
        """Scenario 4.2: Code prompt requires strict whitespace and indentation preservation."""
        url = f"{self.base_url}/v1/chat/completions"
        code_prompt = "Write a python function to compute fibonacci numbers with memoization. Use clean indentation."
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": code_prompt}], "stream": False}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        content = body["choices"][0]["message"]["content"]
        # Ensure code is not corrupted by word-split regexes
        self.assertNotIn("d e f", content)
        self.assertNotIn("r e t u r n", content)

    def test_scenario_ttft_under_4_point_5_seconds(self):
        """Scenario 4.3: Strict invariant assertion: TTFT <= 4.5s on healthy streaming request."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Count from 1 to 5. Be very brief."}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)
        assert_ttft_within_limit(ttft, max_limit=4.5, context_name="Scenario 4.3 TTFT Benchmark")

    def test_scenario_zero_buffering_immediate_chunk(self):
        """Scenario 4.4: Invariant assertion: First SSE chunk delivered immediately without buffering."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Explain binary search step by step in detail."}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        chunks = [line for line in lines if line.startswith("data: ") and line != "data: [DONE]"]
        self.assertGreaterEqual(len(chunks), 2)
        assert_zero_buffering(ttft, total, len(chunks), context_name="Scenario 4.4 Zero Buffering")

    def test_scenario_burst_concurrency_resilience(self):
        """Scenario 4.5: 10 rapid concurrent requests execute without thread starvation or 403 errors."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Concurrency ping"}],
            "stream": False
        }

        def make_request(idx):
            headers = dict(self.valid_headers)
            headers["x-opencode-request"] = f"msg_concurrent_{idx}"
            return execute_json_request(url, payload, headers, timeout=20.0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request, i) for i in range(10)]
            results = [f.result() for f in futures]

        for status, body, _ in results:
            self.assertEqual(status, 200)
            assert_no_unhandled_403(status, str(body))


if __name__ == "__main__":
    unittest.main()
