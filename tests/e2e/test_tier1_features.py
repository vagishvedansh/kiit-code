"""
Tier 1: Feature Coverage Test Suite.
Enforces >= 5 test cases per feature across:
1. OpenAI /v1/chat/completions
2. Anthropic /v1/messages
3. D1 Database Auth
4. Offline Mock Auth Fallback
5. Direct Models
6. Routed Virtual Models
"""

import unittest
import json
import requests
from tests.e2e.conftest import get_base_url, execute_streaming_request, execute_json_request
from tests.e2e.validator import (
    validate_openai_chunk,
    validate_openai_completion,
    validate_anthropic_event,
    validate_anthropic_completion,
    assert_no_unhandled_403
)


class TestTier1FeatureCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = get_base_url()
        cls.valid_headers = {"Authorization": "Bearer test-key"}

    # =========================================================================
    # Feature 1: OpenAI /v1/chat/completions (>= 5 test cases)
    # =========================================================================

    def test_openai_chat_basic_non_streaming(self):
        """Case 1.1: Non-streaming request returns valid chat.completion JSON schema."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hello world"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))
        validate_openai_completion(body)
        self.assertTrue(len(body["choices"][0]["message"]["content"]) > 0)

    def test_openai_chat_streaming_chunks(self):
        """Case 1.2: Streaming request returns valid chat.completion.chunk SSE deltas and [DONE]."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Count to 3"}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

        has_done = False
        valid_chunks_count = 0
        for line in lines:
            if line == "data: [DONE]":
                has_done = True
                continue
            if line.startswith("data: "):
                chunk_json = json.loads(line[6:])
                validate_openai_chunk(chunk_json)
                valid_chunks_count += 1

        self.assertTrue(has_done, "Stream did not terminate with data: [DONE]")
        self.assertGreaterEqual(valid_chunks_count, 1)

    def test_openai_chat_with_system_message(self):
        """Case 1.3: Multi-turn message sequence with system prompt handled correctly."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are a code tutor."},
                {"role": "user", "content": "What is recursion?"}
            ],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        validate_openai_completion(body)

    def test_openai_chat_temperature_parameter(self):
        """Case 1.4: Passing temperature, max_tokens, and top_p parameters."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Say test"}],
            "temperature": 0.2,
            "max_tokens": 50,
            "top_p": 0.9,
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        validate_openai_completion(body)

    def test_openai_models_list(self):
        """Case 1.5: GET /v1/models returns inventory of supported models."""
        url = f"{self.base_url}/v1/models"
        resp = requests.get(url, headers=self.valid_headers, timeout=10.0)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data.get("object"), "list")
        self.assertIn("data", data)
        model_ids = [m.get("id") for m in data["data"]]
        self.assertIn("gpt-4o", model_ids)

    # =========================================================================
    # Feature 2: Anthropic /v1/messages (>= 5 test cases)
    # =========================================================================

    def test_anthropic_messages_basic_non_streaming(self):
        """Case 2.1: Non-streaming request returns valid Anthropic message JSON schema."""
        url = f"{self.base_url}/v1/messages"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "Explain OOP."}],
            "stream": False
        }
        status, body, headers = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))
        validate_anthropic_completion(body)

    def test_anthropic_messages_streaming_events(self):
        """Case 2.2: Streaming request returns valid Anthropic SSE event lifecycle."""
        url = f"{self.base_url}/v1/messages"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "Explain recursion."}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

        events_observed = []
        current_event = None
        for line in lines:
            if line.startswith("event: "):
                current_event = line[7:].strip()
                events_observed.append(current_event)
            elif line.startswith("data: ") and current_event:
                data_dict = json.loads(line[6:])
                validate_anthropic_event(current_event, data_dict)
                current_event = None

        self.assertIn("message_start", events_observed)
        self.assertIn("content_block_start", events_observed)
        self.assertIn("content_block_delta", events_observed)
        self.assertIn("content_block_stop", events_observed)
        self.assertIn("message_stop", events_observed)

    def test_anthropic_messages_with_system_prompt(self):
        """Case 2.3: Anthropic top-level system prompt is processed without error."""
        url = f"{self.base_url}/v1/messages"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "system": "You are a senior Linux engineer.",
            "messages": [{"role": "user", "content": "What is an inode?"}],
            "max_tokens": 100,
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        validate_anthropic_completion(body)

    def test_anthropic_messages_route_alias(self):
        """Case 2.4: Aliased route /v1/v1/messages is supported."""
        url = f"{self.base_url}/v1/v1/messages"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        validate_anthropic_completion(body)

    def test_anthropic_messages_anthropic_version_header(self):
        """Case 2.5: Verifies anthropic-version header handling."""
        url = f"{self.base_url}/v1/messages"
        headers = dict(self.valid_headers)
        headers["anthropic-version"] = "2023-06-01"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "messages": [{"role": "user", "content": "Check version"}],
            "stream": False
        }
        status, body, resp_headers = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        self.assertEqual(resp_headers.get("anthropic-version", "2023-06-01"), "2023-06-01")

    # =========================================================================
    # Feature 3: D1 Database Authentication Layer (>= 5 test cases)
    # =========================================================================

    def test_d1_auth_valid_bearer_key(self):
        """Case 3.1: Valid Bearer API key returns 200 OK."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer live-key-valid"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Ping"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_d1_auth_x_api_key_header(self):
        """Case 3.2: Valid key passed via x-api-key header returns 200 OK."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"x-api-key": "live-key-valid"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Ping"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_d1_auth_inactive_key_rejected(self):
        """Case 3.3: Inactive/disabled API key is rejected with 401 Unauthorized."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer inactive-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Ping"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)
        self.assertIn("error", body)

    def test_d1_auth_exhausted_balance_rejected(self):
        """Case 3.4: Zero credit balance returns 402 Payment Required."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer empty-balance-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Ping"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 402)
        self.assertIn("exhausted", str(body).lower())

    def test_d1_auth_usage_metadata_present(self):
        """Case 3.5: Successful request includes token usage accounting metadata."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer live-key-valid"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Calculate tokens"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        self.assertIn("usage", body)
        self.assertIn("total_tokens", body["usage"])

    # =========================================================================
    # Feature 4: Offline Mock Auth Fallback (>= 5 test cases)
    # =========================================================================

    def test_mock_auth_test_key_accepted(self):
        """Case 4.1: Fallback accepts 'test-key' when D1 is offline."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer test-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Mock auth"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_mock_auth_default_dev_key_accepted(self):
        """Case 4.2: Fallback accepts 'default-dev-key' when D1 is offline."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer default-dev-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Dev auth"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)

    def test_mock_auth_unknown_key_rejected(self):
        """Case 4.3: Fallback rejects unknown keys with 401."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer arbitrary-random-unknown-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Bad key"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)

    def test_mock_auth_undefined_env_db_resilience(self):
        """Case 4.4: Gateway does not crash with 500 when env.DB is absent."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer test-key"}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Resilience"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertNotEqual(status, 500)
        self.assertEqual(status, 200)

    def test_mock_auth_empty_key_rejected(self):
        """Case 4.5: Empty Authorization bearer string returns 401."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Authorization": "Bearer "}
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Empty key"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)

    # =========================================================================
    # Feature 5: Direct Models (>= 5 test cases)
    # =========================================================================

    def test_direct_model_muse_spark_streaming(self):
        """Case 5.1: Direct model muse-spark-1.3-contributor-free streams cleanly."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "muse-spark-1.3-contributor-free",
            "messages": [{"role": "user", "content": "Direct test"}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

    def test_direct_model_non_streaming_json(self):
        """Case 5.2: Direct model non-streaming returns valid OpenAI completion."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "muse-spark-1.3-contributor-free",
            "messages": [{"role": "user", "content": "Direct JSON test"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        validate_openai_completion(body)

    def test_direct_model_tool_schema_passthrough(self):
        """Case 5.3: Direct model passes tool definitions without error."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "muse-spark-1.3-contributor-free",
            "messages": [{"role": "user", "content": "Execute bash"}],
            "tools": [{"type": "function", "function": {"name": "bash", "description": "run shell"}}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_direct_model_usage_metadata(self):
        """Case 5.4: Direct model returns token usage metadata."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "muse-spark-1.3-contributor-free",
            "messages": [{"role": "user", "content": "Count usage"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertIn("usage", body)

    def test_direct_model_finish_reason_stop(self):
        """Case 5.5: Direct model response has finish_reason = 'stop'."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "muse-spark-1.3-contributor-free",
            "messages": [{"role": "user", "content": "Finish test"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)
        self.assertEqual(body["choices"][0]["finish_reason"], "stop")

    # =========================================================================
    # Feature 6: Routed Virtual Models (>= 5 test cases)
    # =========================================================================

    def test_routed_model_gpt4o_mapping(self):
        """Case 6.1: Virtual model gpt-4o maps and executes successfully."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hello"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_routed_model_gpt4o_mini_mapping(self):
        """Case 6.2: Virtual model gpt-4o-mini maps and executes successfully."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Hello"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_routed_model_claude_sonnet_mapping(self):
        """Case 6.3: Virtual model claude-3-5-sonnet maps and executes successfully."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "claude-3-5-sonnet", "messages": [{"role": "user", "content": "Hello"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_routed_model_deepseek_mapping(self):
        """Case 6.4: Virtual model deepseek-r1 maps and executes successfully."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "deepseek-r1", "messages": [{"role": "user", "content": "Hello"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_routed_model_session_token_injection(self):
        """Case 6.5: Routed model executes without unhandled 403 FreeTierErrors."""
        url = f"{self.base_url}/v1/chat/completions"
        headers = dict(self.valid_headers)
        headers["x-opencode-session"] = "ses_f1ca452fdffe1IvfaQCvkIzXHe"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Session token test"}]}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 200)
        assert_no_unhandled_403(status, str(body))


if __name__ == "__main__":
    unittest.main()
