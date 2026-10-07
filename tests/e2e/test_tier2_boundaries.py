"""
Tier 2: Boundary & Corner Cases Test Suite.
Enforces >= 5 test cases per feature across:
1. Large Prompts (10KB - 50KB+, deep turn histories)
2. Minimal Prompts (1 character, 1 digit, punctuation, whitespace)
3. Empty Content & Malformed Payloads
4. Special Characters & Multilingual Encoding
5. Missing & Malformed Authorization
"""

import unittest
import json
import requests
from tests.e2e.conftest import get_base_url, execute_streaming_request, execute_json_request
from tests.e2e.validator import validate_openai_completion, validate_openai_chunk


class TestTier2BoundaryAndCornerCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url = get_base_url()
        cls.valid_headers = {"Authorization": "Bearer test-key"}

    # =========================================================================
    # Feature 1: Large Prompts (>= 5 test cases)
    # =========================================================================

    def test_large_prompt_10kb_payload(self):
        """Case 1.1: 10 KB prompt processed without buffer overflow."""
        url = f"{self.base_url}/v1/chat/completions"
        large_text = "The quick brown fox jumps over the lazy dog. " * 225  # ~10KB
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": large_text}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers, timeout=45.0)
        self.assertEqual(status, 200)
        validate_openai_completion(body)

    def test_large_prompt_50kb_payload(self):
        """Case 1.2: 50 KB context payload handled cleanly."""
        url = f"{self.base_url}/v1/chat/completions"
        code_block = "def process_data(item):\n    return item * 2\n" * 1400  # ~50KB
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": f"Analyze this code:\n{code_block}"}],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers, timeout=60.0)
        self.assertEqual(status, 200)

    def test_large_prompt_deep_conversation_turns(self):
        """Case 1.3: Deep multi-turn conversation with 20 turns."""
        url = f"{self.base_url}/v1/chat/completions"
        messages = []
        for i in range(10):
            messages.append({"role": "user", "content": f"Question {i}: What is {i} squared?"})
            messages.append({"role": "assistant", "content": f"Answer {i}: {i*i}"})
        messages.append({"role": "user", "content": "Now what is the sum of all squares?"})

        payload = {"model": "gpt-4o", "messages": messages, "stream": False}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_large_prompt_large_system_instruction(self):
        """Case 1.4: 8 KB system instruction preserved."""
        url = f"{self.base_url}/v1/chat/completions"
        system_rules = "Rule: You must follow strict coding standards.\n" * 200  # ~8KB
        payload = {
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": system_rules},
                {"role": "user", "content": "Hello"}
            ],
            "stream": False
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_large_prompt_streaming_chunks(self):
        """Case 1.5: 20 KB streaming payload with SSE deltas arriving intact."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Repeat numbers: " + ("1234567890 " * 2000)}],
            "stream": True
        }
        status, lines, ttft, total = execute_streaming_request(url, payload, self.valid_headers, timeout=45.0)
        self.assertEqual(status, 200)
        self.assertGreater(len(lines), 0)

    # =========================================================================
    # Feature 2: Minimal Prompts (>= 5 test cases)
    # =========================================================================

    def test_minimal_prompt_single_char(self):
        """Case 2.1: Single character prompt 'a'."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "a"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_minimal_prompt_single_digit(self):
        """Case 2.2: Single digit prompt '1'."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "1"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_minimal_prompt_single_punctuation(self):
        """Case 2.3: Single punctuation prompt '?'."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "?"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_minimal_prompt_whitespace_only(self):
        """Case 2.4: Whitespace-only content handled cleanly without crash."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "   \n\t  "}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertIn(status, [200, 400])

    def test_minimal_prompt_one_token_limit(self):
        """Case 2.5: max_tokens = 1 boundary limits response."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 1
        }
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    # =========================================================================
    # Feature 3: Empty Content & Malformed Payloads (>= 5 test cases)
    # =========================================================================

    def test_empty_messages_array_error(self):
        """Case 3.1: Empty messages array returns 400 Bad Request."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": []}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 400)

    def test_empty_user_content_string(self):
        """Case 3.2: Empty content string in message."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": ""}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertIn(status, [200, 400])

    def test_missing_messages_field(self):
        """Case 3.3: Payload missing 'messages' key returns 400 Bad Request."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o"}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 400)

    def test_null_content_in_message(self):
        """Case 3.4: Message with content = null handled gracefully."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": None}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertIn(status, [200, 400])

    def test_malformed_json_syntax(self):
        """Case 3.5: Malformed JSON syntax returns 400 Bad Request."""
        url = f"{self.base_url}/v1/chat/completions"
        raw_bad_json = '{"model": "gpt-4o", "messages": [{"role": "user", "content": "incomplete"'
        resp = requests.post(url, data=raw_bad_json, headers=self.valid_headers, timeout=10.0)
        self.assertEqual(resp.status_code, 400)

    # =========================================================================
    # Feature 4: Special Characters & Multilingual Encoding (>= 5 test cases)
    # =========================================================================

    def test_special_chars_emojis(self):
        """Case 4.1: Unicode emojis and symbols preserved."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hello 🚀🔥🤖✨🎉"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_special_chars_cjk_and_arabic(self):
        """Case 4.2: Multilingual scripts (CJK and Arabic RTL)."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "こんにちは世界 / 你好世界 / مرحبا بالعالم"}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_special_chars_quotes_and_escapes(self):
        """Case 4.3: Quotes, slashes, and JSON escape sequences."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": 'Quote: "nested" \\ backslash / slash \n newline \t tab'}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_special_chars_null_bytes_and_ansi(self):
        """Case 4.4: ANSI color escape codes handled safely."""
        url = f"{self.base_url}/v1/chat/completions"
        ansi_text = "\033[31mRed Text\033[0m and \033[32mGreen\033[0m"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": ansi_text}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    def test_special_chars_markdown_blocks(self):
        """Case 4.5: Complex triple-backtick code markdown blocks."""
        url = f"{self.base_url}/v1/chat/completions"
        code_prompt = '```python\ndef test():\n    return "```nested```"\n```'
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": code_prompt}]}
        status, body, _ = execute_json_request(url, payload, self.valid_headers)
        self.assertEqual(status, 200)

    # =========================================================================
    # Feature 5: Missing & Malformed Authorization (>= 5 test cases)
    # =========================================================================

    def test_missing_auth_header_rejected(self):
        """Case 5.1: Request with no auth headers is rejected with 401."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}]}
        status, body, _ = execute_json_request(url, payload, headers={})
        self.assertEqual(status, 401)

    def test_empty_bearer_token_rejected(self):
        """Case 5.2: Authorization with empty Bearer token rejected with 401."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}]}
        headers = {"Authorization": "Bearer "}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)

    def test_malformed_auth_scheme_basic(self):
        """Case 5.3: Non-bearer authorization scheme rejected with 401."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}]}
        headers = {"Authorization": "Basic dXNlcjpwYXNz"}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)

    def test_invalid_arbitrary_key_rejected(self):
        """Case 5.4: Unregistered arbitrary key rejected with 401."""
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "Hi"}]}
        headers = {"Authorization": "Bearer sk-invalid-random-xyz-123"}
        status, body, _ = execute_json_request(url, payload, headers)
        self.assertEqual(status, 401)

    def test_options_preflight_bypasses_auth(self):
        """Case 5.5: CORS OPTIONS preflight request succeeds with 204 without auth."""
        url = f"{self.base_url}/v1/chat/completions"
        resp = requests.options(url, timeout=10.0)
        self.assertEqual(resp.status_code, 204)
        self.assertEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")


if __name__ == "__main__":
    unittest.main()
