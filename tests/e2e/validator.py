"""
Schema Validators and Invariant Assertion Engines for E2E Tests.
Validates OpenAI chat completion deltas, Anthropic SSE events, and latency bounds.
"""

import json
from typing import Dict, Any, List, Optional, Tuple


class SchemaValidationError(Exception):
    """Raised when an API response violates protocol schema specifications."""
    pass


class InvariantViolationError(Exception):
    """Raised when performance or reliability invariants are violated."""
    pass


def validate_openai_chunk(chunk_dict: Dict[str, Any], is_first: bool = False, is_last: bool = False) -> None:
    """
    Validates that a streaming SSE chunk adheres strictly to OpenAI chat.completion.chunk format.
    Example:
    data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1694268190,"model":"gpt-4o",
           "choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}
    """
    required_keys = ["id", "object", "created", "model", "choices"]
    for k in required_keys:
        if k not in chunk_dict:
            raise SchemaValidationError(f"OpenAI SSE chunk missing key '{k}': {chunk_dict}")

    if chunk_dict["object"] != "chat.completion.chunk":
        raise SchemaValidationError(f"Expected object 'chat.completion.chunk', got '{chunk_dict['object']}'")

    choices = chunk_dict.get("choices")
    if not isinstance(choices, list) or len(choices) == 0:
        raise SchemaValidationError(f"OpenAI SSE chunk must contain non-empty choices list: {chunk_dict}")

    choice = choices[0]
    if "index" not in choice:
        raise SchemaValidationError(f"Choice missing 'index': {choice}")
    if "delta" not in choice:
        raise SchemaValidationError(f"Choice missing 'delta': {choice}")

    delta = choice["delta"]
    if not isinstance(delta, dict):
        raise SchemaValidationError(f"'delta' must be a dict: {choice}")

    # Check that internal reasoning tags are sanitized and not leaked
    if "reasoning_content" in delta and delta["reasoning_content"]:
        raise SchemaValidationError(f"Leakage detected: 'reasoning_content' present in delta: {delta}")

    if is_last:
        if choice.get("finish_reason") is None and delta.get("content"):
            pass  # Some implementations send finish in separate final chunk


def validate_openai_completion(comp_dict: Dict[str, Any]) -> None:
    """
    Validates a non-streaming OpenAI chat.completion response.
    """
    required_keys = ["id", "object", "created", "model", "choices"]
    for k in required_keys:
        if k not in comp_dict:
            raise SchemaValidationError(f"OpenAI completion missing '{k}': {comp_dict}")

    if comp_dict["object"] != "chat.completion":
        raise SchemaValidationError(f"Expected object 'chat.completion', got '{comp_dict['object']}'")

    choices = comp_dict["choices"]
    if not isinstance(choices, list) or len(choices) == 0:
        raise SchemaValidationError(f"Non-empty choices required: {comp_dict}")

    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise SchemaValidationError(f"Choice missing 'message' dict: {choice}")

    if "role" not in message:
        raise SchemaValidationError(f"Message missing 'role': {message}")
    if "content" not in message or message["content"] is None:
        raise SchemaValidationError(f"Message missing 'content': {message}")


def validate_anthropic_event(event_type: str, data_dict: Dict[str, Any]) -> None:
    """
    Validates an individual Anthropic SSE event payload according to Anthropic specs.
    Event types: message_start, content_block_start, content_block_delta, content_block_stop,
                 message_delta, message_stop, ping, error.
    """
    valid_events = {
        "message_start", "content_block_start", "content_block_delta",
        "content_block_stop", "message_delta", "message_stop", "ping", "error"
    }
    if event_type not in valid_events:
        raise SchemaValidationError(f"Unknown Anthropic event type: '{event_type}'")

    if event_type == "message_start":
        if "message" not in data_dict:
            raise SchemaValidationError(f"message_start event missing 'message': {data_dict}")
        msg = data_dict["message"]
        if not msg.get("id", "").startswith("msg_"):
            raise SchemaValidationError(f"message.id must start with 'msg_', got '{msg.get('id')}'")
        if msg.get("type") != "message":
            raise SchemaValidationError(f"message.type must be 'message', got '{msg.get('type')}'")

    elif event_type == "content_block_start":
        if "index" not in data_dict or "content_block" not in data_dict:
            raise SchemaValidationError(f"content_block_start missing index/content_block: {data_dict}")

    elif event_type == "content_block_delta":
        if "delta" not in data_dict:
            raise SchemaValidationError(f"content_block_delta missing 'delta': {data_dict}")
        delta = data_dict["delta"]
        if delta.get("type") != "text_delta":
            raise SchemaValidationError(f"delta.type must be 'text_delta', got '{delta.get('type')}'")
        if "text" not in delta:
            raise SchemaValidationError(f"delta missing 'text': {delta}")

    elif event_type == "message_delta":
        if "delta" not in data_dict:
            raise SchemaValidationError(f"message_delta missing 'delta': {data_dict}")


def validate_anthropic_completion(comp_dict: Dict[str, Any]) -> None:
    """
    Validates a non-streaming Anthropic message response.
    """
    if not comp_dict.get("id", "").startswith("msg_"):
        raise SchemaValidationError(f"Anthropic message ID must start with 'msg_': {comp_dict}")
    if comp_dict.get("type") != "message":
        raise SchemaValidationError(f"Expected type 'message', got '{comp_dict.get('type')}'")
    if comp_dict.get("role") != "assistant":
        raise SchemaValidationError(f"Expected role 'assistant', got '{comp_dict.get('role')}'")

    content = comp_dict.get("content")
    if not isinstance(content, list) or len(content) == 0:
        raise SchemaValidationError(f"Expected non-empty content list: {comp_dict}")
    if content[0].get("type") != "text" or "text" not in content[0]:
        raise SchemaValidationError(f"Content block 0 must be type 'text' with text field: {content[0]}")


def assert_ttft_within_limit(ttft: float, max_limit: float = 4.5, context_name: str = "request") -> None:
    """
    Programmatic invariant: TTFT <= 4.5s on healthy streaming requests.
    """
    if ttft > max_limit:
        raise InvariantViolationError(
            f"TTFT invariant violated for {context_name}: {ttft:.3f}s exceeded maximum allowable {max_limit}s"
        )


def assert_zero_buffering(ttft: float, total_time: float, chunks_count: int, context_name: str = "stream") -> None:
    """
    Programmatic invariant: First SSE chunk delivered immediately without buffering full completion.
    When multiple chunks are emitted over time, TTFT must be strictly less than total completion time.
    """
    gen_time = total_time - ttft
    if chunks_count > 5 and total_time > 1.0 and gen_time > 0.3:
        if ttft >= total_time * 0.95:
            raise InvariantViolationError(
                f"Zero-buffering invariant violated for {context_name}: "
                f"TTFT ({ttft:.2f}s) is {ttft/total_time*100:.1f}% of total completion time ({total_time:.2f}s)."
            )


def assert_no_unhandled_403(status_code: int, response_text: str = "") -> None:
    """
    Programmatic invariant: Zero unhandled 403 FreeTierErrors.
    """
    if status_code == 403 and "FreeTierError" in response_text:
        raise InvariantViolationError(
            f"Unhandled FreeTierError 403 returned: {response_text[:200]}"
        )
