# Testing Infrastructure Specification (TEST_INFRA.md)

## 1. Architectural Philosophy & Dual-Track Testing Principles

The testing infrastructure for the **Unified AI Gateway and Proxy** is built on an **independent, dual-track, opaque-box verification model**. In strict accordance with the requirements of `ORIGINAL_REQUEST.md` and `PROJECT.md`, the test harness operates completely isolated from internal implementation assumptions. It exercises the gateway exclusively through public network protocols (HTTP, Server-Sent Events / SSE Web Streams, SOCKS5 circuit hooks).

### 1.1 Dual-Track Principles
1. **Opaque-Box Requirement Grounding**: Test assertions are derived solely from specifications (`ORIGINAL_REQUEST.md`, RFC 8259 JSON, RFC 9110 HTTP, and standard SSE specifications), rather than implementation internals.
2. **Deterministic & Environmental Resilience**: The test suite operates in dual modes:
   - **Live Target Mode**: Benchmarks against live running endpoints (Cloudflare Pages edge layer at `http://127.0.0.1:8788`, high-throughput Go backend at `http://127.0.0.1:8787` or `http://127.0.0.1:8790`, and upstream proxy at `http://127.0.0.1:8767`).
   - **Autonomous Hermetic Mode**: Leverages an embedded, zero-dependency mock fixture harness that validates proxy contract semantics, circuit rotation, model rescue, and schema invariants deterministically in isolated CI/CD pipelines.
3. **Continuous Metric Clocking**: Measures Time-To-First-Token (TTFT), Total Latency, Throughput (chunks/sec, tokens/sec), and HTTP Status Codes with nanosecond/millisecond monotonic clocks (`time.perf_counter()`).

---

## 2. Test Design Methodology

The test suite applies four rigorous software testing methodologies to guarantee comprehensive coverage across edge and backend proxy components:

### 2.1 Category-Partition Method
The input space is partitioned into discrete equivalence classes:
- **Endpoints**: `/v1/chat/completions`, `/v1/messages`, `/v1/v1/messages`, `/v1/models`, `/health`.
- **Protocol Modes**: Streaming (`stream: true`, Server-Sent Events) vs Non-Streaming (`stream: false`, JSON completion).
- **Authentication Credentials**:
  - Valid D1 database key (`Bearer live-key-valid`)
  - Valid mock fallback key (`Bearer test-key`, `Bearer default-dev-key`)
  - Alternative headers (`x-api-key: ...`, `Authorization: Bearer ...`)
  - Invalid / Unknown key (`Bearer unauthorized-invalid-key`)
  - Missing key (no auth headers)
  - Zero/negative balance key (credit exhaustion)
- **Model Resolution**:
  - Direct upstream models (`muse-spark-1.3-contributor-free`)
  - Virtual alias models (`gpt-4o`, `gpt-4o-mini`, `claude-3-5-sonnet`, `deepseek-r1`)
  - Dead / Blocked models triggering rescue (`nemotron-3.5-lightning-free`)

### 2.2 Boundary Value Analysis (BVA)
Extremes and boundary conditions are rigorously probed:
- **Payload Sizes**:
  - Zero-length / whitespace-only content
  - Minimal 1-character token (`"a"`, `"1"`, `"?"`)
  - Large prompt (10 KB to 50 KB+ text payload)
- **Token Limits**:
  - `max_tokens = 1` boundary
  - `max_tokens = 4096` standard
  - `max_tokens = 0` / negative edge cases
- **Streaming Boundaries**:
  - First chunk arrival vs buffer completion time
  - SSE framing: `data: `, double-newline `\n\n`, and `data: [DONE]` terminal markers
- **Credit Balance Boundaries**:
  - User balance = `$0.00` (triggers HTTP 402 Payment Required)
  - User balance > `$0.00` (allows request)

### 2.3 Pairwise Combinatorial Testing
Interaction matrices evaluate pairwise orthogonal feature combinations:
- `[Streaming True, Streaming False]` $\times$ `[Bearer Auth, x-api-key, Mock Fallback, Missing Auth]`
- `[OpenCode Tool Injection]` $\times$ `[Throttled Model (429/503)]` $\times$ `[Model Rescue]`
- `[Tor SOCKS5 Rotation]` $\times$ `[Upstream Rate Limit (429)]` $\times$ `[IsolateSOCKSAuth Fresh Circuit]`
- `[Reasoning Content Output]` $\times$ `[Streaming Delta Sanitization]` $\times$ `[Code Indentation Preservation]`

### 2.4 Real-World Workload Profiles
Simulates realistic, demanding client usage patterns:
- Multi-turn conversational sessions maintaining context history across 5 turns.
- Complex developer code generation requiring strict whitespace, indentation, and escape sequence fidelity.
- Rapid burst concurrency testing gateway connection pooling, mutex lock contention, and circuit rotation under load.

---

## 3. Tiered Test Structure

The test suite is organized into 4 distinct testing tiers:

```
tests/e2e/
├── conftest.py                # Test configuration, fixtures, base URLs
├── validator.py               # Strict schema validators & assertion engines
├── mock_server.py             # Deterministic upstream simulator & proxy fixture
├── test_tier1_features.py     # Tier 1: Feature Coverage (>=5 tests per feature)
├── test_tier2_boundaries.py   # Tier 2: Boundary & Corner Cases (>=5 tests per feature)
├── test_tier3_combinations.py # Tier 3: Cross-Feature Combinations (Pairwise)
└── test_tier4_real_world.py   # Tier 4: Real-World Scenarios (TTFT, Bursts, Multi-Turn)
```

### Tier 1: Feature Coverage (>= 5 test cases per feature)
1. **OpenAI `/v1/chat/completions` Endpoint**:
   - `test_openai_chat_basic_non_streaming`: Valid non-streaming chat returning `chat.completion` JSON.
   - `test_openai_chat_streaming_chunks`: Valid streaming chat returning `chat.completion.chunk` SSE events.
   - `test_openai_chat_with_system_message`: Multi-role messages containing `system` and `user`.
   - `test_openai_chat_temperature_parameter`: Parameter passing (temperature, top_p, max_tokens).
   - `test_openai_models_list`: Querying `/v1/models` returns model inventory with creation timestamps.
2. **Anthropic `/v1/messages` Endpoint**:
   - `test_anthropic_messages_basic_non_streaming`: Valid non-streaming message returning Anthropic JSON.
   - `test_anthropic_messages_streaming_events`: Full SSE event sequence (`message_start`, `content_block_start`, `content_block_delta`, `content_block_stop`, `message_delta`, `message_stop`).
   - `test_anthropic_messages_with_system_prompt`: Top-level `system` prompt parameter handled faithfully.
   - `test_anthropic_messages_route_alias`: Verifies `/v1/v1/messages` endpoint compatibility.
   - `test_anthropic_messages_anthropic_version_header`: Validates `anthropic-version: 2023-06-01` header requirement.
3. **Cloudflare D1 Authentication Layer**:
   - `test_d1_auth_valid_bearer_key`: Authorized request with active key returns 200 OK.
   - `test_d1_auth_x_api_key_header`: Request using `x-api-key` header succeeds.
   - `test_d1_auth_inactive_key_rejected`: Inactive/disabled API key returns 401 Unauthorized.
   - `test_d1_auth_exhausted_balance_rejected`: Zero credit balance returns 402 Payment Required.
   - `test_d1_auth_token_usage_deducted`: Successful requests trigger token usage accounting.
4. **Offline Mock Authentication Fallback**:
   - `test_mock_auth_test_key_accepted`: Fallback accepts `"test-key"` when D1 is offline.
   - `test_mock_auth_default_dev_key_accepted`: Fallback accepts `"default-dev-key"` when D1 is offline.
   - `test_mock_auth_unknown_key_rejected`: Fallback rejects unknown keys with 401 Unauthorized.
   - `test_mock_auth_undefined_env_db_resilience`: When `env.DB` is undefined, gateway falls back gracefully without 500 error.
   - `test_mock_auth_empty_key_rejected`: Empty key string rejected with 401 Unauthorized.
5. **Direct Models**:
   - `test_direct_model_muse_spark_streaming`: Direct request to `muse-spark-1.3-contributor-free` streams successfully.
   - `test_direct_model_non_streaming_json`: Direct non-streaming request returns valid JSON schema.
   - `test_direct_model_tool_schema_passthrough`: OpenCode tool schema handling on direct endpoint.
   - `test_direct_model_usage_metadata`: Verifies prompt and completion token counts in response usage.
   - `test_direct_model_finish_reason_stop`: Checks termination indicator `finish_reason: "stop"`.
6. **Routed Virtual Models**:
   - `test_routed_model_gpt4o_mapping`: Virtual `gpt-4o` maps to operational upstream.
   - `test_routed_model_claude_sonnet_mapping`: Virtual `claude-3-5-sonnet` maps to operational upstream.
   - `test_routed_model_deepseek_mapping`: Virtual `deepseek-r1` maps to operational upstream.
   - `test_routed_model_session_token_injection`: Gateway injects genuine `msg_<hex><base62>` token.
   - `test_routed_model_brand_masking`: Masking replaces internal names with target virtual model brand.

### Tier 2: Boundary & Corner Cases (>= 5 test cases per feature)
1. **Large Prompts**:
   - `test_large_prompt_10kb_text`: 10 KB prompt processed without gateway truncation or buffer overflow.
   - `test_large_prompt_50kb_code_payload`: 50 KB code context handled with streaming chunks intact.
   - `test_large_prompt_high_max_tokens`: Request with `max_tokens: 4096` stream without timeout.
   - `test_large_prompt_deep_message_history`: Conversation with 20 message turns processed cleanly.
   - `test_large_prompt_large_system_instruction`: System instruction exceeding 8 KB preserved.
2. **Minimal Prompts**:
   - `test_minimal_prompt_single_character`: Single character `"a"` processed and answered.
   - `test_minimal_prompt_single_digit`: Single digit `"1"` handled cleanly.
   - `test_minimal_prompt_single_punctuation`: Punctuation mark `"?"` handled cleanly.
   - `test_minimal_prompt_whitespace_only`: Whitespace-only string handled gracefully without panic.
   - `test_minimal_prompt_one_token_completion`: `max_tokens: 1` limits response to exactly 1 token.
3. **Empty Content & Malformed Payloads**:
   - `test_empty_messages_array_error`: `messages: []` returns 400 Bad Request error.
   - `test_empty_user_content_string`: `messages: [{"role": "user", "content": ""}]` handled gracefully.
   - `test_missing_messages_field`: JSON body lacking `messages` field returns 400 Bad Request.
   - `test_null_content_in_message`: `content: null` handled without JSON unmarshal crash.
   - `test_malformed_json_body`: Invalid raw JSON body returns 400 Bad Request.
4. **Special Characters & Multilingual Encoding**:
   - `test_special_chars_emojis_and_unicode`: Emojis (`🚀🔥🤖`) and complex UTF-8 characters preserved.
   - `test_special_chars_cjk_and_arabic`: Multilingual scripts (Chinese, Japanese, Arabic RTL) intact.
   - `test_special_chars_json_and_quotes`: Quotes, backslashes, tabs, and newlines correctly escaped.
   - `test_special_chars_null_bytes_and_control`: Control characters handled safely without socket abort.
   - `test_special_chars_markdown_and_backticks`: Multi-line triple-backtick markdown blocks preserved verbatim.
5. **Missing & Malformed Authorization**:
   - `test_missing_auth_header_rejected`: Omitted Authorization header returns 401 Unauthorized.
   - `test_empty_bearer_token_rejected`: `Authorization: Bearer ` returns 401 Unauthorized.
   - `test_malformed_auth_scheme_rejected`: `Authorization: Basic xyz` returns 401 Unauthorized.
   - `test_invalid_api_key_rejected`: Arbitrary unrecognized key returns 401 Unauthorized.
   - `test_options_preflight_bypasses_auth`: CORS preflight `OPTIONS` returns 204 No Content without auth.

### Tier 3: Cross-Feature Combinations (Pairwise)
1. **Streaming $\times$ Authentication**:
   - `test_pairwise_streaming_with_valid_bearer`: Streaming request authenticated via Bearer token.
   - `test_pairwise_streaming_with_x_api_key`: Streaming request authenticated via `x-api-key`.
   - `test_pairwise_streaming_with_mock_auth`: Streaming request authenticated via mock fallback key.
   - `test_pairwise_streaming_unauthorized_immediate_abort`: Unauthenticated streaming request returns immediate 401 JSON before any SSE header.
   - `test_pairwise_streaming_exhausted_balance_abort`: Zero-balance streaming request returns immediate 402 JSON.
2. **OpenCode Tools $\times$ Dynamic Model Rescue**:
   - `test_pairwise_tools_injected_on_throttled_model`: Throttled initial model request receives tool declarations upon failover.
   - `test_pairwise_dead_model_rescued_to_working_model`: Initial model (`nemotron-3.5-lightning-free`) automatically rescued to `muse-spark-1.3-contributor-free` returning 200 OK.
   - `test_pairwise_session_affinity_preserved_across_rescue`: Genuine `msg_` session header maintained during model failover.
   - `test_pairwise_rescue_returns_streaming_sse`: Rescued model stream maintains SSE delta conformance.
   - `test_pairwise_rescue_sanitizes_reasoning`: Rescued model outputs strip internal CoT / thinking tags.
3. **Tor Circuit Failover $\times$ Rate-Limiting (429/503)**:
   - `test_pairwise_upstream_429_triggers_tor_rotation`: Upstream HTTP 429 triggers Tor SOCKS5 circuit rotation.
   - `test_pairwise_upstream_503_triggers_model_rescue`: Upstream HTTP 503 triggers model rescue with HTTP 200 return.
   - `test_pairwise_isolate_socks_auth_fresh_credentials`: Each retry attempt uses distinct credentials to prevent dirty circuit reuse.
   - `test_pairwise_retry_budget_exhaustion_clean_503`: Complete upstream failure exhausts retry budget and returns structured 503 error.
   - `test_pairwise_zero_unhandled_403_under_circuit_switch`: Circuit switching avoids unhandled 403 FreeTierErrors.
4. **Reasoning Sanitization $\times$ Streaming Delivery**:
   - `test_pairwise_streaming_reasoning_stripped_in_real_time`: SSE chunks with `reasoning_content` are filtered so only sanitized assistant text streams.
   - `test_pairwise_streaming_identity_guard_blocks_suppressed`: `<identity_guard>` prompt instructions never leak to client SSE deltas.
   - `test_pairwise_streaming_whitespace_and_code_preserved`: Code block spaces, indentations, and newlines preserved during sanitization.

### Tier 4: Real-World Scenarios
1. **Multi-Turn Conversational Session**:
   - `test_scenario_multi_turn_conversation_context`: 5 sequential conversation turns maintaining dialogue continuity and role alternation (`user` -> `assistant` -> `user`).
2. **Syntactically Complex Code Generation**:
   - `test_scenario_code_generation_fidelity`: Prompt requesting multi-threaded Python queue code verifies that generated Python syntax is valid and uncorrupted by regex word splitters.
3. **Strict TTFT Latency Benchmark**:
   - `test_scenario_ttft_under_4_point_5_seconds`: Streaming request asserts Time-To-First-Token $\le 4.5\text{s}$ on healthy upstream.
4. **Immediate First SSE Chunk Delivery (Zero-Buffering Assertion)**:
   - `test_scenario_zero_buffering_immediate_chunk`: Asserts that `ttft < total_time * 0.70`, proving that the first chunk was delivered immediately rather than buffering full completion in memory.
5. **High-Frequency Burst Concurrency & Resilience**:
   - `test_scenario_burst_concurrency_resilience`: 10 rapid concurrent requests executed via `concurrent.futures`, verifying zero connection drops, zero thread deadlocks, and zero 403 errors.

---

## 4. Programmatic Assertion Invariants

Every test case strictly enforces deterministic criteria:

| Invariant | Specification | Assertion Check |
|---|---|---|
| **TTFT $\le 4.5\text{s}$** | Time-To-First-Token on healthy streaming upstream $\le 4.5\text{s}$ | `assert ttfb <= 4.5, f"TTFT {ttfb}s exceeded 4.5s limit"` |
| **Zero Buffering** | First chunk delivered immediately to client Web Stream | `assert first_chunk_received_immediately, "Completion buffered"` |
| **Zero Unhandled 403** | No raw `FreeTierError` or unhandled 403 on valid requests | `assert status_code != 403, "Unhandled FreeTierError 403 returned"` |
| **Automatic 429/503 Rescue** | Upstream throttling transparently rescued | `assert status_code == 200, "Failed to rescue 429/503 model"` |
| **OpenAI SSE Delta Conformance** | Adheres to `chat.completion.chunk` delta schema | Validated against `OpenAISSEChunkSchema` |
| **Anthropic SSE Conformance** | Emits standard Anthropic SSE event lifecycle | Validated against `AnthropicSSEEventSchema` |
| **Exit Code 0 & JSON Summary** | Runner exits with code 0 on complete pass | Programmatic `sys.exit(0)` with structured metrics JSON |

---

## 5. Execution Architecture & Running Tests

### 5.1 Test Runner Command
The test suite can be run via two unified mechanisms:

1. **Benchmark Runner & Metrics Engine**:
   ```bash
   python3 run_benchmarks.py
   ```
   * Runs all 4 test tiers, captures TTFT, throughput, and error recovery metrics, saves structured summary to `benchmark_results.json`, and exits with code `0`.

2. **Standard Python Test Runner**:
   ```bash
   python3 -m unittest discover -s tests/e2e -v
   ```

### 5.2 Environment Variables
The test suite supports dynamic environment overrides:
- `GO_BACKEND_URL`: URL of the Go proxy backend (default: `http://127.0.0.1:8787`, fallback `http://127.0.0.1:8790`).
- `EDGE_URL`: URL of Cloudflare Pages edge functions (default: `http://127.0.0.1:8788`).
- `USE_MOCK_HARNESS`: Set to `true` (default in autonomous test runs) to spin up the zero-dependency mock fixture server for testing edge, proxy translation, error injection, and circuit rotation.
- `API_KEY`: API key used for authenticated requests (default: `test-key`).

---

## 6. Coverage Summary & Traceability Matrix

| Requirement | Description | Verified in Test Tier | Status |
|---|---|---|---|
| **R1** | Edge Streaming & D1 Auth (Mock Fallback, Zero Buffering) | Tier 1 (Tests 1.3, 1.4), Tier 2 (Test 2.5), Tier 3 (Test 3.1) | Covered |
| **R2** | High-Throughput Go Backend (Tool injection, Tor rotation, Model rescue) | Tier 1 (Tests 1.5, 1.6), Tier 3 (Tests 3.2, 3.3), Tier 4 (Test 4.5) | Covered |
| **R3** | Protocol Parity (OpenAI & Anthropic streaming, Sanitization) | Tier 1 (Tests 1.1, 1.2), Tier 2 (Tests 2.1-2.4), Tier 3 (Test 3.4) | Covered |
| **R4** | Automated Verification & Benchmarks (TTFT <= 4.5s, exit 0) | Tier 4 (Tests 4.3, 4.4), `run_benchmarks.py` | Covered |
