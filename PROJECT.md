# Project: Unified AI Gateway and Proxy

## Architecture
The system consists of two tightly coordinated architectural layers:
1. **Edge Streaming & Routing Layer (Cloudflare Pages Functions)**:
   - Resides in `functions/`.
   - Validates client API keys against Cloudflare D1 (`env.DB`), with a resilient offline/local mock table fallback when `env.DB` is unavailable.
   - Forwards client requests to the high-throughput Go proxy backend via HTTP streaming.
   - Streams responses chunk-by-chunk using standard Web Streams (`ReadableStream` / `TransformStream`) directly to the client with zero buffering in memory.
   - Handles CORS preflight (`OPTIONS`) and header preservation.

2. **High-Throughput Resilient Go Backend Proxy**:
   - Resides in `main.go`.
   - Implements Direct-First TLS with Chrome_131 fingerprinting (uTLS / cycleTLS).
   - Translates OpenAI-compatible (`/v1/chat/completions`) and Anthropic-compatible (`/v1/messages`) requests to upstream OpenCode `/responses` format.
   - Injects OpenCode tool schemas (`bash`, `read`) and genuine session tokens (`msg_<timestamp_hex><base62>`) to prevent 403 FreeTierErrors.
   - Implements resilient Tor circuit rotation fallback using per-request SOCKS5 credentials (`IsolateSOCKSAuth`) when rate-limited.
   - Dynamically rescues exhausted or throttled models (429 / 503) to verified working alternative models (such as `muse-spark-1.3-contributor-free`) returning HTTP 200 OK.
   - Delivers true SSE chunk streaming with zero intermediate buffering and sanitized reasoning outputs.

3. **E2E Testing & Verification Track**:
   - Independent opaque-box test runner and verification suite testing TTFT <= 4.5s, SSE zero-buffering, zero unhandled 403s, dynamic 429/503 model rescue, and schema compliance across OpenAI and Anthropic protocols.

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | D1 API Key Auth | Validate incoming Bearer API keys against Cloudflare D1 database | M1 | Survey (R1) |
| 2 | Offline Mock Auth Fallback | Graceful fallback to mock key registry when D1 is offline or env.DB undefined | M1 | Survey (R1) |
| 3 | Edge Zero-Buffering Streaming | Passthrough streaming via Web Streams (ReadableStream / TransformStream) | M1 | Survey (R1) |
| 4 | Edge CORS & Preflight | Handle OPTIONS preflight and inject CORS headers for browser clients | M1 | Survey (R1) |
| 5 | Clean Edge Sanitization | Remove regex word splitting corruption from edge responses | M1 | Survey (R1) |
| 6 | Direct-First TLS Fingerprinting | Establish upstream connections with Chrome_131 TLS fingerprint | M2 | Survey (R2) |
| 7 | Tor Circuit Rotation Fallback | SOCKS5 fallback with IsolateSOCKSAuth circuit isolation on rate limiting | M2 | Survey (R2) |
| 8 | OpenCode /responses Translation | Translate OpenAI/Anthropic format to upstream /responses format | M2 | Survey (R2) |
| 9 | Tool Schemas & Session Injection | Inject OpenCode tools (bash, read) and genuine session tokens to stop 403s | M2 | Survey (R2) |
| 10 | Dynamic Model Rescue | Automatically rescue throttled (429/503) models to working models with 200 OK | M2 | Survey (R2) |
| 11 | Build & Scratch Cleanup | Isolate/remove scratch test files causing package main conflicts | M2 | Survey (R2) |
| 12 | OpenAI Protocol Streaming Parity | Full /v1/chat/completions SSE delta format and parameter compatibility | M3 | Survey (R3) |
| 13 | Anthropic Protocol Streaming Parity | Real-time SSE /v1/messages streaming (replacing io.ReadAll buffering) | M3 | Survey (R3) |
| 14 | Reasoning Output Sanitization | Sanitize thinking / reasoning tags from streaming SSE outputs | M3 | Survey (R3) |
| 15 | Non-streaming Format Parity | Valid JSON completions conforming to OpenAI and Anthropic schemas | M3 | Survey (R3) |
| 16 | E2E Opaque-Box Test Suite | 4-tier test harness covering Tiers 1-4 with automated assertions | E2E Track | Survey (R4) |
| 17 | TTFT & Streaming Benchmark | Automated benchmark asserting TTFT <= 4.5s and immediate first chunk delivery | E2E Track | Survey (R4) |
| 18 | 403 & Throttling Failover Verification | Stress test validating zero unhandled 403s and seamless 429/503 rescue | E2E Track | Survey (R4) |
| 19 | 100% E2E Test Suite Pass | Final integration verifying implementation against full test suite | M4 | Survey (Final) |
| 20 | Adversarial Coverage Hardening | Tier 5 white-box challenger stress testing and gap remediation | M4 | Survey (Final) |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Edge Streaming & D1 Auth Layer | Cloudflare Pages functions/ with D1 validation, offline mock fallback, Web Streams zero-buffering, and CORS | none | DONE (Gate PASS: approved by 2 reviewers, 2 challengers, auditor clean) |
| M2 | Go Backend Resilience & Model Rescue | Direct-First Chrome_131 TLS, OpenCode tools/session token injection, Tor circuit rotation, dynamic model rescue, build cleanup | none | IN_PROGRESS (worker_m2 completed) |
| M3 | Protocol & Format Parity | /v1/chat/completions & /v1/messages true streaming SSE, reasoning sanitization, schema parity | M2 | IN_PROGRESS (worker_m3: 01b961d2) |
| M4 | Final Milestone: E2E Verification & Hardening | Phase 1: Pass 100% E2E test suite (Tiers 1-4); Phase 2: Tier 5 Adversarial Coverage Hardening | M1, M2, M3, E2E Track | PLANNED |
| E2E | E2E Testing Track | Independent opaque-box test runner and 4-tier test suite (Tiers 1-4) publishing TEST_READY.md | none (Parallel) | DONE (Published TEST_READY.md, 80 test scenarios, exit code 0) |

## Interface Contracts

### Edge Layer ↔ Go Backend
- **Upstream Target**: `http://127.0.0.1:8080` (or `BACKEND_URL` environment variable).
- **Paths**:
  - `/v1/chat/completions` -> `POST /v1/chat/completions`
  - `/v1/messages` -> `POST /v1/messages`
- **Headers Forwarded**: `Authorization`, `Content-Type: application/json`.
- **Streaming Mode**: `stream: true` forwarded in JSON body; response chunks piped directly via `ReadableStream` with `Content-Type: text/event-stream; charset=utf-8` and `Cache-Control: no-cache`.

### Client ↔ Edge Layer
- **Authentication**: `Authorization: Bearer <api_key>` validated against D1 table `api_keys` (or fallback mock table `{ "test-key": true, "default-dev-key": true }`).
- **CORS**: `OPTIONS` preflight returns `204 No Content` with `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: POST, OPTIONS`, `Access-Control-Allow-Headers: Content-Type, Authorization, x-api-key, anthropic-version`.

### Backend ↔ Upstream OpenCode
- **Endpoint**: Upstream `/responses` or `/chat/completions`
- **Tool Schemas**: `bash` and `read` tools injected into OpenCode payload.
- **Session Tokens**: Genuine OpenCode session identifier format: `msg_<timestamp_hex><base62>`.
- **TLS Client**: Chrome_131 ClientHello fingerprinting.
- **Circuit Failover**: On upstream 429/503 or Tor routing, per-request `IsolateSOCKSAuth` SOCKS5 credentials trigger clean circuits.
- **Model Rescue**: Throttled models fall back through verified operational models (`muse-spark-1.3-contributor-free`, `x-preview-f-free`, `laguna-s-2.1-free`) without client 503 errors.

## Code Layout
- `functions/`: Cloudflare Pages functions router and middleware.
  - `functions/v1/chat/completions.js`: OpenAI route edge handler.
  - `functions/v1/messages.js`: Anthropic route edge handler.
  - `functions/_middleware.js`: Auth & CORS shared middleware.
- `main.go`: High-throughput Go proxy backend.
- `internal/` / `pkg/` (if modularized): Go proxy modules.
- `tests/e2e/`: Opaque-box E2E test suite (harness, test cases, runner).
- `run_benchmarks.py`: Automated verification suite asserting on TTFT, throughput, status codes, and model rescue.
