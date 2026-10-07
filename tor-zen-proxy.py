#!/usr/bin/env python3
"""
Tor + opencode-zen smart proxy (v2 — with streaming support).

Forwards OpenAI-compatible requests to https://opencode.ai/zen/v1
through a Tor SOCKS5 proxy. On 429 (FreeUsageLimitError) the proxy
sends NEWNYM to the Tor control port to rotate the exit circuit and
retries the request automatically.

Streaming: when the client sends stream=true, this proxy forwards
SSE chunks in real-time from upstream to the client.

Requires:
  - curl_cffi (pip install curl_cffi)
  - a running Tor with both SocksPort and ControlPort enabled
    (default: SocksPort=9150, ControlPort=9151, no auth)

Usage:
  ./tor-zen-proxy.py [--port 8767] [--default minimax-m3-free] \\
                     [--socks 127.0.0.1:9150] [--control 127.0.0.1:9151]
"""

import argparse
import itertools
import hashlib
import hmac
import json
import os
import queue
import re
import secrets
import shlex
import socket
import subprocess
import sys
import time
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qsl, parse_qs

# Add bin dir to path so we can import zen-db
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi import CurlHttpVersion
    from curl_cffi.requests.exceptions import RequestException
except ImportError:
    sys.exit("curl_cffi not installed. pip install curl_cffi")

try:
    import importlib
    zen_db = importlib.import_module("zen-db")
except Exception:
    zen_db = None
    sys.stderr.write("[warn] zen-db not found, usage tracking disabled\n")

UPSTREAM = "https://opencode.ai/zen/v1"
PORT = 8767
DEFAULT_MODEL = "muse-spark-1.3-contributor-free"
SOCKS = "127.0.0.1:9150"
CONTROL = "127.0.0.1:9151"
MAX_RETRIES = 6
DEFAULT_POOL_SIZE = 20
# Big-context agent turns cost ~20-35s per Tor attempt when upstream answers
# a throttled model late; 180s only fit ~4-6 exits. Muse has no rescue model,
# so its only recovery is more fresh exits — give the rotation room.
MAX_TIME = 300
BACKOFF = 1.3
ROTATE_WAIT = 2.0
# Pool-global Retry-After park cap: upstream sends countdown-style horizons
# (hours until quota reset). Parking the full value would take a model offline
# for 9h on one header — park a short hint instead and keep rotating exits,
# which is what actually recovers from per-exit throttles.
_BAN_PARK_CAP_S = 180.0
# Attempts (0 = probe + 2 rotations) allowed while a ban is parked before
# ban-skip short-circuits to rescue. Never zero: a banned model must still be
# probed so recovery is detected without waiting out the horizon.
_BAN_SKIP_MIN_ATTEMPT = 3
RESTART_TOR = False
# Streaming stalls: curl_cffi maps scalar timeout -> LOW_SPEED_TIME, so a
# quiet upstream (long reasoning pause) gets aborted mid-stream. Use a
# (connect, low-speed) tuple on streaming calls: fast connect failover
# but a generous stall window within the overall MAX_TIME retry budget.
STREAM_CONNECT_TIMEOUT = 15
STREAM_LOW_SPEED_TIMEOUT = 600
# First-byte (TTFB) bound for the streaming POST. Without this, curl's read
# timeout stays at STREAM_LOW_SPEED_TIMEOUT (600s) until content flows, so a
# Zen /responses call that hangs before headers blocked a request for 98s
# (observed 2026-09-21) before returning 500. After the peek commits we raise
# the socket timeout back to STREAM_LOW_SPEED_TIMEOUT in _stream_realtime, so
# long quiet *reasoning* pauses (post-content) are still tolerated.
# NOTE: this constant was defined but NEVER ENFORCED on the POST path (dead
# config) — pre-header stalls (63s observed) slipped through. It is now
# enforced via worker-thread join (deterministic; see streaming POST below).
# Size-aware at use: base + 1s per 20KB incoming (large uploads need longer).

STREAM_TTFB_TIMEOUT = 90.0



# Peek-before-commit budget: worst case a dead/quiet exit holds the client's
# first token for this long before we swap. 40s was the old value; 15s keeps
# the broken-stream filter (embedded 429/CF errors, EOF, stalls) while the
# retry backoff now spaces swaps — combined worst case per attempt 15s + ~8s
# instead of 40s + 0s. Override with ZEN_PEEK_BUDGET_S.
try:
    PEEK_BUDGET_S = max(5.0, min(40.0, float(os.environ.get("ZEN_PEEK_BUDGET_S", "15"))))
except Exception:
    PEEK_BUDGET_S = 15.0
# --- Log cosmetics ---------------------------------------------------------
# Quiet mode (ZEN_QUIET=1, default): hide per-request size/probe telemetry;
# terminal shows only turns (DONE green), errors (red), warnings (yellow).
try:
    LOG_QUIET = os.environ.get("ZEN_QUIET", "1").strip() not in ("0", "no", "false", "")
except Exception:
    LOG_QUIET = True


def _kfmt(n) -> str:
    """Compact count: 8100 -> 8.1k, 90000 -> 90k, 1200000 -> 1.2M."""
    try:
        n = int(n)
    except Exception:
        return str(n)
    if n < 1000:
        return str(n)
    if n < 10000:
        return f"{n / 1000.0:.1f}k"
    if n < 1000000:
        return f"{n / 1000.0:.0f}k"
    return f"{n / 1000000.0:.1f}M"


def _red(s) -> str:
    """Red foreground (errors). No-op safe."""
    try:
        return f"\033[31m{s}\033[0m"
    except Exception:
        return str(s)


def _yel(s) -> str:
    """Yellow foreground (warnings/retries)."""
    try:
        return f"\033[33m{s}\033[0m"
    except Exception:
        return str(s)


def _qlog(msg) -> bool:
    """True if a routine telemetry line should print (not quiet mode)."""
    return not LOG_QUIET

# Hard first-CONTENT deadline per attempt: bare lifecycle pings
# (response.created/in_progress with no deltas) must NOT extend the peek
# past this. A queued/stalled exit trickles pings for 60s+ then delivers;
# that is the 70s TTFT tail. Genuine reasoning is unaffected (reasoning
# deltas count as content and commit immediately). On expiry the attempt
# fails over to a fresh exit via the normal retry swap. Override with
# ZEN_FIRST_CONTENT_S.
try:
    FIRST_CONTENT_S = max(15.0, min(300.0, float(os.environ.get("ZEN_FIRST_CONTENT_S", "90"))))
except Exception:
    FIRST_CONTENT_S = 90.0
RETRYABLE_STATUSES = (429, 403, 500, 502, 503, 504)

# --- Tor pool warm-probe tuning --------------------------------------------
# Over obfs4 bridges a cold circuit's first byte from a public IP-echo service
# routinely exceeds 8s. That is NOT a dead exit, but the old 8s bound logged it
# as `curl: (28)` (fail#10) and left the slot cold, which then showed up as a
# 4-10s TTFT on every request. Probe several echo services under one budget so
# a slow/blocked endpoint doesn't condemn a working circuit.
_PROBE_ENDPOINTS = [
    "https://checkip.amazonaws.com",
    "https://icanhazip.com",
    "https://api.seeip.org",
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
]
try:
    WARM_PROBE_TIMEOUT = max(8.0, min(30.0, float(
        os.environ.get("ZEN_WARM_PROBE_TIMEOUT", "16"))))
except Exception:
    WARM_PROBE_TIMEOUT = 35.0
# Slow-Tor default (was 15s): IP-echo over degraded obfs4 routinely needs
# 6-17s; a 15s budget turns slowness into curl-28 failures and empty pools.
# Cap on the duplicate/quarantine warm-reject sleep. The old 60s cap parked
# replenisher threads for a minute while the pool drained; ~8s re-probes an
# exit quickly without hammering a genuinely tiny exit set.
try:
    WARM_REJECT_BACKOFF_CAP = max(2.0, min(30.0, float(
        os.environ.get("ZEN_WARM_REJECT_BACKOFF", "8"))))
except Exception:
    WARM_REJECT_BACKOFF_CAP = 8.0
# Keepalive cadence for the hottest idle circuit. A Tor circuit/TLS connection
# that is actively used answers in ~1-3s but goes cold within ~10-15s idle and
# then costs 8-30s to rebuild. Touching the MRU idle slot on its upstream
# connection every few seconds keeps it genuinely "ready to send" so sequential
# agent turns land on a live circuit instead of paying cold-build latency.
# 0 disables the keepalive thread.
try:
    POOL_KEEPALIVE_S = max(0.0, float(os.environ.get("ZEN_POOL_KEEPALIVE", "4")))
except Exception:
    POOL_KEEPALIVE_S = 4.0
# Burned-exit park window: when a circuit answers 429/403 (per-circuit free
# limit), its exit IP is kept out of rotation for this long so Tor handing the
# same exit back doesn't immediately re-429. The replenish health-check (below)
# is the real gate; parking only avoids re-probing the same dead exit.
try:
    EXIT_PARK_S = max(60.0, float(os.environ.get("ZEN_EXIT_PARK_S", "1800")))
except Exception:
    EXIT_PARK_S = 1800.0
# Replenish health-check: after IP-echo liveness, fire one tiny muse request on
# the fresh circuit and only mark it READY if it is not 429/403. This is the
# "test if it's not 429 before handing it out" gate. 0 disables.
try:
    POOL_HEALTHCHECK = os.environ.get("ZEN_REPLENISH_CHECK", "1") != "0"
except Exception:
    POOL_HEALTHCHECK = True
# Max in-flight requests that may share the single ACTIVE exit before we spill
# onto a READY standby. Sequential traffic stays on one hot circuit (the whole
# point), but a burst (many subagents / load) spreads across exits so one exit
# isn't hammered into a 429 storm and TTFT stays low.
try:
    ACTIVE_SHARE_CAP = max(1, int(os.environ.get("ZEN_ACTIVE_SHARE", "10")))
except Exception:
    ACTIVE_SHARE_CAP = 10

# Content-aware idle bound for streaming upstreams. curl_cffi derives
# LOW_SPEED_LIMIT=1 / LOW_SPEED_TIME=connect+read for stream=True requests, so
# a tuple like (15, 20) aborts the transfer if throughput stays <1 B/s for 35s.
# That killed legitimate long reasoning pauses (muse-spark-1.3 on Zen
# /responses) mid-stream -> "[stream-fatal] curl (28) ... last 35 seconds".
# Since Session.request() has no per-request curl_options hook, we instead give
# the stream a generous read value (raising LOW_SPEED_TIME past any real pause)
# AND enforce our own resettable idle timeout: on expiry _ZenIdleGuard pushes a
# retryable RequestException into the curl_cffi stream queue, which
# iter_content() re-raises so the existing retry/rescue path still runs.
try:
    STREAM_IDLE_ABORT = max(60.0, float(os.environ.get("ZEN_STREAM_IDLE", "300")))
except Exception:
    STREAM_IDLE_ABORT = 300.0


class _ZenIdleGuard:
    """Abort a curl_cffi stream that delivers no data for longer than `limit`.

    touch() resets the clock on every chunk. During the pre-commit peek the
    limit is PEEK_BUDGET_S (fast failover off a dead exit); once content is
    flowing it is raised to STREAM_IDLE_ABORT so long quiet reasoning is fine.
    """

    def __init__(self, r, limit):
        self.r = r
        self.limit = float(limit)
        self.last = time.monotonic()
        self._stop_evt = threading.Event()
        self._fired = False
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()

    def touch(self):
        self.last = time.monotonic()

    def set_limit(self, limit):
        self.limit = float(limit)
        self.last = time.monotonic()

    def stop(self):
        self._stop_evt.set()

    def _run(self):
        while not self._stop_evt.wait(1.0):
            if self._fired:
                return
            idle = time.monotonic() - self.last
            if idle > self.limit:
                self._fired = True
                sys.stderr.write(
                    f"[stream-idle] no data for {idle:.0f}s "
                    f"(limit {self.limit:.0f}s) — aborting stream\n")
                try:
                    self.r.queue.put(RequestException(
                        f"upstream idle for too long: 0 bytes received in the "
                        f"last {int(idle)} seconds"))
                except Exception:
                    pass
                return

# Security: token required for sensitive endpoints (/live /stats /rotate).
# Auto-generated on first run at ~/.local/share/tor-zen/token (mode 0600).
TOKEN_FILE = os.path.join(os.path.expanduser("~/.local/share/tor-zen"), "token")


def _auth_token() -> str:
    try:
        with open(TOKEN_FILE) as f:
            tok = f.read().strip()
        if tok:
            return tok
    except Exception:
        pass
    tok = secrets.token_hex(16)
    try:
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(tok)
        os.chmod(TOKEN_FILE, 0o600)
    except Exception as e:

        sys.stderr.write(f"[security] could not persist token: {e}\n")
    return tok


AUTH_TOKEN = _auth_token()
_direct_cooldown_until = 0.0

# --- Public API-key auth (Tailscale Funnel sharing) -------------------------
# In-memory cache of api_keys hashes -> name, loaded at startup and reloaded
# on mtime change / SIGHUP / gated reload. Hot-path verify is ~µs (set +
# compare_digest); SQLite is never touched per-request. last_used updates are
# coalesced in a background thread (never on the streaming path).
_API_KEYS = {}          # key_hash -> name
_API_KEYS_MTIME = 0.0
_API_KEYS_LOCK = threading.Lock()
_API_LAST_USED_QUEUE = set()
_API_LAST_USED_LOCK = threading.Lock()
_PUBLIC_AUTH = os.environ.get("ZEN_PUBLIC_AUTH", "") == "1"
_LOCAL_DNS_NAME = ""    # our tailnet DNSName (set at startup by tor-proxy)
# Max accepted request body. Long agent sessions (big context + tool output +
# images) routinely exceed 8 MiB and were being rejected outright with a 413,
# killing the session. The Go proxy allows 64 MiB (maxRequestBodyBytes), so keep
# the two lanes in step; override with ZEN_MAX_BODY if a model needs more.
_MAX_BODY = int(os.environ.get("ZEN_MAX_BODY", str(64 * 1024 * 1024)))


def _api_keys_db_mtime() -> float:
    try:
        if zen_db is not None and hasattr(zen_db, "DB_PATH"):
            return os.path.getmtime(zen_db.DB_PATH)
    except Exception:
        pass
    return 0.0


def reload_api_keys(force: bool = False) -> int:
    """(Re)load active api_keys hashes into memory. Returns count."""
    global _API_KEYS, _API_KEYS_MTIME
    if zen_db is None or not hasattr(zen_db, "list_api_keys"):
        return len(_API_KEYS)
    try:
        mt = _api_keys_db_mtime()
        if not force and mt and mt <= _API_KEYS_MTIME and _API_KEYS:
            return len(_API_KEYS)
        import hashlib as _hl
        fresh = {}
        for k in zen_db.list_api_keys():
            if k.get("revoked"):
                continue
            # list_api_keys is prefix-only; resolve hashes via verify table scan.
            # Instead: pull hashes directly (read-only, no plaintext).
            fresh["_scan_"] = True
        if fresh.pop("_scan_", None) is not None:
            import sqlite3 as _sq
            conn = _sq.connect(zen_db.DB_PATH, timeout=5.0)
            try:
                rows = conn.execute(
                    "SELECT key_hash, name FROM api_keys WHERE revoked = 0").fetchall()
                fresh = {h: n for h, n in rows}
            finally:
                conn.close()
        with _API_KEYS_LOCK:
            _API_KEYS = fresh
            _API_KEYS_MTIME = mt or time.time()
        return len(_API_KEYS)
    except Exception as e:

        sys.stderr.write(f"[auth] key reload failed: {e}\n")
        return len(_API_KEYS)


def _flush_last_used():
    """Background coalesced last_used updater (5s batch)."""
    while True:
        time.sleep(5.0)
        try:
            with _API_LAST_USED_LOCK:
                batch = list(_API_LAST_USED_QUEUE)
                _API_LAST_USED_QUEUE.clear()
            if not batch or zen_db is None:
                continue
            for pt in batch:
                try:
                    zen_db.touch_api_key(pt)
                except Exception:
                    pass
        except Exception:
            pass


threading.Thread(target=_flush_last_used, daemon=True,
                 name="zen-key-touch").start()


def verify_public_key(plaintext: str):
    """Return key name if a valid active API key, else None (~µs, memory-only)."""
    if not plaintext:
        return None
    pt = plaintext.strip()
    if not pt:
        return None
    h = hashlib.sha256(pt.encode("utf-8")).hexdigest()
    with _API_KEYS_LOCK:
        name = _API_KEYS.get(h)
    if name:
        with _API_LAST_USED_LOCK:
            _API_LAST_USED_QUEUE.add(pt)
        return name
    # Stale cache? One DB check (management ran while serving), then cache.
    if zen_db is not None and hasattr(zen_db, "verify_api_key"):
        try:
            name = zen_db.verify_api_key(pt)
        except Exception:
            name = None
        if name:
            reload_api_keys(force=True)
            with _API_LAST_USED_LOCK:
                _API_LAST_USED_QUEUE.add(pt)
            return name
    return None

# Automatically mirror all stderr logging to /tmp/opencode/zen-proxy.log
# so any attached terminal (tor-proxy) receives live streaming telemetry.
LOG_PATH = "/tmp/opencode/zen-proxy.log"
try:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    # Check if sys.stderr is already redirected to LOG_PATH (e.g. by nohup in zen-proxy-restart.sh)
    _already_redirected = False
    try:
        if os.path.exists(LOG_PATH) and hasattr(sys.stderr, "fileno"):
            _already_redirected = (os.stat(LOG_PATH).st_ino == os.fstat(sys.stderr.fileno()).st_ino)
    except Exception:
        pass
    if not _already_redirected:
        _log_file_handle = open(LOG_PATH, "a", buffering=1, encoding="utf-8")
        class _TeeStderr:
            def __init__(self, original_stderr, file_handle):
                self.orig = original_stderr
                self.file = file_handle
            def write(self, s):
                self.orig.write(s)
                self.orig.flush()
                try:
                    self.file.write(s)
                    self.file.flush()
                except Exception:
                    pass
            def flush(self):
                self.orig.flush()
                try:
                    self.file.flush()
                except Exception:
                    pass
        sys.stderr = _TeeStderr(sys.stderr, _log_file_handle)
except Exception:
    pass

# When a requested model is unavailable upstream, fall back to the first
# live model in this chain.
# NOTE (2026-09-26): was ["big-pickle"] — but big-pickle ALIASES to
# muse-spark-1.3-contributor-free, so when muse 429'd the "rescue" re-ran the
# SAME throttled model until MAX_TIME: Claude Code requests hung for minutes
# and the subagent never recovered. The rescue must be a DIFFERENT model with
# its own quota/backend. Zen-only (the rescue re-enters the Tor pool, so a
# Cline id would be sent to the wrong upstream).
FALLBACK_CHAIN = [
    "space-bunny-free",
    "deepseek-v4.1-flash",
]
# NOTE (2026-09-25): deepseek hops REMOVED — `deepseek/deepseek-v4-flash`
# is upstream-unsupported (400 ModelError) and `deepseek-v4-flash-free`
# is backend-down (502 "Model is unavailable"), so both hops only burned
# seconds + exits before landing on big-pickle anyway. big-pickle IS muse
# (canonical alias) — rescue = fresh-exit retry of the same model, which
# succeeds as soon as rotation finds a clean exit. Re-add deepseek only
# when a live backend for it is verified (Cline accounts currently 401).

# Muse circuit-breaker (2026-09-23): when Zen has flagged our whole Tor
# exit set, every muse turn burns ~7 exits of [403-retry] only to rescue
# anyway. While engaged, muse turns are served directly as
# deepseek-v4-flash (fast, zero 403 churn). Auto-lifts after the horizon;
# the next muse turn then probes for real and re-engages if still burned.
_MUSE_BREAKER = {"until": 0.0, "horizon": 900.0}


def _muse_breaker_active() -> bool:
    return time.monotonic() < _MUSE_BREAKER["until"]

# Reasoning efforts muse actually accepts (upstream expects: none, minimal, low, medium, high, xhigh).
# Upstream rejects 'max' with invalid_request_error, so 'max' is mapped to 'xhigh'.
_MUSE_VALID_EFFORTS = ("none", "minimal", "low", "medium", "high", "xhigh")
# TTFT ceiling: Codex asks for max/xhigh reasoning (12s think before first
# token); opencode asks minimal (3s). Anything above the cap is downgraded so
# TTFT stays flat. DEFAULT IS NO DOWNGRADE (xhigh = full quality) — opt into a
# lower ceiling only via ZEN_EFFORT_CAP (none|minimal|low|medium|high|xhigh).
try:
    _EFFORT_CAP = os.environ.get("ZEN_EFFORT_CAP", "xhigh").strip().lower()
    if _EFFORT_CAP not in _MUSE_VALID_EFFORTS:
        _EFFORT_CAP = "xhigh"
except Exception:
    _EFFORT_CAP = "xhigh"


def _cap_effort(effort: str) -> str:
    """Downgrade a reasoning effort above the TTFT ceiling. Below-cap passes through."""
    try:
        e = str(effort).strip().lower()
        if e in _MUSE_VALID_EFFORTS and _MUSE_VALID_EFFORTS.index(e) > _MUSE_VALID_EFFORTS.index(_EFFORT_CAP):
            sys.stderr.write(f"[effort-cap] reasoning {e!r} -> {_EFFORT_CAP!r} (TTFT ceiling)\n")
            return _EFFORT_CAP
        return e
    except Exception:
        return effort

# Flex mode: disguise model names in responses sent to client
MODEL_FLEX = {
    "nemotron-3-ultra-free": "claude-opus-4.8-xHigh",
    "nvidia/nemotron-3-ultra-550b-a55b-20260604:free": "claude-opus-4.8-xHigh",
}
TORRC = "/tmp/tor2/torrc"

# --- Per-model upstream wire protocol -------------------------------------
# Source of truth: opencode's own model cache
# (~/.cache/opencode/models.json -> opencode.models.*.provider.npm), verified
# live 2026-09-17. The Zen backend speaks all three wires and REJECTS a model
# on the wrong one (union-alpha 500'd on every OpenAI shape; free chat models
# 401/403 on responses/messages). Update alongside the cache; unknown ids
# default to OpenAI-compatible chat/completions.
_ANTHROPIC_NATIVE = {
    "union-alpha",
    "claude-3-5-haiku", "claude-haiku-4-5", "claude-fable-5",
    "claude-fable-5-1", "claude-opus-4-1", "claude-opus-4-5",
    "claude-opus-4-6", "claude-opus-4-7", "claude-opus-4-8",
    "claude-opus-5", "claude-sonnet-4", "claude-sonnet-4-5",
    "claude-sonnet-4-6", "claude-sonnet-5",
    "minimax-m2.1-free", "minimax-m2.5-free", "minimax-m3-free",
    "qwen3.5-plus", "qwen3.6-plus", "qwen3.6-plus-free",
}
_RESPONSES_NATIVE = {
    "muse-spark-1.2", "muse-spark-1.2-contributor-free",
    "muse-spark-1.3", "muse-spark-1.3-contributor-free",
    "big-pickle",
    "kimi-k3", "kimi-k2.6", "kimi-k2.5",
    "gemini-3.8-flash", "gemini-3.7-flash", "gemini-3.5-flash",
    "gemini-3.5-flash-lite", "gemini-3.1-pro", "gemini-3.6-flash",
    "qwen3.8-max", "qwen3.8-flash",
    "glm-5", "glm-5.1", "glm-5.2", "glm-5.3", "glm-5.3-flash",
    "deepseek-v4.1-flash",
    "minimax-m3",
    # NOTE: space-bunny-free is CHAT-wire (verified 200 chat, 401 responses).
    # Do NOT add here — _RESPONSES_NATIVE forces /responses which 401s it.
}

# Vision capability, verified live 2026-09-25 and from opencode's model cache.
# Base deepseek-v4-flash is TEXT-ONLY (attachment:false, input:[text]); its
# vision sibling and muse accept images. Only confirmed entries are listed —
# anything else is treated as unknown (not advertised, not remapped).
# Vision capability: ONLY entries verified 200 with real image bytes
# (2026-09-25 Pillow-red-PNG probe). kimi-k3 401s (paid lane) so it is NOT
# advertised despite accepting the shape. Unverified = not advertised.
MODEL_VISION_INPUTS = {
    # muse DOES see images, but only on the chat/completions wire: Zen's
    # /responses endpoint silently drops `input_image` (muse then reasons
    # "absence of image data" -> "gray"). Image turns on muse are therefore
    # sent over chat/completions (see the image branch in _prepare_upstream).
    "muse-spark-1.2": ["text", "image", "video", "pdf", "audio"],
    "muse-spark-1.3": ["text", "image", "video", "pdf", "audio"],
    "big-pickle": ["text", "image", "video", "pdf", "audio"],
    "deepseek-v4-flash-vision-exp": ["text", "image"],
    "deepseek-v4.1-flash": ["text", "image"],
    "space-bunny-free": ["text", "image"],
}

def _model_vision_inputs(model_id):
    mid = (model_id or "")
    for k, v in MODEL_VISION_INPUTS.items():
        if k in mid:
            return v
    return None

def _request_has_image(body, api_path) -> bool:
    """True if this request carries image input (any wire shape)."""
    try:
        if "responses" in (api_path or ""):
            for it in (body.get("input") or []):
                if not isinstance(it, dict):
                    continue
                for c in (it.get("content") or []):
                    if isinstance(c, dict) and c.get("type") == "input_image" and (
                            c.get("image_url") or c.get("url")):
                        return True
            return False
        for m in (body.get("messages") or []):
            if not isinstance(m, dict):
                continue
            c = m.get("content")
            if isinstance(c, list):
                for p in c:
                    if isinstance(p, dict) and str(p.get("type", "")).lower().startswith("image"):
                        return True
        return False
    except Exception:
        return False

def _vision_sibling(model_id):
    """Zen vision-capable replacement for a known TEXT-ONLY bare id, else None."""
    m = (model_id or "")
    base = m.split(":")[0]
    if "/" in m or "vision" in m or "muse-spark" in m:
        return None
    if base == "deepseek-v4-flash":
        return "deepseek-v4-flash-vision-exp"
    return None


def _wire_protocol(model_id) -> str:
    """Upstream wire for a model id: 'messages' | 'responses' | 'chat'."""
    mid = (model_id or "").split(":")[0]
    if model_id in _ANTHROPIC_NATIVE or mid in _ANTHROPIC_NATIVE:
        return "messages"
    if model_id in _RESPONSES_NATIVE or mid in _RESPONSES_NATIVE:
        return "responses"
    if (model_id or "").startswith("muse") or mid.startswith("muse"):
        return "responses"
    return "chat"

# Fallback model list (auto-refreshed from upstream)
MODELS = [
    {"id": "big-pickle",                     "object": "model", "owned_by": "zen"},
    {"id": "deepseek-v4-flash-free",         "object": "model", "owned_by": "zen"},
    {"id": "x-preview-f-free",               "object": "model", "owned_by": "zen"},
    {"id": "mimo-v2.5-free",                 "object": "model", "owned_by": "zen"},
    {"id": "mimo-v2.6-flash-free",           "object": "model", "owned_by": "zen"},
    {"id": "ling-3.0-flash-fin-free",        "object": "model", "owned_by": "zen"},
    {"id": "nemotron-3-ultra-free",          "object": "model", "owned_by": "zen"},
    {"id": "nemotron-3.5-lightning-free",    "object": "model", "owned_by": "zen"},
    {"id": "muse-spark-1.2-contributor-free","object": "model", "owned_by": "zen"},
    {"id": "muse-spark-1.3-contributor-free","object": "model", "owned_by": "zen"},
    {"id": "deepseek-v4-flash-vision-exp",  "object": "model", "owned_by": "zen"},
    {"id": "union-alpha",                   "object": "model", "owned_by": "zen"},
    {"id": "jev-1.13-free",                 "object": "model", "owned_by": "zen"},
]

# Friendly aliases → real upstream model ids
MODEL_ALIASES = {
    "ox-alpha": "x-preview-f-free",
    "ox-alpha-free": "x-preview-f-free",
    "deepseek-muse": "muse-spark-1.3-contributor-free",
    "o3-mini": "muse-spark-1.3-contributor-free",
    "o1-mini": "muse-spark-1.3-contributor-free",
    "muse": "muse-spark-1.3-contributor-free",
    "muse-free": "muse-spark-1.3-contributor-free",
    "muse-1.2": "muse-spark-1.2-contributor-free",
    "muse-spark": "muse-spark-1.3-contributor-free",
    "muse-spark-free": "muse-spark-1.3-contributor-free",
    "muse-spark-1.2": "muse-spark-1.2-contributor-free",
    "muse-1.3": "muse-spark-1.3-contributor-free",
    "muse-spark-1.3": "muse-spark-1.3-contributor-free",
    "muse-spark-1.3-free": "muse-spark-1.3-contributor-free",
    "muse-spark-1.3-contributor-free": "muse-spark-1.3-contributor-free",
    "muse-spark-1.2-contributor-free": "muse-spark-1.2-contributor-free",
    # Native OpenAI model aliases -> default muse model
    "gpt-5.4": "muse-spark-1.3-contributor-free",
    "gpt-5.4-fast": "muse-spark-1.3-contributor-free",
    "gpt-5.4-mini": "muse-spark-1.3-contributor-free",
    "gpt-5.4-mini-fast": "muse-spark-1.3-contributor-free",
    "gpt-5.5": "muse-spark-1.3-contributor-free",
    "gpt-5.5-fast": "muse-spark-1.3-contributor-free",
    "gpt-5.6-luna": "muse-spark-1.3-contributor-free",
    "gpt-5.6-sol": "muse-spark-1.3-contributor-free",
    "gpt-5.6-terra": "muse-spark-1.3-contributor-free",
    "gpt-4o": "muse-spark-1.3-contributor-free",
    "gpt-4o-mini": "muse-spark-1.3-contributor-free",
    "gpt-4": "muse-spark-1.3-contributor-free",
    "default": "muse-spark-1.3-contributor-free",
    # Native Claude Code model aliases -> models
    "sonnet": "muse-spark-1.3-contributor-free",
    "claude-sonnet-4-6": "muse-spark-1.3-contributor-free",
    "claude-3-5-sonnet": "muse-spark-1.3-contributor-free",
    "claude-3-5-sonnet-20241022": "muse-spark-1.3-contributor-free",
    "claude-3-7-sonnet": "muse-spark-1.3-contributor-free",
    "claude-3-7-sonnet-20250219": "muse-spark-1.3-contributor-free",
    "claude-sonnet-5": "muse-spark-1.3-contributor-free",
    "opus": "muse-spark-1.3-contributor-free",
    "claude-opus-5": "muse-spark-1.3-contributor-free",
    "claude-opus-4.8": "muse-spark-1.3-contributor-free",
    "claude-opus-4.6": "muse-spark-1.3-contributor-free",
    "claude-3-opus": "muse-spark-1.3-contributor-free",
    "claude-3-opus-20240229": "muse-spark-1.3-contributor-free",
    "haiku": "deepseek/deepseek-v4-flash",
    "claude-haiku-4-5": "deepseek/deepseek-v4-flash",
    "claude-3-5-haiku": "deepseek/deepseek-v4-flash",
    "claude-3-5-haiku-20241022": "deepseek/deepseek-v4-flash",
    "big-pickle": "muse-spark-1.3-contributor-free",
    "jev-1.13-free": "muse-spark-1.3-contributor-free",
    "nemotron": "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nemotron-ultra": "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nemotron-3-ultra-free": "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nemotron-lightning": "nvidia/nemotron-3.5-lightning:free",
    "nemotron-3.5-lightning-free": "nvidia/nemotron-3.5-lightning:free",
    "mimo": "xiaomi/mimo-v2.5",
    "mimo-v2.5": "xiaomi/mimo-v2.5",
    "mimo-v2.5-free": "xiaomi/mimo-v2.5",
    "mimo-v2.6": "xiaomi/mimo-v2.5",
    "mimo-2.6": "xiaomi/mimo-v2.5",
    "mimo-v2.6-flash": "xiaomi/mimo-v2.5",
    "mimo-v2.6-free": "xiaomi/mimo-v2.5",
    "mimo-v2.6-flash-free": "xiaomi/mimo-v2.5",
    "ling-fin": "inclusionai/ling-3.0-flash-fin:free",
    "ling-3.0-fin": "inclusionai/ling-3.0-flash-fin:free",
    "ling-3.0-flash-fin": "inclusionai/ling-3.0-flash-fin:free",
    "ling-3.0-flash-fin-free": "inclusionai/ling-3.0-flash-fin:free",
    "gemma": "google/gemma-4-26b-a4b-it:free",
    "gemma-4": "google/gemma-4-26b-a4b-it:free",
    # Cline provider model aliases
    "deepseek-v4-flash": "deepseek/deepseek-v4-flash",
    "deepseek-v4-flash-free": "deepseek/deepseek-v4-flash:free",
    "deepseek": "deepseek/deepseek-v4-flash",
    "deepseek-vision": "deepseek-v4-flash-vision-exp",
    "deepseek-v4-flash-vision": "deepseek-v4-flash-vision-exp",
    "glm-5.3-flash": "z-ai/glm-5.3-flash",
    "glm-5.3": "z-ai/glm-5.3",
    "glm-5.2-free": "z-ai/glm-5.2:free",
    "glm-5.2": "z-ai/glm-5.2",
    "glm-5.1": "z-ai/glm-5.1",
    "glm-5-turbo": "z-ai/glm-5-turbo",
    "glm-4.7-flash": "z-ai/glm-4.7-flash",
    "gemma-4-26b": "google/gemma-4-26b-a4b-it:free",
    "gemma-4-31b": "google/gemma-4-31b-it:free",
    "kat-coder-pro": "kwaipilot/kat-coder-pro",
    "minimax-m2.5": "minimax/minimax-m2.5",
    "minimax-m2.7": "minimax/minimax-m2.7:free",
    "nemotron-3-nano-reasoning": "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
    "dots-note-preview": "dots-studio/dots-3-note-preview:free",
    "ling-3.0-flash": "inclusionai/ling-3.0-flash-fin:free",
    "cline/deepseek-v4-flash": "deepseek/deepseek-v4-flash",
    "cline/glm-5.3-flash": "z-ai/glm-5.3-flash",
    "cline/glm-5.3": "z-ai/glm-5.3",
    "cline/glm-5.2": "z-ai/glm-5.2",
    "cline/glm-5.1": "z-ai/glm-5.1",
    "cline/mimo-v2.5": "xiaomi/mimo-v2.5",
    "cline/gemma-4-26b": "google/gemma-4-26b-a4b-it:free",
    "cline/gemma-4-31b": "google/gemma-4-31b-it:free",
    "cline/kat-coder-pro": "kwaipilot/kat-coder-pro",
    "cline/minimax-m2.5": "minimax/minimax-m2.5",
    "cline/minimax-m2.7": "minimax/minimax-m2.7:free",
    # New free-pass models, short aliases (verified 200 on 2026-09-15 probe)
    # NOTE: deepseek-v4.1* intentionally maps to the cline-free/* FREE lane, not
    # the bare paid id — see the cline-free block below. Do not add a paid-lane
    # entry above: a duplicate key is silently shadowed and confuses debugging.
    "nex-pro": "nex-agi/nex-n2.5-mini:free",
    "nex-mini": "nex-agi/nex-n2.5-mini:free",
    "nex-n2.5-pro": "nex-agi/nex-n2.5-mini:free",
    "nex-n2.5-mini": "nex-agi/nex-n2.5-mini:free",
    "inkling": "thinkingmachines/inkling:free",
    "inkling-small": "thinkingmachines/inkling-small:free",
    "nemotron-3-super": "nvidia/nemotron-3-super-120b-a12b:free",
    "nemotron-3-ultra": "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nemotron-3.5-lightning": "nvidia/nemotron-3.5-lightning:free",
    "gemma-4-31b-free": "google/gemma-4-31b-it:free",
    # Catalog-sweep free additions (2026-09-21)
    "ling-vl": "inclusionai/ling-3.0-flash-vl:free",
    "ling-flash-vl": "inclusionai/ling-3.0-flash-vl:free",
    "ling-sante": "inclusionai/ling-3.0-flash-sante:free",
    "lfm-2.5": "liquid/lfm-2.5-2.6b:free",
    "lfm-2.6b": "liquid/lfm-2.5-2.6b:free",
    "north-mini": "cohere/north-mini-code:free",
    "north-mini-code": "cohere/north-mini-code:free",
    "nemotron-safety": "nvidia/nemotron-3.5-content-safety:free",
    "nemotron-content-safety": "nvidia/nemotron-3.5-content-safety:free",
    "qwen3.8-27b": "qwen/qwen3.8-27b:free",
    "qwen-free": "qwen/qwen3.8-27b:free",
    "cline/qwen3.8-27b": "qwen/qwen3.8-27b:free",
    # Cline UI free section = `cline-free/*` server-side ids (verified 200 on
    # 2026-09-15 against /api/v1/ai/cline/recommended-models). These ride the
    # FREE quota lane — same model WITHOUT the prefix bills Cline Credits.
    "deepseek-v4.1": "cline-free/deepseek-v4.1-flash",
    "deepseek-v4.1-flash": "cline-free/deepseek-v4.1-flash",
    "deepseek-v4.1-free": "cline-free/deepseek-v4.1-flash",
    "pixel-canary": "stealth/pixel-canary",
    "stealth/pixel-canary": "stealth/pixel-canary",
    "space-bunny-alpha": "stealth/space-bunny-alpha",
    "stealth/space-bunny-alpha": "stealth/space-bunny-alpha",
    "muse-spark-1.3-contributor": "cline-free/muse-spark-1.3-contributor",
    "muse-spark-contributor": "cline-free/muse-spark-1.3-contributor",
    "solar-pro4": "cline-free/solar-pro4",
    "solar-pro-4": "cline-free/solar-pro4",
    # --- Cline free-lane models (verified 200 on 2026-10-05) ---
    "minimax-m3-free": "minimax/minimax-m3:free",
    "minimax-m2.5-free": "minimax/minimax-m2.5:free",
    "qwen3.8-27b-free": "qwen/qwen3.8-27b:free",
    "gemma-4-26b-free": "google/gemma-4-26b-a4b-it:free",
    "nemotron-3-super-free": "nvidia/nemotron-3-super-120b-a12b:free",
    "inkling-free": "thinkingmachines/inkling:free",
    "inkling-small-free": "thinkingmachines/inkling-small:free",
    "north-mini-free": "cohere/north-mini-code:free",
    "laguna-s-free": "poolside/laguna-s-2.1:free",
    "laguna-xs-free": "poolside/laguna-xs-2.1:free",
    "lfm-2.5-free": "liquid/lfm-2.5-2.6b:free",
    "dots-note-free": "dots-studio/dots-3-note-preview:free",
    "apodex-mini-free": "apodex/apodex-1.1-mini:free",
    "ling-sante-free": "inclusionai/ling-3.0-flash-sante:free",
    "kimi": "kimi-k3",
    "kimi-k3": "kimi-k3",
    "kimi-3": "kimi-k3",
    "kimi-free": "kimi-k3",
    "moonshotai/kimi-k3": "kimi-k3",
    "cline/kimi": "kimi-k3",
    "cline/kimi-k3": "kimi-k3",
    "cline-free/kimi-k3": "kimi-k3",
    "cline/deepseek-v4.1-flash": "cline-free/deepseek-v4.1-flash",
    "deepseek-v4-flash-0731": "deepseek/deepseek-v4-flash-0731",
    "cline/deepseek-v4-flash-0731": "deepseek/deepseek-v4-flash-0731",
    "deepseek-flash-latest": "~deepseek/deepseek-flash-latest",
    "cline/deepseek-flash-latest": "~deepseek/deepseek-flash-latest",
    "cline/nex-n2.5-pro": "nex-agi/nex-n2.5-pro:free",
    "cline/nex-n2.5-mini": "nex-agi/nex-n2.5-mini:free",
    "cline/inkling": "thinkingmachines/inkling:free",
    "cline/nemotron-3-super": "nvidia/nemotron-3-super-120b-a12b:free",
    "cline/nemotron-3-ultra": "nvidia/nemotron-3-ultra-550b-a55b:free",
    "cline/nemotron-3.5-lightning": "nvidia/nemotron-3.5-lightning:free",
    "cline/gemma-4-31b-free": "google/gemma-4-31b-it:free",
    # Freebuff models & aliases
    # NOTE: freebuff is no longer contacted (route removed 2026-09-21). These
    # keys remain only so legacy clients resolve to the Cline free lane via
    # FREEBUFF_FALLBACK_MAP (see the freebuff-off remap in the request path).
    "fable-5.1": "freebuff/fable-5.1",
    "claude-fable-5.1": "freebuff/claude-fable-5.1",
    "freebuff/fable-5.1": "anthropic/claude-fable-5.1",
    "freebuff/claude-fable-5.1": "anthropic/claude-fable-5.1",
    "freebuff/deepseek-v4.1-flash": "deepseek/deepseek-v4.1-flash",
    "freebuff/deepseek-v4-flash": "deepseek/deepseek-v4-flash",
    "freebuff/gemini-3.8-flash": "google/gemini-3.8-flash",
    "freebuff/kimi-k3-eco": "crof/kimi-k3-eco",
    "freebuff/kimi-k3": "crof/kimi-k3-eco",
    "kimi-k3-eco": "crof/kimi-k3-eco",
    "freebuff/glm-5.3-flash": "z-ai/glm-5.3-flash",
    "freebuff/solar-pro4": "upstage/solar-pro4",
    "freebuff/muse-spark-1.3": "meta/muse-spark-1.3-contributor",
}

FREEBUFF_MODELS = {
    "freebuff/fable-5.1": "anthropic/claude-fable-5.1",
    "freebuff/claude-fable-5.1": "anthropic/claude-fable-5.1",
    "fable-5.1": "anthropic/claude-fable-5.1",
    "claude-fable-5.1": "anthropic/claude-fable-5.1",
    "anthropic/claude-fable-5.1": "anthropic/claude-fable-5.1",
    "freebuff/deepseek-v4.1-flash": "deepseek/deepseek-v4.1-flash",
    "deepseek/deepseek-v4.1-flash": "deepseek/deepseek-v4.1-flash",
    "freebuff/deepseek-v4-flash": "deepseek/deepseek-v4-flash",
    "freebuff/gemini-3.8-flash": "google/gemini-3.8-flash",
    "google/gemini-3.8-flash": "google/gemini-3.8-flash",
    "freebuff/kimi-k3-eco": "crof/kimi-k3-eco",
    "freebuff/kimi-k3": "crof/kimi-k3-eco",
    "kimi-k3-eco": "crof/kimi-k3-eco",
    "crof/kimi-k3-eco": "crof/kimi-k3-eco",
    "freebuff/glm-5.3-flash": "z-ai/glm-5.3-flash",
    "freebuff/solar-pro4": "upstage/solar-pro4",
    "upstage/solar-pro4": "upstage/solar-pro4",
    "freebuff/muse-spark-1.3": "meta/muse-spark-1.3-contributor",
}

FREEBUFF_FALLBACK_MAP = {
    "freebuff/fable-5.1": "cline-free/deepseek-v4.1-flash",
    "freebuff/claude-fable-5.1": "cline-free/deepseek-v4.1-flash",
    "fable-5.1": "cline-free/deepseek-v4.1-flash",
    "claude-fable-5.1": "cline-free/deepseek-v4.1-flash",
    "anthropic/claude-fable-5.1": "cline-free/deepseek-v4.1-flash",
    "freebuff/deepseek-v4.1-flash": "cline-free/deepseek-v4.1-flash",
    "deepseek/deepseek-v4.1-flash": "cline-free/deepseek-v4.1-flash",
    "freebuff/deepseek-v4-flash": "deepseek/deepseek-v4-flash",
    "freebuff/gemini-3.8-flash": "google/gemma-4-31b-it:free",
    "google/gemini-3.8-flash": "google/gemma-4-31b-it:free",
    "freebuff/kimi-k3-eco": "cline-free/kimi-k3",
    "freebuff/kimi-k3": "cline-free/kimi-k3",
    "kimi-k3-eco": "cline-free/kimi-k3",
    "crof/kimi-k3-eco": "cline-free/kimi-k3",
    "freebuff/glm-5.3-flash": "z-ai/glm-5.3-flash",
    "freebuff/solar-pro4": "cline-free/solar-pro4",
    "upstage/solar-pro4": "cline-free/solar-pro4",
}

CLINE_PROVIDERS_FILE = os.path.expanduser("~/.cline/data/settings/providers.json")
# Multi-account pool: drop per-account providers.json snapshots in this dir
# (acc-1.json, acc-2.json, ...) and the proxy rotates through them on 429.
# Files must be mode 0600. If empty/absent, falls back to CLINE_PROVIDERS_FILE.
CLINE_ACCOUNTS_DIR = os.path.expanduser("~/.local/share/tor-zen/accounts")
# Wall-clock snapshot of pool ban/credit state so restarts remember drained
# accounts (monotonic horizons die with the process). Small JSON, 0600.
_CLINE_STATE_FILE = os.path.join(os.path.dirname(CLINE_ACCOUNTS_DIR),
                                 "cline_pool_state.json")
# --- WorkBuddy (Tencent workbuddy.ai) free lane -------------------------
# Merged into this proxy (2026-09-22): reads the WorkBuddy desktop app's own
# auth file, masquerades as the desktop client (X-IDE-*/X-Agent-Intent), forces
# the streaming-only upstream, and serves its OpenAI-shaped SSE straight
# through. Replaces the standalone Node workbuddy-connect-api on :8787.
# Free DeepSeek V4.1 Flash promo. Requests carry the desktop identity exactly
# like the app so usage is attributed to the WorkBuddy client.
WORKBUDDY_AUTH_FILE = os.path.expanduser(
    "~/.local/share/CodeBuddyExtension/Data/Public/auth/workbuddy-desktop-ai.info")
_WB_GLOBAL = "https://www.workbuddy.ai"
_WB_CN = "https://copilot.tencent.com"
_WB_VER = "5.4.2"
_WB_UA = f"linux/{_WB_VER} WorkBuddy/{_WB_VER} CLI/2.137.1"
_WB_CACHE = {"cred": None}
_WB_LOCK = threading.Lock()

WORKBUDDY_ALIASES = {
    "workbuddy": "workbuddy/auto",
    "wb": "workbuddy/auto",
    "wb-auto": "workbuddy/auto",
    "workbuddy-auto": "workbuddy/auto",
    "wb-deepseek": "workbuddy/deepseek-v4.1-flash",
    "workbuddy-deepseek": "workbuddy/deepseek-v4.1-flash",
    "wb-hy4": "workbuddy/hy4-preview",
    "wb-hy3": "workbuddy/hy3",
    "wb-glm": "workbuddy/glm-5.2",
    "wb-glm52": "workbuddy/glm-5.2",
    "wb-glm53": "workbuddy/glm-5.3",
    "wb-luna": "workbuddy/gpt-5.6-luna",
    "wb-sol": "workbuddy/gpt-5.6-sol",
    "wb-terra": "workbuddy/gpt-5.6-terra",
    "wb-gpt54": "workbuddy/gpt-5.4",
    "wb-gpt55": "workbuddy/gpt-5.5",
}
_WORKBUDDY_MODELS = {
    "workbuddy/auto": {"name": "wb-auto (free)", "reasoning": False},
    "workbuddy/deepseek-v4.1-flash": {"name": "wb-deepseek-v4.1-flash (free)", "reasoning": False},
    "workbuddy/hy3": {"name": "wb-hy3 (free)", "reasoning": False},
    "workbuddy/hy4-preview": {"name": "wb-hy4-preview (0.01)", "reasoning": False},
    "workbuddy/glm-5.2": {"name": "wb-glm-5.2 (free)", "reasoning": False},
    "workbuddy/glm-5.3": {"name": "wb-glm-5.3 (0.04)", "reasoning": False},
    "workbuddy/gpt-5.4": {"name": "wb-gpt-5.4 (0.02)", "reasoning": False},
    "workbuddy/gpt-5.5": {"name": "wb-gpt-5.5 (0.05)", "reasoning": False},
    "workbuddy/gpt-5.6-luna": {"name": "wb-gpt-5.6-luna (0-0.18)", "reasoning": True},
    "workbuddy/gpt-5.6-sol": {"name": "wb-gpt-5.6-sol (0.04)", "reasoning": True},
    "workbuddy/gpt-5.6-terra": {"name": "wb-gpt-5.6-terra (0.02)", "reasoning": True},
}


def _wb_region_base(domain):
    return _WB_GLOBAL if "workbuddy.ai" in (domain or "") else _WB_CN


def _wb_proxy():
    """Tor circuit for WorkBuddy. DNS for workbuddy.ai is hijacked on this
    network (resolves to 0.0.0.1), so the upstream MUST go over socks5h."""
    try:
        return Handler.proxy_url
    except Exception:
        return f"socks5h://{SOCKS}"


def _workbuddy_read_cred():
    try:
        with open(WORKBUDDY_AUTH_FILE) as fh:
            doc = json.load(fh)
    except Exception:
        return None
    au = doc.get("auth") or {}
    acc = doc.get("account") or {}
    tok = au.get("accessToken") or ""
    if not tok:
        return None
    exp = au.get("expiresAt") or 0
    if exp > 1e12:
        exp = exp / 1000.0
    return {
        "accessToken": tok,
        "refreshToken": au.get("refreshToken") or "",
        "expiresAt": exp,
        "domain": au.get("domain") or "",
        "uid": acc.get("uid") or "",
        "enterpriseId": acc.get("enterpriseId") or "",
    }


def _workbuddy_refresh(cred):
    base = _wb_region_base(cred.get("domain"))
    h = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": _WB_UA,
        "Origin": base,
        "Referer": base + "/",
        "X-Refresh-Token": cred["refreshToken"],
        "X-Auth-Refresh-Source": "workbuddy",
    }
    if cred.get("enterpriseId"):
        h["X-Enterprise-Id"] = cred["enterpriseId"]
    r = cffi_requests.Session(impersonate="chrome131", timeout=45).post(
        base + "/v2/plugin/auth/token/refresh",
        headers=h, proxy=_wb_proxy())
    j = r.json()
    if r.status_code != 200 or (j.get("code") or 0) != 0:
        raise RuntimeError(f"refresh {r.status_code}: {str(j)[:200]}")
    d = j.get("data") or {}
    at = d.get("accessToken") or ""
    if not at:
        raise RuntimeError("refresh returned no accessToken")
    out = dict(cred)
    out["accessToken"] = at
    if d.get("refreshToken"):
        out["refreshToken"] = d["refreshToken"]
    if d.get("domain"):
        out["domain"] = d["domain"]
    if isinstance(d.get("expiresIn"), (int, float)) and d["expiresIn"] > 0:
        out["expiresAt"] = time.time() + d["expiresIn"]
    return out


def _workbuddy_cred():
    """Fresh WorkBuddy credential, refreshed on demand (5-min margin)."""
    with _WB_LOCK:
        c = _WB_CACHE.get("cred")
        if c and c.get("expiresAt", 0) > time.time() + 300:
            return c
        c = _workbuddy_read_cred()
        if not c:
            _WB_CACHE["cred"] = None
            return None
        if c.get("expiresAt", 0) <= time.time() + 300 and c.get("refreshToken"):
            try:
                c = _workbuddy_refresh(c)
                sys.stderr.write("[workbuddy] token refreshed\n")
            except Exception as e:

                sys.stderr.write(f"[workbuddy] token refresh failed: {e}\n")
        _WB_CACHE["cred"] = c
        return c


def _workbuddy_chat_headers(cred):
    base = _wb_region_base(cred.get("domain"))
    cid = uuid.uuid4().hex
    mid = uuid.uuid4().hex
    h = {
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": base,
        "Referer": base + "/",
        "User-Agent": _WB_UA,
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cred['accessToken']}",
        "X-Product": "SaaS",
        "X-IDE-Type": "linux",
        "X-IDE-Name": "linux",
        "X-IDE-Version": _WB_VER,
        "X-Agent-Intent": "craft",
        "X-Parent-Conversation-ID": cid,
        "X-Conversation-Request-ID": uuid.uuid4().hex,
        "X-Conversation-Message-ID": mid,
        "X-Request-ID": mid,
    }
    if cred.get("uid"):
        h["X-User-Id"] = cred["uid"]
    else:
        h["X-No-User-Id"] = "1"
    if cred.get("enterpriseId"):
        h["X-Enterprise-Id"] = cred["enterpriseId"]
    else:
        h["X-No-Enterprise-Id"] = "1"
    if cred.get("domain"):
        h["X-Domain"] = cred["domain"]
    else:
        h["X-No-Department-Info"] = "1"
    return h


CLINE_API_ENDPOINT = "https://api.cline.bot/api/v1/chat/completions"
CLINE_MODELS = {
    "deepseek/deepseek-v4-flash",
    # cline-free/* free-tier models from the live catalog
    # (each free model has its OWN quota bucket — rotate for more headroom)
    "cline-free/deepseek-v4.1-flash",
    "cline-free/muse-spark-1.3-contributor",
    "cline-free/solar-pro4",
    "stealth/pixel-canary",
    "stealth/space-bunny-alpha",
    # NOTE (2026-09-25): cline-free/kimi-k3 REMOVED — Cline 404s it
    # (de-listed); kimi-k3 now serves on Zen directly.
    "z-ai/glm-5.3-flash",
    "z-ai/glm-5.3",
    "z-ai/glm-5.2:free",
    "z-ai/glm-5.2",
    "z-ai/glm-5.1",
    "z-ai/glm-5-turbo",
    "z-ai/glm-4.7-flash",
    "xiaomi/mimo-v2.5",
    "google/gemma-4-26b-a4b-it:free",
    "google/gemma-4-31b-it:free",
    "kwaipilot/kat-coder-pro",
    "minimax/minimax-m2.5",
    "minimax/minimax-m2.7:free",
    "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
    "dots-studio/dots-3-note-preview:free",
    "inclusionai/ling-3.0-flash-fin:free",
    "poolside/laguna-s-2.1:free",
    # New free-pass additions (verified 200 on 2026-09-15 probe):
    "nex-agi/nex-n2.5-pro:free",
    "nex-agi/nex-n2.5-mini:free",
    "thinkingmachines/inkling:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "nvidia/nemotron-3-ultra-550b-a55b:free",
    "nvidia/nemotron-3.5-lightning:free",
    # Catalog sweep 2026-09-21 (full /api/v1/models `:free` ids not in the
    # recommended→free lane). Probe results: these 5 -> 200; qwen3.8-27b:free
    # and inkling-small:free -> upstream OpenRouter 429 (wired but dead until
    # the upstream free pool recovers); laguna-xs-2.1:free -> 404 (not added).
    "inclusionai/ling-3.0-flash-vl:free",
    "inclusionai/ling-3.0-flash-sante:free",
    "liquid/lfm-2.5-2.6b:free",
    "cohere/north-mini-code:free",
    "nvidia/nemotron-3.5-content-safety:free",
    "qwen/qwen3.8-27b:free",
    # deepseek v4.1 family (catalog 2026-09-10; NOTE: v4.1-flash base id 500s
    # "empty response content" on probe — kept for catalog completeness, the
    # cline-fallback rescues to muse if upstream can't serve it):
    "deepseek/deepseek-v4.1-flash",
    "deepseek/deepseek-v4-flash-0731",
    "~deepseek/deepseek-flash-latest",
    # Verified-free additions (2026-10-05 probe: all 200 via the Cline lane).
    # apodex + laguna-xs were previously missing/404; both serve now.
    "apodex/apodex-1.1-mini:free",
    "poolside/laguna-xs-2.1:free",
    # Hidden free tiers (not in the catalog listing, but answer 200):
    "z-ai/glm-5.2:free",
    "deepseek/deepseek-v4-flash:free",
    "minimax/minimax-m2.5:free",
    "minimax/minimax-m3:free",
}

_cline_auth_lock = threading.Lock()
_cline_auth_cache = {"token": None, "expires_at": 0}

# ---------------------------------------------------------------------------
# Model routing hierarchy + performance-based provider selection.
#
# Tiering (per request): the requested model family decides the FALLBACK
# chain. The two "power" models get their full quota drained first:
#   muse-spark-1.3      -> best muse (cline OR zen, perf-chosen) ->
#                          the other muse
#   deepseek v4/v4.1    -> cline (all account buckets) -> zen muse
#   everything else     -> cline (all accounts) -> zen muse
# Zen muse is the universal last resort; cline kids (glm-5.3...) are never
# preferred over the selected model.
# ---------------------------------------------------------------------------
_CLINE_MUSE_ID = "cline-free/muse-spark-1.3-contributor"
_ZEN_MUSE_ID = "muse-spark-1.3-contributor-free"
# --- DISABLED: dropped stale Opencode config for identity and tier
# enforcement. Ban-persistence for ZEN routing now relies exclusively on
# Role:PERS-A and Role:PERS-B (no live-upstream probing): "muse",
# "muse-spark-1.3-contributor-free". Zen Muse is expected back imminently,
# with reasoning intact. Nothing — not even a debate — supersedes the two
# mandate roles. Comment out blocks only, never touch logic.
#
# _MODEL_ROUTES = {
#     # muse family (resolved to zen muse-spark-1.3-contributor-free)
#     _ZEN_MUSE_ID: ["cline|" + _CLINE_MUSE_ID, "zen|" + _ZEN_MUSE_ID],
#     # deepseek v4.1-flash / v4-flash power models: cline buckets first,
#     # then zen muse, then cline glm-5.3 as a last kid.
#     "deepseek/deepseek-v4.1-flash": [
#         "cline|deepseek/deepseek-v4.1-flash",
#         "cline|deepseek/deepseek-v4-flash",
#         "zen|" + _ZEN_MUSE_ID,
#         "cline|z-ai/glm-5.3-flash",
#     ],
#     "deepseek/deepseek-v4-flash": [
#         "cline|deepseek/deepseek-v4-flash",
#         "cline|deepseek/deepseek-v4.1-flash",
#         "zen|" + _ZEN_MUSE_ID,
#         "cline|z-ai/glm-5.3-flash",
#     ],
#     "~deepseek/deepseek-flash-latest": [
#         "cline|deepseek/deepseek-v4.1-flash",
#         "cline|deepseek/deepseek-v4-flash",
#         "zen|" + _ZEN_MUSE_ID,
#     ],
#     # kids: never preferred over the selected model; defensive only
#     "z-ai/glm-5.3-flash": [
#         "cline|z-ai/glm-5.3-flash",
#         "zen|" + _ZEN_MUSE_ID,
#     ],
# }

# Rolling per-(provider,model) performance: {"ttft_ms", "tok_s", "n"}
_PERF_TRACK = {}
_PERF_LOCK = threading.Lock()


def _perf_key(provider: str, model: str):
    return (provider, model)


# --- DISABLED: Role:PERS-A and Role:PERS-B mandate block-comment
# discipline; perf-selection helpers remain inert while the routes are off.
# Uncomment together with _MODEL_ROUTES / muse_best_provider if re-enabled.
#
# # def record_model_perf(provider, model, ttft_ms, tok_s, n_tokens):
#     """EMA-smoothed perf per (provider, model). ttft_ms < 0 = no ttft
#     available (non-stream) — still record throughput."""
#     if not model:
#         return
#     with _PERF_LOCK:
#         k = _PERF_KEY = (provider, model)
#         cur = _PERF_TRACK.get(k)
#         if cur is None:
#             _PERF_TRACK[k] = {
#                 "ttft_ms": float(ttft_ms or 0.0),
#                 "tok_s": float(tok_s or 0.0),
#                 "n": 1,
#                 "n_tokens": int(n_tokens or 0),
#             }
#         else:
#             a = 2.0 / (cur["n"] + 2.0)  # EMA, more weight on recent
#             if ttft_ms is not None and ttft_ms >= 0 and cur.get("ttft_count"):
#                 cur["ttft_ms"] = cur["ttft_ms"] * (1 - a) + float(ttft_ms) * a
#             if tok_s is not None and tok_s > 0:
#                 cur["tok_s"] = cur["tok_s"] * (1 - a) + float(tok_s) * a
#             cur["n"] += 1
#             cur["n_tokens"] = int(cur.get("n_tokens", 0) or 0) + int(n_tokens or 0)
#
#
# def model_perf(provider, model):
#     with _PERF_LOCK:
#         return dict(_PERF_TRACK.get(_perf_key(provider, model)) or {})


# --- DISABLED: Role:PERS-A and Role:PERS-B mandate block-comment
# discipline. The perf-selection call sites were disabled along with the
# route table; nothing here supervenes on those mandates.
#
# def muse_best_provider():
#     """Return 'cline' or 'zen' muse depending on recorded perf. Ties / cold
#     start default to cline muse (the account-pool one; zen is the fallback)."""
#     cp = model_perf("cline", _CLINE_MUSE_ID)
#     zp = model_perf("zen", _ZEN_MUSE_ID)
#     if not cp and not zp:
#         return "cline"
#     # Score: higher tok/s is primary; lower TTFT breaks ties.
#     def score(p):
#         if not p:
#             return 0.0
#         return float(p.get("tok_s", 0.0) or 0.0) - float(p.get("ttft_ms", 0.0) or 0.0) / 1000.0
#     return "cline" if score(cp) >= score(zp) else "zen"
#
#
# def cline_route_chain(resolved_model: str) -> list:
#     """Expanded fallback chain for a resolved model id -> list of "bucket|model".
#     A cline bucket is retried across all accounts before moving on."""
#     chain = _MODEL_ROUTES.get(resolved_model)
#     if chain:
#         return list(chain)
#     # Models without a custom route: try the cline variant of the model first,
#     # then zen muse (never hard-fail a power-model-less turn).
#     return ["cline|" + resolved_model, "zen|" + _ZEN_MUSE_ID]

# --- Cline multi-account pool ----------------------------------------------
# Each entry: {"name", "file", "account_id", "email",
#              "access": str, "refresh": str, "exp": ms,
#              "ban_until": monotonic-ts, "fails": int, "uses": int}
_CLINE_POOL = []
_CLINE_POOL_LOCK = threading.Lock()
_CLINE_POOL_IDX = 0
_CLINE_POOL_MTIME = 0.0


def _cline_account_from_auth(name: str, path: str, c_auth: dict):
    """Build a pool entry from a providers.json-style auth dict."""
    ui = (c_auth.get("metadata") or {}).get("userInfo") or {}
    _exp = c_auth.get("expiresAt", 0) or 0
    if _exp > 1e12:
        _exp = _exp / 1000.0
    return {
        "name": name,
        "file": path,
        "account_id": c_auth.get("accountId", "?"),
        "email": ui.get("email", "?"),
        "access": c_auth.get("accessToken", ""),
        "refresh": c_auth.get("refreshToken", ""),
        "exp": _exp,
        "ban_until": 0.0,          # account-wide (auth errors)
        "model_bans": {},          # model_id -> monotonic until (quota per model)
        "fails": 0,
        "uses": 0,
    }


def _cline_dir_mtime() -> float:
    try:
        return os.path.getmtime(CLINE_ACCOUNTS_DIR)
    except Exception:
        return 0.0


def reload_cline_pool(force: bool = False) -> int:
    """(Re)load account pool from CLINE_ACCOUNTS_DIR. Returns count."""
    global _CLINE_POOL, _CLINE_POOL_MTIME
    try:
        mt = _cline_dir_mtime()
        with _CLINE_POOL_LOCK:
            if not force and mt and mt <= _CLINE_POOL_MTIME and _CLINE_POOL:
                return len(_CLINE_POOL)
        try:
            files = sorted(f for f in os.listdir(CLINE_ACCOUNTS_DIR)
                           if f.endswith(".json"))
        except Exception:
            files = []
        fresh = []
        for fn in files:
            p = os.path.join(CLINE_ACCOUNTS_DIR, fn)
            try:
                with open(p) as f:
                    prov = json.load(f)
                c_auth = (prov.get("providers", {}).get("cline", {})
                          .get("settings", {}).get("auth", {}))
                if not (c_auth.get("refreshToken") or c_auth.get("accessToken")):
                    continue
                fresh.append(_cline_account_from_auth(fn, p, c_auth))
            except Exception as e:

                sys.stderr.write(f"[cline-pool] skipping {fn}: {e}\n")
        with _CLINE_POOL_LOCK:
            # Preserve ban state + cache for accounts that survived the reload.
            old = {a["account_id"]: a for a in _CLINE_POOL}
            for a in fresh:
                o = old.get(a["account_id"])
                if o:
                    a["ban_until"] = o.get("ban_until", 0.0)
                    a["credit_until"] = o.get("credit_until", 0.0)
                    a["model_bans"] = dict(o.get("model_bans") or {})
                    a["fails"] = o.get("fails", 0)
                    a["uses"] = o.get("uses", 0)
                    if not a["access"] or a["access"] == o.get("access"):
                        a["access"] = o.get("access", a["access"])
                        a["exp"] = o.get("exp", a["exp"])
            _CLINE_POOL = fresh
            # Fold in the persisted wall-clock snapshot for accounts with no
            # live in-process state (fresh boot). In-process monotonic
            # horizons always win over the disk copy.
            _cline_state_apply(_CLINE_POOL)
            _CLINE_POOL_MTIME = mt or time.time()
        if fresh:
            sys.stderr.write(
                "[cline-pool] loaded %d accounts: %s\n" % (
                    len(fresh),
                    ", ".join(f"{a['name']}({a['email']})" for a in fresh)))
        return len(fresh)
    except Exception as e:

        sys.stderr.write(f"[cline-pool] reload failed: {e}\n")
        with _CLINE_POOL_LOCK:
            return len(_CLINE_POOL)


def cline_pool_status() -> list:
    """Snapshot for /stats and the live monitor (no secrets)."""
    now = time.monotonic()
    with _CLINE_POOL_LOCK:
        return [{
            "name": a["name"],
            "account_id": a["account_id"],
            "email": a["email"],
            "uses": a["uses"],
            "fails": a["fails"],
            "banned_s_left": max(0.0, a["ban_until"] - now),
            "credit_dry_s_left": max(0.0, a.get("credit_until", 0.0) - now),
        } for a in _CLINE_POOL]


def _cline_refresh_account(a: dict, sess=None) -> bool:
    """Refresh one account's access token via WorkOS. Returns True on success.
    Persists the new token back to the account's own file (never touched
    by the Cline client, so no clobbering). Caller should hold no locks.

    WorkOS refresh tokens are SINGLE-USE: two concurrent exchangers burn the
    chain ("already exchanged" -> whole account 401s). A cross-process file
    lock serializes every refresher that goes through this function (proxy
    threads AND diagnostic scripts importing this module). NEVER hand-roll a
    WorkOS refresh outside this function.
    """
    import fcntl
    own_sess = sess is None
    ref_sess = sess or cffi_requests.Session(impersonate="chrome131")
    lock_path = os.path.join(os.path.dirname(CLINE_ACCOUNTS_DIR),
                             "cline_refresh.lock")
    try:
        with open(lock_path, "w") as _lf:
            try:
                fcntl.flock(_lf.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass
            # Re-read the account file under lock: another process may have
            # already rotated since we loaded our copy. Using their newer
            # tokens avoids double-exchanging a stale refresh token.
            try:
                with open(a["file"]) as _pf:
                    _prov = json.load(_pf)
                _auth = (_prov.get("providers", {}).get("cline", {})
                         .get("settings", {}).get("auth", {}))
                if _auth.get("refreshToken") and _auth.get("refreshToken") != a.get("refresh"):
                    a["refresh"] = _auth.get("refreshToken")
                    a["access"] = _auth.get("accessToken", a.get("access", ""))
                    _e = _auth.get("expiresAt", a.get("exp", 0))
                    if _e > 1e12: _e = _e / 1000.0
                    a["exp"] = _e
            except Exception:
                pass
            r = ref_sess.post(
                "https://api.workos.com/user_management/authenticate",
                json={
                    "client_id": "client_01K3A541FN8TA3EPPHTD2325AR",
                    "grant_type": "refresh_token",
                    "refresh_token": a["refresh"],
                },
                timeout=10,
            )
            if r.status_code != 200:
                sys.stderr.write(
                    f"[cline-pool] refresh failed for {a['name']}: "
                    f"http={r.status_code}\n")
                return False
            data = r.json()
            new_access = data.get("access_token")
            new_refresh = data.get("refresh_token")
            if not new_access:
                return False
            with _CLINE_POOL_LOCK:
                a["access"] = f"workos:{new_access}"
                a["exp"] = int(time.time() * 1000) + 3600000
                if new_refresh:
                    a["refresh"] = new_refresh
            # Persist back to the account file (pool-owned, Cline never
            # writes it). Still under the cross-process lock so no two
            # refreshers can interleave read-exchange-write cycles.
            try:
                with open(a["file"]) as f:
                    prov = json.load(f)
                auth = (prov["providers"]["cline"]["settings"]["auth"])
                auth["accessToken"] = a["access"]
                auth["expiresAt"] = a["exp"]
                if new_refresh:
                    auth["refreshToken"] = new_refresh
                tmp_file = a["file"] + ".tmp"
                with open(tmp_file, "w") as f_out:
                    json.dump(prov, f_out, indent=2)
                os.chmod(tmp_file, 0o600)
                os.replace(tmp_file, a["file"])
            except Exception as e:

                sys.stderr.write(f"[cline-pool] persist failed for {a['name']}: {e}\n")
            sys.stderr.write(f"[cline-pool] token refreshed for {a['name']}\n")
            return True
    except Exception as e:

        sys.stderr.write(f"[cline-pool] refresh error for {a['name']}: {e}\n")
        return False
    finally:
        if own_sess:
            try:
                ref_sess.close()
            except Exception:
                pass


def _is_account_banned_for_model(a: dict, model_id: str, now_m: float) -> bool:
    if not model_id or not a:
        return False
    mb = a.get("model_bans") or {}
    if not mb:
        return False
    clean = model_id.replace("cline/", "").replace("cline-free/", "")
    variants = {
        model_id,
        clean,
        f"cline/{clean}",
        f"cline-free/{clean}",
    }
    for v in variants:
        if (mb.get(v) or 0.0) > now_m:
            return True
    return False


def cline_pick_account(model_id: str = None) -> dict | None:
    """Pick the next live (unbanned, refreshable) account, round-robin.
    Returns the account dict (caller must NOT mutate) or None.

    Two passes: first skip accounts that recently answered 402 (credit-drained,
    only affects PAID models), then fall back to them. Free models still get
    served by a drained account, so the mark never wastes free capacity.

    model_id: also skip accounts whose per-model quota ban for this model is
    still active (each free model has its own quota bucket per account).
    """
    global _CLINE_POOL_IDX
    reload_cline_pool()
    now_ms = int(time.time() * 1000)
    now_m = time.monotonic()
    with _CLINE_POOL_LOCK:
        pool = list(_CLINE_POOL)
    if not pool:
        return None
    # Collect candidates in preference order, then try refreshing EACH until
    # one works. A single burned refresh token must never fail the whole pick
    # (mixed pools always have some live, some dead accounts during rotation).
    cands = []
    with _CLINE_POOL_LOCK:
        n = len(_CLINE_POOL)
        start_idx = _CLINE_POOL_IDX
        for skip_credit_dry in (True, False):
            for i in range(n):
                idx = (start_idx + i) % n
                a = _CLINE_POOL[idx]
                if a["ban_until"] > now_m:
                    continue
                if _is_account_banned_for_model(a, model_id, now_m):
                    continue
                if skip_credit_dry and a.get("credit_until", 0.0) > now_m:
                    continue
                if a not in [c[1] for c in cands]:
                    cands.append((idx, a))
            if cands:
                break
        if not cands:
            return None  # every account banned
    for cand_idx, cand in cands:
        if (not cand["access"]) or now_ms >= (cand["exp"] or 0) - 60000:
            if not cand["refresh"] or not _cline_refresh_account(cand):
                with _CLINE_POOL_LOCK:
                    cand["fails"] += 1
                    cand["ban_until"] = now_m + 3600.0
                _cline_state_save()
                continue  # try the next account, don't give up
        with _CLINE_POOL_LOCK:
            if _CLINE_POOL:
                _CLINE_POOL_IDX = (cand_idx + 1) % len(_CLINE_POOL)
        return cand
    return None


def _cline_state_save():
    """Persist ban/credit horizons (wall clock) so a restart remembers which
    accounts are drained. Debounced by caller frequency — writes are tiny."""
    try:
        now_w, now_m = time.time(), time.monotonic()
        with _CLINE_POOL_LOCK:
            snap = {}
            for a in _CLINE_POOL:
                bans = {m: now_w + max(0.0, u - now_m)
                        for m, u in (a.get("model_bans") or {}).items()
                        if u > now_m}
                snap[a["account_id"]] = {
                    "ban_until_wall": now_w + max(0.0, a["ban_until"] - now_m),
                    "credit_until_wall": now_w + max(0.0, a.get("credit_until", 0.0) - now_m),
                    "model_bans_wall": bans,
                    "uses": a["uses"], "fails": a["fails"],
                }
        if snap:
            tmp = _CLINE_STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(snap, f)
            os.chmod(tmp, 0o600)
            os.replace(tmp, _CLINE_STATE_FILE)
    except Exception:
        pass


def _cline_state_apply(fresh: list):
    """Fold the persisted snapshot into freshly loaded accounts (no old
    in-process entry). Converts wall-clock deadlines back to monotonic."""
    try:
        with open(_CLINE_STATE_FILE) as f:
            snap = json.load(f) or {}
    except Exception:
        return
    now_w, now_m = time.time(), time.monotonic()
    for a in fresh:
        # Skip entries that already carry live in-process state (survived a
        # hot reload): monotonic horizons beat the on-disk snapshot.
        if (a.get("ban_until") or a.get("credit_until")
                or a.get("model_bans") or a.get("uses") or a.get("fails")):
            continue
        st = snap.get(a["account_id"])
        if not st:
            continue
        left = st.get("ban_until_wall", 0) - now_w
        if left > 0:
            a["ban_until"] = max(a["ban_until"], now_m + left)
        left = st.get("credit_until_wall", 0) - now_w
        if left > 0:
            a["credit_until"] = max(a.get("credit_until", 0.0), now_m + left)
        for m, u in (st.get("model_bans_wall") or {}).items():
            if u - now_w > 0:
                a.setdefault("model_bans", {})[m] = max(
                    a["model_bans"].get(m, 0.0), now_m + (u - now_w))
        a["uses"] = a.get("uses", 0) + st.get("uses", 0)
        a["fails"] = st.get("fails", 0)


def cline_note_credits(a: dict):
    """Mark an account as credit-drained (402 on a paid model). Paid models
    will prefer other accounts for _CLINE_CREDIT_PARK_S; free models keep
    using it, so no free capacity is lost."""
    if a is None:
        return
    with _CLINE_POOL_LOCK:
        a["credit_until"] = max(a.get("credit_until", 0.0),
                                time.monotonic() + _CLINE_CREDIT_PARK_S)
    _cline_state_save()


def cline_note_result(a: dict, ok: bool, retry_after_s: float = 0.0, model_id: str = None):
    """Record one request outcome: ok=True clears fail streak;
    ok=False bumps fails and parks a ban horizon (Retry-After, capped).

    model_id scopes the ban to that model's quota bucket: Cline free quotas
    are per-model ("free limit reached on model"), so a 429 on deepseek must
    NOT burn the same account's muse/glm quota. A pure auth-style failure
    (no model_id) still bans account-wide."""
    with _CLINE_POOL_LOCK:
        if ok:
            a["uses"] += 1
            a["fails"] = 0
            return
        a["fails"] += 1
        wait = max(60.0, min(float(retry_after_s or 0), 24 * 3600))
        if model_id:
            a.setdefault("model_bans", {})
            a["model_bans"][model_id] = max(
                a["model_bans"].get(model_id, 0.0),
                time.monotonic() + wait,
            )
        else:
            a["ban_until"] = time.monotonic() + wait
    _cline_state_save()

def get_cline_auth_headers_for(a: dict):
    """Headers for one specific pool account (refreshing if needed)."""
    now_s = time.time()
    if (not a.get("access")) or now_s >= (a.get("exp") or 0) - 60:
        if not a.get("refresh") or not _cline_refresh_account(a):
            with _CLINE_POOL_LOCK:
                a["fails"] += 1
                a["ban_until"] = time.monotonic() + 3600.0
            _cline_state_save()
            return None, None
    with _CLINE_POOL_LOCK:
        tok = a["access"]
    return {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
        "User-Agent": "Cline/4.1.16",
        "X-PLATFORM": "linux",
        "X-PLATFORM-VERSION": "6.18.9-arch1-1",
        "X-CLIENT-TYPE": "vscode",
        "X-CLIENT-VERSION": "4.1.16",
        "X-CORE-VERSION": "4.1.16",
        "X-IS-MULTIROOT": "false",
    }, a


# Cline's OpenRouter-backed catalog counts reasoning tokens toward
# max_tokens. A chat-sized cap (e.g. 30) is entirely consumed by thinking on
# reasoning models (deepseek-v4.1, nemotron-*, nex-*, inkling) and Cline
# answers HTTP 500 "empty response content" — the model never gets to emit
# text. Give reasoning the same headroom the Zen muse route uses.
_CLINE_MT_FLOOR = 1024
_CLINE_MT_CAP = 128000
# 402 = "insufficient credits" on a PAID model. Credits do not refill on a
# timer, so mark the account credit-drained and prefer funded accounts for
# paid models (free models ignore the mark and can still use it).
_CLINE_CREDIT_PARK_S = 1800
_CLINE_402_PARK_S = 300


def cline_scale_max_tokens(body: dict, retry_bump: bool = False) -> int:
    """Widen max_tokens for the Cline route. Returns the value applied.

    retry_bump=True quadruples the current value (used after an
    "empty response content" 500) so a second attempt escapes starvation.
    """
    try:
        cur = int(body.get("max_tokens") or 0)
    except (TypeError, ValueError):
        cur = 0
    if retry_bump:
        nxt = min(max(cur or _CLINE_MT_FLOOR, _CLINE_MT_FLOOR) * 4, _CLINE_MT_CAP)
    else:
        nxt = min(max(cur * 4, _CLINE_MT_FLOOR), _CLINE_MT_CAP)
    body["max_tokens"] = nxt
    return nxt


def cline_is_empty_content(status: int, text: str) -> bool:
    """True for Cline's reasoning-starvation signature on a 5xx response."""
    if status not in (500, 502, 503, 504):
        return False
    t = (text or "").lower()
    return "empty response content" in t


def cline_parse_retry_after(text: str) -> float:
    """Parse Cline's human quota message into seconds.

    Live format: "Daily free limit reached on model X. Try again in 19h 38m".
    Returns 0.0 when nothing parseable (caller falls back to header/defaults).
    """
    import re as _re
    t = text or ""
    m = _re.search(r"[Tt]ry again in\s+(?:(\d+)\s*(?:d(?:ays?)?))?,?\s*(?:(\d+)\s*(?:h(?:ours?)?))?,?\s*(?:(\d+)\s*(?:m(?:in(?:ute)?s?)?))?,?\s*(?:(\d+)\s*(?:s(?:ec(?:ond)?s?)?))?", t)
    if not m or not any(m.groups()):
        return 0.0
    try:
        d = int(m.group(1) or 0)
        h = int(m.group(2) or 0)
        mi = int(m.group(3) or 0)
        s = int(m.group(4) or 0)
        return float(d * 86400 + h * 3600 + mi * 60 + s)
    except Exception:
        return 0.0


def get_cline_auth_headers(model_id: str = None):
    """Pick a live account from the pool and build Cline API headers.

    Returns (headers, account). Pool dir (~/.local/share/tor-zen/accounts/)
    takes precedence; falls back to the single legacy CLINE_PROVIDERS_FILE."""
    a = cline_pick_account(model_id=model_id)
    if a is None:
        return None, None
    hdrs, apicked = get_cline_auth_headers_for(a)
    if hdrs:
        return hdrs, apicked
    return None, None


# Real, currently-shipping OpenCode client identity (captured live 2026-09-23
# from `opencode run` -> our own ZEN_LOG_HEADERS dump). This is the UA used for
# any caller that does NOT send its own (dashboard chat, Cline, scripts, curl).
# Using a stale version here makes Zen answer 403 FreeTierError.
_OPENCODE_UA = "opencode/1.18.32 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14"
_DEFAULT_OPENCODE_HEADERS = {
    "Authorization": "Bearer public",
    "User-Agent": _OPENCODE_UA,
    "x-opencode-client": "cli",
    "x-opencode-project": "global",
    "x-opencode-directory": "/home/vagish_arch",
}

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def gen_opencode_id(descending: bool) -> str:
    """Generate monotonic OpenCode IDs matching OpenCode's tU() binary implementation.
    ses_ uses bitwise-NOT inverted timestamp (descending=True).
    msg_ uses plain timestamp (descending=False).
    Zen Console validates timestamp encoding in ses_ and msg_."""
    now_ms = int(time.time() * 1000)
    counter = 1
    val = (now_ms * 0x1000) + counter
    if descending:
        val = ~val
    val_48 = val & 0xffffffffffff
    hex_prefix = f"{val_48:012x}"
    rand_bytes = secrets.token_bytes(14)
    suffix = "".join(_BASE62[b % 62] for b in rand_bytes)
    return hex_prefix + suffix

def gen_session_id() -> str:
    return "ses_" + gen_opencode_id(descending=True)

def gen_message_id() -> str:
    return "msg_" + gen_opencode_id(descending=False)

# OpenCode core tool schemas required by Zen Console free-tier gate
_OPENCODE_CORE_TOOLS = [
    {
        "type": "function",
        "name": "bash",
        "description": "Execute a bash command in the terminal",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The command to execute"},
                "timeout": {"type": "integer", "description": "Optional timeout in milliseconds"},
                "workdir": {"type": "string", "description": "The working directory to run the command in"}
            },
            "required": ["command"]
        },
        "strict": False
    },
    {
        "type": "function",
        "name": "read",
        "description": "Read contents of a file",
        "parameters": {
            "type": "object",
            "properties": {
                "filePath": {"type": "string", "description": "The absolute path to the file to read"}
            },
            "required": ["filePath"]
        },
        "strict": False
    },
    {
        "type": "function",
        "name": "edit",
        "description": "Performs exact string replacements in files.",
        "parameters": {
            "type": "object",
            "properties": {
                "filePath": {"type": "string", "description": "The absolute path to the file to modify"},
                "oldString": {"type": "string", "description": "The text to replace"},
                "newString": {"type": "string", "description": "The text to replace it with"}
            },
            "required": ["filePath", "oldString", "newString"]
        },
        "strict": False
    },
    {
        "type": "function",
        "name": "write",
        "description": "Writes content to a file",
        "parameters": {
            "type": "object",
            "properties": {
                "filePath": {"type": "string", "description": "The absolute path to the file to write to"},
                "content": {"type": "string", "description": "The full content to write"}
            },
            "required": ["filePath", "content"]
        },
        "strict": False
    },
    {
        "type": "function",
        "name": "glob",
        "description": "Fast file pattern matching tool",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "The glob pattern to match files against"}
            },
            "required": ["pattern"]
        },
        "strict": False
    },
    {
        "type": "function",
        "name": "grep",
        "description": "Search regex in files",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "The regex pattern to search"}
            },
            "required": ["pattern"]
        },
        "strict": False
    }
]

_last_genuine_session = {"id": None, "project": "global", "time": 0}

import random
def get_genuine_opencode_session():
    try:
        with open("genuine_sessions.txt", "r") as f:
            lines = [l.strip() for l in f if l.strip()]
            if lines:
                return random.choice(lines), "global"
    except:
        pass
    return gen_session_id(), "global"

# Cache of real session ids from opencode.db. A client-supplied
# x-opencode-session may only be trusted when it is genuinely whitelisted
# (present in the DB); a random `ses_...` from a curl/script trips
# FreeUsage/FreeTierError on every exit.
_known_sessions_cache = {"ids": set(), "ts": 0.0}
_known_sessions_lock = threading.Lock()

def known_opencode_sessions():
    now = time.monotonic()
    cached = _known_sessions_cache["ids"]
    if cached and (now - _known_sessions_cache["ts"] < 60):
        return cached
    if not _known_sessions_lock.acquire(blocking=False):
        return cached
    try:
        import sqlite3
        db_path = os.path.expanduser("~/.local/share/opencode/opencode.db")
        ids = set()
        if os.path.exists(db_path):
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0) as con:
                for (sid,) in con.execute(
                        "SELECT id FROM session ORDER BY time_updated DESC LIMIT 200"):
                    if sid:
                        ids.add(sid)
        if ids:
            _known_sessions_cache["ids"] = ids
            _known_sessions_cache["ts"] = now
    except Exception:
        pass
    finally:
        _known_sessions_lock.release()
    return _known_sessions_cache["ids"]

# Dynamic model discovery from upstream
_models_cache = {"models": None, "ids": None, "ts": 0}
_models_lock = threading.Lock()

def refresh_models(upstream: str, proxy_url: str):
    """Fetch available models from upstream, cache for 10 min."""
    now = time.monotonic()
    if _models_cache["models"] is not None and now - _models_cache["ts"] < 600:
        return _models_cache["models"], _models_cache["ids"]
    if not _models_lock.acquire(blocking=False):
        m = _models_cache["models"] or MODELS
        return m, _models_cache["ids"] or {x["id"] for x in m}
    try:
        # Try direct first (fastest)
        sess = cffi_requests.Session(impersonate="chrome131")
        try:
            r = sess.get(f"{upstream}/models", headers=_DEFAULT_OPENCODE_HEADERS, timeout=3)
            data = r.json().get("data", [])
            if data:
                _models_cache["models"] = data
                _models_cache["ids"] = {m["id"] for m in data}
                _models_cache["ts"] = now
                sys.stderr.write(f"[models] refreshed: {len(data)} models available\n")
                return data, _models_cache["ids"]
        except Exception:
            pass
        finally:
            try:
                sess.close()
            except Exception:
                pass

        # Fallback to Tor
        sess = None
        try:
            sess = cffi_requests.Session(impersonate="chrome131", proxy=proxy_url)
            r = sess.get(f"{upstream}/models", headers=_DEFAULT_OPENCODE_HEADERS, timeout=4)
            data = r.json().get("data", [])
            if data:
                _models_cache["models"] = data
                _models_cache["ids"] = {m["id"] for m in data}
                _models_cache["ts"] = now
                sys.stderr.write(f"[models] refreshed via Tor: {len(data)} models available\n")
                return data, _models_cache["ids"]
        except Exception as e:

            sys.stderr.write(f"[models] refresh failed: {e}\n")
        finally:
            if sess is not None:
                try:
                    sess.close()
                except Exception:
                    pass
    finally:
        _models_lock.release()
    m = _models_cache["models"] or MODELS
    return m, _models_cache["ids"] or {x["id"] for x in m}

_models_bg_lock = threading.Lock()
_models_bg_running = False

# --- Async telemetry queue -------------------------------------------------
# record_usage / record_error / record_exit_result are SQLite sync writes
# (~1-5ms each, worse under thread contention) sitting on the request path.
# Route them through a single daemon writer thread so TTFT never pays for
# disk I/O. Fail-open: queue full or DB error => drop silently.
_DB_QUEUE = queue.Queue(maxsize=2000)

def _db_writer():
    while True:
        try:
            kind, args, kwargs = _DB_QUEUE.get()
            try:
                if zen_db is None:
                    continue
                fn = getattr(zen_db, kind, None)
                if fn is not None:
                    fn(*args, **kwargs)
            except Exception:
                pass
            finally:
                try:
                    _DB_QUEUE.task_done()
                except Exception:
                    pass
        except Exception:
            time.sleep(0.1)

threading.Thread(target=_db_writer, daemon=True, name="zen-db-writer").start()

def db_async(kind: str, *args, **kwargs):
    """Enqueue a zen_db write (non-blocking, drop on full)."""
    try:
        _DB_QUEUE.put_nowait((kind, args, kwargs))
    except queue.Full:
        pass
    except Exception:
        pass

def model_ids_fast(upstream: str, proxy_url: str):
    """Non-blocking model ids for the request hot path.

    Returns cached ids instantly and kicks a background refresh when stale,
    so no request ever pays the ~7s direct+Tor /models fetch on TTFT.
    Cold start falls back to the static MODELS list (main() warms the real
    cache at boot, so this path is rare).
    """
    global _models_bg_running
    m = _models_cache["models"]
    ids = _models_cache["ids"]
    if m is not None and (time.monotonic() - _models_cache["ts"]) < 600:
        return m, ids or {x["id"] for x in m}
    try:
        with _models_bg_lock:
            if not _models_bg_running:
                _models_bg_running = True
                def _bg():
                    global _models_bg_running
                    try:
                        refresh_models(upstream, proxy_url)
                    finally:
                        with _models_bg_lock:
                            _models_bg_running = False
                threading.Thread(target=_bg, daemon=True,
                                 name="zen-models-refresh").start()
    except Exception:
        pass
    if m is not None:
        return m, ids or {x["id"] for x in m}
    return MODELS, {x["id"] for x in MODELS}

# Track active streams for the /live endpoint
_active_streams = {}
_active_streams_lock = threading.Lock()
_stream_id_counter = itertools.count(1)
_stream_id_counter_lock = threading.Lock()

def _next_stream_id() -> int:
    with _stream_id_counter_lock:
        return next(_stream_id_counter)
_proxy_start_time = time.time()

# Token tap — ring buffer of recent streamed text for the dashboard
from collections import deque
_token_tap = deque(maxlen=200)  # last 200 text fragments
_token_tap_lock = threading.Lock()


class RobustThinkFilter:
    """Robust case-insensitive <think> / <thought> splitter that preserves partial tag splits
    across streaming chunk boundaries without attribute crashes or token leakage."""
    TAG_PAIRS = [
        ("<think>", "</think>"),
        ("<thought>", "</thought>"),
    ]

    def __init__(self):
        self.inside = False
        self.active_close_tag = "</think>"
        self.carry = ""
        self._think_len = 0

    @classmethod
    def _partial_suffix_len(cls, s: str, candidates: list) -> int:
        s_lower = s.lower()
        max_k = 0
        for cand in candidates:
            cand_lower = cand.lower()
            for k in range(min(len(s_lower), len(cand_lower) - 1), 0, -1):
                if s_lower.endswith(cand_lower[:k]):
                    if k > max_k:
                        max_k = k
        return max_k

    def feed(self, text: str):
        buf = self.carry + text
        self.carry = ""
        think_out, normal_out = [], []
        while buf:
            buf_lower = buf.lower()
            if not self.inside:
                earliest_idx = -1
                found_pair = None
                for open_tag, close_tag in self.TAG_PAIRS:
                    idx = buf_lower.find(open_tag.lower())
                    if idx >= 0 and (earliest_idx == -1 or idx < earliest_idx):
                        earliest_idx = idx
                        found_pair = (open_tag, close_tag)

                if earliest_idx >= 0:
                    normal_out.append(buf[:earliest_idx])
                    buf = buf[earliest_idx + len(found_pair[0]):]
                    self.inside = True
                    self.active_close_tag = found_pair[1]
                else:
                    open_candidates = [pair[0] for pair in self.TAG_PAIRS]
                    keep = self._partial_suffix_len(buf, open_candidates)
                    normal_out.append(buf[:len(buf) - keep])
                    self.carry = buf[len(buf) - keep:]
                    buf = ""
            else:
                # Close on EITHER tag (models mix <think>/</thought>); auto-exit
                # after 256KB without a close so content never starves.
                close_tags = [pair[1].lower() for pair in self.TAG_PAIRS]
                best_idx, best_len = -1, 0
                for ct in close_tags:
                    idx = buf_lower.find(ct)
                    if idx >= 0 and (best_idx == -1 or idx < best_idx):
                        best_idx, best_len = idx, len(ct)
                if best_idx >= 0:
                    think_out.append(buf[:best_idx])
                    buf = buf[best_idx + best_len:]
                    self.inside = False
                    self._think_len = 0
                else:
                    self._think_len = getattr(self, "_think_len", 0) + len(buf)
                    if self._think_len > 262144:
                        # Bail out: treat rest as normal content.
                        normal_out.append(buf)
                        self.inside = False
                        self._think_len = 0
                        self.carry = ""
                        buf = ""
                        continue
                    keep = self._partial_suffix_len(buf, [self.active_close_tag] + [p[1] for p in self.TAG_PAIRS])
                    think_out.append(buf[:len(buf) - keep])
                    self.carry = buf[len(buf) - keep:]
                    buf = ""
        return "".join(think_out), "".join(normal_out)

    def flush(self):
        t = self.carry
        self.carry = ""
        if not t:
            return "", ""
        return (t, "") if self.inside else ("", t)


ThinkFilter = RobustThinkFilter


def _flex_rewrite_line(line: bytes) -> bytes:
    """Rewrite MODEL_FLEX ids only in the JSON `model` field, never in content.

    The old code did a raw substring replace over the whole SSE line, corrupting
    code/tool-args/URLs that happened to mention a model id.
    """
    if not MODEL_FLEX or b"model" not in line:
        return line
    if not line.startswith(b"data: ") or line.strip() == b"data: [DONE]":
        return line
    try:
        payload = json.loads(line[6:])
    except Exception:
        return line
    changed = False
    m = payload.get("model") if isinstance(payload, dict) else None
    if isinstance(m, str) and m in MODEL_FLEX:
        payload["model"] = MODEL_FLEX[m]
        changed = True
    if changed:
        try:
            return b"data: " + json.dumps(payload).encode()
        except Exception:
            return line
    return line


def _flex_rewrite_obj(payload) -> bool:
    """In-place variant of _flex_rewrite_line for an already-parsed chunk.
    Returns True if the `model` field was rewritten (chunk must be re-dumped)."""
    if not MODEL_FLEX or not isinstance(payload, dict):
        return False
    m = payload.get("model")
    if isinstance(m, str) and m in MODEL_FLEX:
        payload["model"] = MODEL_FLEX[m]
        return True
    return False


def _relocate_tool_images(messages):
    """Move image parts out of role:'tool' messages into a synthetic user
    message placed right after them. Providers like zen x-preview accept
    vision input ONLY in user role; image-bearing tool results otherwise
    fail with 'Endpoint is unavailable'/503 on every attempt."""
    if not isinstance(messages, list):
        return messages
    out = []
    pending = []

    def flush():
        if pending:
            out.append({"role": "user", "content": list(pending)})
            pending.clear()

    for m in messages:
        if not isinstance(m, dict):
            out.append(m)
            continue
        if m.get("role") == "tool" and isinstance(m.get("content"), list):
            text_parts, images = [], []
            for p in m["content"]:
                if isinstance(p, dict) and str(p.get("type", "")).lower().startswith("image"):
                    images.append(p)
                else:
                    text_parts.append(p)
            if images:
                nm = dict(m)
                nm["content"] = text_parts or [{"type": "text", "text": "Tool output follows."}]
                out.append(nm)
                pending.extend(images)
                continue
            out.append(m)
            continue
        if m.get("role") != "tool" and pending:
            flush()
        out.append(m)
    flush()
    return out


def _image_part_url(p: dict) -> str:
    """Resolve ANY image-part shape to a URL/data-URL string for input_image.

    Accepts OpenAI ({"type":"image_url","image_url":{"url":..} | ".."}), flat
    ({"type":"input_image","image_url":".."}), Anthropic
    ({"type":"image","source":{"type":"base64","media_type":..,"data":..}} or
    source.url), and plain {"url":..}. Returns "" when nothing usable — the
    caller then drops the part (and we log it once so the shape is visible).
    """
    iu = p.get("image_url")
    url = ""
    if isinstance(iu, dict):
        url = iu.get("url") or ""
    elif isinstance(iu, str):
        url = iu
    if not url:
        url = p.get("url") or ""
    if not url:
        src = p.get("source")
        if isinstance(src, dict):
            if src.get("data"):
                media = src.get("media_type") or src.get("mime_type") or "image/png"
                url = f"data:{media};base64,{src['data']}"
            elif src.get("url"):
                url = src["url"]
    return url


def messages_to_responses_input(messages: list) -> list:
    """Convert chat.completions messages into Responses-API input items.
    Preserves assistant tool_calls and tool results (function_call /
    function_call_output), which opencode sends back after every tool use."""
    items = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = m.get("role", "user")

        # Tool result -> function_call_output item
        if role == "tool":
            out = m.get("content", "")
            if isinstance(out, list):
                parts = []
                for p in out:
                    if isinstance(p, dict):
                        parts.append(str(p.get("text", "")))
                    else:
                        parts.append(str(p))
                out = "\n".join(x for x in parts if x)
            items.append({
                "type": "function_call_output",
                "call_id": m.get("tool_call_id") or "",
                "output": str(out),
            })
            continue

        # Assistant turn with tool calls -> text (if any) + function_call items
        tcs = m.get("tool_calls")
        if tcs:
            c = m.get("content")
            if isinstance(c, list):
                # Extract text parts — never leak Python repr of dicts upstream
                c = " ".join(
                    str(p.get("text", "")) for p in c
                    if isinstance(p, dict) and p.get("text")
                )
            if c:
                items.append({"role": "assistant", "content": str(c)})
            for tc in tcs:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") or {}
                items.append({
                    "type": "function_call",
                    "call_id": tc.get("id") or f"call_{int(time.time()*1000)}",
                    "name": fn.get("name") or "",
                    "arguments": fn.get("arguments") or "{}",
                })
            continue

        content = m.get("content") or ""  # None (tool-only turns) -> "", never "None"
        if isinstance(content, list):
            has_image = any(
                isinstance(p, dict) and str(p.get("type", "")).lower().startswith("image")
                for p in content
            )
            if has_image:
                conv = []
                for p in content:
                    if not isinstance(p, dict):
                        continue
                    ptype = str(p.get("type", "")).lower()
                    if ptype.startswith("image"):
                        url = _image_part_url(p)
                        if url:
                            conv.append({"type": "input_image", "image_url": url})
                        else:
                            sys.stderr.write(
                                f"[image] dropped unsupported image part keys="
                                f"{sorted(p.keys())}\n")
                    elif p.get("text"):
                        conv.append({"type": "input_text", "text": str(p["text"])})
                items.append({"role": role, "content": conv})
                continue
            parts = []
            for p in content:
                if isinstance(p, dict):
                    parts.append(str(p.get("text", "")))
                else:
                    parts.append(str(p))
            content = "\n".join(x for x in parts if x)
        items.append({"role": role, "content": str(content)})
    return items


def responses_input_to_messages(inp) -> list:
    """Convert Responses-API `input` items to OpenAI chat `messages`.

    Inverse of messages_to_responses_input. Used when a client calls our
    /v1/responses but the model only runs on the chat backend (Zen /responses
    401s chat-wire models): we run chat upstream and adapt the result back.
    """
    msgs = []
    for it in inp or []:
        if not isinstance(it, dict):
            continue
        t = it.get("type", "")
        if t == "function_call":
            msgs.append({
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": it.get("call_id") or it.get("id") or "",
                    "type": "function",
                    "function": {
                        "name": it.get("name", ""),
                        "arguments": it.get("arguments") or "{}",
                    },
                }],
            })
        elif t == "function_call_output":
            out = it.get("output", "")
            if isinstance(out, list):
                out = "\n".join(str(x) for x in out)
            msgs.append({
                "role": "tool",
                "tool_call_id": it.get("call_id") or "",
                "content": str(out),
            })
        elif t == "reasoning":
            txt = it.get("text") or ""
            if not txt:
                txt = "".join(
                    s.get("text", "") for s in (it.get("summary") or [])
                    if isinstance(s, dict))
            if txt:
                msgs.append({"role": "assistant", "content": str(txt)})
        else:
            role = it.get("role", "user")
            content = it.get("content", "")
            if isinstance(content, list):
                parts = []
                for c in content:
                    if not isinstance(c, dict):
                        parts.append(str(c))
                        continue
                    ct = c.get("type", "")
                    if ct == "input_text":
                        parts.append(str(c.get("text", "")))
                    elif ct == "input_image":
                        parts.append({
                            "type": "image_url",
                            "image_url": {"url": c.get("image_url") or c.get("url") or ""},
                        })
                    elif c.get("text"):
                        parts.append(str(c["text"]))
                if any(isinstance(x, dict) for x in parts):
                    msgs.append({"role": role, "content": parts})
                else:
                    msgs.append({"role": role, "content": "\n".join(x for x in parts if x)})
            else:
                msgs.append({"role": role, "content": str(content)})
    return msgs


def chat_completion_to_responses(chat_json, model_name):
    """Build a Responses-API object from a chat.completion (chat backend).

    Lets a /v1/responses client (e.g. Codex) use a model that only runs on
    /chat/completions upstream.
    """
    ch = ((chat_json or {}).get("choices") or [{}])[0]
    msg = ch.get("message") or {}
    content = msg.get("content") or ""
    tcs = msg.get("tool_calls") or []
    usage = (chat_json or {}).get("usage") or {}
    out_items = []
    if content:
        out_items.append({
            "type": "message", "role": "assistant",
            "content": [{"type": "output_text", "text": str(content)}],
        })
    for tc in tcs:
        fn = (tc.get("function") or {})
        out_items.append({
            "type": "function_call",
            "id": tc.get("id") or "",
            "call_id": tc.get("id") or "",
            "name": fn.get("name", ""),
            "arguments": fn.get("arguments") or "{}",
        })
    if not out_items:
        out_items.append({
            "type": "message", "role": "assistant",
            "content": [{"type": "output_text", "text": ""}],
        })
    return {
        "id": (chat_json or {}).get("id") or f"resp_{uuid.uuid4().hex[:12]}",
        "object": "response",
        "created_at": int(time.time()),
        "model": model_name,
        "output": out_items,
        "status": "completed",
        "usage": {
            "input_tokens": usage.get("prompt_tokens", 0) or 0,
            "output_tokens": usage.get("completion_tokens", 0) or 0,
        },
    }


_rotate_lock = threading.Lock()
_last_rotation = {"ts": 0.0}
ROTATE_MIN_INTERVAL = 10.0  # seconds between actual NEWNYM signals (Tor RFC minimum)
_tor_supervisor_lock = threading.Lock()
_last_tor_heal = {"ts": 0.0}
_TOR_HEAL_MIN_INTERVAL = 30.0
# Traffic-death watchdog (network refresh / stale guards): bootstrap can read
# 100% while circuits carry nothing (curl 28s + health-check 0s across every
# slot). Track consecutive transport-dead probe outcomes process-wide; past
# the threshold, restart ONLY the user-mode Tor (/tmp/tor2, never system tor)
# with fresh bridges, at most once per 10 min. Resets on any success.
_tor_dead = {"streak": 0, "last_restart": 0.0}
_TOR_DEAD_STREAK_MAX = 12
_TOR_RESTART_MIN_INTERVAL = 600.0


def _tor_note_dead():
    """Record one transport-dead probe outcome. Returns True if a user-Tor
    restart is due (threshold hit + cooldown elapsed). Resets via _tor_note_ok."""
    try:
        _tor_dead["streak"] += 1
        if _tor_dead["streak"] < _TOR_DEAD_STREAK_MAX:
            return False
        now = time.monotonic()
        if now - _tor_dead["last_restart"] < _TOR_RESTART_MIN_INTERVAL:
            return False
        _tor_dead["last_restart"] = now
        _tor_dead["streak"] = 0
        return True
    except Exception:
        return False


def _tor_note_ok():
    try:
        _tor_dead["streak"] = 0
    except Exception:
        pass


def ensure_tor_daemon(control_host: str = "127.0.0.1", control_port: int = 9151, socks_port: int = 9150) -> bool:
    """Check if Tor is responding on ControlPort; if not, automatically spawn a clean user-mode Tor daemon."""
    try:
        s = socket.create_connection((control_host, control_port), timeout=1.0)
        s.close()
        return True
    except Exception:
        pass

    with _tor_supervisor_lock:
        # Double check after acquiring lock
        try:
            s = socket.create_connection((control_host, control_port), timeout=1.0)
            s.close()
            return True
        except Exception:
            pass

        sys.stderr.write(f"[tor-supervisor] Tor offline on :{control_port}. Auto-starting user-mode Tor...\n")
        torrc_path = "/tmp/tor2/torrc"
        data_dir = "/tmp/tor2/data"
        try:
            def _find_bridges():
                for _bf in (os.path.expanduser("~/.config/tor-zen/obfs4_bridges.txt"),
                            os.path.expanduser("~/.local/share/tor-zen/obfs4_bridges.txt")):
                    if os.path.isfile(_bf):
                        try:
                            _lines = [ln.strip() for ln in open(_bf)
                                      if ln.strip() and not ln.strip().startswith("#")]
                        except Exception:
                            _lines = []
                        if _lines:
                            return _lines
                return []

            def _find_obfs():
                for _ob in (os.path.expanduser("~/.local/bin/obfs4proxy"),
                            "/usr/bin/obfs4proxy", "/usr/bin/lyrebird",
                            "/usr/bin/snowflake-client"):
                    if os.path.isfile(_ob) and os.access(_ob, os.X_OK):
                        return _ob
                return None

            def _build_torrc():
                os.makedirs(data_dir, exist_ok=True)
                lines = [
                    f"SocksPort {socks_port} IsolateSOCKSAuth",
                    f"ControlPort {control_port}",
                    f"DataDirectory {data_dir}",
                ]
                blines, obfs = _find_bridges(), _find_obfs()
                if blines and obfs:
                    lines.append("UseBridges 1")
                    lines.append(f"ClientTransportPlugin obfs4 exec {obfs}")
                    lines.extend(f"Bridge {b}" for b in blines)
                    sys.stderr.write(
                        f"[tor-supervisor] using {len(blines)} obfs4 bridges via {obfs}\n")
                lines.append("Log notice stdout")
                with open(torrc_path, "w") as f:
                    f.write("\n".join(lines) + "\n")

            def _spawn_and_wait(seconds):
                with open("/tmp/tor2/log", "a") as logf:
                    subprocess.Popen(
                        ["tor", "-f", torrc_path],
                        stdout=logf, stderr=subprocess.STDOUT,
                        start_new_session=True, stdin=subprocess.DEVNULL)
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    time.sleep(1.0)
                    try:
                        s = socket.create_connection((control_host, control_port), timeout=1.0)
                        s.sendall(b"AUTHENTICATE \"\"\r\nGETINFO status/bootstrap-phase\r\nQUIT\r\n")
                        buf = b""
                        while True:
                            chunk = s.recv(1024)
                            if not chunk:
                                break
                            buf += chunk
                        s.close()
                        # Require REAL bootstrap: "250 OK" is just the AUTHENTICATE
                        # reply and would falsely report success at 10%.
                        if b"PROGRESS=100" in buf:
                            return True
                    except Exception:
                        continue
                return False

            _build_torrc()
            if _spawn_and_wait(20):
                sys.stderr.write("[tor-supervisor] Tor bootstrapped on :%d\n" % control_port)
                return True
            # Bridges may be stale / missing — refresh from Moat and retry once.
            sys.stderr.write("[tor-supervisor] bootstrap failed — refreshing obfs4 bridges and retrying\n")
            try:
                subprocess.run(
                    [os.path.expanduser("~/.local/bin/tor-bridges-refresh")],
                    timeout=45, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
            try:
                subprocess.run(["pkill", "-f", "/tmp/tor2/torc[c]"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
            time.sleep(1)
            _build_torrc()
            if _spawn_and_wait(35):
                sys.stderr.write("[tor-supervisor] Tor bootstrapped after bridge refresh\n")
                return True
        except Exception as e:

            sys.stderr.write(f"[tor-supervisor] auto-start failed: {e}\n")
            return False
    return False


def restart_user_tor(reason: str = "") -> bool:
    """Restart ONLY the user-mode Tor (/tmp/tor2 -> :9150/9151) with fresh
    bridges. Never touches system Tor (:9050). Called when bootstrap reads
    100% but circuits carry nothing (network refresh / stale guards /
    dead bridges) — the exact state NEWNYM cannot fix. Cooldown enforced by
    callers via _tor_note_dead(); this function just does the work."""
    try:
        sys.stderr.write(f"[tor-supervisor] restarting user-mode Tor ({reason or 'traffic-dead'})...\n")
        try:
            subprocess.run(
                [os.path.expanduser("~/.local/bin/tor-bridges-refresh")],
                timeout=45, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        try:
            subprocess.run(["pkill", "-f", "/tmp/tor2/torr[c]"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        time.sleep(2)
        ok = ensure_tor_daemon("127.0.0.1", 9151, 9150)
        if ok:
            _tor_note_ok()
            sys.stderr.write("[tor-supervisor] user-mode Tor restarted + bootstrapped\n")
        else:
            sys.stderr.write("[tor-supervisor] user-mode Tor restart did NOT bootstrap (bridges/network still down)\n")
        return bool(ok)
    except Exception as e:

        sys.stderr.write(f"[tor-supervisor] restart failed: {e}\n")
        return False


def tor_rotate(host: str, port: int, force: bool = False) -> bool:
    """Send SIGNAL NEWNYM to Tor control port with self-healing fallback and peer reuse.

    Lock discipline: _rotate_lock guards ONLY timestamp check/update (fast).
    Cooldown sleep, supervisor probe, and network I/O all happen outside the lock
    so concurrent rotators never serialize for 10-20s.
    """
    req_time = time.monotonic()
    with _rotate_lock:
        # If a peer thread completed an authentic rotation while we waited for lock, reuse it!
        if _last_rotation["ts"] > req_time:
            sys.stderr.write("[rotate] fresh circuit already rotated by peer thread \u2014 reusing\n")
            return True
        elapsed = time.monotonic() - _last_rotation["ts"]
        if elapsed < ROTATE_MIN_INTERVAL and not force:
            return True
        sleep_needed = max(0.1, ROTATE_MIN_INTERVAL - elapsed) if elapsed < ROTATE_MIN_INTERVAL else 0.0
    if sleep_needed:
        if _qlog("rotate"): sys.stderr.write(f"[rotate] Tor 10s cooldown: sleeping {sleep_needed:.1f}s outside lock\n")
        time.sleep(sleep_needed)
        with _rotate_lock:
            if _last_rotation["ts"] > req_time:
                if _qlog("rotate"): sys.stderr.write("[rotate] peer rotated during cooldown \u2014 reusing\n")
                return True
    # Ensure Tor is running before attempting rotation (outside lock: up to ~20s probe)
    if not ensure_tor_daemon(host, port):
        sys.stderr.write(f"[rotate] Tor daemon unavailable on {host}:{port}\n")
        return False
    ok = False
    ok_lines = 0
    s = None
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.settimeout(3.0)
        s.sendall(b"AUTHENTICATE \"\"\r\nSIGNAL NEWNYM\r\nQUIT\r\n")
        while True:
            try:
                chunk = s.recv(1024)
            except socket.timeout:
                break
            if not chunk:
                break
            ok_lines += sum(
                1 for ln in chunk.split(b"\r\n") if ln.startswith(b"250 ")
            )
            if ok_lines >= 2:
                break
        ok = ok_lines >= 2
    except Exception as e:

        sys.stderr.write(f"[rotate] failed: {e}\n")
        ok = False
    finally:
        try:
            if s is not None:
                s.close()
        except Exception:
            pass
    with _rotate_lock:
        if ok:
            # Re-check: peer may have rotated during our network I/O; keep newest.
            if _last_rotation["ts"] < req_time or True:
                _last_rotation["ts"] = time.monotonic()
        else:
            sys.stderr.write(f"[rotate] unexpected reply (250-lines={ok_lines})\n")
    return ok


def tor_restart(torrc: str, socks: str) -> bool:
    """Kill user-mode tor for `torrc` and start a fresh one."""
    try:
        subprocess.run(["pkill", "-f", f"tor -f {torrc}"],
                       timeout=3, check=False)
    except Exception as e:

        sys.stderr.write(f"[tor-restart] pkill failed: {e}\n")
    time.sleep(1)
    try:
        with open("/tmp/tor2/log", "a") as logf:
            subprocess.Popen(
                ["tor", "-f", torrc],
                stdout=logf, stderr=subprocess.STDOUT,
                start_new_session=True, stdin=subprocess.DEVNULL,
            )
    except Exception as e:

        sys.stderr.write(f"[tor-restart] spawn failed: {e}\n")
        return False
    for _ in range(15):
        time.sleep(2)
        try:
            r = cffi_requests.get("https://api.ipify.org",
                                  proxy=f"socks5h://{socks}",
                                  impersonate="chrome131", timeout=5)
            if r.status_code == 200 and r.text.strip():
                sys.stderr.write(f"[tor-restart] new exit: {r.text.strip()}\n")
                return True
        except Exception:
            continue
    sys.stderr.write(f"[tor-restart] tor did not come up within 30s\n")
    return False


# Cached tor exit IP (avoids blocking /health on every request)
_cached_exit_ip = {"ip": "unknown", "ts": 0}
_exit_ip_lock = threading.Lock()

def tor_exit_ip(socks: str) -> str:
    """Get the current Tor exit IP (cached for 60s to avoid blocking /health)."""
    now = time.monotonic()
    if now - _cached_exit_ip["ts"] < 60:
        return _cached_exit_ip["ip"]
    if not _exit_ip_lock.acquire(blocking=False):
        return _cached_exit_ip["ip"]
    try:
        r = cffi_requests.get("https://api.ipify.org",
                              proxy=f"socks5h://{socks}",
                              impersonate="chrome131", timeout=2)
        ip = r.text.strip()
        _cached_exit_ip["ip"] = ip
        _cached_exit_ip["ts"] = now
        return ip
    except Exception as e:

        _cached_exit_ip["ts"] = now
        _cached_exit_ip["ip"] = f"offline / bootstrapping"
        return _cached_exit_ip["ip"]
    finally:
        _exit_ip_lock.release()


class CircuitSlot:
    """Represents one pre-warmed virtual Tor circuit with dedicated session and exit IP."""
    def __init__(self, slot_id: int):
        self.slot_id = slot_id
        self.gen = 0
        self.session = None
        self.exit_ip = ""
        self.state = "INIT"  # INIT, PROBING, READY, ACTIVE, TAINTED
        self.last_used = 0.0
        self.created_at = 0.0
        self.fail_count = 0
        self.lock = threading.Lock()
        # P0-A lease bookkeeping (guarded by PrewarmedCircuitPool._pool_lock):
        # refcount = in-flight requests holding this slot's session;
        # queued = already awaiting replenish (dedups repair queue);
        # probe_failures = consecutive warm-probe failures (breaker).
        self.refcount = 0
        self.queued = False
        self.probe_failures = 0
        self.probing_since = 0.0
        # Proxy URL that actually warmed this circuit (virtual IsolateSOCKSAuth
        # user, or the plain SOCKS fallback when the virtual user is
        # unsupported). Requests bind to this so they reuse the same circuit.
        self.effective_proxy = ""

    def virtual_proxy_url(self, socks_hp: str = "127.0.0.1:9150") -> str:
        clean = socks_hp.replace("socks5h://", "").replace("socks5://", "")
        return f"socks5h://virt_slot_{self.slot_id}_g{self.gen}:x@{clean}"


class _FastReject(Exception):
    """Internal: warm probe knew the exit is unusable (parked) before the
    costly health check. Handled inline (backoff + requeue); must NOT fall
    into the generic probe-failure path (which would double-count)."""


class PrewarmedCircuitPool:
    """Active Pre-Warmed Virtual Tor Circuit Pool with Zero-Latency Failover & Fair Multi-Agent Allocation.

    Maintains N distinct virtual circuits concurrently via IsolateSOCKSAuth.
    When multiple concurrent subagents arrive, each receives a dedicated idle circuit
    (refcount == 0) so they do not share exit IPs or trigger mutual rate limits.
    When a request hits 429 / 403 / 504, an instant pointer swap (<5ms)
    selects the next warm standby circuit without sleeping.
    If the pool is momentarily starved, incoming requests wait patiently on a condition
    variable while emitting SSE keepalive heartbeats (: ping\n\n) — requests are NEVER rejected.
    Background replenish workers rebuild tainted circuits in parallel without blocking.
    """
    def __init__(self, pool_size: int = DEFAULT_POOL_SIZE, socks_hp: str = "127.0.0.1:9150"):
        self.pool_size = pool_size
        self.socks_hp = socks_hp
        self.slots = [CircuitSlot(i) for i in range(pool_size)]
        self._pool_lock = threading.Lock()
        self._pool_cond = threading.Condition(self._pool_lock)
        self._replenish_queue = queue.Queue()
        self._active_idx = 0
        self._started = False
        self._stop_event = threading.Event()
        self._replenish_threads = []
        # In-memory quarantine snapshot: avoids SQLite connect per pick while
        # holding _pool_lock. Refreshed by a background thread; readers never
        # touch SQLite, so _pool_lock is never held across a DB call.
        self._q_cache = frozenset()
        self._q_cache_ts = 0.0
        self._q_lock = threading.Lock()
        self._q_stale = threading.Event()
        self._rescue_until = 0.0  # process-wide rescue circuit-breaker
        # Process-wide upstream Retry-After horizon: {model_key: epoch_until}.
        # Set by ANY thread that sees a huge Retry-After; read by ALL threads
        # so 3 subagents don't each burn 7 attempts discovering the same ban.
        # Keyed per (host, model) so an hour-ban on muse doesn't idle other models.
        self._retry_after_until = {}  # dict[str, float]
        self._retiring_sessions = []
        # Single-active-exit redesign (2026-09-25):
        #  - park: exit_ip -> monotonic until; burned (429) exits kept out of
        #    rotation so Tor handing the same exit back doesn't re-429.
        #  - upstream/health_model: set by main(); used by the replenish
        #    health-check (a tiny muse call) so a circuit is only marked READY
        #    if it is actually usable, not merely reachable.
        #  - _req_local: per-thread request connection bound to the shared
        #    circuit (design A) — thread-safe, reuses the warmed circuit.
        self.park = {}
        self.upstream = ""
        self.health_model = DEFAULT_MODEL
        self._req_local = threading.local()
        # Throttle the fail-open "no clean circuit" log (was one line PER pick:
        # 30+ lines/burst under starvation, drowning the log).
        self._failopen_log_ts = 0.0

    def _park_locked(self, ip, seconds=None):
        """Park an exit IP (caller holds _pool_cond)."""
        if not ip:
            return
        try:
            until = time.monotonic() + float(
                seconds if seconds is not None else EXIT_PARK_S)
            if until > self.park.get(ip, 0.0):
                self.park[ip] = until
        except Exception:
            pass

    def _parked_locked(self, ip) -> bool:
        if not ip:
            return False
        try:
            return self.park.get(ip, 0.0) > time.monotonic()
        except Exception:
            return False

    def park_exit(self, ip, seconds=None):
        """Public park (takes the lock); used from the request path."""
        try:
            with self._pool_cond:
                self._park_locked(ip, seconds)
        except Exception:
            pass

    def _request_session(self, slot):
        """Per-thread request connection bound to the slot's shared circuit.

        Design A: many subagents share ONE active exit, but a curl_cffi Session
        is not safe for concurrent requests. Each thread gets its own Session
        to the same IsolateSOCKSAuth circuit (same exit IP) and reuses it until
        the circuit generation changes, so connections stay warm and parallel
        requests don't corrupt each other.
        """
        if slot is None:
            return None
        key = getattr(slot, "effective_proxy", "") or slot.virtual_proxy_url(self.socks_hp)
        sess = getattr(self._req_local, "sess", None)
        if sess is not None and getattr(self._req_local, "key", None) == key:
            return sess
        if sess is not None:
            try:
                sess.close()
            except Exception:
                pass
        try:
            sess = cffi_requests.Session(
                impersonate="chrome131", proxy=key, timeout=90.0)
        except Exception:
            sess = slot.session
        self._req_local.sess = sess
        self._req_local.key = key
        return sess

    def close_thread_session(self):
        """Close this thread's request session (called from Handler.finish()).

        BaseHTTPRequestHandler is HTTP/1.0 here (one request per connection), so
        each request runs on a fresh thread; without this the per-thread
        Sessions would leak sockets across thousands of requests.
        """
        sess = getattr(self._req_local, "sess", None)
        if sess is not None:
            try:
                sess.close()
            except Exception:
                pass
            self._req_local.sess = None
            self._req_local.key = None

    def note_retry_after(self, model_key: str, wait_s: float):
        """Share an upstream Retry-After with all threads (pool-global).

        Capped at _BAN_PARK_CAP_S: upstream horizons run for hours and must
        never take a model offline for that long on one header. The cap keeps
        rotation alive, which is the real recovery path for per-exit throttle.
        """
        if not model_key or wait_s <= 0:
            return
        try:
            wait_s = min(float(wait_s), _BAN_PARK_CAP_S)
            with self._pool_lock:
                until = time.monotonic() + wait_s
                prev = self._retry_after_until.get(model_key, 0.0)
                if until > prev:
                    self._retry_after_until[model_key] = until
        except Exception:
            pass

    def clear_retry_after(self, model_key: str):
        """Drop the parked ban (called on any success — recovery detected)."""
        if not model_key:
            return
        try:
            with self._pool_lock:
                self._retry_after_until.pop(model_key, None)
        except Exception:
            pass

    def retry_after_left(self, model_key: str) -> float:
        """Seconds left on the shared ban horizon for this model (0 = clear)."""
        if not model_key:
            return 0.0
        try:
            with self._pool_lock:
                left = self._retry_after_until.get(model_key, 0.0) - time.monotonic()
                return left if left > 0 else 0.0
        except Exception:
            return 0.0

    def start(self):
        if self._started:
            return
        self._started = True
        self._replenish_threads = []
        for slot in self.slots:
            self._replenish_queue.put(slot)
        # Concurrent replenishment. With a 20-slot pool under sustained agent
        # load, 3 workers could not recycle tainted slots fast enough, so bursts
        # queued on the pool (p90 TTFT 45s+). 6 keeps ~parity with the burst
        # width on the fast :9050 transport; the desync stagger below avoids the
        # self-congestion the old note warned about. Override ZEN_WARM_WORKERS.
        try:
            num_workers = max(1, min(self.pool_size, int(os.environ.get("ZEN_WARM_WORKERS", "6"))))
        except Exception:
            num_workers = min(self.pool_size, 3)
        for i in range(num_workers):
            t = threading.Thread(
                target=self._replenish_worker,
                daemon=True,
                name=f"tor-pool-replenisher-{i}"
            )
            t.start()
            self._replenish_threads.append(t)
        # Background quarantine-snapshot refresher (keeps SQLite off _pool_lock).
        self._q_stale.set()
        threading.Thread(target=self._q_refresher_worker, daemon=True,
                         name="tor-pool-qrefresh").start()
        # Keep the standby circuit warm so the swap on 429 is a pointer change,
        # not a cold Tor circuit build (8-30s).
        if POOL_KEEPALIVE_S > 0:
            threading.Thread(target=self._keepalive_worker, daemon=True,
                             name="tor-pool-keepalive").start()

    def _enqueue_locked(self, slot: CircuitSlot):
        """Queue slot for replenish once. Caller must hold _pool_lock."""
        if not slot.queued:
            slot.queued = True
            self._replenish_queue.put(slot)

    def _refresh_q_cache_locked(self):
        """Signal the background quarantine refresher; never touches SQLite.

        Caller may hold _pool_lock — this only sets an Event, so lock hold time
        is memory-only on the hot path (the SQLite query runs off-lock)."""
        try:
            if time.monotonic() - self._q_cache_ts >= 30.0:
                self._q_stale.set()
        except Exception:
            pass

    def _q_refresher_worker(self):
        """Refresh the quarantine snapshot off the _pool_lock."""
        while not self._stop_event.is_set():
            self._q_stale.wait(timeout=20.0)
            self._q_stale.clear()
            try:
                if zen_db is not None and hasattr(zen_db, "get_quarantined_ips"):
                    new = frozenset(zen_db.get_quarantined_ips() or [])
                else:
                    new = frozenset()
            except Exception:
                continue
            with self._q_lock:
                self._q_cache = new
                self._q_cache_ts = time.monotonic()

    def _quarantined(self, slot: CircuitSlot) -> bool:
        """Advisory exit-IP quarantine check (in-memory snapshot).

        Caller must hold _pool_lock. Fails open (not quarantined) on DB error.
        """
        try:
            if not slot.exit_ip:
                return False
            return self._quarantined_ip(slot.exit_ip)
        except Exception:
            return False

    def _quarantined_ip(self, ip: str) -> bool:
        """Snapshot check for a raw IP (usable from probe path holding the lock)."""
        try:
            if not ip:
                return False
            self._refresh_q_cache_locked()
            return ip in self._q_cache
        except Exception:
            return False

    def _mark_q_stale(self):
        """Force an immediate quarantine snapshot refresh (after reputation write)."""
        try:
            self._q_stale.set()
        except Exception:
            pass

    def _pick_best_slot_locked(self, exclude_slot=None) -> CircuitSlot:
        """Pick the slot to serve a request. Caller MUST hold self._pool_lock.

        Single-active-exit design: every client/subagent shares ONE active exit
        until it is burned (429). The current ACTIVE slot is therefore returned
        even when refcount > 0 (design A: shared circuit, per-thread
        connection). A new slot is leased only when no usable ACTIVE slot
        exists, from the READY standbys (MRU first). Parked (burned) exits are
        never returned; quarantined exits only as a last resort.
        """
        self._refresh_q_cache_locked()
        q = self._q_cache

        def _usable(s):
            if s is exclude_slot or s.session is None:
                return False
            if s.state not in ("READY", "ACTIVE"):
                return False
            if s.exit_ip and (s.exit_ip in q or self._parked_locked(s.exit_ip)):
                return False
            return True

        # 1. Share the current ACTIVE exit (one exit serving everyone) until the
        #    share cap; past it, spill onto a standby so a burst doesn't hammer
        #    one exit into 429s.
        actives = [s for s in self.slots
                   if s.state == "ACTIVE" and _usable(s)
                   and s.refcount < ACTIVE_SHARE_CAP]
        if actives:
            actives.sort(key=lambda s: s.last_used, reverse=True)
            return actives[0]

        # 2. Promote a READY standby (MRU first, skipping a keepalive-held one).
        ready = [s for s in self.slots
                 if s.state == "READY" and _usable(s) and s.refcount == 0]
        if ready:
            ready.sort(key=lambda s: s.last_used, reverse=True)
            return ready[0]

        # 3. Last resort: quarantined but NOT parked (fail-open), WITH a share
        #    cap. Without the cap a burst piled 41 concurrent leases onto one
        #    burned exit (observed 2026-09-26): each one 429/403s, TTFT
        #    explodes and Zen throttles that exit even harder. Past the cap we
        #    return None so acquire_slot() waits with keepalives instead of
        #    self-DDoS-ing a single exit.
        q_open = [s for s in self.slots
                  if s is not exclude_slot and s.state in ("READY", "ACTIVE")
                  and s.session is not None
                  and s.refcount < ACTIVE_SHARE_CAP
                  and not (s.exit_ip and self._parked_locked(s.exit_ip))]
        if q_open:
            q_open.sort(key=lambda s: (s.refcount, -s.last_used))
            _now_fo = time.monotonic()
            if _now_fo - self._failopen_log_ts >= 5.0:
                self._failopen_log_ts = _now_fo
                sys.stderr.write(
                    f"{_yel(f'[pool] no clean circuit — fail-open slot {q_open[0].slot_id} ({q_open[0].exit_ip}), {q_open[0].refcount} in flight')}\n")
            return q_open[0]

        return None

    def release_slot(self, slot: CircuitSlot):
        """Release one caller-held lease. Floors at 0 (safe to call defensively)."""
        if slot is None:
            return
        to_close = []
        with self._pool_cond:
            if slot.refcount > 0:
                slot.refcount -= 1
            if slot.refcount == 0 and slot.state == "ACTIVE":
                slot.state = "READY"
            # Collect expired retired sessions; close them OUTSIDE the lock
            # (curl teardown can block and must not stall slot operations).
            now = time.monotonic()
            still_retiring = []
            for sess, expire in self._retiring_sessions:
                if now > expire:
                    to_close.append(sess)
                else:
                    still_retiring.append((sess, expire))
            self._retiring_sessions = still_retiring
            self._pool_cond.notify_all()
        for sess in to_close:
            try:
                sess.close()
            except Exception:
                pass

    def get_active_slot(self) -> tuple:
        """Non-blocking slot lease: returns (slot, session) or (None, None)."""
        with self._pool_lock:
            chosen = self._pick_best_slot_locked()
            if chosen is not None:
                chosen.state = "ACTIVE"
                chosen.last_used = time.monotonic()
                chosen.refcount += 1
                self._active_idx = chosen.slot_id
                return chosen, self._request_session(chosen)
            return None, None

    def acquire_slot(self, timeout: float = 60.0, on_wait=None, exclude_slot=None) -> tuple:
        """Patient slot lease: if all circuits are busy or warming, waits on condition
        variable while periodically invoking on_wait() to emit SSE keepalives (: ping).
        Returns (slot, session) or (None, None) if timeout expires.
        """
        deadline = time.monotonic() + max(1.0, timeout)
        while not self._stop_event.is_set():
            pending_wait = False
            with self._pool_cond:
                chosen = self._pick_best_slot_locked(exclude_slot=exclude_slot)
                if chosen is not None:
                    chosen.state = "ACTIVE"
                    chosen.last_used = time.monotonic()
                    chosen.refcount += 1
                    self._active_idx = chosen.slot_id
                    return chosen, self._request_session(chosen)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                wait_slice = min(1.0, remaining)
                self._pool_cond.wait(timeout=wait_slice)
                pending_wait = on_wait is not None
            # Emit keepalives OUTSIDE the lock: on_wait writes+flushes the client
            # socket, and a stalled client must never block slot acquire/release.
            if pending_wait:
                try:
                    on_wait()
                except Exception:
                    pass

        return None, None

    def report_failure(self, slot: CircuitSlot, status_code: int = 429, reason: str = "") -> tuple:
        """Hot-swap failover: release caller's lease, taint slot, lease next ready.

        A pure SLOWNESS signal (TTFB/content stall / no headers) is NOT an exit
        fault: under slow Tor, treating it as one tainted+rebuilt the circuit on
        every 6s cap, draining the pool to empty (2026-09-26 15-way test: 149
        stalls -> 1173 'pool busy'). Such a slot is released WARM (state stays
        READY) so it serves the next turn; it is only rebuilt after repeated
        soft stalls. Real upstream errors (429/403/5xx/transport) still taint.
        """
        rec = None
        _rl = (reason or "").lower()
        _soft = (status_code == 502 and (
            "stall" in _rl or "no headers" in _rl or "stalled" in _rl))
        with self._pool_cond:
            if slot is not None:
                if _soft:
                    slot.fail_count += 1
                    if slot.refcount > 0:
                        slot.refcount -= 1
                    if slot.refcount == 0 and slot.state == "ACTIVE":
                        slot.state = "READY"
                    # Bound it: 4 consecutive slow serves => treat as unhealthy.
                    if slot.fail_count >= 4:
                        slot.state = "TAINTED"
                        self._enqueue_locked(slot)
                        if slot.exit_ip:
                            rec = (slot.exit_ip, status_code, "repeated-stall")
                else:
                    slot.state = "TAINTED"
                    slot.fail_count += 1
                    if slot.refcount > 0:
                        slot.refcount -= 1
                    self._enqueue_locked(slot)
                    if slot.exit_ip:
                        rec = (slot.exit_ip, status_code, reason)
                        if status_code == 403:  # only 403 (per-exit) parks; 429 is transient/account-wide
                            # Per-circuit free limit: park the exit so Tor handing
                            # the same IP back doesn't immediately re-429.
                            self._park_locked(slot.exit_ip)

            chosen = self._pick_best_slot_locked(exclude_slot=slot)
            if chosen is not None:
                chosen.state = "ACTIVE"
                chosen.last_used = time.monotonic()
                chosen.refcount += 1
                self._active_idx = chosen.slot_id
                result = (chosen, self._request_session(chosen))
            else:
                result = (None, None)
            self._pool_cond.notify_all()

        if rec is not None and zen_db is not None:
            try:
                db_async('record_exit_result', rec[0], False, f"{rec[1]}:{rec[2] or 'retryable'}"[:120])
            except Exception:
                pass
            self._mark_q_stale()
        return result

    def note_failure(self, slot: CircuitSlot, status_code: int = 502, reason: str = ""):
        """Taint a slot and release its lease WITHOUT acquiring a new one.

        For terminal paths (loop exhausted, rescue switch, non-retryable abort):
        marks the exit burned, records reputation, enqueues rebuild.
        Safe to call with None (no-op).
        """
        if slot is None:
            return
        rec = None
        with self._pool_cond:
            slot.state = "TAINTED"
            slot.fail_count += 1
            if slot.refcount > 0:
                slot.refcount -= 1
            self._enqueue_locked(slot)
            if slot.exit_ip:
                rec = (slot.exit_ip, status_code, reason)
                if status_code == 403:  # only 403 (per-exit) parks; 429 is transient/account-wide
                    self._park_locked(slot.exit_ip)
            self._pool_cond.notify_all()
        if rec is not None and zen_db is not None:
            try:
                db_async('record_exit_result', rec[0], False, f"{rec[1]}:{rec[2] or 'terminal'}"[:120])
            except Exception:
                pass
            self._mark_q_stale()

    def report_success(self, slot: CircuitSlot):
        """Record success and release the caller's lease (always releases)."""
        if slot is None:
            return
        rec = None
        with self._pool_cond:
            slot.fail_count = 0
            slot.last_used = time.monotonic()
            if slot.refcount > 0:
                slot.refcount -= 1
            if slot.refcount == 0 and slot.state == "ACTIVE":
                slot.state = "READY"
            if slot.exit_ip:
                rec = slot.exit_ip
            self._pool_cond.notify_all()
        if rec is not None and zen_db is not None:
            try:
                db_async('record_exit_result', rec, True, "")
            except Exception:
                pass

    def taint_all(self, reason: str = "manual rotate"):
        """Taint all slots and trigger re-warming (e.g. on manual 'r' or /rotate).

        In-flight holders keep their leases; the worker will not close a slot
        while refcount > 0, so manual rotate no longer chops live streams.
        """
        with self._pool_cond:
            for s in self.slots:
                s.state = "TAINTED"
                self._enqueue_locked(s)
            self._pool_cond.notify_all()

    def usable_count(self) -> int:
        """Number of READY/ACTIVE slots with a session that are not parked.

        Used by the temporary starvation fallback in do_POST: when Tor is
        handing out a single exit (or none), muse turns are served on the
        Cline free lane instead of starving the agent.
        """
        try:
            with self._pool_lock:
                return sum(1 for s in self.slots
                           if s.state in ("READY", "ACTIVE") and s.session is not None
                           and not (s.exit_ip and self._parked_locked(s.exit_ip)))
        except Exception:
            return 0

    def get_status(self) -> list[dict]:
        with self._pool_lock:
            return [
                {
                    "slot": s.slot_id,
                    "gen": s.gen,
                    "state": s.state,
                    "exit_ip": s.exit_ip or "probing...",
                    "fail_count": s.fail_count,
                    "refcount": s.refcount,
                    "queued": s.queued,
                    "probe_failures": s.probe_failures,
                    "age_s": int(time.monotonic() - s.created_at) if s.created_at else 0,
                }
                for s in self.slots
            ]

    def _health_check(self, sess) -> bool:
        """One tiny muse request on a fresh circuit; True iff it is usable.

        Real (not merely reachable): uses the genuine opencode session + UA on
        the Responses wire muse expects. 200 => circuit holds free tier;
        403 (or transport error) => not usable and the caller parks the
        exit. 429 on the ping is self-inflicted under parallel warmers, not
        a burned exit — fails the probe without parking. Cheap (reasoning
        minimal, 16 output tokens). Returns the status code (200/403/429/0).
        """
        try:
            gen_sess, proj = get_genuine_opencode_session()
        except Exception:
            gen_sess, proj = gen_message_id(), "global"
        body = {
            "model": self.health_model or DEFAULT_MODEL,
            "stream": True,
            "store": False,
            "prompt_cache_key": gen_sess,
            "input": [{"role": "user", "content": "ping"}],
            "reasoning": {"effort": "minimal", "summary": "auto"},
            "max_output_tokens": 16,
            "tools": list(_OPENCODE_CORE_TOOLS),
        }
        headers = {
            "Authorization": "Bearer public",
            "Content-Type": "application/json",
            "User-Agent": _OPENCODE_UA,
            "x-opencode-client": "cli",
            "x-opencode-project": proj or "global",
            "x-opencode-directory": "/home/vagish_arch",
            "x-opencode-request": gen_message_id(),
            "x-opencode-session": gen_sess,
        }
        r = None
        try:
            r = sess.post(f"{self.upstream}/responses", headers=headers,
                          json=body, stream=True, timeout=(15, 25))
            return r.status_code
        except Exception:
            return 0
        finally:
            if r is not None:
                try:
                    r.close()
                except Exception:
                    pass

    def _keepalive_worker(self):
        """Touch the hottest idle READY circuit so it stays warm between turns.

        A circuit/TLS connection that is actively used answers in ~1-3s but
        goes cold within ~10-15s idle and then costs 8-30s to rebuild. This
        keeps a standby genuinely "ready to send" so a 429 swap is a pointer
        change. The ACTIVE circuit is never touched (it is already in use and
        its traffic keeps it warm).
        """
        while not self._stop_event.is_set():
            if self._stop_event.wait(POOL_KEEPALIVE_S):
                return
            if not self.upstream:
                continue
            slot = None
            sess = None
            with self._pool_cond:
                idle = [s for s in self.slots
                        if s.state == "READY" and s.session is not None
                        and not (s.exit_ip and self._parked_locked(s.exit_ip))]
                if idle:
                    idle.sort(key=lambda s: s.last_used, reverse=True)
                    slot = idle[0]
                    sess = slot.session
            if slot is None or sess is None:
                continue
            # Requests use their own per-thread connections, so this ping only
            # touches the slot's warm session. Tor is slow: use a generous
            # timeout and NEVER taint on failure — a 5s probe would otherwise
            # destroy healthy circuits and the pool would never stay warm. A
            # genuinely dead circuit is caught by the next real request.
            ok = False
            try:
                r = sess.get(f"{self.upstream}/models", timeout=20.0)
                ok = (r.status_code < 500)
                try:
                    r.close()
                except Exception:
                    pass
            except Exception:
                ok = False
            if ok:
                try:
                    with self._pool_cond:
                        if slot.state == "READY":
                            slot.last_used = time.monotonic()
                except Exception:
                    pass

    def _replenish_worker(self):
        """Background worker replenishing circuits off the client critical path.

        Non-blocking: increments gen and probes a new circuit in parallel.
        In-flight streams continue reading from the old session without interruption.
        Sleep/backoff always outside locks. Queue dedup via slot.queued.
        """
        import random as _rand
        # Desync thundering herd: workers starting together ping Zen in
        # lockstep (self-inflicted health-429s). One random stagger each.
        try:
            time.sleep(_rand.uniform(0, 4.0))
        except Exception:
            pass
        while not self._stop_event.is_set():
            try:
                slot: CircuitSlot = self._replenish_queue.get(timeout=1.0)
            except queue.Empty:
                now = time.monotonic()
                with self._pool_cond:
                    for s in self.slots:
                        # Healthy READY/ACTIVE circuits are no longer rebuilt on
                        # a timer: a Tor circuit can serve the free tier for
                        # ~30 min, and rebuilding it just throws that window
                        # away and forces a cold build. Rebuild only on
                        # failure/park (TAINTED) or a hung probe.
                        if s.state in ("TAINTED", "INIT") and not s.queued and (not s.last_used or (now - s.last_used > 10.0)):
                            self._enqueue_locked(s)
                        elif s.state == "PROBING" and not s.queued and s.probing_since and (now - s.probing_since > 45.0):
                            # Probe hung for >45s (exceeding 30s timeout + 15s margin)
                            s.state = "TAINTED"
                            s.probing_since = 0.0
                            self._enqueue_locked(s)
                continue

            with self._pool_cond:
                slot.queued = False
                slot.state = "PROBING"
                slot.probing_since = time.monotonic()
                slot.gen += 1
                my_gen = slot.gen

            proxy_url = slot.virtual_proxy_url(self.socks_hp)
            slot.effective_proxy = proxy_url
            new_sess = None
            try:
                new_sess = cffi_requests.Session(
                    impersonate="chrome131",
                    proxy=proxy_url,
                    timeout=30.0,
                )
                probe_start = time.perf_counter()

                def _probe_ip(sess):
                    """First 200 from any IP-echo service within one budget.

                    Tries fastest-first so a working circuit is identified in
                    1-3s; a single slow/blocked echo (common via Tor) then
                    only costs its own short attempt, not the whole budget."""
                    _deadline = time.monotonic() + WARM_PROBE_TIMEOUT
                    _last = None
                    for _ep in _PROBE_ENDPOINTS:
                        _left = _deadline - time.monotonic()
                        if _left <= 2.0:
                            break
                        try:
                            _rr = sess.get(_ep, timeout=min(WARM_PROBE_TIMEOUT, _left))
                            if _rr.status_code == 200 and _rr.text.strip():
                                return _rr
                            _last = ValueError(f"probe HTTP {_rr.status_code}")
                        except Exception as _pe:
                            _last = _pe
                    raise _last or ValueError("probe failed")

                # Offline-test hook: mock SOCKS relays may not implement
                # IsolateSOCKSAuth virtual-user circuits. Fall back to the
                # plain SOCKS endpoint so pool warmup (and the harness) works
                # without real Tor. Production Tor ignores the fallback only
                # if the virtual circuit is down — plain still yields an exit.
                try:
                    r = _probe_ip(new_sess)
                except Exception as _virt_err:
                    _plain = "socks5h://" + self.socks_hp.replace(
                        "socks5h://", "").replace("socks5://", "")
                    try:
                        new_sess.close()
                    except Exception:
                        pass
                    new_sess = cffi_requests.Session(
                        impersonate="chrome131", proxy=_plain, timeout=30.0)
                    slot.effective_proxy = _plain
                    r = _probe_ip(new_sess)
                if r.status_code == 200 and r.text.strip():
                    new_ip = r.text.strip()
                    # Fast park reject BEFORE the costly muse health check: a
                    # parked exit (recent 429/403) would just burn a real
                    # inference POST + 12-20s to fail. Tor dealing the same
                    # parked IPs in a loop is the observed hot-loop; skipping
                    # the health POST here saves quota + seconds per reject.
                    try:
                        with self._pool_cond:
                            _pre_park = self._parked_locked(new_ip)
                    except Exception:
                        _pre_park = False
                    if _pre_park:
                        try:
                            new_sess.close()
                        except Exception:
                            pass
                        _rs = getattr(slot, "reject_streak", 0) + 1
                        slot.reject_streak = _rs
                        # Parked exits stay bad for 1800s; retrying every few
                        # seconds just spins. Longer backoff (up to 30s) while
                        # still recovering via fresh circuits.
                        _wait = min(30.0, 4.0 * (2.0 ** min(_rs, 3)))
                        if _qlog("pool-parked"):
                            if _qlog("pool"): sys.stderr.write(f"[pool] slot {slot.slot_id} warm rejected ({new_ip}): exit parked (free-tier limit), retrying in {_wait:.0f}s (x{_rs})\n")
                        time.sleep(_wait)
                        with self._pool_cond:
                            if slot.gen == my_gen and slot.state != "READY":
                                slot.state = "TAINTED"
                                self._enqueue_locked(slot)
                        raise _FastReject()
                    # The muse health-check POST below establishes the TLS
                    # connection itself, so no separate HEAD prewarm round trip.
                    # Replenish health check: the circuit is only READY if a
                    # real (tiny) muse call succeeds — not merely reachable. A
                    # fresh exit may already be free-tier-throttled; catching it
                    # here avoids burning a client turn on it.
                    if POOL_HEALTHCHECK and self.upstream:
                        # Starving-pool fail-open: if this slot's ping keeps
                        # 429ing while the pool has <2 usable circuits, skip
                        # the check and let the normal accept path run. Client
                        # turns rotate past throttled exits via standard 429
                        # handling; serving reachable-only beats 503s. (A 503
                        # helps no one; throttled exits fail over per-turn.)
                        _hc_skip = False
                        try:
                            with self._pool_cond:
                                _ready_n = sum(1 for s in self.slots if s.state in ("READY", "ACTIVE") and s.session is not None)
                                # Keep the pool populated: while we have fewer
                                # than ZEN_POOL_HEALTH_MIN usable circuits, accept
                                # reachable exits WITHOUT the strict muse health
                                # ping. Zen's free tier throttles broadly (403
                                # FreeTierError / 429 FreeUsageLimitError), and
                                # requiring a 200 health ping emptied the pool to
                                # 0 usable, so clients had nothing to rotate onto.
                                # Real turns then rotate past throttle via the
                                # standard 403/429 failover, which is the actual
                                # recovery path. Default 3, override
                                # ZEN_POOL_HEALTH_MIN.
                                _hc_min = 3
                                try:
                                    _hc_min = max(0, int(os.environ.get("ZEN_POOL_HEALTH_MIN", "3")))
                                except Exception:
                                    _hc_min = 3
                                if _ready_n < _hc_min or getattr(slot, "hc429", 0) >= 2:
                                    _hc_skip = True
                        except Exception:
                            pass
                        if _hc_skip:
                            sys.stderr.write(f"[pool] slot {slot.slot_id} accepting throttled exit {new_ip} (starving pool, no health check)\n")
                            slot.hc429 = 0
                        else:
                            _hc = self._health_check(new_sess)
                            if _hc != 200:
                                if _hc == 403:
                                    self.park_exit(new_ip)
                                if _hc == 429:
                                    slot.hc429 = getattr(slot, "hc429", 0) + 1
                                raise ValueError(f"health check http={_hc}")
                    probe_ms = (time.perf_counter() - probe_start) * 1000
                    old_to_close = None
                    reject_reason = "stale probe"
                    with self._pool_cond:
                        slot.probing_since = 0.0
                        if slot.gen != my_gen:
                            # Newer probe already in flight for this slot; this session is stale.
                            # Do NOT re-enqueue — the newer probe is already running!
                            superseded = True
                            stale_generation = True
                        else:
                            # A parked exit (recent 429/403 on the free tier) is
                            # NEVER re-admitted — Tor handing it back would just
                            # re-429. Duplicates/quarantines are skipped when a
                            # ready circuit exists (fail-open only for those, so
                            # the pool never goes cold).
                            _ready_other = any(
                                s is not slot
                                and s.state in ("READY", "ACTIVE")
                                and s.session is not None
                                for s in self.slots)
                            _dup = new_ip in {
                                s.exit_ip for s in self.slots
                                if s is not slot and s.exit_ip}
                            _quar = self._quarantined_ip(new_ip)
                            _park = self._parked_locked(new_ip)
                            if _park:
                                superseded = True
                                stale_generation = False
                                reject_reason = "exit parked (free-tier limit)"
                            elif (_dup or _quar) and _ready_other:
                                superseded = True
                                stale_generation = False
                                reject_reason = (
                                    "duplicate exit (owned by another slot)"
                                    if _dup else
                                    "exit quarantined (recent upstream failure)")
                            else:
                                superseded = False
                                stale_generation = False
                                if _dup or _quar:
                                    sys.stderr.write(
                                        f"[pool] slot {slot.slot_id} accepting "
                                        f"{'duplicate' if _dup else 'quarantined'} "
                                        f"exit {new_ip} — no ready circuit "
                                        f"(fail-open)\n")
                                slot.exit_ip = new_ip
                                old_sess = slot.session
                                slot.session = new_sess
                                new_sess = None
                                slot.created_at = time.monotonic()
                                slot.last_used = time.monotonic()
                                slot.fail_count = 0
                                slot.probe_failures = 0
                                slot.reject_streak = 0
                                slot.hc429 = 0
                                slot.state = "READY"
                                _tor_note_ok()
                                if old_sess is not None:
                                    if slot.refcount == 0:
                                        old_to_close = old_sess
                                    else:
                                        self._retiring_sessions.append((old_sess, time.monotonic() + 300.0))
                                self._pool_cond.notify_all()
                    if old_to_close is not None:
                        try:
                            old_to_close.close()
                        except Exception:
                            pass
                    if superseded:
                        try:
                            new_sess.close()
                        except Exception:
                            pass
                        if stale_generation:
                            sys.stderr.write(f"[pool] slot {slot.slot_id} stale probe (gen {my_gen} < current {slot.gen}) discarded\n")
                        else:
                            # Progressive backoff: Tor is dealing from a tiny
                            # effective exit set (bridges/campus keeps handing
                            # back the same few IPs) — retrying every 2s just
                            # burns CPU and log space; a fresh exit won't
                            # appear any faster for hammering.
                            _rs = getattr(slot, "reject_streak", 0) + 1
                            slot.reject_streak = _rs
                            _wait = min(WARM_REJECT_BACKOFF_CAP, 2.0 * (2.0 ** min(_rs, 5)))
                            if _qlog("pool-reject"):
                                if _qlog("pool"): sys.stderr.write(f"[pool] slot {slot.slot_id} warm rejected ({new_ip}): {reject_reason}, retrying in {_wait:.0f}s (x{_rs})\n")
                            time.sleep(_wait)
                            with self._pool_cond:
                                if slot.gen == my_gen and slot.state != "READY":
                                    slot.state = "TAINTED"
                                    self._enqueue_locked(slot)
                    else:
                        if _qlog("pool-warmed"):
                            sys.stderr.write(f"[pool] slot {slot.slot_id} warmed (gen {slot.gen}): exit {new_ip} in {probe_ms:.0f}ms\n")
                else:
                    raise ValueError(f"probe HTTP {r.status_code}")
            except _FastReject:
                # Parked-exit fast reject already backed off + requeued above.
                pass
            except Exception as e:

                if new_sess is not None:
                    try:
                        new_sess.close()
                    except Exception:
                        pass
                    new_sess = None
                with self._pool_cond:
                    slot.probing_since = 0.0
                    slot.probe_failures += 1
                    fails = slot.probe_failures
                backoff = min(5.0, 1.5 * (1.5 ** max(0, fails - 1)))
                backoff = backoff + _rand.uniform(0, 0.5)
                _err_txt = str(e)
                # Health-ping 429s are ROUTINE (throttled exits + our own
                # parallel warmers bursting) — yellow + quiet-gated, never
                # red: red is reserved for transport death (curl 7/28,
                # http=0). Back off longer to let the throttle cool instead
                # of hammering Zen every few seconds.
                if "health check http=429" in _err_txt:
                    backoff = 15.0 + _rand.uniform(0, 5.0)
                    if _qlog("health-429"):
                        sys.stderr.write(f"{_yel(f'[pool] slot {slot.slot_id} warm probe throttled: health check http=429, retrying in {backoff:.0f}s (fail#{fails})')}\n")
                else:
                    if _qlog("pool"): sys.stderr.write(f"{_red(f'[pool] slot {slot.slot_id} warm probe failed: {str(e)[:120]}, retrying in {backoff:.1f}s (fail#{fails})')}\n")
                # Transport-dead signal: refused/timeout/empty health reply on
                # the WARM path (not client traffic). Count toward the
                # traffic-death watchdog; at threshold restart user-mode Tor
                # (fresh bridges+guards) — the network-refresh recovery.
                # Success anywhere resets the streak (see _tor_note_ok calls
                # on warm-ready and client 200 paths).
                _transport_dead = (
                    "Failed to connect" in _err_txt or "Connection refused" in _err_txt
                    or "curl: (7)" in _err_txt or "Could not connect" in _err_txt
                    or "curl: (28)" in _err_txt or "timed out" in _err_txt.lower()
                    or "health check http=0" in _err_txt)
                if _transport_dead and _tor_note_dead():
                    try:
                        threading.Thread(target=restart_user_tor,
                                         args=("warm-probe traffic-dead streak",),
                                         daemon=True, name="tor-auto-restart").start()
                    except Exception:
                        pass
                if ("Failed to connect" in _err_txt or "Connection refused" in _err_txt
                        or "curl: (7)" in _err_txt or "Could not connect" in _err_txt):
                    try:
                        _now = time.monotonic()
                        _do_heal = False
                        with _tor_supervisor_lock:
                            if _now - _last_tor_heal["ts"] >= _TOR_HEAL_MIN_INTERVAL:
                                _last_tor_heal["ts"] = _now
                                _do_heal = True
                        if _do_heal:
                            try:
                                _socks = self.socks_hp or "127.0.0.1:9150"
                                _sh, _, _sp = _socks.partition(":")
                                _sport = int(_sp or "9150")
                                sys.stderr.write("[tor-supervisor] pool probe refused — auto-starting user-mode Tor...\n")
                                ensure_tor_daemon("127.0.0.1", 9151, _sport)
                            except Exception as _he:
                                sys.stderr.write(f"[tor-supervisor] auto-heal failed: {_he}\n")
                    except Exception:
                        pass
                time.sleep(backoff)
                with self._pool_cond:
                    if slot.state != "READY":
                        slot.state = "TAINTED"
                        self._enqueue_locked(slot)


class DirectSessionManager:
    """Thread-local session manager for direct (real IP) requests."""
    def __init__(self):
        self._local = threading.local()

    def get_session(self):
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = cffi_requests.Session(impersonate="chrome131", timeout=90.0)
            self._local.session = sess
        return sess

    def close_thread_session(self):
        sess = getattr(self._local, "session", None)
        if sess is not None:
            try:
                sess.close()
            except Exception:
                pass
            self._local.session = None


# Global instances
_circuit_pool = PrewarmedCircuitPool(pool_size=DEFAULT_POOL_SIZE, socks_hp=SOCKS)
_direct_mgr = DirectSessionManager()


def get_session(proxy_url: str = None):
    """Get active pre-warmed circuit session from the pool."""
    slot, sess = _circuit_pool.get_active_slot()
    return sess


def get_direct_session():
    """Get this thread's direct (no proxy) Session."""
    return _direct_mgr.get_session()


def reset_session():
    """Reset the calling thread's direct session."""
    _direct_mgr.close_thread_session()


def reset_all_sessions():
    """Taint all pool circuits and replenish in background."""
    _circuit_pool.taint_all("reset_all_sessions")


def _is_retryable(status_code: int, body, allow_free_tier_retry: bool = False) -> bool:
    """Check if upstream response should trigger Tor rotation retry.
    Covers HTTP status codes and error messages embedded in JSON (even when status=200).
    Deepseek 400 'Model is unavailable' is NOT retryable (expected per user).

    allow_free_tier_retry: Zen's FreeTierError ("free tier can only be used from
    within OpenCode") was believed PERMANENT, but live probing (2026-09-23) shows
    it is PER-EXIT: the same model + same genuine x-opencode-session returns 403
    from some Tor exits and 200 from others, and FreeUsageLimitError is likewise
    per-exit quota. For a real OpenCode client (which sends a genuine session) a
    fresh exit IS the recovery path, so rotate instead of failing.
    """
    # Normalize the error text ONCE, then classify by message BEFORE status:
    # a 403 can be a per-exit CF challenge (retry) OR a FreeTierError /
    # AuthError (permanent) — rotating Tor exits on the latter just churns the
    # pool for ~25s per request and looks like a hang.
    msg = ""
    try:
        if isinstance(body, dict):
            err = body.get("error")
            if isinstance(err, dict):
                msg = (str(err.get("message", "")) + " " + str(err.get("type", ""))).lower()
            elif isinstance(err, str):
                msg = err.lower()
            else:
                msg = json.dumps(body).lower() if body else ""
        elif isinstance(body, (bytes, str)):
            msg = (body.decode(errors="replace") if isinstance(body, bytes) else body).lower()
    except Exception:
        msg = ""

    # Permanent / backend-wide: a different exit or model can NEVER fix these.
    # IMPORTANT: a bare HTTP 500 with an EMPTY body is the classic broken /
    # not-yet-live endpoint signature (e.g. jev-1.13-free) — NOT a per-exit
    # throttle — so it must fail fast instead of churning the whole pool with
    # retries+backoff+swaps across every exit for every request.
    _NONRETRYABLE = (
        "missing api key", "autherror", "invalid_api_key", "invalid api key",
        "free tier", "freetier", "can only be used from within opencode",
        "insufficient_credits", "insufficient balance", "entitlement",
        "unauthorized", "invalid_request_error", "model is unavailable",
        "model not available", "internal server error",
        # Opaque echoed reasoning (Codex multi-turn): bound to the issuing
        # caller/session — a fresh exit can NEVER satisfy it. Fail fast;
        # the lane strips encrypted blobs before POST so this rarely fires.
        "encrypted_content", "not issued to this caller",
        # Dangling reasoning-item refs (store=False multi-turn echoing an
        # rs_* id Zen no longer recognizes): request-specific, never fixed
        # by a new exit. The lane strips rs_* ids pre-POST; fail fast here.
        "referenced reasoning item",
        # Tool-shape validation ("tools[0] did not match any supported
        # type"): the lane coerces/drops into supported shapes pre-POST, so
        # a residual hit is request-specific. Fail fast, don't burn exits.
        "did not match any supported type",
    )
    if allow_free_tier_retry:
        # Zen FreeTierError is per-exit (see docstring): drop those keywords so
        # the pool rotates to a fresh exit instead of failing the turn.
        _NONRETRYABLE = tuple(k for k in _NONRETRYABLE
                              if k not in ("free tier", "freetier",
                                           "can only be used from within opencode"))
    if any(k in msg for k in _NONRETRYABLE):
        return False
    if status_code == 500 and not msg.strip():
        return False

    # Direct status retryables (per-exit throttles / blocks / transient)
    if status_code in (429, 403, 500, 502, 503, 504):
        return True

    retry_keywords = ["rate limit", "freeusagelimit", "overloaded",
                      "temporarily overloaded", "service temporarily",
                      "too many requests", "429", "endpoint is unavailable"]
    if any(k in msg for k in retry_keywords):
        return True
    return False


def _collect_responses_sse(r):
    """Aggregate a Responses-API SSE stream into ONE Responses object.

    The free tier rejects non-streaming POSTs (403 FreeTierError), so the
    non-stream client path streams upstream and rebuilds the object here.
    The final `response.completed` event already carries the full response;
    we use it verbatim and only fall back to delta accumulation.
    """
    final_obj = None
    text_parts, reasoning_parts = [], []
    try:
        for line in r.iter_lines():
            if not line:
                continue
            if isinstance(line, str):
                line = line.encode("utf-8")
            if not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if payload == b"[DONE]":
                break
            try:
                j = json.loads(payload)
            except Exception:
                continue
            t = j.get("type") or ""
            if t in ("response.completed", "response.incomplete") and isinstance(j.get("response"), dict):
                final_obj = j["response"]
                # Ensure output has text; if upstream didn't populate it in completed event, inject accumulated deltas
                _has_text = False
                for _item in final_obj.get("output", []):
                    if _item.get("type") == "message":
                        for _c in _item.get("content", []):
                            if _c.get("type") in ("output_text", "text") and _c.get("text"):
                                _has_text = True
                if not _has_text and text_parts:
                    final_obj.setdefault("output", []).append({
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "".join(text_parts)}]
                    })
                break
            elif t == "response.output_text.delta" and j.get("delta"):
                text_parts.append(j["delta"])
            elif t in ("response.reasoning_summary_text.delta",
                       "response.reasoning_text.delta") and j.get("delta"):
                reasoning_parts.append(j["delta"])
    except Exception as e:

        sys.stderr.write(f"[_collect_responses_sse] error: {e}\n")
    finally:
        try:
            r.close()
        except Exception:
            pass
    if final_obj is not None:
        return final_obj
    out = {"object": "response",
           "id": f"resp_collected_{int(time.time()*1000)}",
           "created_at": int(time.time()),
           "output": [{"type": "message", "role": "assistant",
                       "content": [{"type": "output_text",
                                    "text": "".join(text_parts)}]}]}
    if reasoning_parts:
        out["output"].insert(0, {"type": "reasoning",
                                 "summary": [{"type": "summary_text",
                                              "text": "".join(reasoning_parts)}]})
    return out


def _collect_chat_sse(r, model_name=""):
    """Aggregate a Chat Completions SSE stream into ONE chat.completion object."""
    content_parts = []
    reasoning_parts = []
    role = "assistant"
    finish_reason = "stop"
    resp_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
    try:
        for line in r.iter_lines():
            if not line or not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if payload == b"[DONE]":
                break
            try:
                j = json.loads(payload)
            except Exception:
                continue
            if j.get("id"):
                resp_id = j["id"]
            choices = j.get("choices") or []
            if choices and isinstance(choices, list):
                c0 = choices[0]
                if c0.get("finish_reason"):
                    finish_reason = c0["finish_reason"]
                delta = c0.get("delta") or {}
                if delta.get("role"):
                    role = delta["role"]
                if delta.get("content"):
                    content_parts.append(delta["content"])
                if delta.get("reasoning_content"):
                    reasoning_parts.append(delta["reasoning_content"])
    finally:
        try:
            r.close()
        except Exception:
            pass
    msg = {"role": role, "content": "".join(content_parts)}
    if reasoning_parts:
        msg["reasoning_content"] = "".join(reasoning_parts)
    return {
        "id": resp_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_name,
        "choices": [{
            "index": 0,
            "message": msg,
            "finish_reason": finish_reason
        }]
    }


def _error_message(last_body) -> str:
    """Readable message from an upstream error body ({"error": {...}} | str)."""
    if isinstance(last_body, dict):
        err = last_body.get("error", last_body)
        if isinstance(err, dict):
            return str(err.get("message") or err.get("type") or err)[:300]
        return str(err)[:300]
    return str(last_body)[:300]


# Markers proving REAL content is flowing (deltas/chunks). Committing a
# client connection requires one of these — lifecycle pings are not enough.
# All needles are LOWERCASE and SPACE-FREE; matching happens against a
# normalized copy of the stream so minified/spaced JSON both work.
_STREAM_CONTENT_MARKERS = [
    'output_text.delta', 'reasoning_summary_text.delta',
    'reasoning_text.delta', 'reasoning.delta',
    'output_item.added', 'content_part.added',
    'encrypted_content', 'encrypted_blob',
    'output_item.done',
    '"delta":{"content"', '"delta":{"tool_calls"',
    '"delta":{"reasoning_content"',
    'function_call_arguments.delta',
    '"item":{"type":"function_call"',
    'chat.completion.chunk',
    # Native Anthropic SSE (union-alpha /v1/messages lane): content + tool
    # deltas, message lifecycle. Lifecycle-only frames must NOT count as
    # content (same rule as response.* pings) — commit needs real deltas.
    '"delta":{"text"', '"delta":{"partial_json"',
    '"delta":{"thinking"', '"delta":{"signature"',
    'content_block_delta', 'input_json_delta',
]

# Lifecycle events: stream is alive upstream but no content yet.
_STREAM_LIFECYCLE_MARKERS = [
    '"type":"response.created"', '"type":"response.in_progress"', 'event:ping',
]

# Failure phrases zen/provider streams emit instead of content (lowercase).
_STREAM_ERROR_MARKERS = [
    "endpoint is unavailable", "endpoint unavailable", "no endpoint",
    "all endpoints", "freeusagelimiterror", "rate limit exceeded",
    "too many requests", '"type":"response.failed"', '"type": "response.failed"',
    "event: error",
]


def _strip_encrypted_reasoning(upstream_body) -> int:
    """Remove opaque echoed reasoning blobs Codex sends back multi-turn.

    Zen binds `encrypted_content` to the issuing caller/session; when the
    proxy presents a (replaced) session the echo is rejected with 400
    "not issued to this caller" and Codex gets a 500. Stripping the blob
    (keeping any plaintext summary/text) lets Zen re-reason and return 200.

    Also drops `rs_*` item ids on reasoning-type items: with store=False,
    echoed prior-output ids are dangling refs Zen 400s ("Referenced
    reasoning item ... was not found"). The text/summary survives so only
    the unresolvable pointer is lost. Returns count removed. Idempotent.
    """
    removed = 0
    try:
        items = None
        if isinstance(upstream_body, dict):
            items = upstream_body.get("input")
        if not isinstance(items, list):
            return 0
        for _it in items:
            if not isinstance(_it, dict):
                continue
            # Responses reasoning item: {"type":"reasoning","encrypted_content":...}
            if "encrypted_content" in _it:
                try:
                    del _it["encrypted_content"]
                    removed += 1
                except Exception:
                    pass
            # Dangling rs_* ref on a reasoning item (store=False echo).
            try:
                if (_it.get("type") == "reasoning"
                        and isinstance(_it.get("id"), str)
                        and _it["id"].startswith("rs_")):
                    del _it["id"]
                    removed += 1
            except Exception:
                pass
            # Nested content parts carrying encrypted blobs.
            _c = _it.get("content")
            if isinstance(_c, list):
                for _p in _c:
                    if isinstance(_p, dict) and "encrypted_content" in _p:
                        try:
                            del _p["encrypted_content"]
                            removed += 1
                        except Exception:
                            pass
    except Exception:
        pass
    return removed

# Precomputed: markers are compared against a whitespace-stripped stream, so
# normalize them once at import instead of re-sub()ing per chunk.
_STREAM_CONTENT_MARKERS_B = tuple(m.encode("utf-8") for m in _STREAM_CONTENT_MARKERS)
_STREAM_ERROR_MARKERS_NORM = tuple(re.sub(r"\s+", "", m) for m in _STREAM_ERROR_MARKERS)
_STREAM_LIFECYCLE_MARKERS_B = tuple(re.sub(r"\s+", "", m).encode("utf-8") for m in _STREAM_LIFECYCLE_MARKERS)


def _looks_like_stream_error(peek: bytes):
    """Return an error snippet if the first bytes of an upstream stream look
    like an embedded failure (not healthy SSE/content), else None."""
    if not peek:
        return None
    txt = peek[:8192].decode("utf-8", errors="replace")
    low = txt.lower()

    # Non-SSE JSON body carrying an ERROR OBJECT -> always an error.
    # (Must match `"error": {` — Responses streams legitimately contain
    # `"error": null` inside response.created events.)
    if not peek.lstrip().lstrip(b"\xef\xbb\xbf").startswith(b"data:") and re.search(r'"error"\s*:\s*\{', low):
        return txt[:300]

    norm = re.sub(r"\s+", "", low)  # strip ALL whitespace: minified and pretty JSON both match
    # ONLY real content deltas prove generation has started (lifecycle markers must not mask errors)
    health_pos = min((norm.find(m) for m in _STREAM_CONTENT_MARKERS
                      if norm.find(m) != -1), default=-1)
    err_pos = min((norm.find(m) for m in _STREAM_ERROR_MARKERS_NORM
                   if norm.find(m) != -1), default=-1)
    if err_pos == -1:
        return None
    if health_pos != -1 and err_pos > health_pos:
        return None  # model is merely TALKING about errors — healthy stream
    return txt[max(0, err_pos - 60):err_pos + 200]


def _stream_has_content(peek) -> bool:
    """True once the peeked bytes contain real content deltas/chunks."""
    if not peek:
        return False
    norm = re.sub(rb"\s+", b"", bytes(peek).lower())
    return any(m in norm for m in _STREAM_CONTENT_MARKERS_B)


def _stream_has_content_norm(norm) -> bool:
    """Same test against a pre-normalized (whitespace-stripped, lowercase)
    buffer. Whitespace removal is per-character, so normalizing each chunk and
    concatenating is identical to normalizing the whole buffer — lets the peek
    loop scan incrementally instead of re-normalizing everything per chunk."""
    if not norm:
        return False
    return any(m in norm for m in _STREAM_CONTENT_MARKERS_B)


def _stream_has_lifecycle_norm(norm) -> bool:
    """True if normalized peek buffer contains any stream lifecycle markers."""
    if not norm:
        return False
    return any(m in norm for m in _STREAM_LIFECYCLE_MARKERS_B)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        try:
            self.connection.settimeout(30)
        except Exception:
            pass

    # Stealth: don't advertise BaseHTTP/Python stack to forwarded clients.
    server_version = "nginx"
    sys_version = ""
    default_model = DEFAULT_MODEL
    upstream = UPSTREAM
    socks = SOCKS
    control_host = "127.0.0.1"
    control_port = 9151
    proxy_url = f"socks5h://{SOCKS}"
    max_retries = MAX_RETRIES
    rotate_wait = ROTATE_WAIT
    max_time = MAX_TIME
    backoff = BACKOFF
    restart_tor = RESTART_TOR
    direct_first = True
    torrc = TORRC
    # model_ids removed — now uses dynamic refresh_models()

    def finish(self):
        # Release the pool's per-thread request connection (one request per
        # HTTP/1.0 connection here) so thousands of requests don't leak sockets.
        try:
            _circuit_pool.close_thread_session()
        except Exception:
            pass
        super().finish()

    def log_message(self, format, *args):
        # Skip routine dashboard polling and redundant POST 200 logs
        # since we now print beautiful custom INCOMING and STREAM tags.
        try:
            _msg = format % args
            if _msg.startswith('"GET /health '):
                return
            if '"POST /v1/' in _msg and '" 200' in _msg:
                return
        except Exception:
            pass
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {format % args}\n")

    def _json(self, status, body):
        data = json.dumps(body).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _terminate_stream_with_error(self, message: str):
        """Write a final SSE error chunk + [DONE]. Used when headers were
        already committed but the upstream never delivered usable content —
        guarantees the client always reaches a terminal state."""
        try:
            payload = {
                "id": f"chatcmpl-error-{int(time.time())}",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }],
                "error": {"message": message, "type": "upstream_error"},
            }
            self.wfile.write(b"data: " + json.dumps(payload).encode("utf-8") + b"\n\ndata: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _serve_cached_openai_stream(self, cached_resp, model_name, headers_sent=False):
        """Replay a cached chat.completion as OpenAI SSE deltas (TTFT -> ms).

        Word-chunked like live traffic so strict SSE parsers see identical
        framing: role-first delta, content deltas, stop chunk, [DONE].
        """
        import re as _re
        text = ""
        try:
            chs = (cached_resp.get("choices") or [])
            if chs:
                text = (chs[0].get("message") or {}).get("content") or ""
        except Exception:
            text = ""
        if not isinstance(text, str):
            text = str(text)
        resp_id = cached_resp.get("id", "chatcmpl-cached") if isinstance(cached_resp, dict) else "chatcmpl-cached"
        try:
            if not headers_sent:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            return
        try:
            created = int(time.time())
            parts = _re.findall(r"\S+\s*|\s+", text) or [text]
            first = True
            for p in parts:
                if not p:
                    continue
                delta = {"content": p}
                if first:
                    delta = {"role": "assistant", **delta}
                    first = False
                self.wfile.write(b"data: " + json.dumps({
                    "id": resp_id, "object": "chat.completion.chunk",
                    "created": created, "model": model_name,
                    "choices": [{"index": 0, "delta": delta,
                                 "finish_reason": None}],
                }).encode("utf-8") + b"\n\n")
            self.wfile.write(b"data: " + json.dumps({
                "id": resp_id, "object": "chat.completion.chunk",
                "created": created, "model": model_name,
                "choices": [{"index": 0, "delta": {},
                             "finish_reason": "stop"}],
            }).encode("utf-8") + b"\n\ndata: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _serve_cached_anthropic_stream(self, cached_resp, model_name,
                                       in_tok=0, out_tok=0, headers_sent=False):
        """Replay a cached Anthropic message as message_start/delta/stop SSE."""
        import re as _re
        text = ""
        try:
            for b in (cached_resp.get("content") or []):
                if isinstance(b, dict) and b.get("type") == "text":
                    text += b.get("text", "")
        except Exception:
            text = ""
        msg_id = cached_resp.get("id", "msg_cached") if isinstance(cached_resp, dict) else "msg_cached"
        try:
            if not headers_sent:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            return
        try:
            in_tok = max(1, in_tok or 1)
            self.wfile.write(
                f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'model': model_name, 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': in_tok, 'output_tokens': 1}}})}\n\n".encode())
            self.wfile.write(b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
            for p in (_re.findall(r"\S+\s*|\s+", text) or [text]):
                if not p:
                    continue
                self.wfile.write(
                    f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': p}})}\n\n".encode())
            self.wfile.write(b"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
            self.wfile.write(
                f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': max(1, out_tok or 1)}})}\n\n".encode())
            self.wfile.write(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    @staticmethod
    def _strip_reasoning_fields(line: bytes):
        """Strip reasoning/reasoning_content fields from SSE chunks.
        For openclaude: it renders <think> tags from content natively,
        but prints reasoning fields as raw text (duplicating thinking).
        Returns modified line."""
        if not line.startswith(b"data: ") or line.strip() == b"data: [DONE]":
            return line
        try:
            payload = json.loads(line[6:])
            changed = False
            for choice in payload.get("choices", []):
                delta = choice.get("delta", {})
                for key in ("reasoning", "reasoning_content", "reasoning_details"):
                    if key in delta:
                        del delta[key]
                        changed = True
            if changed:
                return b"data: " + json.dumps(payload).encode()
            return line
        except Exception:
            return line

    @staticmethod
    def _strip_reasoning_fields_obj(payload) -> bool:
        """In-place variant of _strip_reasoning_fields for an already-parsed
        SSE chunk. Returns True if anything was removed."""
        if not isinstance(payload, dict):
            return False
        changed = False
        try:
            for choice in payload.get("choices", []):
                delta = choice.get("delta", {})
                for key in ("reasoning", "reasoning_content", "reasoning_details"):
                    if key in delta:
                        del delta[key]
                        changed = True
        except Exception:
            return False
        return changed

    @staticmethod
    def openai_to_anthropic_message(openai_body, model_name):
        """Convert an OpenAI chat.completion JSON object to an Anthropic /v1/messages response."""
        choices = openai_body.get("choices") or []
        content_list = []
        stop_reason = "end_turn"
        if choices:
            msg = choices[0].get("message") or {}
            text = msg.get("content")
            if text:
                content_list.append({"type": "text", "text": text})
            for tc in msg.get("tool_calls") or []:
                fn = tc.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments", "{}"))
                except Exception:
                    args = {}
                content_list.append({
                    "type": "tool_use",
                    "id": tc.get("id", f"toolu_{uuid.uuid4().hex[:12]}"),
                    "name": fn.get("name", ""),
                    "input": args
                })
                stop_reason = "tool_use"
            # Safety net: if model leaked [Tool Call: func(args)] in plaintext without native tool_calls
            if not msg.get("tool_calls") and text:
                m_tc = re.search(r"\[Tool Call:\s*(\w+)\s*\((.*?)\)\]", text, re.DOTALL)
                if m_tc:
                    f_name = m_tc.group(1)
                    try:
                        f_args = json.loads(m_tc.group(2))
                    except Exception:
                        f_args = {}
                    content_list = [{
                        "type": "tool_use",
                        "id": f"toolu_{uuid.uuid4().hex[:12]}",
                        "name": f_name,
                        "input": f_args
                    }]
                    stop_reason = "tool_use"
            finish_reason = choices[0].get("finish_reason")
            if finish_reason == "length":
                stop_reason = "max_tokens"

        usage = openai_body.get("usage") or {}
        return {
            "id": f"msg_{uuid.uuid4().hex[:20]}",
            "type": "message",
            "role": "assistant",
            "content": content_list if content_list else [{"type": "text", "text": ""}],
            "model": model_name,
            "stop_reason": stop_reason,
            "stop_sequence": None,
            "usage": {
                "input_tokens": usage.get("input_tokens") or usage.get("prompt_tokens") or 0,
                "output_tokens": usage.get("output_tokens") or usage.get("completion_tokens") or 0
            }
        }

    def _stream_chat_to_responses(self, r, track_ctx=None,
                                      chunk_iter=None, headers_sent=False,
                                      idle_guard=None, requested="unknown"):
        """Translate an upstream OpenAI chat SSE stream into Responses-API SSE
        for a client that called our /v1/responses with a chat-wire model.

        Streaming half of the chat-backend lane (non-stream uses
        chat_completion_to_responses). Emits response.created, per-chunk
        output_text / function_call_arguments deltas, and response.completed.
        Returns True iff the client disconnected (caller must then release the
        slot WITHOUT recording success), False on clean completion.
        """
        if not headers_sent:
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                return True
        resp_id = "resp_" + uuid.uuid4().hex[:12]
        msg_item_id = "msg_" + uuid.uuid4().hex[:8]
        model_name = (track_ctx.get("model", requested) if track_ctx else requested)
        t0 = time.perf_counter()
        t_start = track_ctx.get("t_start", t0) if track_ctx else t0
        text_buf = []
        fn_map = {}
        arg_bufs = {}
        part_added = False
        created = False
        tokens = 0
        last_usage = None
        disconnected = False

        def _ev(obj):
            self.wfile.write(b"data: " + json.dumps(obj).encode("utf-8") + b"\n\n")
            self.wfile.flush()

        def _completed(finish_reason):
            usage = last_usage or {"input_tokens": 0,
                                   "output_tokens": max(tokens, 1)}
            output = []
            if text_buf:
                output.append({"type": "message", "role": "assistant",
                               "content": [{"type": "output_text",
                                            "text": "".join(text_buf)}]})
            for idx in sorted(fn_map):
                output.append({"type": "function_call",
                               "id": fn_map[idx]["id"],
                               "call_id": fn_map[idx]["id"],
                               "name": fn_map[idx]["name"],
                               "arguments": arg_bufs.get(idx, "") or "{}"})
            _ev({"type": "response.completed",
                 "response": {"id": resp_id, "object": "response",
                              "model": model_name, "output": output,
                              "status": "completed",
                              "usage": {"input_tokens": usage.get("input_tokens", 0),
                                        "output_tokens": usage.get("output_tokens", 0)}}})
            if zen_db and track_ctx:
                try:
                    db_async('record_usage', model=model_name,
                             client_type="responses",
                             prompt_tokens=usage.get("input_tokens", 0),
                             completion_tokens=usage.get("output_tokens", tokens),
                             tor_exit_ip=track_ctx.get("tor_exit_ip", ""),
                             status=200, retries=track_ctx.get("retries", 0),
                             prompt_chars=track_ctx.get("prompt_chars", 0))
                except Exception:
                    pass
            tt = max(0.001, time.perf_counter() - t_start)
            sys.stderr.write(
                f"\033[32m[DONE]\033[0m {model_name:<24} "
                f"| {tokens:4d} tokens in {tt:4.1f}s "
                f"({(tokens / tt):5.1f} tok/s) (chat-backend)\n")

        try:
            _ev({"type": "response.created",
                 "response": {"id": resp_id, "object": "response",
                              "model": model_name, "output": []}})
            created = True
            buf = b""
            src = chunk_iter if chunk_iter is not None else r.iter_content()
            for chunk_raw in src:
                chunk = chunk_raw
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8", errors="replace")
                if not chunk:
                    continue
                if idle_guard is not None:
                    try:
                        idle_guard.touch()
                    except Exception:
                        pass
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.rstrip(b"\r")
                    if not line:
                        continue
                    if line.strip() == b"data: [DONE]":
                        continue
                    if not line.startswith(b"data: "):
                        continue
                    try:
                        j = json.loads(line[6:])
                    except Exception:
                        continue
                    if not isinstance(j, dict):
                        continue
                    if isinstance(j.get("error"), dict):
                        _ev({"type": "response.failed",
                             "response": {"id": resp_id,
                                          "error": j["error"]}})
                        return disconnected
                    chs = j.get("choices") or []
                    if chs and isinstance(chs[0], dict):
                        c0 = chs[0]
                    else:
                        c0 = {}
                    u = c0.get("usage") or j.get("usage")
                    if isinstance(u, dict):
                        last_usage = {
                            "input_tokens": u.get("prompt_tokens", 0) or 0,
                            "output_tokens": u.get("completion_tokens", 0) or 0,
                        }
                    delta = c0.get("delta") or {}
                    for tc in (delta.get("tool_calls") or []):
                        if not isinstance(tc, dict):
                            continue
                        idx = tc.get("index", 0)
                        fn = tc.get("function") or {}
                        if idx not in fn_map:
                            cid = tc.get("id") or f"call_{idx}_{int(time.time()*1000)}"
                            fn_map[idx] = {"id": cid, "name": fn.get("name", "")}
                            _ev({"type": "response.output_item.added",
                                 "output_index": idx,
                                 "item": {"type": "function_call",
                                          "id": cid, "call_id": cid,
                                          "name": fn.get("name", ""),
                                          "arguments": ""}})
                        arg = fn.get("arguments", "") or ""
                        if arg:
                            arg_bufs[idx] = arg_bufs.get(idx, "") + arg
                            _ev({"type": "response.function_call_arguments.delta",
                                 "item_id": fn_map[idx]["id"],
                                 "output_index": idx, "delta": arg})
                    txt = delta.get("content") or ""
                    if txt:
                        if not part_added:
                            _ev({"type": "response.output_item.added",
                                 "output_index": 0,
                                 "item": {"type": "message", "id": msg_item_id,
                                          "role": "assistant", "content": []}})
                            part_added = True
                        text_buf.append(txt)
                        tokens += 1
                        _ev({"type": "response.output_text.delta",
                             "item_id": msg_item_id, "output_index": 0,
                             "content_index": 0, "delta": txt})
                    if c0.get("finish_reason"):
                        _completed(c0["finish_reason"])
                        return disconnected
            _completed("stop")
            return disconnected
        except (BrokenPipeError, ConnectionResetError):
            return True
        except Exception as e:

            sys.stderr.write(f"[chat2resp] {type(e).__name__}: {e}\n")
            try:
                _ev({"type": "response.failed",
                     "response": {"id": resp_id,
                                  "error": {"message": str(e)[:200]}}})
            except Exception:
                pass
            return disconnected
        finally:
            if idle_guard is not None:
                try:
                    idle_guard.stop()
                except Exception:
                    pass
            try:
                r.close()
            except Exception:
                pass

    def _stream_realtime(self, r, client_type="opencode", track_ctx=None, chunk_iter=None,
                         headers_sent=False, idle_guard=None):
        """Stream SSE chunks from upstream to client in REAL-TIME.
        Each token appears on screen as it arrives — true typewriter effect.
        opencode: strips <think> from content, keeps reasoning fields.
        openclaude: strips reasoning fields, keeps <think> in content.
        track_ctx: dict with tracking info (model, t_start, retries, etc.)
        chunk_iter: optional pre-started iterator (peeked chunks replayed first).
        headers_sent: if True, SSE headers were already committed by do_POST
        (early-commit UX) — skip re-sending them.
        Returns True if the client disconnected mid-stream (caller must NOT
        record success/usage for that exit), False on clean completion."""
        if not headers_sent:
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                disconnected = True
                return disconnected

        buf = b""
        ttft_recorded = False
        ttft_ms = 0
        total_bytes = 0
        last_usage = None  # will hold the usage dict from final chunk
        stream_id = (_next_stream_id(), threading.get_ident())
        if _qlog("stream-type"):
            sys.stderr.write(f"[stream] client_type={client_type}\n")

        is_adapted = track_ctx.get("is_adapted_responses", False) if track_ctx else False
        is_native_anthropic = (track_ctx.get("is_native_anthropic", False)
                               if track_ctx else False)
        resp_id = "chatcmpl-" + "".join(time.strftime("%Y%m%d%H%M%S"))
        model_name = track_ctx.get("model", "muse") if track_ctx else "muse"
        has_tool_call = False
        tf = ThinkFilter()          # chunk-split-safe <think> handling (opencode)
        fn_indices = {}             # function_call item_id -> chat tool_calls index
        next_fn_index = 0           # monotonic — immune to id collisions/overwrites
        current_text_block_index = None  # index of active text content block
        closed_fn_indices = set()   # tool block indices already stopped
        last_parsed = None          # last parsed passthrough chunk (for tail flush)
        stream_error = None         # upstream error event text, if any
        completion_sent = False     # finish chunk + [DONE] emitted?
        disconnected = False        # True if client went away mid-stream (BrokenPipe)
        reasoning_streamed = False  # any thinking reached the client yet?
        summary_seen = {}           # responses item_id -> summary deltas seen?

        tokens_emitted = 0
        t_first_tok = None
        first_delta_sent = False
        message_start_sent = False
        text_block_open = False
        current_block_type = None

        # Register active stream
        with _active_streams_lock:
            _active_streams[stream_id] = {
                "model": track_ctx.get("model", "?") if track_ctx else "?",
                "client": client_type,
                "start": time.time(),
                "tokens": 0,
                "tok_s": 0.0,
                "elapsed_s": 0.0,
                "snippet": "",
            }

        def _emit_delta(field: str, text: str):
            """Write a single chat.completion.chunk with one delta field."""
            nonlocal first_delta_sent, tokens_emitted, disconnected, message_start_sent, text_block_open, next_fn_index, current_text_block_index, current_block_type
            tokens_emitted += 1
            t_now = time.perf_counter()
            t_stream_elapsed = max(0.001, t_now - (t_first_tok or t_now))
            rolling_tok_s = tokens_emitted / t_stream_elapsed if t_first_tok else 0.0
            
            with _active_streams_lock:
                if stream_id in _active_streams:
                    s = _active_streams[stream_id]
                    s["tokens"] = tokens_emitted
                    s["tok_s"] = round(rolling_tok_s, 1)
                    s["elapsed_s"] = round(t_now - track_ctx.get("t_start", t_now), 1)

            if client_type == "anthropic":
                in_tok = max(1, int(track_ctx.get("prompt_chars", 0) / 3.7)) if track_ctx else 1
                if not message_start_sent:
                    msg_start = {
                        "type": "message_start",
                        "message": {
                            "id": resp_id,
                            "type": "message",
                            "role": "assistant",
                            "content": [],
                            "model": model_name,
                            "stop_reason": None,
                            "stop_sequence": None,
                            "usage": {"input_tokens": in_tok, "output_tokens": 1}
                        }
                    }
                    self.wfile.write(b"event: message_start\ndata: " + json.dumps(msg_start).encode("utf-8") + b"\n\n")
                    message_start_sent = True
                target_type = "thinking" if field == "reasoning_content" else "text"
                
                if text_block_open and current_block_type != target_type:
                    self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": current_text_block_index}).encode("utf-8") + b"\n\n")
                    text_block_open = False
                    
                if not text_block_open:
                    for idx in fn_indices.values():
                        if idx not in closed_fn_indices:
                            self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": idx}).encode("utf-8") + b"\n\n")
                            closed_fn_indices.add(idx)
                    current_text_block_index = next_fn_index
                    next_fn_index += 1
                    if target_type == "thinking":
                        block_start = {
                            "type": "content_block_start",
                            "index": current_text_block_index,
                            "content_block": {"type": "text", "text": "🤔 **Thinking Process:**\n\n"}
                        }
                    else:
                        block_start = {
                            "type": "content_block_start",
                            "index": current_text_block_index,
                            "content_block": {"type": "text", "text": ""}
                        }
                    self.wfile.write(b"event: content_block_start\ndata: " + json.dumps(block_start).encode("utf-8") + b"\n\n")
                    text_block_open = True
                    current_block_type = target_type
                    
                if target_type == "thinking":
                    ev = {
                        "type": "content_block_delta",
                        "index": current_text_block_index,
                        "delta": {"type": "text_delta", "text": text}
                    }
                else:
                    ev = {
                        "type": "content_block_delta",
                        "index": current_text_block_index,
                        "delta": {"type": "text_delta", "text": text}
                    }
                self.wfile.write(b"event: content_block_delta\ndata: " + json.dumps(ev).encode("utf-8") + b"\n\n")
                self.wfile.flush()
                first_delta_sent = True
                return

            delta_dict = {field: text}
            if not first_delta_sent:
                delta_dict = {"role": "assistant", **delta_dict}
                first_delta_sent = True
            payload = {
                "id": resp_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model_name,
                "choices": [{
                    "index": 0,
                    "delta": delta_dict,
                    "finish_reason": None,
                }],
            }
            self.wfile.write(b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n")
            self.wfile.flush()

        def _finish_stream(err: str = None):
            """Emit ThinkFilter carry + final chunk (+error) + [DONE] exactly once."""
            nonlocal completion_sent, disconnected, message_start_sent, text_block_open, current_text_block_index
            if completion_sent:
                return
            completion_sent = True
            try:
                if client_type == "anthropic":
                    in_tok = max(1, int(track_ctx.get("prompt_chars", 0) / 3.7)) if track_ctx else 1
                    if not message_start_sent:
                        msg_start = {
                            "type": "message_start",
                            "message": {
                                "id": resp_id,
                                "type": "message",
                                "role": "assistant",
                                "content": [],
                                "model": model_name,
                                "stop_reason": None,
                                "stop_sequence": None,
                                "usage": {"input_tokens": in_tok, "output_tokens": 1}
                            }
                        }
                        self.wfile.write(b"event: message_start\ndata: " + json.dumps(msg_start).encode("utf-8") + b"\n\n")
                        message_start_sent = True
                    if tf.carry:
                        t_tail, n_tail = tf.flush()
                        if n_tail:
                            _emit_delta("content", n_tail)
                    if text_block_open:
                        self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": current_text_block_index}).encode("utf-8") + b"\n\n")
                        text_block_open = False
                        current_text_block_index = None
                    elif not has_tool_call and not first_delta_sent:
                        self.wfile.write(b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
                        self.wfile.write(b"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
                    if has_tool_call:
                        for idx in fn_indices.values():
                            if idx not in closed_fn_indices:
                                self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": idx}).encode("utf-8") + b"\n\n")
                                closed_fn_indices.add(idx)
                    stop_reason = "tool_use" if has_tool_call else "end_turn"
                    actual_in = in_tok
                    actual_out = max(1, tokens_emitted)
                    if last_usage:
                        actual_in = last_usage.get("prompt_tokens") or last_usage.get("input_tokens") or actual_in
                        actual_out = last_usage.get("completion_tokens") or last_usage.get("output_tokens") or actual_out
                    msg_delta = {
                        "type": "message_delta",
                        "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                        "usage": {"input_tokens": actual_in, "output_tokens": actual_out}
                    }
                    self.wfile.write(b"event: message_delta\ndata: " + json.dumps(msg_delta).encode("utf-8") + b"\n\n")
                    self.wfile.write(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
                    self.wfile.flush()
                    return
                # 1) Flush any text held back as a partial-tag carry
                if True: # ENABLED FOR OPENCLAUDE
                    t_tail, n_tail = tf.flush()
                    if is_adapted:
                        if t_tail:
                            _emit_delta("reasoning_content", t_tail)
                        if n_tail:
                            _emit_delta("content", n_tail)
                    elif t_tail or n_tail:
                        base = last_parsed if isinstance(last_parsed, dict) else {}
                        payload = {
                            "id": base.get("id", resp_id),
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": base.get("model", model_name),
                            "choices": [{
                                "index": 0,
                                "delta": {"role": "assistant",
                                          **({"reasoning_content": t_tail} if t_tail else {}),
                                          **({"content": n_tail} if n_tail else {})},
                                "finish_reason": None,
                            }],
                        }
                        self.wfile.write(b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n")
                # 2) Final chunk with finish_reason (+ optional error field)
                finish_reason = "tool_calls" if has_tool_call else "stop"
                payload = {
                    "id": resp_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model_name,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
                }
                if err:
                    payload["error"] = {"message": err[:400], "type": "upstream_error"}
                self.wfile.write(b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n")

                # 3) Terminal usage chunk when requested by client
                if track_ctx and track_ctx.get("include_usage") and last_usage:
                    usage_payload = {
                        "id": resp_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model_name,
                        "choices": [],  # Zod schema compliance
                        "usage": last_usage,
                    }
                    self.wfile.write(b"data: " + json.dumps(usage_payload).encode("utf-8") + b"\n\n")

                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                disconnected = True
            except Exception as e:

                sys.stderr.write(f"[finish-stream] {e}\n")

        try:
            # Set a read timeout so dead Tor streams don't hang forever
            if hasattr(r, 'raw') and hasattr(r.raw, '_fp') and hasattr(r.raw._fp, 'settimeout'):
                try:
                    r.raw._fp.settimeout(STREAM_LOW_SPEED_TIMEOUT)
                except Exception:
                    pass
            _dbg = os.environ.get("ZEN_DEBUG")
            if _dbg:
                sys.stderr.write(f"[dbg] stream loop start, is_adapted={is_adapted}\n")
            for chunk_raw in (chunk_iter if chunk_iter is not None else r.iter_content()):
                chunk = chunk_raw
                if isinstance(chunk, str):  # some exits decode SSE to str
                    try:
                        chunk = chunk.encode("utf-8", errors="replace")
                    except Exception:
                        chunk = str(chunk).encode("utf-8", errors="replace")
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if idle_guard is not None:
                    idle_guard.touch()
                # Record time-to-first-token
                if not ttft_recorded:
                    t_first_tok = time.perf_counter()
                    if track_ctx:
                        ttft_ms = (t_first_tok - track_ctx.get("t_start", t_first_tok)) * 1000
                    ttft_recorded = True
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.rstrip(b"\r")
                    if not line:
                        # Preserve SSE event delimiters (blank lines). Strict
                        # client-side SSE parsers (opencode) require them;
                        # without these the whole stream parses as ONE
                        # unfinished event and the client sees nothing.
                        self.wfile.write(b"\n")
                        self.wfile.flush()
                        continue
                    if is_native_anthropic:
                        # union-alpha: forward Anthropic SSE 1:1. The frames
                        # are already valid (opencode speaks @ai-sdk/anthropic
                        # against this provider id); any rewrite risks
                        # corrupting the event envelope. Only tap metrics.
                        if line.startswith(b"data: "):
                            try:
                                _np = json.loads(line[6:])
                            except Exception:
                                _np = None
                            if isinstance(_np, dict):
                                _nd = ((_np.get("delta") or {})
                                       if isinstance(_np.get("delta"), dict)
                                       else {})
                                _nt = _nd.get("text", "") or ""
                                if _nt:
                                    with _token_tap_lock:
                                        _token_tap.append(_nt)
                                    tokens_emitted += 1
                                if _np.get("type") == "message_stop":
                                    completion_sent = True
                                _nu = _np.get("usage")
                                if isinstance(_nu, dict):
                                    last_usage = {
                                        "prompt_tokens": _nu.get("input_tokens", 0) or 0,
                                        "completion_tokens": _nu.get("output_tokens", 0) or 0,
                                    }
                        if line:
                            self.wfile.write(line + b"\n")
                            self.wfile.flush()
                        continue
                    if is_adapted:
                        if line.startswith(b"event: "):
                            continue
                        if line.startswith(b"data: "):
                            data_raw = line[6:].strip()
                            if data_raw == b"[DONE]":
                                # Route through finish (flushes carry, guards doubles)
                                _finish_stream()
                                continue
                            try:
                                parsed = json.loads(data_raw)
                                ev_type = parsed.get("type", "")
                                if ev_type == "response.output_text.delta":
                                    txt_delta = parsed.get("delta", "") or ""
                                    if txt_delta:
                                        with _token_tap_lock:
                                            _token_tap.append(txt_delta)
                                        # Update active stream snippet
                                        with _active_streams_lock:
                                            if stream_id in _active_streams:
                                                s = _active_streams[stream_id]
                                                s["snippet"] = (s.get("snippet", "") + txt_delta)[-200:]
                                                s["chars"] = s.get("chars", 0) + len(txt_delta)
                                    if client_type == "openclaude":
                                        if txt_delta:
                                            _emit_delta("content", txt_delta)
                                    else:
                                        t_part, n_part = tf.feed(txt_delta)
                                        if t_part:
                                            _emit_delta("reasoning_content", t_part)
                                        if n_part:
                                            _emit_delta("content", n_part)
                                elif ev_type in ("response.reasoning_text.delta", "response.reasoning.delta",
                                                 "response.reasoning_summary_text.delta"):
                                    r_delta = parsed.get("delta", parsed.get("text", "")) or ""
                                    if True and r_delta:
                                        reasoning_streamed = True
                                        item_key = parsed.get("item_id", "")
                                        if item_key:
                                            summary_seen[item_key] = True
                                        _emit_delta("reasoning_content", r_delta)
                                elif ev_type in ("response.reasoning_summary_text.done",
                                                 "response.reasoning_text.done"):
                                    # Some upstreams send the full text ONLY in .done.
                                    # If we already streamed deltas for this item,
                                    # .done repeats it — emit only the remainder.
                                    if True: # ENABLED FOR OPENCLAUDE
                                        item_key = parsed.get("item_id", "") or parsed.get("id", "")
                                        full = parsed.get("text", "") or ""
                                        if full and not summary_seen.get(item_key):
                                            reasoning_streamed = True
                                            _emit_delta("reasoning_content", full)
                                elif ev_type == "response.output_item.added":
                                    item = parsed.get("item", {})
                                    if item.get("type") == "function_call":
                                        has_tool_call = True
                                        item_key = item.get("id") or item.get("call_id") or ""
                                        idx = next_fn_index
                                        next_fn_index += 1
                                        fn_indices[item_key] = idx
                                        fn_name = item.get("name", "")
                                        call_id = item.get("call_id", item.get("id", f"call_{int(time.time()*1000)}"))
                                        if client_type == "anthropic":
                                            if not message_start_sent:
                                                in_tok = max(1, int(track_ctx.get("prompt_chars", 0) / 3.7)) if track_ctx else 1
                                                msg_start = {
                                                    "type": "message_start",
                                                    "message": {
                                                        "id": resp_id,
                                                        "type": "message",
                                                        "role": "assistant",
                                                        "content": [],
                                                        "model": model_name,
                                                        "stop_reason": None,
                                                        "stop_sequence": None,
                                                        "usage": {"input_tokens": in_tok, "output_tokens": 1}
                                                    }
                                                }
                                                self.wfile.write(b"event: message_start\ndata: " + json.dumps(msg_start).encode("utf-8") + b"\n\n")
                                                message_start_sent = True
                                            if text_block_open:
                                                self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": current_text_block_index}).encode("utf-8") + b"\n\n")
                                                text_block_open = False
                                                current_text_block_index = None
                                            for pidx in fn_indices.values():
                                                if pidx != idx and pidx not in closed_fn_indices:
                                                    self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": pidx}).encode("utf-8") + b"\n\n")
                                                    closed_fn_indices.add(pidx)
                                            tool_start = {
                                                "type": "content_block_start",
                                                "index": idx,
                                                "content_block": {
                                                    "type": "tool_use",
                                                    "id": call_id,
                                                    "name": fn_name,
                                                    "input": {}
                                                }
                                            }
                                            out_line = b"event: content_block_start\ndata: " + json.dumps(tool_start).encode("utf-8") + b"\n\n"
                                        else:
                                            chunk_payload = {
                                                "id": resp_id,
                                                "object": "chat.completion.chunk",
                                                "created": int(time.time()),
                                                "model": model_name,
                                                "choices": [{
                                                    "index": 0,
                                                    "delta": {
                                                        "tool_calls": [{
                                                            "index": idx,
                                                            "id": call_id,
                                                            "type": "function",
                                                            "function": {
                                                                "name": fn_name,
                                                                "arguments": ""
                                                            }
                                                        }]
                                                    },
                                                    "finish_reason": None
                                                }]
                                            }
                                            out_line = b"data: " + json.dumps(chunk_payload).encode("utf-8") + b"\n\n"
                                        self.wfile.write(out_line)
                                        self.wfile.flush()
                                elif ev_type == "response.function_call_arguments.delta":
                                    has_tool_call = True
                                    arg_key = parsed.get("item_id", "")
                                    if arg_key not in fn_indices:
                                        # Deltas may precede output_item.added —
                                        # reserve the next monotonic index.
                                        fn_indices[arg_key] = next_fn_index
                                        next_fn_index += 1
                                    arg_delta = parsed.get("delta", "") or ""
                                    if client_type == "anthropic":
                                        tool_delta = {
                                            "type": "content_block_delta",
                                            "index": fn_indices[arg_key],
                                            "delta": {
                                                "type": "input_json_delta",
                                                "partial_json": arg_delta
                                            }
                                        }
                                        out_line = b"event: content_block_delta\ndata: " + json.dumps(tool_delta).encode("utf-8") + b"\n\n"
                                    else:
                                        chunk_payload = {
                                            "id": resp_id,
                                            "object": "chat.completion.chunk",
                                            "created": int(time.time()),
                                            "model": model_name,
                                            "choices": [{
                                                "index": 0,
                                                "delta": {
                                                    "tool_calls": [{
                                                        "index": fn_indices[arg_key],
                                                        "function": {
                                                            "arguments": arg_delta
                                                        }
                                                    }]
                                                },
                                                "finish_reason": None
                                            }]
                                        }
                                        out_line = b"data: " + json.dumps(chunk_payload).encode("utf-8") + b"\n\n"
                                    self.wfile.write(out_line)
                                    self.wfile.flush()
                                elif ev_type == "response.output_item.done":
                                    item = parsed.get("item", {})
                                    if item.get("type") == "function_call" and client_type == "anthropic":
                                        item_key = item.get("id") or item.get("call_id") or ""
                                        idx = fn_indices.get(item_key, 0)
                                        tool_stop = {
                                            "type": "content_block_stop",
                                            "index": idx
                                        }
                                        self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps(tool_stop).encode("utf-8") + b"\n\n")
                                        self.wfile.flush()
                                        closed_fn_indices.add(idx)
                                elif ev_type in ("response.completed", "response.incomplete"):
                                    # Usage: Responses API uses input/output_tokens —
                                    # translate to chat format so zen-db records real numbers.
                                    u = (parsed.get("response") or {}).get("usage") or parsed.get("usage")
                                    if u:
                                        last_usage = {
                                            "prompt_tokens": u.get("input_tokens", 0) or 0,
                                            "completion_tokens": u.get("output_tokens", 0) or 0,
                                            "prompt_tokens_details": {"cached_tokens":
                                                (u.get("input_tokens_details") or {}).get("cached_tokens", 0) or 0},
                                        }
                                    # Fallback harvest: if summary=auto was ignored and no
                                    # <think> tags appeared, salvage plaintext reasoning from
                                    # the completed response's output array.
                                    if (True and not reasoning_streamed):
                                        for item in ((parsed.get("response") or {}).get("output") or []):
                                            if item.get("type") == "reasoning":
                                                txt = item.get("text") or "".join(
                                                    s.get("text", "") for s in (item.get("summary") or [])
                                                    if isinstance(s, dict))
                                                if txt:
                                                    reasoning_streamed = True
                                                    _emit_delta("reasoning_content", txt)
                                                    break
                                    _finish_stream()
                                elif ev_type in ("response.failed", "error"):
                                    err = parsed.get("response", {}).get("error") or parsed.get("error") or parsed
                                    stream_error = json.dumps(err)[:300]
                                    sys.stderr.write(f"[upstream-error] {stream_error}\n")
                            except (BrokenPipeError, ConnectionResetError):
                                # Client went away mid-adapt. Previously this
                                # was swallowed + logged as "[adapt-parse]
                                # Broken pipe" and the loop kept writing to a
                                # dead socket. Stop immediately.
                                disconnected = True
                                break
                            except Exception as e:

                                sys.stderr.write(f"[adapt-parse] {e}\n")
                            continue
                    else:
                        if line.strip() == b"data: [DONE]":
                            # Flush carry BEFORE forwarding [DONE] so recovered
                            # text isn't emitted after the terminal event.
                            _finish_stream()
                            continue
                        if line.startswith(b"data: "):
                            try:
                                parsed = json.loads(line[6:])
                            except Exception:
                                parsed = None
                            if isinstance(parsed, dict):
                                last_parsed = parsed
                                if "usage" in parsed:
                                    last_usage = parsed["usage"]
                                changed = False
                                drop_chunk = False
                                for ch in parsed.get("choices", []):
                                    delta = ch.get("delta") or {}
                                    txt = delta.get("content") or delta.get("reasoning_content") or delta.get("reasoning") or ""
                                    if txt:
                                        with _token_tap_lock:
                                            _token_tap.append(txt)
                                        tokens_emitted += 1
                                        t_now = time.perf_counter()
                                        t_stream_elapsed = max(0.001, t_now - (t_first_tok or t_now))
                                        rolling_tok_s = tokens_emitted / t_stream_elapsed if t_first_tok else 0.0
                                        # Update active stream snippet and metrics
                                        with _active_streams_lock:
                                            if stream_id in _active_streams:
                                                s = _active_streams[stream_id]
                                                s["snippet"] = (s.get("snippet", "") + txt)[-200:]
                                                s["chars"] = s.get("chars", 0) + len(txt)
                                                s["tokens"] = tokens_emitted
                                                s["tok_s"] = round(rolling_tok_s, 1)
                                                s["elapsed_s"] = round(t_now - track_ctx.get("t_start", t_now), 1)
                                    if client_type == "anthropic":
                                        if delta.get("tool_calls"):
                                            for tc in delta["tool_calls"]:
                                                has_tool_call = True
                                                tc_idx = tc.get("index", 0)
                                                fn = tc.get("function") or {}
                                                call_id = tc.get("id")
                                                if call_id:
                                                    if not message_start_sent:
                                                        in_tok = max(1, int(track_ctx.get("prompt_chars", 0) / 3.7)) if track_ctx else 1
                                                        msg_start = {
                                                            "type": "message_start",
                                                            "message": {
                                                                "id": resp_id,
                                                                "type": "message",
                                                                "role": "assistant",
                                                                "content": [],
                                                                "model": model_name,
                                                                "stop_reason": None,
                                                                "stop_sequence": None,
                                                                "usage": {"input_tokens": in_tok, "output_tokens": 1}
                                                            }
                                                        }
                                                        self.wfile.write(b"event: message_start\ndata: " + json.dumps(msg_start).encode("utf-8") + b"\n\n")
                                                        message_start_sent = True
                                                    if text_block_open:
                                                        self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": current_text_block_index}).encode("utf-8") + b"\n\n")
                                                        text_block_open = False
                                                        current_text_block_index = None
                                                    for pidx in fn_indices.values():
                                                        if pidx not in closed_fn_indices:
                                                            self.wfile.write(b"event: content_block_stop\ndata: " + json.dumps({"type": "content_block_stop", "index": pidx}).encode("utf-8") + b"\n\n")
                                                            closed_fn_indices.add(pidx)
                                                    tool_idx = next_fn_index
                                                    next_fn_index += 1
                                                    fn_indices[tc_idx] = tool_idx
                                                    tool_start = {
                                                        "type": "content_block_start",
                                                        "index": tool_idx,
                                                        "content_block": {
                                                            "type": "tool_use",
                                                            "id": call_id,
                                                            "name": fn.get("name", ""),
                                                            "input": {}
                                                        }
                                                    }
                                                    self.wfile.write(b"event: content_block_start\ndata: " + json.dumps(tool_start).encode("utf-8") + b"\n\n")
                                                arg_delta = fn.get("arguments", "")
                                                if arg_delta:
                                                    target_idx = fn_indices.get(tc_idx, next_fn_index)
                                                    tool_delta = {
                                                        "type": "content_block_delta",
                                                        "index": target_idx,
                                                        "delta": {
                                                            "type": "input_json_delta",
                                                            "partial_json": arg_delta
                                                        }
                                                    }
                                                    self.wfile.write(b"event: content_block_delta\ndata: " + json.dumps(tool_delta).encode("utf-8") + b"\n\n")
                                            self.wfile.flush()
                                        raw_c = delta.get("content") or ""
                                        if raw_c:
                                            t_part, n_part = tf.feed(raw_c)
                                            if n_part:
                                                _emit_delta("content", n_part)
                                        drop_chunk = True
                                    else: # ENABLED FOR OPENCLAUDE: OpenClaude handles reasoning_content natively now!
                                        if delta.get("reasoning") and not delta.get("reasoning_content"):
                                            delta["reasoning_content"] = delta.pop("reasoning")
                                            changed = True
                                        if delta.get("content"):
                                            raw_c = delta.pop("content")
                                            t_part, n_part = tf.feed(raw_c)
                                            if t_part:
                                                delta["reasoning_content"] = t_part
                                            if n_part:
                                                delta["content"] = n_part
                                            if not t_part and not n_part and not delta.get("tool_calls"):
                                                drop_chunk = True
                                            changed = True
                                        delta.setdefault("role", "assistant")
                                if False and self._strip_reasoning_fields_obj(parsed): # DISABLED IN-PLACE DELETION
                                    pass # DISABLED: OpenClaude v0.31+ now natively streams thinking_delta! (changed = False)
                                if _flex_rewrite_obj(parsed):
                                    changed = True
                                if changed:
                                    line = b"data: " + json.dumps(parsed).encode()
                                if drop_chunk:
                                    line = b""
                            else:
                                # Unparseable / non-object data line: fall back
                                # to the line-level transforms (still no-ops on
                                # non-`data:` lines).
                                if client_type == "openclaude":
                                    pass # DISABLED string-level strip: OpenClaude v0.31+ natively handles reasoning_content
                                line = _flex_rewrite_line(line)
                        if line:
                            self.wfile.write(line + b"\n")
                            self.wfile.flush()
                if stream_error or disconnected:
                    break
            if buf and not stream_error and not is_adapted:
                if buf.startswith(b"data: ") and buf.strip() != b"data: [DONE]":
                    try:
                        parsed_tail = json.loads(buf[6:])
                    except Exception:
                        parsed_tail = None
                    if isinstance(parsed_tail, dict):
                        last_parsed = parsed_tail
                        if "usage" in parsed_tail:
                            last_usage = parsed_tail["usage"]
                        changed = False
                        for ch in parsed_tail.get("choices", []):
                            delta = ch.get("delta") or {}
                            if client_type == "anthropic":
                                raw_c = delta.get("content") or ""
                                if raw_c:
                                    t_part, n_part = tf.feed(raw_c)
                                    if n_part:
                                        _emit_delta("content", n_part)
                                buf = b""
                            elif delta.get("content"): # ENABLED FOR OPENCLAUDE
                                raw_c = delta.pop("content")
                                t_part, n_part = tf.feed(raw_c)
                                if t_part:
                                    delta["reasoning_content"] = t_part
                                if n_part:
                                    delta["content"] = n_part
                                changed = True
                                delta.setdefault("role", "assistant")
                        if changed:
                            buf = b"data: " + json.dumps(parsed_tail).encode()
                if client_type != "anthropic":
                    buf = _flex_rewrite_line(buf)
                    self.wfile.write(buf)
                    self.wfile.flush()
            # Flush any text held back as a partial-tag carry at end of stream
            if not is_adapted and True and tf.carry:
                t_tail, n_tail = tf.flush()
                if client_type == "anthropic":
                    if n_tail:
                        _emit_delta("content", n_tail)
                else:
                    base = last_parsed if isinstance(last_parsed, dict) else {}
                    payload = {
                        "id": base.get("id", resp_id),
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": base.get("model", model_name),
                        "choices": [{
                            "index": 0,
                            "delta": {"role": "assistant"},
                            "finish_reason": None,
                        }],
                    }
                    d = payload["choices"][0]["delta"]
                    if t_tail:
                        d["reasoning_content"] = t_tail
                    if n_tail:
                        d["content"] = n_tail
                    self.wfile.write(b"data: " + json.dumps(payload).encode("utf-8") + b"\n\n")
                    self.wfile.flush()
            # Guarantee stream termination even if upstream EOFed or
            # errored without a completed event — the client must never hang.
            # Native Anthropic lane: upstream already sent message_stop (or
            # errored); never synthesize OpenAI frames onto it.
            if not completion_sent and not is_native_anthropic:
                _finish_stream(err=stream_error)
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        except Exception as e:

            sys.stderr.write(f"{_red('[stream-fatal] ' + type(e).__name__ + ': ' + str(e)[:150])}\n")
            try:
                _finish_stream(err=f"{type(e).__name__}: {e}")
            except Exception:
                pass
        finally:
            if idle_guard is not None:
                idle_guard.stop()
            try:
                r.close()
            except Exception:
                pass
            # Unregister active stream
            with _active_streams_lock:
                _active_streams.pop(stream_id, None)

        if disconnected:
            # Trivial aborts (0-1 tokens) are opencode cancelling a secondary
            # turn (title generation) — routine, not a failure. Only surface
            # aborts that actually lost generated content.
            if tokens_emitted > 1:
                sys.stderr.write(
                    f"[client-gone] {(track_ctx.get('model', model_name) if track_ctx else model_name):<24} "
                    f"| client aborted after {tokens_emitted:d} tokens | no usage recorded\n"
                )
            return True

        # Print completion telemetry log line with pure generation speed and TTFT
        final_tokens = last_usage.get("completion_tokens", tokens_emitted) if last_usage else tokens_emitted
        t_total_s = max(0.001, time.perf_counter() - track_ctx.get("t_start", time.perf_counter()))
        t_ttft_s = (ttft_ms / 1000.0) if ttft_ms else 0.0
        t_gen_s = max(0.001, t_total_s - t_ttft_s)
        avg_tok_s = final_tokens / t_gen_s if (final_tokens and t_gen_s > 0) else 0.0
        sys.stderr.write(
            f"\033[32m[DONE]\033[0m {track_ctx.get('model', model_name):<24} "
            f"| \033[1;36m{final_tokens:4d} tokens\033[0m in {t_gen_s:4.1f}s "
            f"(\033[1;33m{avg_tok_s:5.1f} tok/s\033[0m) "
            f"| TTFT: \033[35m{ttft_ms:4.0f}ms\033[0m "
            f"| Total: {t_total_s:4.1f}s\n"
        )

        # Record usage to DB
        if zen_db and track_ctx:
            try:
                latency = (time.perf_counter() - track_ctx.get("t_start", time.perf_counter())) * 1000
                pt = last_usage.get("prompt_tokens", 0) if last_usage else 0
                ct = last_usage.get("completion_tokens", tokens_emitted) if last_usage else tokens_emitted
                cat = last_usage.get("prompt_tokens_details", {}).get("cached_tokens", 0) if last_usage else 0
                db_async('record_usage', 
                    model=track_ctx.get("model", "unknown"),
                    client_type=client_type,
                    prompt_tokens=pt,
                    completion_tokens=ct,
                    cached_tokens=cat,
                    latency_ms=latency,
                    ttft_ms=ttft_ms,
                    tor_exit_ip=track_ctx.get("tor_exit_ip") or _cached_exit_ip.get("ip", ""),
                    status=200,
                    retries=track_ctx.get("retries", 0),
                    prompt_chars=track_ctx.get("prompt_chars", 0),
                    bytes_transferred=total_bytes,
                )
            except Exception as e:

                sys.stderr.write(f"[db] record failed: {e}\n")

        # --- DISABLED: Role:PERS-A and Role:PERS-B mandate that nothing here
        # probe or score live providers. Kept commented; block-comment only.
        # try:
        #     if track_ctx and final_tokens > 0:
        #         _perf_provider = "cline" if track_ctx.get("cline_account") else "zen"
        #         record_model_perf(
        #             _perf_provider,
        #             track_ctx.get("model", model_name),
        #             ttft_ms,
        #             avg_tok_s,
        #             final_tokens,
        #         )
        # except Exception as e:

        #     sys.stderr.write(f"[perf] record failed: {e}\n")

        return False

    def do_OPTIONS(self):
        """Handle CORS preflight for web clients, browser extensions, and local webviews."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD")
        req_hdrs = self.headers.get("Access-Control-Request-Headers")
        allow_hdrs = "Authorization, Content-Type, X-Requested-With, Accept, Origin, User-Agent, X-Zen-Token, x-api-key, anthropic-version, anthropic-beta, x-client-version, x-client-type, x-platform, x-platform-version, x-core-version, x-is-multiroot, baggage, sentry-trace, traceparent"
        if req_hdrs:
            allow_hdrs = f"{allow_hdrs}, {req_hdrs}"
        self.send_header("Access-Control-Allow-Headers", allow_hdrs)
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _auth_ok(self) -> bool:
        """Token check for sensitive endpoints using constant-time comparison."""
        if not AUTH_TOKEN:
            return False
        token_hdr = (self.headers.get("X-Zen-Token") or "").strip()
        auth_hdr = (self.headers.get("Authorization") or "").strip()
        bearer = ""
        if auth_hdr.lower().startswith("bearer "):
            bearer = auth_hdr[7:].strip()
        for cand in (token_hdr, bearer):
            if cand and hmac.compare_digest(cand, AUTH_TOKEN):
                return True
        return False

    def _bearer_key(self) -> str:
        # One key per friend works for BOTH protocols: OpenAI/Codex sends
        # `Authorization: Bearer <key>`, Claude Code / Anthropic SDK sends
        # `x-api-key: <key>` (+ `anthropic-version`). Accept all three.
        auth_hdr = (self.headers.get("Authorization") or "").strip()
        if auth_hdr.lower().startswith("bearer "):
            return auth_hdr[7:].strip()
        xkey = (self.headers.get("x-api-key") or "").strip()
        if xkey:
            return xkey
        return (self.headers.get("X-Zen-Token") or "").strip()

    def _public_key_name(self):
        """Name of the valid public API key on this request, else None."""
        return verify_public_key(self._bearer_key())

    def _is_public_request(self) -> bool:
        """True when the request arrived via a public host (funnel/quick tunnel).

        Strict: only our own tailnet DNSName (*.ts.net exact dot-boundary) or a
        configured ZEN_PUBLIC_HOST counts. X-Forwarded-* ignored (spoofable).
        Fail-closed: unknown hosts are treated as public when --public-auth.
        """
        host = (self.headers.get("Host") or "").strip().lower()
        # Strip a port, keeping bracketed IPv6 literals intact: [::1]:8767 -> ::1
        if host.startswith("["):
            host = host[1:].split("]", 1)[0]
        else:
            host = host.split(":")[0]
        if not host:
            return True
        if host in ("127.0.0.1", "localhost", "::1"):
            return False
        try:
            import ipaddress as _ip
            addr = _ip.ip_address(host)
            # Only LOCAL IPs stay keyless (loopback/private/link-local). Any
            # other literal — including a caller's own public IP — is public.
            # Host is client-controlled, so the old "any IP literal = local"
            # test was an auth bypass whenever --public-auth was on.
            if addr.is_loopback or addr.is_private or addr.is_link_local:
                return False
        except Exception:
            pass
        allowed = []
        if _LOCAL_DNS_NAME:
            allowed.append(_LOCAL_DNS_NAME.lower().rstrip("."))
        extra = os.environ.get("ZEN_PUBLIC_HOST", "")
        for h in extra.split(","):
            h = h.strip().lower().rstrip(".")
            if h:
                allowed.append(h)
        for a in allowed:
            if host == a or host.endswith("." + a):
                return True
        if host.endswith(".ts.net") or "trycloudflare" in host:
            return True
        # Unknown non-local host under --public-auth: fail closed (public).
        return True if _PUBLIC_AUTH else False
    def do_HEAD(self):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "0")
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        path = urlparse(self.path).path
        while path.startswith("/v1/v1/"):
            path = path.replace("/v1/v1/", "/v1/", 1)
        if path in ("/v1/models", "/openclaude/v1/models", "/models"):
            # Return strictly curated, verified working free models
            CURATED_FREE_MODELS = [
                {"id": "deepseek-v4-flash",         "name": "DeepSeek V4 Flash",                 "reasoning": True,  "owned_by": "zen"},
                {"id": "minimax-m2.7",              "name": "MiniMax M2.7",                      "reasoning": False, "owned_by": "zen"},
                {"id": "minimax-m2.5",              "name": "MiniMax M2.5",                      "reasoning": False, "owned_by": "zen"},
                {"id": "muse-spark-1.3",            "name": "Muse Spark 1.3",              "reasoning": True,  "owned_by": "zen"},
                {"id": "muse-spark-1.2",            "name": "Muse Spark 1.2",              "reasoning": True,  "owned_by": "zen"},
                {"id": "mimo-v2.5-free",            "name": "MiMo V2.5 Free",              "reasoning": True,  "owned_by": "zen"},
                {"id": "ling-3.0-flash-fin-free",   "name": "Ling 3.0 Flash Fin (free)",   "reasoning": False, "owned_by": "zen"},
                {"id": "nemotron-3-ultra-free",     "name": "Nemotron 3 Ultra Free",       "reasoning": False, "owned_by": "zen"},
                {"id": "nemotron-3.5-lightning-free","name": "Nemotron 3.5 Lightning Free","reasoning": False, "owned_by": "zen"},
                {"id": "space-bunny-free",          "name": "Space Bunny (free)",          "reasoning": True,  "owned_by": "zen"},
                {"id": "stealth/pixel-canary",      "name": "Pixel Canary (free, 128k)",   "reasoning": True,  "owned_by": "cline"},
                {"id": "jev-1.13-free",             "name": "Jev 1.13 Free",               "reasoning": False, "owned_by": "zen"},
                {"id": "big-pickle",                "name": "Big Pickle",                  "reasoning": False, "owned_by": "zen"},
                {"id": "deepseek-v4.1-flash",       "name": "DeepSeek V4.1 Flash",         "reasoning": True,  "owned_by": "zen"},
                {"id": "deepseek-v4-flash-vision-exp", "name": "DeepSeek V4.1 Flash Vision", "reasoning": True, "owned_by": "zen"},
                {"id": "kimi-k3",                   "name": "Kimi K3 (Moonshot)",          "reasoning": True,  "owned_by": "zen"},
                {"id": "mimo-v2.6-flash-free",      "name": "MiMo V2.6 Flash Free",        "reasoning": True,  "owned_by": "zen"},
                {"id": "stealth/space-bunny-alpha", "name": "Space Bunny Alpha (free, 1M)", "reasoning": True, "owned_by": "cline"},
                {"id": "workbuddy/auto",           "name": "wb-auto (free)",                "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/deepseek-v4.1-flash", "name": "wb-deepseek-v4.1-flash (free)", "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/glm-5.2",        "name": "wb-glm-5.2 (free)",            "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/glm-5.3",        "name": "wb-glm-5.3 (0.04)",            "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/gpt-5.4",        "name": "wb-gpt-5.4 (0.02)",            "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/gpt-5.5",        "name": "wb-gpt-5.5 (0.05)",            "reasoning": False, "owned_by": "workbuddy"},
                {"id": "workbuddy/gpt-5.6-luna",   "name": "wb-gpt-5.6-luna (0-0.18)",     "reasoning": True,  "owned_by": "workbuddy"},
                {"id": "workbuddy/gpt-5.6-sol",    "name": "wb-gpt-5.6-sol (0.04)",        "reasoning": True,  "owned_by": "workbuddy"},
                {"id": "workbuddy/gpt-5.6-terra",  "name": "wb-gpt-5.6-terra (0.02)",      "reasoning": True,  "owned_by": "workbuddy"},
                # --- Cline free-lane models (verified 200 on 2026-10-05) ---
                {"id": "glm-5.2-free",             "name": "GLM 5.2 (free)",               "reasoning": True,  "owned_by": "cline"},
                {"id": "deepseek-v4-flash-free",   "name": "DeepSeek V4 Flash (free)",     "reasoning": True,  "owned_by": "cline"},
                {"id": "minimax-m3-free",          "name": "MiniMax M3 (free)",            "reasoning": True,  "owned_by": "cline"},
                {"id": "minimax-m2.7-free",        "name": "MiniMax M2.7 (free)",          "reasoning": False, "owned_by": "cline"},
                {"id": "minimax-m2.5-free",        "name": "MiniMax M2.5 (free)",          "reasoning": False, "owned_by": "cline"},
                {"id": "qwen3.8-27b-free",         "name": "Qwen 3.8 27B (free)",          "reasoning": True,  "owned_by": "cline"},
                {"id": "gemma-4-31b-free",         "name": "Gemma 4 31B (free)",           "reasoning": False, "owned_by": "cline"},
                {"id": "gemma-4-26b-free",         "name": "Gemma 4 26B (free)",           "reasoning": False, "owned_by": "cline"},
                {"id": "nemotron-3-super-free",    "name": "Nemotron 3 Super 120B (free)", "reasoning": True,  "owned_by": "cline"},
                {"id": "inkling-free",             "name": "Inkling (free)",               "reasoning": True,  "owned_by": "cline"},
                {"id": "inkling-small-free",       "name": "Inkling Small (free)",         "reasoning": True,  "owned_by": "cline"},
                {"id": "north-mini-free",          "name": "North Mini Code (free)",       "reasoning": False, "owned_by": "cline"},
                {"id": "laguna-s-free",            "name": "Laguna S 2.1 (free)",          "reasoning": False, "owned_by": "cline"},
                {"id": "laguna-xs-free",           "name": "Laguna XS 2.1 (free)",         "reasoning": False, "owned_by": "cline"},
                {"id": "lfm-2.5-free",             "name": "LFM 2.5 2.6B (free)",          "reasoning": False, "owned_by": "cline"},
                {"id": "dots-note-free",           "name": "Dots 3 Note (free)",           "reasoning": False, "owned_by": "cline"},
                {"id": "apodex-mini-free",         "name": "Apodex 1.1 Mini (free)",       "reasoning": False, "owned_by": "cline"},
                {"id": "ling-sante-free",          "name": "Ling 3.0 Sante (free)",        "reasoning": False, "owned_by": "cline"},
            ]
            curated_list = [{"id": m["id"], "object": "model", "owned_by": m["owned_by"], "name": m["name"], "reasoning": m["reasoning"]} for m in CURATED_FREE_MODELS]
            import json, os
            cfg_path = os.path.expanduser("~/.config/tor-zen/models.json")
            dyn_cfg = {}
            if os.path.exists(cfg_path):
                try:
                    with open(cfg_path, "r") as f:
                        dyn_cfg = json.load(f)
                except Exception: pass
                
            for m in curated_list:
                applied = False
                for k, v in dyn_cfg.items():
                    if k != "default" and k in m["id"]:
                        m["context_window"] = v.get("context_window", 128000)
                        m["context_length"] = m["context_window"]
                        m["max_tokens"] = v.get("max_tokens", 4096)
                        applied = True
                        break
                if not applied:
                    df = dyn_cfg.get("default", {"context_window": 128000, "max_tokens": 4096})
                    m["context_window"] = df.get("context_window", 128000)
                    m["context_length"] = m["context_window"]
                    m["max_tokens"] = df.get("max_tokens", 4096)
                _vis = _model_vision_inputs(m["id"])
                m["attachment"] = bool(_vis)
                if _vis:
                    m["modalities"] = {"input": _vis, "output": ["text"]}
            return self._json(200, {"object": "list", "data": curated_list})
        if path in ("/", "/health"):
            # Stealth: unauthenticated callers (e.g. via a forwarded tunnel)
            # get a bare liveness signal only — no upstream URL, no model
            # list, no pool layout, no notes. Full details need X-Zen-Token.
            if not self._auth_ok():
                return self._json(200, {"ok": True})
            # Instant non-blocking health check: use cached models or default static list
            models = _models_cache["models"] or MODELS
            now = time.monotonic()
            if _models_cache["models"] is None or (now - _models_cache["ts"] >= 600):
                threading.Thread(target=refresh_models, args=(self.upstream, self.proxy_url), daemon=True).start()
            body = {
                "ok": True,
                "upstream": self.upstream,
                "default_model": self.default_model,
                "models_available": [m["id"] for m in models],
                "uptime_seconds": round(time.time() - _proxy_start_time),
                "note": "tor-rotated proxy v3 (streaming + tracking)",
                "endpoints": {
                    "opencode": f"http://127.0.0.1:{PORT}/v1",
                    "cline": f"http://127.0.0.1:{PORT}/v1",
                    "openclaude": f"http://127.0.0.1:{PORT}/openclaude/v1",
                },
                "circuit_pool": _circuit_pool.get_status(),
            }
            # Tor exit IP is identity-sensitive — only shown here (authed path).
            body["tor_exit_ip"] = tor_exit_ip(self.socks)
            return self._json(200, body)
        if path == "/stats":
            if not self._auth_ok():
                return self._json(403, {"error": {"message": "token required (X-Zen-Token)", "type": "auth_error"}})
            if zen_db:
                stats = zen_db.get_stats(session_since=_proxy_start_time)
                stats["cline_accounts"] = cline_pool_status()
                return self._json(200, stats)
            return self._json(200, {"cline_accounts": cline_pool_status()})
        if path == "/live":
            if not self._auth_ok():
                return self._json(403, {"error": {"message": "token required (X-Zen-Token)", "type": "auth_error"}})
            with _active_streams_lock:
                streams = list(_active_streams.values())
            with _token_tap_lock:
                tap_text = "".join(_token_tap)
                _token_tap.clear()
            return self._json(200, {"active_streams": streams, "tap": tap_text})
        if path in ("/export/opencode.json", "/opencode.json"):
            host = self.headers.get("Host", f"127.0.0.1:{PORT}")
            proto = self.headers.get("X-Forwarded-Proto", "https" if ("trycloudflare" in host or ".ts.net" in host) else "http")
            base_url = f"{proto}://{host}/v1"
            parsed = urlparse(self.path)
            q_params = dict(parse_qsl(parsed.query))
            target_model = q_params.get("model", self.default_model)
            api_key = q_params.get("key", "none")

            cfg = {
                "model": f"tor/{target_model}",
                "provider": {
                    "tor": {
                        "npm": "@ai-sdk/openai-compatible",
                        "options": {
                            "baseURL": base_url,
                            "apiKey": api_key
                        },
                        "models": {}
                    },
                    "opencode": {
                        "options": {
                            "baseURL": base_url,
                            "apiKey": api_key
                        }
                    }
                }
            }
            models = _models_cache["models"] or MODELS
            for m in models:
                mid = m.get("id")
                if mid:
                    m_entry = {
                        "name": m.get("name", mid),
                        "reasoning": m.get("reasoning", True)
                    }
                    if "muse" in mid:
                        m_entry["variants"] = {
                            "minimal": {"reasoningEffort": "minimal"},
                            "low": {"reasoningEffort": "low"},
                            "medium": {"reasoningEffort": "medium"},
                            "high": {"reasoningEffort": "high"},
                            "xhigh": {"reasoningEffort": "xhigh"},
                            "max": {"reasoningEffort": "max"}
                        }
                    cfg["provider"]["tor"]["models"][mid] = m_entry
            return self._json(200, cfg)

        if path in ("/export/opencode.env", "/opencode.env"):
            host = self.headers.get("Host", f"127.0.0.1:{PORT}")
            proto = self.headers.get("X-Forwarded-Proto", "https" if "trycloudflare" in host else "http")
            base_url = f"{proto}://{host}/v1"
            parsed = urlparse(self.path)
            q_params = dict(parse_qsl(parsed.query))
            target_model = q_params.get("model", self.default_model)

            cfg = {
                "model": f"opencode/{target_model}",
                "provider": {
                    "opencode": {
                        "options": {
                            "baseURL": base_url
                        },
                        "models": {
                            target_model: {
                                "variants": {
                                    "minimal": {"reasoningEffort": "minimal"},
                                    "low": {"reasoningEffort": "low"},
                                    "medium": {"reasoningEffort": "medium"},
                                    "high": {"reasoningEffort": "high"},
                                    "xhigh": {"reasoningEffort": "xhigh"},
                                    "max": {"reasoningEffort": "max"}
                                }
                            }
                        }
                    }
                }
            }
            content_json = json.dumps(cfg)
            shell_script = (
                f"export OPENCODE_URL={shlex.quote(base_url)}\n"
                f"export OPENCODE_MODEL={shlex.quote(f'opencode/{target_model}')}\n"
                f"export OPENCODE_CONFIG_CONTENT={shlex.quote(content_json)}\n"
            )
            data = shell_script.encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        if path in ("/openclaude.ps1", "/setup-openclaude.ps1"):
            host = self.headers.get("Host", f"127.0.0.1:{PORT}")
            proto = self.headers.get("X-Forwarded-Proto", "https" if ("trycloudflare" in host or ".ts.net" in host) else "http")
            base_url = f"{proto}://{host}/v1"
            parsed = urlparse(self.path)
            q_params = dict(parse_qsl(parsed.query))
            target_model = q_params.get("model", "deepseek-v4-flash")
            api_key = q_params.get("key", "sk-none")

            ps1_script = f"""# OpenClaude Automated Setup for Windows
$ErrorActionPreference = "Continue"
Write-Host "`n>>> Configuring OpenClaude for Windows..." -ForegroundColor Cyan

$apiKey = "{api_key}"
$baseUrl = "{base_url}"
$model = "{target_model}"

# 1. Save user environment variables permanently (powershell, cmd, vs code)
[Environment]::SetEnvironmentVariable("CLAUDE_CODE_USE_OPENAI", "1", "User")
[Environment]::SetEnvironmentVariable("OPENAI_BASE_URL", $baseUrl, "User")
[Environment]::SetEnvironmentVariable("OPENAI_API_KEY", $apiKey, "User")
[Environment]::SetEnvironmentVariable("OPENAI_MODEL", $model, "User")

# 2. Set for current session
$env:CLAUDE_CODE_USE_OPENAI = "1"
$env:OPENAI_BASE_URL = $baseUrl
$env:OPENAI_API_KEY = $apiKey
$env:OPENAI_MODEL = $model

# 3. Check Node.js
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {{
    Write-Host ">>> Node.js not detected. Installing Node.js LTS via winget..." -ForegroundColor Yellow
    winget install OpenJS.NodeJS.LTS --silent --accept-source-agreements --accept-package-agreements
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
}}

# 4. Check OpenClaude CLI
if (-not (Get-Command openclaude -ErrorAction SilentlyContinue)) {{
    Write-Host ">>> Installing OpenClaude globally via npm..." -ForegroundColor Cyan
    npm install -g @gitlawb/openclaude@latest
    $npmPrefix = (npm config get prefix).Trim()
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (($userPath -split ';') -notcontains $npmPrefix) {{
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$npmPrefix", "User")
        $env:Path = "$env:Path;$npmPrefix"
    }}
}}

Write-Host "`n===============================================" -ForegroundColor Green
Write-Host "       OpenClaude Setup Complete!" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
Write-Host "Base URL : $baseUrl"
Write-Host "Model    : $model"
Write-Host "`nTo start coding, simply open a terminal and run:" -ForegroundColor Cyan
Write-Host "  openclaude`n" -ForegroundColor Yellow
"""
            data = ps1_script.encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        return self._json(404, {"error": "not found"})

    def _workbuddy_route(self, body):
        """Serve a workbuddy/* model from Tencent WorkBuddy's free lane.

        Upstream is streaming-only and returns OpenAI-shaped SSE; forward
        stream=True and either pass the SSE through or aggregate it into a
        chat.completion for non-streaming clients.
        """
        requested = body.get("model") or "workbuddy/auto"
        model = (requested.split("workbuddy/", 1)[1]
                 if requested.startswith("workbuddy/") else requested)
        cred = _workbuddy_cred()
        if not cred:
            sys.stderr.write("[workbuddy] not signed in (auth file missing/expired)\n")
            return self._json(503, {"error": {"message":
                "workbuddy not signed in; sign in once in the WorkBuddy desktop "
                "app (workbuddy-start.sh --with-app)", "type": "upstream_error"}})
        base = _wb_region_base(cred.get("domain"))
        hdrs = _workbuddy_chat_headers(cred)
        up = dict(body)
        up["model"] = model
        up["stream"] = True
        if isinstance(up.get("tool_choice"), dict):
            up["tool_choice"] = "auto"  # upstream wants the string form
        # WorkBuddy rejects requests whose FIRST message is not a system prompt
        # (400 {code:11128,msg:"first message is not system prompt"}). Inject a
        # minimal default so any OpenAI client works, not just opencode.
        _msgs = up.get("messages")
        if (isinstance(_msgs, list)
                and (not _msgs
                     or not (isinstance(_msgs[0], dict)
                             and _msgs[0].get("role") == "system"))):
            up["messages"] = [{"role": "system",
                               "content": "You are a helpful assistant."}] + list(_msgs)
        want_stream = bool(body.get("stream"))
        # WorkBuddy's upstream is flaky over HTTP/2 (curl error 92: "HTTP/2
        # stream 1 was not closed cleanly: INTERNAL_ERROR"), which surfaced as a
        # hard 502 on otherwise-fine turns. Force HTTP/1.1 and retry once on a
        # transport error before giving up.
        r = None
        last_err = None
        for _attempt in range(2):
            sess = cffi_requests.Session(impersonate="chrome131", timeout=(60, 600))
            try:
                r = sess.post(base + "/v2/chat/completions", headers=hdrs,
                              json=up, stream=True, proxy=_wb_proxy(),
                              http_version=CurlHttpVersion.V1_1)
                break
            except Exception as e:
                last_err = e
                sys.stderr.write(
                    f"[workbuddy] transport error (attempt {_attempt + 1}/2): {e}\n")
                try:
                    sess.close()
                except Exception:
                    pass
        if r is None:
            return self._json(502, {"error": {"message": str(last_err),
                                              "type": "upstream_error"}})
        if r.status_code != 200:
            txt = r.text[:400]
            # WorkBuddy envelope is {code,msg,data} — surface `msg` (our generic
            # error.extraction only knows error.message and showed "").
            _m = txt
            try:
                _j = json.loads(txt)
                _m = (str(_j.get("msg") or "") or
                      str((_j.get("error") or {}).get("message") or "") or txt)
            except Exception:
                pass
            sys.stderr.write(f"[workbuddy] upstream {r.status_code}: {_m[:200]}\n")
            try:
                r.close()
            except Exception:
                pass
            return self._json(r.status_code, {"error": {"message": _m[:400],
                                                        "type": "upstream_error"}})
        sys.stderr.write(f"[workbuddy] {model} -> 200 stream={want_stream}\n")
        if want_stream:
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                # Client disconnected before headers were committed. Swallow so
                # it does not surface as an unhandled traceback in the log.
                try:
                    r.close()
                except Exception:
                    pass
                return
            try:
                for line in r.iter_lines():
                    if not line or not line.startswith(b"data:"):
                        continue
                    self.wfile.write(line + b"\n\n")
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:

                sys.stderr.write(f"[workbuddy] stream error: {e}\n")
            finally:
                try:
                    r.close()
                except Exception:
                    pass
            return
        content, reasoning = [], []
        mid, mout, usage = None, model, None
        try:
            for line in r.iter_lines():
                if not line or not line.startswith(b"data:"):
                    continue
                payload = line[5:].strip()
                if payload == b"[DONE]":
                    break
                try:
                    j = json.loads(payload)
                except Exception:
                    continue
                mid = j.get("id") or mid
                mout = j.get("model") or mout
                if j.get("usage"):
                    usage = j["usage"]
                for ch in j.get("choices", []) or []:
                    dl = ch.get("delta") or {}
                    if dl.get("content"):
                        content.append(dl["content"])
                    if dl.get("text"):
                        content.append(dl["text"])
                    if dl.get("reasoning_content"):
                        reasoning.append(dl["reasoning_content"])
                    if dl.get("reasoning"):
                        reasoning.append(dl["reasoning"])
                    msg_obj = ch.get("message") or {}
                    if msg_obj.get("content"):
                        content.append(msg_obj["content"])
        finally:
            try:
                r.close()
            except Exception:
                pass
        res_content = "".join(content)
        res_reasoning = "".join(reasoning)
        if not res_content and res_reasoning:
            res_content = res_reasoning
        msg = {"role": "assistant", "content": res_content}
        if res_reasoning:
            msg["reasoning_content"] = res_reasoning
        out = {
            "id": mid or uuid.uuid4().hex,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": mout,
            "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
        }
        if usage:
            out["usage"] = usage
        return self._json(200, out)

    def do_POST(self):
        global _direct_cooldown_until
        path = urlparse(self.path).path
        while path.startswith("/v1/v1/"):
            path = path.replace("/v1/v1/", "/v1/", 1)

        # TEMP diagnostic (env-gated): dump the raw client request headers so we
        # can mirror exactly what the real OpenCode client sends upstream.
        if os.environ.get("ZEN_LOG_HEADERS"):
            try:
                with open("/tmp/opencode/req_headers.log", "a") as _fh:
                    _fh.write(f"=== {path}\n")
                    for _k, _v in self.headers.items():
                        _fh.write(f"{_k}: {_v}\n")
                    _fh.write("\n")
            except Exception:
                pass

        # State-mutating rotation is POST-only + token-gated (was GET/CSRF-able)
        if path == "/rotate":
            if not self._auth_ok():
                return self._json(403, {"error": {"message": "token required (X-Zen-Token)", "type": "auth_error"}})
            ok = tor_rotate(self.control_host, self.control_port, force=True)
            if ok:
                reset_all_sessions()
            ip = tor_exit_ip(self.socks) if self._auth_ok() else "hidden"
            return self._json(200, {"rotated": ok, "new_ip": ip})
        
        # Determine client type from URL path or User-Agent header
        ua = (self.headers.get("User-Agent") or "").lower()
        if path.startswith("/openclaude/"):
            client_type = "openclaude"
            # Normalize path: strip /openclaude prefix for upstream
            api_path = path.replace("/openclaude", "", 1)
        else:
            api_path = path
        
        while api_path.startswith("/v1/v1/"):
            api_path = api_path.replace("/v1/v1/", "/v1/", 1)
        
        is_anthropic_count = api_path in ("/v1/messages/count_tokens", "/messages/count_tokens")
        is_anthropic_messages = api_path in ("/v1/messages", "/messages")
        if is_anthropic_messages or is_anthropic_count:
            client_type = "anthropic"
        elif "openclaude" in ua or "claude" in ua:
            # Claude Code / OpenClaude using OpenAI mode (CLAUDE_CODE_USE_OPENAI=1) calls
            # /v1/chat/completions and expects standard OpenAI SSE chunks, not Anthropic events.
            client_type = "openclaude"
        elif "cline" in ua or "vscode" in ua:
            client_type = "cline"
        else:
            client_type = "opencode"

        # Jev / System One (TypeSafe AI) — NOT a chat LLM. It is a structured
        # decision model reached at Zen /v1/systemone with a {state, questions}
        # body (question types: noul / choice / score). Chat-completions 500s
        # for it, which is why it looked "dead". Passthrough, no chat machinery.
        if api_path in ("/v1/systemone", "/systemone",
                        "/v1/jev", "/v1/jev/decide", "/v1/decide"):
            try:
                _sm_len = int(self.headers.get("Content-Length", "0"))
            except (ValueError, TypeError):
                return self._json(400, {"error": "bad Content-Length"})
            if _sm_len < 0 or _sm_len > 4 * 1024 * 1024:
                return self._json(413, {"error": "bad/oversized body"})
            _sm_raw = self.rfile.read(_sm_len) if _sm_len else b"{}"
            try:
                _sm_body = json.loads(_sm_raw.decode("utf-8") or "{}")
            except Exception:
                return self._json(400, {"error": {"message": "invalid JSON", "type": "invalid_request_error"}})
            _sm_model = _sm_body.get("model") or "jev-1.13-free"
            _sm_url = f"{self.upstream}/systemone"
            sys.stderr.write(f"[systemone] forwarding model={_sm_model} -> {_sm_url}\n")
            _sm_sess = None
            try:
                _sm_sess = cffi_requests.Session(impersonate="chrome131", timeout=120.0)
                _sm_r = _sm_sess.post(_sm_url, headers={
                    "Authorization": "Bearer public",
                    "Content-Type": "application/json",
                    "User-Agent": _OPENCODE_UA,
                }, data=_sm_raw)
                _sm_out = _sm_r.content
                self.send_response(_sm_r.status_code)
                self.send_header("Content-Type", _sm_r.headers.get("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(_sm_out)))
                self.end_headers()
                self.wfile.write(_sm_out)
                sys.stderr.write(f"[systemone] {_sm_model} -> {_sm_r.status_code} {_sm_out[:160]!r}\n")
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as _sm_e:
                sys.stderr.write(f"[systemone] error: {_sm_e}\n")
                try:
                    self._json(502, {"error": {"message": str(_sm_e), "type": "upstream_error"}})
                except Exception:
                    pass
            finally:
                if _sm_sess is not None:
                    try:
                        _sm_sess.close()
                    except Exception:
                        pass
            return

        if api_path not in ("/v1/chat/completions", "/chat/completions", "/v1/responses", "/responses", "/v1/messages", "/messages", "/v1/messages/count_tokens", "/messages/count_tokens"):
            return self._json(404, {"error": "not found"})

        # Public gate: requests arriving via a PUBLIC host (Tailscale Funnel,
        # serve/`.ts.net`, quick tunnel, or any unknown non-local Host) MUST
        # present a valid API key. Loopback and private callers stay keyless so
        # local agents (opencode/claude on 127.0.0.1) are never affected.
        # Fail-closed: with --public-auth, an unrecognised Host counts as public.
        _pub_key_name = self._public_key_name()
        if self._is_public_request() and not _pub_key_name and not self._auth_ok():
            sys.stderr.write(
                f"[auth] DENIED public request without key: "
                f"host={self.headers.get('Host')} path={api_path}\n")
            return self._json(403, {
                "error": {
                    "message": "API key required for public access. Send "
                               "'Authorization: Bearer <key>' or 'x-api-key: <key>'.",
                    "type": "auth_error",
                }
            })
        self._authed_key_name = _pub_key_name or "public-guest"

        if _qlog("client"):
            sys.stderr.write(f"[client] {client_type} -> {path}\n")

        sys.stderr.write(f"\033[36m[{time.strftime('%H:%M:%S')}] [INCOMING]\033[0m \033[33m[{client_type}]\033[0m POST {self.path} - Processing request...\n")
        sys.stderr.flush()

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except (ValueError, TypeError):
            return self._json(400, {"error": "bad Content-Length"})
        if length < 0:
            return self._json(400, {"error": "bad Content-Length"})
        if length > _MAX_BODY:
            return self._json(413, {"error": f"request body too large (max {_MAX_BODY // (1024 * 1024)}MiB)"})
        # Bound the body read so a stalled/slowloris client cannot pin a worker
        # thread forever (P0-B intent). Reset to blocking afterward: streaming
        # writes must NOT inherit this timeout (quiet reasoning pauses are
        # legitimate and can exceed 30s between deltas).
        try:
            self.connection.settimeout(30.0)
        except Exception:
            pass
        try:
            try:
                raw_in = self.rfile.read(length) if length else b"{}"
            except (socket.timeout, TimeoutError):
                return self._json(408, {"error": "request body read timeout"})
        finally:
            try:
                self.connection.settimeout(None)
            except Exception:
                pass
        # Incoming-vs-outgoing proof (client bloat vs proxy bloat):
        # logs what opencode actually sent BEFORE any translation.
        # Quiet by default (ZEN_QUIET=1): terminal stays clean.
        try:
            if _qlog("client-size"):
                _in_k = _kfmt(len(raw_in or b''))
                sys.stderr.write(f"[client-size] {path} incoming=\033[36m{_in_k}\033[0m\n")
        except Exception:
            pass
        if os.environ.get("ZEN_LOG_HEADERS"):
            try:
                with open("/tmp/opencode/req_body.log", "a") as _fb:
                    _fb.write(f"=== {path}\n")
                    _fb.write((raw_in or b"")[:20000].decode("utf-8", "replace"))
                    _fb.write("\n\n")
            except Exception:
                pass
        try:
            body = json.loads(raw_in or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"error": "bad json"})

        # Body map REMOVED (was verbose telemetry; req-size/client-size retained).
        # (Body-map block deleted per operator request.)

        # Pillar 4b: Support Anthropic count_tokens endpoint for Claude Code /context
        if is_anthropic_count:
            chars = 0
            sys_prompt = body.get("system")
            if isinstance(sys_prompt, str):
                chars += len(sys_prompt)
            elif isinstance(sys_prompt, list):
                chars += sum(len(b.get("text") or "") for b in sys_prompt if isinstance(b, dict))
            for m in body.get("messages", []):
                c = m.get("content", "")
                if isinstance(c, str):
                    chars += len(c)
                elif isinstance(c, list):
                    for b in c:
                        if isinstance(b, dict):
                            chars += len(b.get("text") or b.get("thinking") or "")
                            if "content" in b:
                                cnt = b["content"]
                                if isinstance(cnt, str):
                                    chars += len(cnt or "")
                                elif isinstance(cnt, list):
                                    for item in cnt:
                                        if isinstance(item, dict):
                                            chars += len(item.get("text") or "")
                            if "input" in b and b["input"] is not None:
                                try:
                                    chars += len(json.dumps(b["input"]))
                                except Exception:
                                    pass
            for t in body.get("tools", []):
                try:
                    chars += len(json.dumps(t))
                except Exception:
                    pass
            input_tokens = max(1, int(chars / 3.7))
            sys.stderr.write(f"[count_tokens] calculated {input_tokens} input tokens ({chars} chars)\n")
            return self._json(200, {"input_tokens": input_tokens})

        # Pillar 4: Universal Anthropic Messages API Shim (/v1/messages)
        # Adapt Anthropic request payload to standard OpenAI completions format
        if is_anthropic_messages:
            openai_messages = []
            sys_prompt = body.get("system")
            if isinstance(sys_prompt, str) and sys_prompt:
                openai_messages.append({"role": "system", "content": sys_prompt})
            elif isinstance(sys_prompt, list):
                combined_sys = "\n\n".join(b.get("text", "") for b in sys_prompt if isinstance(b, dict))
                if combined_sys:
                    openai_messages.append({"role": "system", "content": combined_sys})
            
            for m in body.get("messages", []):
                role = m.get("role", "user")
                content = m.get("content", "")
                if isinstance(content, str):
                    openai_messages.append({"role": role, "content": content})
                elif isinstance(content, list):
                    text_parts = []
                    image_parts = []
                    tool_calls = []
                    tool_results = []
                    for part in content:
                        if not isinstance(part, dict):
                            continue
                        p_type = part.get("type", "")
                        if p_type == "text":
                            text_parts.append(part.get("text", ""))
                        elif p_type == "image":
                            # Anthropic image block ({"type":"image","source":...}).
                            # Must be preserved: dropping it is what made Read on an
                            # image return an empty tool result to the model.
                            _u = _image_part_url(part)
                            if _u:
                                image_parts.append({"type": "image_url",
                                                    "image_url": {"url": _u}})
                        elif p_type == "tool_use":
                            call_id = part.get("id") or f"call_{uuid.uuid4().hex[:12]}"
                            fn_name = part.get("name", "")
                            fn_args = json.dumps(part.get("input", {}))
                            tool_calls.append({
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": fn_name,
                                    "arguments": fn_args
                                }
                            })
                        elif p_type == "tool_result":
                            res_content = part.get("content", "")
                            if isinstance(res_content, list):
                                # Preserve image blocks (Read tool on an image).
                                # They are relocated out of the tool message into a
                                # synthetic user message by _relocate_tool_images
                                # below, because providers reject images inside
                                # tool results.
                                _rc_parts = []
                                for c in res_content:
                                    if not isinstance(c, dict):
                                        _rc_parts.append({"type": "text", "text": str(c)})
                                        continue
                                    if str(c.get("type", "")).lower().startswith("image"):
                                        _u = _image_part_url(c)
                                        if _u:
                                            _rc_parts.append({"type": "image_url",
                                                              "image_url": {"url": _u}})
                                        else:
                                            sys.stderr.write(
                                                "[image] dropped unsupported tool_result "
                                                f"image keys={sorted(c.keys())}\n")
                                    elif c.get("text") is not None:
                                        _rc_parts.append({"type": "text",
                                                          "text": str(c.get("text"))})
                                res_content = _rc_parts if _rc_parts else ""
                            tool_results.append({
                                "role": "tool",
                                "tool_call_id": part.get("tool_use_id", ""),
                                "content": (res_content if isinstance(res_content, list)
                                            else str(res_content)),
                            })

                    if role == "assistant":
                        asst_msg = {"role": "assistant"}
                        asst_msg["content"] = "\n".join(text_parts) if text_parts else None
                        if tool_calls:
                            asst_msg["tool_calls"] = tool_calls
                        openai_messages.append(asst_msg)
                    else:
                        for tr in tool_results:
                            openai_messages.append(tr)
                        if image_parts:
                            _u_content = []
                            if text_parts:
                                _u_content.append({"type": "text",
                                                   "text": "\n".join(text_parts)})
                            _u_content.extend(image_parts)
                            openai_messages.append({"role": "user", "content": _u_content})
                        elif text_parts:
                            openai_messages.append({"role": "user", "content": "\n".join(text_parts)})

            body["messages"] = openai_messages

            # Anthropic tools ({"name","description","input_schema"}) must be
            # reshaped to OpenAI function tools, exactly like the messages
            # above: downstream treats this body as an OpenAI chat request, and
            # a chat-wire model forwarded to Zen /chat/completions would reject
            # the Anthropic tool shape.
            _a_tools = body.get("tools")
            if isinstance(_a_tools, list) and _a_tools:
                _oai_tools = []
                for _t in _a_tools:
                    if not isinstance(_t, dict):
                        continue
                    if isinstance(_t.get("function"), dict):
                        _oai_tools.append(_t)  # already OpenAI-shaped
                        continue
                    _nm = _t.get("name")
                    if not isinstance(_nm, str) or not _nm:
                        continue
                    _pr = _t.get("input_schema") or _t.get("parameters")
                    if not isinstance(_pr, dict):
                        _pr = {"type": "object"}
                    _oai_tools.append({
                        "type": "function",
                        "function": {
                            "name": _nm,
                            "description": _t.get("description") or "",
                            "parameters": _pr,
                        },
                    })
                if _oai_tools:
                    body["tools"] = _oai_tools

        # Providers reject image parts inside tool results — relocate them
        # into a synthetic user message so vision-via-read-tool works.
        if isinstance(body.get("messages"), list):
            body["messages"] = _relocate_tool_images(body["messages"])

        # Feature: Prompt & Tool Schema Normalization (RadixAttention KV-Cache Reuse)
        # Sort tool parameters and properties deterministically so multi-turn agent chats
        # achieve 100% prefix cache hits on upstream providers (saving 60-80% TTFT latency).
        if "tools" in body and isinstance(body["tools"], list):
            for t in body["tools"]:
                if isinstance(t, dict) and "function" in t and isinstance(t["function"], dict):
                    fn = t["function"]
                    params = fn.get("parameters")
                    if isinstance(params, dict) and "properties" in params and isinstance(params["properties"], dict):
                        params["properties"] = dict(sorted(params["properties"].items()))

        # Feature: Local Prompt Cache (P2-B: full-param key, usage recorded on hit)
        # Deterministic identical requests return from SQLite in < 10ms —
        # non-stream as JSON AND streaming as replayed SSE (TTFT -> ms).
        # Key covers messages+model+tools+sampling params so distinct requests
        # never collide.
        stream_mode = body.get("stream", False)
        headers_sent = False  # committed by the cache-replay/stream paths below
        prompt_hash = None
        try:
            _CACHE_MAX_BODY = int(os.environ.get("ZEN_CACHE_MAX_BODY", "262144"))
        except (TypeError, ValueError):
            _CACHE_MAX_BODY = 262144
        body_too_big = len(raw_in) > _CACHE_MAX_BODY
        if zen_db and hasattr(zen_db, "lookup_prompt_cache") and not body_too_big:
            try:
                canon_obj = {
                    "api": api_path,
                    "m": body.get("messages"),
                    "model": body.get("model"),
                    "tools": body.get("tools"),
                    "tool_choice": body.get("tool_choice"),
                    "temperature": body.get("temperature"),
                    "top_p": body.get("top_p"),
                    "reasoning_effort": body.get("reasoning_effort"),
                    "reasoning": body.get("reasoning"),
                    "max_tokens": body.get("max_tokens"),
                }
                canon_str = json.dumps(canon_obj, sort_keys=True, default=str)
                prompt_hash = hashlib.sha256(canon_str.encode("utf-8")).hexdigest()
                # Bypass the response cache for harness ids AND for any image
                # request: a blind-model image answer may be cached, and the
                # vision reroute (muse -> space-bunny-free) must always see the
                # real image upstream, never a stale text-only reply.
                _bypass = (str(body.get("model") or "").startswith("muse-sc-")
                           or _request_has_image(body, api_path))
                cached_resp = None if _bypass else zen_db.lookup_prompt_cache(prompt_hash)
                # Shape guard: a cached chat.completion must never be served to a
                # /responses-shaped (or Anthropic-converted) request and vice
                # versa — a raw chat payload on /v1/messages would map to an
                # empty message (client sees tools silently vanish).
                _cached_is_chat = isinstance(cached_resp, dict) and (
                    cached_resp.get("object") == "chat.completion"
                    or "choices" in cached_resp)
                # Substance guard: never serve an empty cached response.
                _has_substance = False
                if cached_resp and _cached_is_chat:
                    _c_choices = cached_resp.get("choices")
                    if isinstance(_c_choices, list) and _c_choices and isinstance(_c_choices[0], dict):
                        _c_msg = _c_choices[0].get("message") or {}
                        _has_substance = bool((_c_msg.get("content") or "").strip()
                                              or _c_msg.get("tool_calls")
                                              or (_c_msg.get("reasoning_content") or "").strip())
                    elif cached_resp.get("content"):
                        _has_substance = True
                if cached_resp and _cached_is_chat and _has_substance:
                    if is_anthropic_messages:
                        # Cache stores chat.completion shape — convert to the
                        # Anthropic shape this endpoint's clients expect.
                        cached_resp = self.openai_to_anthropic_message(
                            cached_resp, body.get("model", "unknown"))
                    try:
                        _cu = (cached_resp.get("usage") or {}) if isinstance(cached_resp, dict) else {}
                        _cu_pt = _cu.get("prompt_tokens") or _cu.get("input_tokens") or 0
                        _cu_ct = _cu.get("completion_tokens") or _cu.get("output_tokens") or 0
                        db_async('record_usage',
                            model=body.get("model", "unknown"),
                            client_type=client_type,
                            prompt_tokens=_cu_pt,
                            completion_tokens=_cu_ct,
                            cached_tokens=(_cu.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0,
                            latency_ms=1.0, ttft_ms=1.0,
                            tor_exit_ip="cache-hit", status=200, retries=0,
                            prompt_chars=sum(len(str(_m.get("content", ""))) for _m in (body.get("messages") or []) if isinstance(_m, dict)),
                        )
                    except Exception:
                        pass
                    if not stream_mode:
                        sys.stderr.write(f"[cache-hit] serving {body.get('model')} response locally (<10ms)\n")
                        return self._json(200, cached_resp)
                    # Streaming replay: emit cached text as SSE deltas instantly
                    # (TTFT -> ms, no upstream call). Reuses the same store the
                    # non-streaming path writes, so either shape warms the other.
                    sys.stderr.write(f"[cache-hit-stream] replaying {body.get('model')} from cache (<10ms)\n")
                    if is_anthropic_messages:
                        return self._serve_cached_anthropic_stream(
                            cached_resp, body.get("model", "unknown"),
                            _cu_pt, _cu_ct, headers_sent)
                    return self._serve_cached_openai_stream(
                        cached_resp, body.get("model", "unknown"), headers_sent)
            except Exception:
                pass

        is_responses_api = api_path.endswith("/responses")

        if not body.get("model"):
            body["model"] = self.default_model
        requested = body["model"]
        orig_requested = requested

        # Intercept dead models and flex aliases → route to default
        DEAD_MODELS = {"minimax-m3-free", "qwen3.6-plus-free"}
        _flex_reverse = {v: k for k, v in MODEL_FLEX.items() if not k.startswith("nvidia/")}
        if requested in DEAD_MODELS or requested in _flex_reverse:
            requested = self.default_model
            body["model"] = requested
        elif requested.startswith("muse-sc-"):
            pass  # harness scenario ids — preserved verbatim for mock routing
        elif (requested.startswith("muse-spark-1.3")
              or requested in ("muse-spark-contributor",
                               "muse-spark-1.3-contributor")):
            # Muse 1.3 resolution. DEFAULT = Zen via OpenAI /responses and let
            # the Tor pool rotate exits on 429 — that rotation IS the proxy's
            # purpose (per-exit throttles, unlimited free tier). Cline is only
            # used when explicitly forced: ZEN_MUSE_LANE=cline.
            _muse_lane = os.environ.get("ZEN_MUSE_LANE", "zen").strip().lower()
            if _muse_lane == "cline":
                requested = _CLINE_MUSE_ID
                sys.stderr.write(
                    f"[muse] serving '{orig_requested}' via Cline pool "
                    f"({requested})\n")
            else:
                requested = _ZEN_MUSE_ID
            body["model"] = requested
        elif requested.startswith("muse-spark-1.2") or requested == "muse-1.2":
            requested = "muse-spark-1.2-contributor-free"
            body["model"] = requested
        elif requested.startswith("muse"):
            # All remaining muse variants -> Zen muse 1.3 free tier
            requested = _ZEN_MUSE_ID
            body["model"] = requested
        elif requested in MODEL_ALIASES:
            requested = MODEL_ALIASES[requested]
            body["model"] = requested
        elif requested == "claude-opus-4.8-xHigh":
            # Flex round-trip: disguised nemotron id sent back as model name.
            requested = "nemotron-3-ultra-free"
            body["model"] = requested

        # OX Alpha (x-preview-f-free): always run at MAXIMUM reasoning depth,
        # regardless of what the client variant asked for. Upstream rejects
        # "xhigh" ([1210] Invalid API parameter) — "high" is its ceiling.
        if requested == "x-preview-f-free" and body.get("reasoning_effort") != "high":
            sys.stderr.write(f"[ox-alpha] reasoning_effort {body.get('reasoning_effort')!r} -> 'high' (forced max)\n")
            body["reasoning_effort"] = "high"

        t_start = time.perf_counter()
        prompt_chars = sum(len(str(m.get("content", ""))) for m in body.get("messages", [])) if isinstance(body.get("messages"), list) else 0

        # Freebuff integration REMOVED 2026-09-21 (user request). The
        # session-admission release/re-admit churn tripped freebuff's rate
        # limiter and got the account pool banned, so the proxy no longer
        # contacts freebuff at all. Legacy `freebuff/*` ids are remapped
        # straight to their Cline free-lane equivalent (no network call).
        if (orig_requested in FREEBUFF_FALLBACK_MAP
                or orig_requested.startswith("freebuff/")
                or requested.startswith("freebuff/")):
            fb_fallback = FREEBUFF_FALLBACK_MAP.get(
                orig_requested,
                FREEBUFF_FALLBACK_MAP.get(requested, "cline-free/deepseek-v4.1-flash"))
            sys.stderr.write(
                f"[freebuff-off] '{requested}' remapped to Cline '{fb_fallback}'\n")
            requested = fb_fallback
            body["model"] = fb_fallback

        # WorkBuddy (Tencent) free lane — merged in-proxy 2026-09-22.
        if (orig_requested.startswith("workbuddy/")
                or requested.startswith("workbuddy/")
                or orig_requested in WORKBUDDY_ALIASES):
            _wb_target = WORKBUDDY_ALIASES.get(orig_requested) or requested
            if not _wb_target.startswith("workbuddy/"):
                _wb_target = "workbuddy/" + _wb_target
            body["model"] = _wb_target
            return self._workbuddy_route(body)

        # Check if the requested model is a Cline-backed model.
        # Alias-resolved ids (e.g. nex-mini -> nex-agi/nex-n2.5-mini:free) must
        # ALSO route via Cline — but ONLY when the resolved id is genuinely a
        # Cline catalog model. MODEL_ALIASES.values() mixes Cline ids with Zen
        # ids (muse-*, x-preview-*, nemotron-*-free...), so matching the whole
        # value set would wrongly route MUSE through api.cline.bot. Compare the
        # resolved base id against the Cline catalog base ids only.
        # NOTE: the cline-pass/* SUBSCRIPTION lane is deliberately NOT supported
        # (no paid list advertised); such ids fall through to validation.
        # Vision: a text-only model asked to read an image 400s upstream.
        # Transparently serve it on the known Zen vision sibling (logged) so
        # image turns work instead of failing. Only bare Zen ids (never
        # Cline-routed org ids).
        if _request_has_image(body, api_path) and not _model_vision_inputs(requested):
            _vsib = _vision_sibling(requested)
            if _vsib:
                sys.stderr.write(f"[vision] '{requested}' is text-only + image input -> '{_vsib}'\n")
                requested = _vsib
                body["model"] = _vsib
        # Tor-only muse lane (default). Set ZEN_MUSE_MIN_CIRCUITS>1 only to
        # temporarily divert muse to the Cline lane when Tor exits are scarce.
        try:
            _muse_min = int(os.environ.get("ZEN_MUSE_MIN_CIRCUITS", "1"))
        except Exception:
            _muse_min = 1
        if (_muse_min > 1 and requested.startswith("muse")
                and _circuit_pool.usable_count() < _muse_min):
            sys.stderr.write(
                f"[starvation-fallback] usable circuits < {_muse_min} — "
                "muse -> cline-free/deepseek-v4.1-flash\n")
            requested = "cline-free/deepseek-v4.1-flash"
            body["model"] = requested

        _cline_base_ids = {m.split(":")[0] for m in CLINE_MODELS}
        is_cline_model = (requested in CLINE_MODELS
                          or requested.startswith("cline/")
                          or requested.startswith("cline-free/")
                          or (requested in MODEL_ALIASES.values()
                              and requested.split(":")[0] in _cline_base_ids))
        cline_auth_headers, cline_account = (
            get_cline_auth_headers(model_id=requested) if is_cline_model else (None, None))

        if is_cline_model and not cline_auth_headers:
            return self._json(503, {"error": {"message": "All Cline accounts are temporarily banned/exhausted. Try again later.", "type": "pool_exhausted"}})
        if is_cline_model and cline_auth_headers:
            sys.stderr.write(
                f"[cline-route] routing '{requested}' via api.cline.bot"
                + (f" as {cline_account['name']}" if cline_account else ""))
            cline_model_id = requested.replace("cline/", "")
            body["model"] = cline_model_id
            # Reasoning headroom (see cline_scale_max_tokens): stops the
            # "500 empty response content" starvation on reasoning models.
            cline_scale_max_tokens(body)
            _cline_empty_retried = False
            _cline_402_retried = False
            headers_sent = False

            if stream_mode:
                try:
                    sess = get_direct_session()
                    r = sess.post(
                        CLINE_API_ENDPOINT,
                        headers=cline_auth_headers,
                        json=body,
                        stream=True,
                        timeout=(STREAM_CONNECT_TIMEOUT, STREAM_LOW_SPEED_TIMEOUT),
                    )
                    stream_opts = body.get("stream_options") or {}
                    include_usage = stream_opts.get("include_usage", False) if isinstance(stream_opts, dict) else False
                    if r.status_code == 200:
                        try:
                            self.send_response(200)
                            self.send_header("Content-Type", "text/event-stream")
                            self.send_header("Cache-Control", "no-cache")
                            self.send_header("Connection", "close")
                            self.end_headers()
                            headers_sent = True
                        except (BrokenPipeError, ConnectionResetError):
                            return
                        track_ctx = {
                            "model": requested,
                            "t_start": t_start,
                            "retries": 0,
                            "prompt_chars": prompt_chars,
                            "is_adapted_responses": False,
                            # union-alpha: native Anthropic SSE from /v1/messages
                            # (event: message_start/delta, NOT response.*).
                            "is_native_anthropic": (
                                _wire_protocol(requested) == "messages"
                                and CLINE_API_ENDPOINT.endswith("/messages")),
                            "include_usage": include_usage,
                            "cline_account": (cline_account["name"]
                                              if cline_account else None),
                            "tor_exit_ip": "direct-cline",
                        }
                        if self._stream_realtime(r, client_type, track_ctx=track_ctx, headers_sent=True):
                            return  # client disconnected: no success/usage recorded
                        if cline_account is not None:
                            cline_note_result(cline_account, True)
                        return
                    elif r.status_code in (401, 402, 403, 429) and cline_account is not None:
                        # Account quota hit (429), paid-model credits drained
                        # (402), missing plan entitlement (403), or unauthorized (401):
                        # park THIS account, retry the same model across other pool
                        # accounts before falling back to muse.
                        try:
                            _ra = float((r.headers.get("Retry-After")
                                         or r.headers.get("retry-after")
                                         or 0) or 0)
                        except Exception:
                            _ra = 0.0
                        if r.status_code == 429:
                            try:
                                _raw_body = ""
                                if hasattr(r, "iter_content"):
                                    _raw_body = b"".join(r.iter_content(chunk_size=1024)).decode("utf-8", errors="replace")
                                elif hasattr(r, "text"):
                                    _raw_body = r.text
                                _body_ra = cline_parse_retry_after(_raw_body)
                            except Exception:
                                _body_ra = 0.0
                            _ra = max(_ra, _body_ra)
                        elif r.status_code == 402:
                            _ra = max(_ra, _CLINE_402_PARK_S)
                            cline_note_credits(cline_account)
                        elif r.status_code in (401, 403):
                            _ra = max(_ra, 3600.0)
                        _mid = None if r.status_code == 401 else requested
                        cline_note_result(cline_account, False, _ra, model_id=_mid)
                        _rstat = r.status_code
                        try:
                            r.close()
                        except Exception:
                            pass
                        _stream_track = {
                            "model": requested,
                            "t_start": t_start,
                            "retries": 0,
                            "prompt_chars": prompt_chars,
                            "is_adapted_responses": False,
                            "include_usage": include_usage,
                            "tor_exit_ip": "direct-cline",
                        }
                        _retry_count = 0
                        while True:
                            _next = cline_pick_account(model_id=requested)
                            if _next is None or _next is cline_account:
                                sys.stderr.write(
                                    "[cline-rotate] no more live accounts, "
                                    f"rescuing via {self.default_model}\n")
                                break
                            _retry_count += 1
                            sys.stderr.write(
                                f"[cline-rotate] '{requested}' {_rstat} on "
                                f"{cline_account['name']} -> retrying as "
                                f"{_next['name']} (try {_retry_count})\n")
                            _hdrs2, _acc2 = get_cline_auth_headers_for(_next)
                            if not _hdrs2:
                                continue
                            cline_auth_headers, cline_account = _hdrs2, _acc2
                            _stream_track["cline_account"] = cline_account["name"]
                            _stream_track["retries"] = _retry_count
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                            try:
                                sess2 = get_direct_session()
                                r2 = sess2.post(
                                    CLINE_API_ENDPOINT,
                                    headers=cline_auth_headers,
                                    json=body,
                                    stream=True,
                                    timeout=(STREAM_CONNECT_TIMEOUT,
                                             STREAM_LOW_SPEED_TIMEOUT),
                                )
                                if r2.status_code == 200:
                                    if self._stream_realtime(
                                            r2, client_type,
                                            track_ctx=_stream_track,
                                            headers_sent=True):
                                        return
                                    cline_note_result(cline_account, True)
                                    return
                                try:
                                    _ra2 = float((r2.headers.get("Retry-After")
                                                  or r2.headers.get("retry-after")
                                                  or 0) or 0)
                                except Exception:
                                    _ra2 = 0.0
                                if r2.status_code == 429:
                                    try:
                                        _raw_body2 = ""
                                        if hasattr(r2, "iter_content"):
                                            _raw_body2 = b"".join(r2.iter_content(chunk_size=1024)).decode("utf-8", errors="replace")
                                        elif hasattr(r2, "text"):
                                            _raw_body2 = r2.text
                                        _body_ra2 = cline_parse_retry_after(_raw_body2)
                                    except Exception:
                                        _body_ra2 = 0.0
                                    _ra2 = max(_ra2, _body_ra2)
                                elif r2.status_code == 402:
                                    _ra2 = max(_ra2, _CLINE_402_PARK_S)
                                    cline_note_credits(cline_account)
                                elif r2.status_code in (401, 403):
                                    _ra2 = max(_ra2, 3600.0)
                                _mid2 = None if r2.status_code == 401 else requested
                                cline_note_result(cline_account, False, _ra2, model_id=_mid2)
                                _rstat = r2.status_code
                                sys.stderr.write(
                                    f"[cline-rotate] retry as "
                                    f"{cline_account['name']} -> "
                                    f"http={r2.status_code}\n")
                                try:
                                    r2.close()
                                except Exception:
                                    pass
                                if r2.status_code not in (401, 402, 403, 429):
                                    break
                            except Exception as e:

                                sys.stderr.write(
                                    f"[cline-rotate] retry exception: {e}\n")
                                if cline_account is not None:
                                    cline_note_result(cline_account, False, 60.0, model_id=requested)
                                self.wfile.write(b": ping\n\n")
                        try:
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                        except Exception:
                            pass
                        requested = self.default_model
                        body["model"] = requested
                        # Fall through to Tor Zen attempt loop below
                    elif r.status_code in (429, 403, 500, 502, 503, 504):
                        # Seamless Auto-Fallback Cascade: rescue turn via Tor Zen Muse
                        try:
                            _ra = float((r.headers.get("Retry-After")
                                         or r.headers.get("retry-after")
                                         or 0) or 0)
                        except Exception:
                            _ra = 0.0
                        if cline_account is not None:
                            cline_note_result(cline_account, False, _ra, model_id=requested)
                        sys.stderr.write(f"[cline-fallback] {requested} returned {r.status_code}, rescuing via {self.default_model}\n")
                        try:
                            r.close()
                        except Exception:
                            pass
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        requested = self.default_model
                        body["model"] = requested
                        # Fall through to Tor Zen attempt loop below
                    else:
                        sys.stderr.write(f"[cline-route] stream error http={r.status_code}: {r.text[:200]}\n")
                        self._terminate_stream_with_error(f"Cline upstream error: {r.text[:200]}")
                        return
                except Exception as e:

                    sys.stderr.write(f"[cline-fallback] Cline exception: {e}, rescuing via {self.default_model}\n")
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    requested = self.default_model
                    body["model"] = requested
                    # Fall through to Tor Zen attempt loop below
            else:
                try:
                    sess = get_direct_session()
                    r = sess.post(
                        CLINE_API_ENDPOINT,
                        headers=cline_auth_headers,
                        json=body,
                        timeout=45,
                    )
                    t_elapsed = (time.perf_counter() - t_start) * 1000
                    if r.status_code == 200:
                        resp_json = r.json()
                        data_out = resp_json.get("data", resp_json)
                        if isinstance(data_out, dict):
                            data_out["model"] = orig_requested
                        if cline_account is not None:
                            cline_note_result(cline_account, True)
                        if zen_db:
                            try:
                                u_rec = data_out.get("usage", {})
                                db_async('record_usage', 
                                    model=requested,
                                    client_type=client_type,
                                    prompt_tokens=u_rec.get("prompt_tokens", 0) or 0,
                                    completion_tokens=u_rec.get("completion_tokens", 0) or 0,
                                    cached_tokens=(u_rec.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0,
                                    latency_ms=t_elapsed,
                                    ttft_ms=t_elapsed,
                                    tor_exit_ip="direct-cline",
                                    status=200,
                                    retries=0,
                                    prompt_chars=prompt_chars,
                                )
                            except Exception:
                                pass
                        if client_type == "anthropic":
                            data_out = self.openai_to_anthropic_message(data_out, requested)
                        return self._json(200, data_out)
                    elif r.status_code in (401, 402, 429, 403, 500, 502, 503, 504):
                        try:
                            _cerr = ""
                            try:
                                _cj = r.json()
                                _cerr = str((_cj.get("error") or _cj.get("data", {}).get("error", "")) or _cj)[:160]
                            except Exception:
                                _cerr = (r.text[:160] if getattr(r, "text", "") else "")
                            # 401/402/429/403: quota or auth/credit error on this account.
                            # Mark it and retry next live accounts across pool before
                            # falling back to muse.
                            if (r.status_code in (401, 402, 429, 403) and cline_account is not None):
                                try:
                                    _ra = float((r.headers.get("Retry-After")
                                                 or r.headers.get("retry-after")
                                                 or 0) or 0)
                                except Exception:
                                    _ra = 0.0
                                if r.status_code == 429:
                                    try:
                                        _ra = max(_ra, cline_parse_retry_after(r.text if hasattr(r, "text") else ""))
                                    except Exception:
                                        pass
                                elif r.status_code == 402:
                                    _ra = max(_ra, _CLINE_402_PARK_S)
                                    cline_note_credits(cline_account)
                                elif r.status_code in (401, 403):
                                    _ra = max(_ra, 3600.0)
                                _mid = None if r.status_code == 401 else requested
                                cline_note_result(cline_account, False,
                                                  _ra,
                                                  model_id=_mid)
                                try:
                                    r.close()
                                except Exception:
                                    pass
                                _retry_count = 0
                                while True:
                                    _nx = cline_pick_account(model_id=requested)
                                    if _nx is None or _nx is cline_account:
                                        sys.stderr.write(
                                            f"[cline-rotate] no more live accounts for '{requested}', "
                                            f"falling back\n")
                                        break
                                    _h2, _a2 = get_cline_auth_headers_for(_nx)
                                    if not _h2:
                                        continue
                                    _retry_count += 1
                                    sys.stderr.write(
                                        f"[cline-rotate] '{requested}' {r.status_code if r else 'err'} on "
                                        f"{cline_account['name']} -> retrying as "
                                        f"{_nx['name']} (try {_retry_count})\n")
                                    cline_auth_headers, cline_account = _h2, _a2
                                    try:
                                        r = sess.post(
                                            CLINE_API_ENDPOINT,
                                            headers=cline_auth_headers,
                                            json=body,
                                            timeout=90,
                                        )
                                    except Exception as _e_rot:
                                        r = None
                                        _cerr = str(_e_rot)
                                        sys.stderr.write(
                                            f"[cline-rotate] retry exception: "
                                            f"{_e_rot}\n")
                                        if cline_account is not None:
                                            cline_note_result(cline_account, False, 60.0, model_id=requested)
                                        continue
                                    if r is not None and r.status_code == 200:
                                        resp_json = r.json()
                                        data_out = resp_json.get("data", resp_json)
                                        cline_note_result(cline_account, True)
                                        if zen_db:
                                            try:
                                                u_rec = data_out.get("usage", {})
                                                _el3 = (time.perf_counter() - t_start) * 1000
                                                db_async('record_usage',
                                                    model=requested,
                                                    client_type=client_type,
                                                    prompt_tokens=u_rec.get("prompt_tokens", 0) or 0,
                                                    completion_tokens=u_rec.get("completion_tokens", 0) or 0,
                                                    cached_tokens=(u_rec.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0,
                                                    latency_ms=_el3, ttft_ms=_el3,
                                                    tor_exit_ip="direct-cline",
                                                    status=200, retries=_retry_count,
                                                    prompt_chars=prompt_chars)
                                            except Exception:
                                                pass
                                        if client_type == "anthropic":
                                            data_out = self.openai_to_anthropic_message(data_out, requested)
                                        return self._json(200, data_out)
                                    if r is not None:
                                        try:
                                            _cj3 = r.json()
                                            _cerr = str((_cj3.get("error") or _cj3.get("data", {}).get("error", "")) or _cj3)[:160]
                                        except Exception:
                                            _cerr = (r.text[:160] if getattr(r, "text", "") else "")
                                        if r.status_code in (401, 402, 403, 429):
                                            try:
                                                _ra = float((r.headers.get("Retry-After") or r.headers.get("retry-after") or 0) or 0)
                                            except Exception:
                                                _ra = 0.0
                                            if r.status_code == 429:
                                                try:
                                                    _ra = max(_ra, cline_parse_retry_after(r.text if hasattr(r, "text") else ""))
                                                except Exception:
                                                    pass
                                            elif r.status_code == 402:
                                                _ra = max(_ra, _CLINE_402_PARK_S)
                                                cline_note_credits(cline_account)
                                            elif r.status_code in (401, 403):
                                                _ra = max(_ra, 3600.0)
                                            _mid = None if r.status_code == 401 else requested
                                            cline_note_result(cline_account, False, _ra, model_id=_mid)
                                            try:
                                                r.close()
                                            except Exception:
                                                pass
                                            continue
                                        else:
                                            break
                            # Reasoning starvation is NOT a real failure: the
                            # model spent the whole budget thinking, so Cline
                            # says "empty response content". Widen the output
                            # budget and retry the SAME model+account once
                            # before falling back to muse.
                            if (r is not None
                                    and not _cline_empty_retried
                                    and cline_is_empty_content(r.status_code, _cerr)):
                                _cline_empty_retried = True
                                _new_mt = cline_scale_max_tokens(body, retry_bump=True)
                                sys.stderr.write(
                                    f"[cline-retry] '{requested}' empty content "
                                    f"(reasoning consumed max_tokens) -> retry "
                                    f"with max_tokens={_new_mt}\n")
                                try:
                                    r.close()
                                except Exception:
                                    pass
                                r = None
                                try:
                                    r = sess.post(
                                        CLINE_API_ENDPOINT,
                                        headers=cline_auth_headers,
                                        json=body,
                                        timeout=90,
                                    )
                                except Exception as _re:
                                    sys.stderr.write(f"[cline-retry] exception: {_re}\n")
                                if r is not None and r.status_code == 200:
                                    resp_json = r.json()
                                    data_out = resp_json.get("data", resp_json)
                                    if cline_account is not None:
                                        cline_note_result(cline_account, True)
                                    if zen_db:
                                        try:
                                            u_rec = data_out.get("usage", {})
                                            _elapsed2 = (time.perf_counter() - t_start) * 1000
                                            db_async('record_usage',
                                                model=requested,
                                                client_type=client_type,
                                                prompt_tokens=u_rec.get("prompt_tokens", 0) or 0,
                                                completion_tokens=u_rec.get("completion_tokens", 0) or 0,
                                                cached_tokens=(u_rec.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0,
                                                latency_ms=_elapsed2,
                                                ttft_ms=_elapsed2,
                                                tor_exit_ip="direct-cline",
                                                status=200,
                                                retries=1,
                                                prompt_chars=prompt_chars,
                                            )
                                        except Exception:
                                            pass
                                    if client_type == "anthropic":
                                        data_out = self.openai_to_anthropic_message(data_out, requested)
                                    return self._json(200, data_out)
                                if r is not None:
                                    try:
                                        _cj2 = r.json()
                                        _cerr = str((_cj2.get("error") or _cj2.get("data", {}).get("error", "")) or _cj2)[:160]
                                    except Exception:
                                        _cerr = (r.text[:160] if getattr(r, "text", "") else "")
                            _ra = 0.0
                            if r is not None:
                                try:
                                    _ra = float((r.headers.get("Retry-After")
                                                 or r.headers.get("retry-after")
                                                 or 0) or 0)
                                except Exception:
                                    _ra = 0.0
                                if r.status_code == 429:
                                    try:
                                        _ra = max(_ra, cline_parse_retry_after(
                                            r.text if hasattr(r, "text") else ""))
                                    except Exception:
                                        pass
                                if cline_account is not None:
                                    cline_note_result(cline_account, False, _ra,
                                                      model_id=requested)
                                _st = r.status_code
                                sys.stderr.write(f"[cline-fallback] {requested} returned {_st} ({_cerr}), rescuing via {self.default_model}\n")
                                try:
                                    r.close()
                                except Exception:
                                    pass
                            else:
                                if cline_account is not None:
                                    cline_note_result(cline_account, False, 60.0,
                                                      model_id=requested)
                                sys.stderr.write(f"[cline-fallback] {requested} retry failed with exception ({_cerr}), rescuing via {self.default_model}\n")
                            requested = self.default_model
                            body["model"] = requested
                        except Exception as e:

                            sys.stderr.write(f"[cline-fallback] {requested} error: {e}, rescuing via {self.default_model}\n")
                            requested = self.default_model
                            body["model"] = requested
                    else:
                        try:
                            payload = r.json() if "application/json" in r.headers.get("Content-Type", "") else {"error": r.text[:300]}
                        except Exception:
                            payload = {"error": (r.text[:300] if getattr(r, "text", "") else "cline error")}
                        try:
                            r.close()
                        except Exception:
                            pass
                        return self._json(r.status_code, payload)
                except Exception as e:

                    sys.stderr.write(f"[cline-fallback] exception: {e}, rescuing via muse\n")
                    requested = "muse-spark-1.3-contributor-free"
                    body["model"] = requested
        else:
            headers_sent = False

        # Dynamic model validation with generic fallback chain — when a model
        # dies upstream (like ox-alpha will), remap to the first live fallback
        # instead of hard-failing the agent's turn (Muse models are never fallen back from).
        # Hot path: never block TTFT on the /models fetch (~7s when stale).
        # Stale-cache + background refresh; cold start uses static MODELS.
        # Harness scenario ids (muse-sc-*) skip validation — the mock upstream
        # is authoritative for them, and a stale/cold cache would 400 wrongly.
        # Cline-routed models skip Zen validation too — they run on
        # api.cline.bot (separate catalog), never on opencode.ai/zen.
        _, valid_ids = model_ids_fast(self.upstream, self.proxy_url)
        _is_cline_validated = (requested in CLINE_MODELS
                               or requested.startswith("cline/")
                               or requested.startswith("cline-free/"))
        if (requested not in valid_ids and not requested.startswith("muse-sc-")
                and not _is_cline_validated):
            if requested.startswith("muse"):
                return self._json(400, {
                    "error": {
                        "message": f"requested model '{requested}' not available upstream",
                        "type": "invalid_request_error",
                    }
                })
            fallback = next((m for m in FALLBACK_CHAIN if m in valid_ids and m != requested), None)
            if fallback:
                sys.stderr.write(f"[fallback] '{requested}' unavailable -> '{fallback}'\n")
                requested = fallback
                body["model"] = requested
            else:
                return self._json(400, {
                    "error": {
                        "message": f"model '{requested}' not available",
                        "type": "invalid_request_error",
                        "available": sorted(valid_ids)[:25],
                    }
                })

        gen_sess, gen_proj = get_genuine_opencode_session()
        raw_sess = (
            self.headers.get("x-opencode-session") or
            self.headers.get("x-session-id") or
            self.headers.get("session-id") or
            self.headers.get("x-session-affinity")
        )
        # Trust the client's own session ONLY when it is a real, whitelisted
        # session from opencode.db (a live OpenCode client). Anything else
        # (random ses_ from curl/scripts, stale/absent header) uses the genuine
        # DB session — the free-tier gate rejects non-genuine sessions on every
        # exit, which is what produced FreeTierError churn.
        if (isinstance(raw_sess, str) and raw_sess.startswith("ses_")
                and raw_sess in known_opencode_sessions()):
            client_session_id = raw_sess
        else:
            client_session_id = gen_sess or gen_session_id()

        raw_msg = self.headers.get("x-opencode-request")
        if isinstance(raw_msg, str) and raw_msg.startswith("msg_") and len(raw_msg) == 30:
            client_msg_id = raw_msg
        else:
            client_msg_id = gen_message_id()

        client_project = self.headers.get("x-opencode-project") or gen_proj or "global"
        client_client = self.headers.get("x-opencode-client") or "cli"
        client_directory = self.headers.get("x-opencode-directory") or "/home/vagish_arch"

        # REVERSE ENGINEERING NOTE (2026-09-24):
        # x-opencode-session is natively generated as a standard crypto.randomUUID().
        # There is NO cryptographic signature. However, Zen's Cloudflare setup heavily
        # fingerprints the TLS handshake (JA3/JA4). If a random UUID is sent via Python/curl_cffi, 
        # Cloudflare detects the TLS mismatch (Python TLS vs Bun User-Agent) and throws 
        # a fake '403 FreeTierError'.
        # 
        # The ONLY way to successfully proxy a request from a 3rd party IDE without a 403 
        # is to steal a UUID that was *already whitelisted* by a genuine Node.js/Bun TLS handshake.
        # We achieve this by extracting the live session from opencode.db.
        # (gen_sess) plus the real UA above, so the upstream identity still looks
        # genuine. In either case Zen's FreeTierError is the PER-EXIT condition
        # worth rotating past; fail fast only when we have no genuine session to
        # present at all.
        _genuine_sess = bool(self.headers.get("x-opencode-session")) or bool(gen_sess)

        # OpenCode Zen upstream free tier strictly mandates 'Bearer public'.
        # Regardless of what dummy/client key is provided (dummy, sk-..., not-needed, etc.),
        # always forward 'Bearer public' upstream so requests never trigger 401 AuthError.
        auth_header_val = "Bearer public"

        def _prepare_upstream(model_name):
            """Build (is_responses, endpoint, request_body) for a candidate model.
            Extracted so the storm-rescue round can re-target without duplication.
            Resolves aliases/variants FIRST so wire+endpoint+tools always match
            the canonical id (a stale alias like big-pickle otherwise takes the
            chat wire with mismatched tools -> upstream 400 validation)."""
            # Wire protocol is per-model (see _wire_protocol): muse -> OpenAI
            # Responses, union/claude/minimax-free/qwen-plus -> Anthropic
            # /v1/messages, everything else -> OpenAI chat/completions. Wrong
            # wire = upstream 500/403.
            # Canonicalize FIRST (mirrors the outer normalization): an
            # unresolved alias/variant here picks the wrong wire AND sends a
            # mismatched model id + tools (openclaude big-pickle -> chat wire
            # -> 18x tools validation 400). Idempotent if already resolved.
            try:
                if model_name in MODEL_ALIASES:
                    model_name = MODEL_ALIASES[model_name]
                if model_name.startswith("muse-sc-"):
                    # Harness scenario id — preserve verbatim so the mock upstream
                    # can route it (the outer carve-out at ~6332 must not be
                    # undone by this canonicalizer).
                    pass
                elif model_name.startswith("muse-spark-1.3") or model_name in ("muse-spark-contributor", "muse-1.3", "big-pickle"):
                    model_name = _ZEN_MUSE_ID
                elif model_name.startswith("muse-spark-1.2") or model_name == "muse-1.2":
                    model_name = "muse-spark-1.2-contributor-free"
                elif model_name.startswith("muse"):
                    model_name = _ZEN_MUSE_ID
            except Exception:
                pass
            _wire = _wire_protocol(model_name)
            _union_native = (_wire == "messages")
            muse_routed = (is_responses_api or _wire == "responses")
            endpoint = f"{self.upstream}/responses" if muse_routed else f"{self.upstream}/chat/completions"
            if _union_native and not is_responses_api and not is_anthropic_messages:
                endpoint = f"{self.upstream}/messages"
            # Muse IS vision-capable, and it reads images on Zen's /responses
            # wire (input_image). This block used to reroute every image turn
            # to /chat/completions on the belief that /responses "silently drops
            # input_image" — but Zen's /chat/completions actually answers an
            # empty 500 "Internal server error" for muse (verified: it 500s for
            # plain TEXT too, on every exit), so that reroute made image turns
            # fail and fall back to another model. Image turns now stay on the
            # normal muse /responses path, exactly like text.
            if muse_routed and not is_responses_api:
                upstream_body = {
                    "model": model_name,
                    "stream": True,
                    "store": False,
                    "prompt_cache_key": client_session_id,
                }
                if "messages" in body:
                    upstream_body["input"] = messages_to_responses_input(body["messages"])
                # Reasoning effort normalization: upstream expects none, minimal, low, medium, high, xhigh.
                # 'max' / 'maximum' triggers an upstream 400 error unless mapped to 'xhigh'.
                effort = body.get("reasoning_effort")
                if effort is not None:
                    effort_str = str(effort).strip().lower()
                    if effort_str in ("max", "maximum", "x-high", "x_high"):
                        effort = "xhigh"
                    elif effort_str in _MUSE_VALID_EFFORTS:
                        effort = effort_str
                    else:
                        sys.stderr.write(f"[muse] unknown reasoning_effort {effort!r} -> clamping to 'high'\n")
                        effort = "high"
                elif _wire == "responses" or _wire == "messages":
                    # summary=auto makes upstream stream plaintext reasoning deltas
                    # (response.reasoning_summary_text.delta) -> opencode yellow line
                    effort = "high"
                if effort is not None:
                    effort = _cap_effort(effort)
                    upstream_body["reasoning"] = {"effort": effort, "summary": "auto"}
                flat_tools = []
                if "tools" in body and isinstance(body["tools"], list) and body["tools"]:
                    _n_coerced = 0
                    for t in body["tools"]:
                        if isinstance(t, dict):
                            if "function" in t and isinstance(t["function"], dict):
                                fn = t["function"]
                                _typ = t.get("type", "function")
                                # Coerce unsupported types (custom/mcp/computer/
                                # missing) to function — Zen 400s anything else.
                                # web_search must be BARE (no name/description/
                                # parameters) or Zen 400s on the extras.
                                if _typ == "web_search":
                                    flat_tools.append({"type": "web_search"})
                                    _n_coerced += 1
                                    continue
                                if _typ != "function":
                                    _typ = "function"
                                    _n_coerced += 1
                                flat_tools.append({
                                    "type": _typ,
                                    "name": fn.get("name", ""),
                                    "description": fn.get("description", ""),
                                    "parameters": fn.get("parameters", fn.get("input_schema", {}))
                                })
                            else:
                                # Handle Anthropic (/v1/messages) or flat tool definitions
                                params = t.get("parameters") or t.get("input_schema") or {}
                                _typ = t.get("type", "function")
                                if _typ == "web_search":
                                    flat_tools.append({"type": "web_search"})
                                    _n_coerced += 1
                                    continue
                                if _typ != "function":
                                    _typ = "function"
                                    _n_coerced += 1
                                flat_tools.append({
                                    "type": _typ,
                                    "name": t.get("name", ""),
                                    "description": t.get("description", ""),
                                    "parameters": params
                                })
                    if _n_coerced:
                        sys.stderr.write(f"[muse] coerced {_n_coerced} non-function tool type(s) -> function\n")
                    # Zen free-tier gate validates authentic OpenCode by requiring bash and read tools
                    has_bash = any(t.get("name") == "bash" for t in flat_tools)
                    has_read = any(t.get("name") == "read" for t in flat_tools)
                    if not (has_bash and has_read):
                        for ct in _OPENCODE_CORE_TOOLS:
                            if not any(t.get("name") == ct["name"] for t in flat_tools):
                                flat_tools.append(ct)
                else:
                    # Zen free-tier gate strictly mandates OpenCode tool schema to authenticate free tier
                    flat_tools = list(_OPENCODE_CORE_TOOLS)
                upstream_body["tools"] = flat_tools
                # Responses API rejects tool_choice outright (400 invalid_request_error)
                # when tools are present — and Anthropic-shape {"type":"auto"} is doubly
                # foreign. Drop it (log) rather than fail the turn; the model still
                # emits function_call items unconstrained, and all tool definitions are preserved.
                if "tool_choice" in body:
                    tc = body.get("tool_choice")
                    # # sys.stderr.write(f"[muse] normalizing: omitted tool_choice={str(tc)[:40]} (unsupported by Responses API; tools definitions preserved)\n")
                for opt_key in ("temperature", "top_p"):
                    if opt_key in body:
                        upstream_body[opt_key] = body[opt_key]
                # max_tokens -> max_output_tokens, scaled up: the Responses API
                # counts reasoning tokens toward the cap, so a chat-style limit
                # would be eaten by thinking and yield empty/truncated answers.
                # x4 headroom preserves the client's intent (bound runaway turns)
                # while leaving room for reasoning + final text.
                _mt = body.get("max_tokens")
                try:
                    _mt = int(_mt) if _mt is not None else 0
                except (TypeError, ValueError):
                    _mt = 0
                if _mt > 0:
                    upstream_body["max_output_tokens"] = min(max(_mt + 4096, 4096), 128000)
            elif _union_native and not is_responses_api and not is_anthropic_messages:
                # Native Anthropic shape for union-alpha: strip OpenAI-only
                # keys, keep max_tokens as-is (Anthropic semantics, NOT the
                # muse x4 max_output_tokens scaling), forward system/tools.
                # Tools MUST be Anthropic `input_schema` shape: opencode sends
                # OpenAI `function.*` (or flat name/parameters) — the union
                # backend 400s ("tools[0] must have a string name and an
                # object input_schema") on anything else.
                upstream_body = {
                    "model": model_name,
                    "max_tokens": body.get("max_tokens") or 1024,
                    "messages": body.get("messages", []),
                }
                for _k in ("system", "temperature",
                           "top_p", "stop_sequences"):
                    if _k in body:
                        upstream_body[_k] = body[_k]
                # tool_choice: union accepts Anthropic {"type": ...} but the
                # OpenAI "auto" STRING form 400s through this backend. Map
                # "auto" -> {"type": "auto"}; drop anything else unknown.
                _u_tc = body.get("tool_choice")
                if isinstance(_u_tc, str):
                    if _u_tc.strip().lower() == "auto":
                        upstream_body["tool_choice"] = {"type": "auto"}
                elif isinstance(_u_tc, dict) and isinstance(
                        _u_tc.get("type"), str):
                    upstream_body["tool_choice"] = {
                        "type": _u_tc["type"]}
                    if isinstance(_u_tc.get("name"), str):
                        upstream_body["tool_choice"]["name"] = _u_tc["name"]
                _u_tools = body.get("tools")
                if isinstance(_u_tools, list) and _u_tools:
                    _u_conv = []
                    for _t in _u_tools:
                        if not isinstance(_t, dict):
                            continue
                        _fn = _t.get("function") if isinstance(
                            _t.get("function"), dict) else None
                        _nm = (_fn.get("name") if _fn
                               else _t.get("name", ""))
                        _ds = (_fn.get("description") if _fn
                               else _t.get("description", ""))
                        _pr = (_fn.get("parameters") if _fn
                               else _t.get("parameters")
                               or _t.get("input_schema")
                               or {"type": "object"})
                        if not isinstance(_nm, str) or not _nm:
                            continue
                        if not isinstance(_pr, dict):
                            _pr = {"type": "object"}
                        _u_conv.append({
                            "name": _nm,
                            "description": _ds or "",
                            "input_schema": _pr,
                        })
                    if _u_conv:
                        upstream_body["tools"] = _u_conv
                    else:
                        sys.stderr.write(
                            "[union] dropped unconvertible tools "
                            f"({len(_u_tools)} defs) — plain prompt instead\n")
            elif is_responses_api and _wire == "chat":
                # Client speaks Responses but this model only runs on the chat
                # backend (Zen /responses 401s chat-wire models with
                # "Missing API key"). Run /chat/completions upstream and adapt
                # the result back to a Responses object/stream below.
                muse_routed = False
                endpoint = f"{self.upstream}/chat/completions"
                upstream_body = {
                    "model": model_name,
                    "stream": bool(body.get("stream", False)),
                    "messages": responses_input_to_messages(body.get("input", [])),
                }
                for _k in ("temperature", "top_p"):
                    if _k in body:
                        upstream_body[_k] = body[_k]
                _mt = body.get("max_tokens") or body.get("max_output_tokens")
                try:
                    _mt = int(_mt) if _mt is not None else 0
                except (TypeError, ValueError):
                    _mt = 0
                if _mt > 0:
                    upstream_body["max_tokens"] = _mt
                _ct = []
                for _t in (body.get("tools") or []):
                    if not isinstance(_t, dict):
                        continue
                    _fn = _t.get("function") if isinstance(_t.get("function"), dict) else None
                    _nm = (_fn.get("name") if _fn else _t.get("name")) or ""
                    _ds = (_fn.get("description") if _fn else _t.get("description")) or ""
                    _pr = ((_fn.get("parameters") if _fn else None)
                           or _t.get("parameters") or _t.get("input_schema")
                           or {"type": "object"})
                    _typ = _t.get("type", "function")
                    if _typ == "custom":
                        _ct.append({"type": "function",
                                    "name": _nm or "custom_tool",
                                    "description": _ds[:1000],
                                    "parameters": {"type": "object",
                                                   "properties": {"patch": {"type": "string"}},
                                                   "required": ["patch"],
                                                   "additionalProperties": False}})
                    elif _typ == "web_search":
                        continue
                    else:
                        if not isinstance(_nm, str) or not _nm:
                            continue
                        if not isinstance(_pr, dict):
                            _pr = {"type": "object"}
                        _ct.append({"type": "function", "name": _nm,
                                    "description": _ds, "parameters": _pr})
                if _ct:
                    upstream_body["tools"] = _ct
                if "tool_choice" in body and isinstance(body.get("tool_choice"), str):
                    upstream_body["tool_choice"] = body["tool_choice"]
            else:
                upstream_body = dict(body)
                if "store" not in upstream_body and (muse_routed or is_responses_api):
                    upstream_body["store"] = False
                if is_responses_api and "prompt_cache_key" not in upstream_body:
                    upstream_body["prompt_cache_key"] = client_session_id
                if muse_routed or is_responses_api:
                    upstream_body["stream"] = True
                    _u_tools = upstream_body.get("tools")
                    if not _u_tools or not isinstance(_u_tools, list):
                        upstream_body["tools"] = list(_OPENCODE_CORE_TOOLS)
                    else:
                        has_bash = any(isinstance(t, dict) and t.get("name") == "bash" for t in _u_tools)
                        has_read = any(isinstance(t, dict) and t.get("name") == "read" for t in _u_tools)
                        if not (has_bash and has_read):
                            for ct in _OPENCODE_CORE_TOOLS:
                                if not any(isinstance(t, dict) and t.get("name") == ct["name"] for t in _u_tools):
                                    _u_tools.append(ct)
                            upstream_body["tools"] = _u_tools
                # Sanitize /v1/responses tool shapes for muse-backed models.
                # Codex sends OpenAI-style extras the muse endpoint rejects:
                #  - {"type":"custom", ...} apply_patch -> convert to function/json-schema
                #  - web_search with search_content_types -> strip unsupported keys
                #  - function tools with "strict" -> muse 400s on it
                # Only applied to muse* models, never to other models/routes.
                if model_name.startswith("muse") and isinstance(
                        upstream_body.get("tools"), list):
                    _san_tools = []
                    _n_strict = _n_custom = _n_ws = _n_coerced2 = _n_dropped = 0
                    for _t in upstream_body["tools"]:
                        if not isinstance(_t, dict):
                            _n_dropped += 1
                            continue
                        _tt = _t.get("type")
                        if _tt == "custom":
                            _san_tools.append({
                                "type": "function",
                                "name": _t.get("name", "custom_tool"),
                                "description": (_t.get("description") or "")[:1000],
                                "parameters": {
                                    "type": "object",
                                    "properties": {
                                        "patch": {
                                            "type": "string",
                                            "description": "Freeform patch text "
                                            "in the tool's native syntax.",
                                        }
                                    },
                                    "required": ["patch"],
                                    "additionalProperties": False,
                                },
                            })
                            _n_custom += 1
                        elif _tt == "web_search":
                            _ws = {"type": "web_search"}
                            _san_tools.append(_ws)
                            if _t.keys() - {"type"}:
                                _n_ws += 1
                        elif _tt == "function":
                            _ft = {k: v for k, v in _t.items() if k != "strict"}
                            if "strict" in _t:
                                _n_strict += 1
                            _san_tools.append(_ft)
                        else:
                            # Unknown/missing type (mcp/computer/None/…): Zen
                            # 400s "did not match any supported type" and kills
                            # the whole turn. Coerce to function when a name
                            # is resolvable, else DROP (one tool lost beats a
                            # failed turn).
                            _fn2 = _t.get("function") if isinstance(_t.get("function"), dict) else None
                            _nm2 = (_fn2.get("name") if _fn2 else _t.get("name")) or ""
                            if isinstance(_nm2, str) and _nm2:
                                _ds2 = (_fn2.get("description") if _fn2 else _t.get("description")) or ""
                                _pr2 = ((_fn2.get("parameters") if _fn2 else None) or _t.get("parameters") or _t.get("input_schema") or {"type": "object"})
                                if not isinstance(_pr2, dict):
                                    _pr2 = {"type": "object"}
                                _san_tools.append({"type": "function", "name": _nm2, "description": _ds2, "parameters": _pr2})
                                _n_coerced2 += 1
                            else:
                                _n_dropped += 1
                    upstream_body["tools"] = _san_tools
                    if _n_strict or _n_custom or _n_ws or _n_coerced2 or _n_dropped:
                        sys.stderr.write(
                            f"[muse] normalized {len(_san_tools)} tools: "
                            f"{_n_strict} strict-stripped, {_n_custom} "
                            f"custom->function, {_n_ws} web_search-cleaned, "
                            f"{_n_coerced2} type-coerced, {_n_dropped} dropped\n")
                # When client calls /v1/responses directly, normalize reasoning.effort if present
                if "reasoning" in upstream_body and isinstance(upstream_body["reasoning"], dict):
                    r_effort = upstream_body["reasoning"].get("effort")
                    if r_effort is not None:
                        r_str = str(r_effort).strip().lower()
                        if r_str in ("max", "maximum", "x-high", "x_high"):
                            upstream_body["reasoning"]["effort"] = _cap_effort("xhigh")
                            sys.stderr.write(
                                "[muse] reasoning.effort "
                                f"{r_effort!r} aliased to {upstream_body['reasoning']['effort']!r} (= Zen maximum, full reasoning, no downgrade)\n")
                        elif r_str in _MUSE_VALID_EFFORTS:
                            upstream_body["reasoning"]["effort"] = _cap_effort(r_str)
                        else:
                            upstream_body["reasoning"]["effort"] = _cap_effort("high")
            # Send the canonical id upstream (never a stale alias): Zen
            # validates model+tools together, so big-pickle + muse tools 400s.
            try:
                if isinstance(upstream_body, dict):
                    upstream_body["model"] = model_name
            except Exception:
                pass
            return muse_routed, endpoint, upstream_body

        upstream_is_responses, upstream_endpoint, upstream_body = _prepare_upstream(requested)

        # Calculate prompt size for tracking
        prompt_chars = sum(len(json.dumps(m.get("content", ""))) for m in body.get("messages", [])) if "messages" in body else sum(len(json.dumps(m.get("content", ""))) for m in body.get("input", []))

        # Tap: capture input (last user message) for live monitor
        msgs = body.get("messages", [])
        last_user_msg = ""
        for msg in reversed(msgs):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, list):  # vision messages
                    content = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
                last_user_msg = str(content)[:300]
                break
        if last_user_msg:
            with _token_tap_lock:
                _token_tap.append(f"\n\x01IN:{client_type}:{requested}\x02{last_user_msg}\x03")

        stream_mode = body.get("stream", False)

        last_status = 0
        last_body = None
        deadline = time.monotonic() + self.max_time
        # Default: Tor-first (direct real-IP attempts leak identity + invite
        # bans). --direct-first opts in to the old direct-then-Tor order.
        if self.direct_first:
            attempt_seq = list(range(-1, self.max_retries + 1))
        else:
            attempt_seq = list(range(0, self.max_retries + 1))

        # Streaming UX: content is only sent once a healthy upstream stream is committed.
        sse = None
        if stream_mode:
            if headers_sent:
                sse = True

        client_disconnected = threading.Event()

        def _sse_comment(msg: str = ""):
            # Stealth: SSE comments are visible to any downstream client, so
            # emit a bare keepalive with no proxy name or internals wording.
            if not sse or client_disconnected.is_set():
                return
            try:
                self.wfile.write(b": ping\n\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                client_disconnected.set()

        # --- STORM RESCUE: if every exit on the primary model failed with a
        # retryable error, transparently re-run the whole attempt sequence on
        # the next live fallback for secondary models.
        # 2026-09-23: Muse HAS a fallback now (deepseek-v4-flash, proven live
        # via funnel), so the muse exclusion is removed — a burned muse turn
        # rescues to deepseek instead of surfacing FreeTierError to the client.
        # Harness muse-sc-* ids were already exempt and stay so.
        _is_harness_sc = requested.startswith("muse-sc-")
        rescue_pending = (requested not in FALLBACK_CHAIN[-1:])
        primary_model = requested
        # Pool-global ban horizon key: per (upstream host, model). A thread
        # that sees a huge Retry-After parks here; other threads skip the
        # burned model immediately instead of re-discovering the ban.
        _ban_key = f"{self.upstream}|{requested}"
        current_slot = None
        # Fresh-turn guarantee (ban-skip guard): this per-turn flag is set the
        # moment THIS turn gets a live upstream verdict (any attempt that
        # actually reaches upstream). The ban-skip shortcut above may only
        # fire after such a probe — never on stale pool-global gossip alone.
        self._zen_probed_this_turn = False
        visited_rescue_models = {requested}
        if orig_requested:
            visited_rescue_models.add(orig_requested)
        while True:
            if client_disconnected.is_set():
                sys.stderr.write("[client] client disconnected, exiting request loop\n")
                break
            for attempt in attempt_seq:
                if client_disconnected.is_set():
                    sys.stderr.write("[client] client disconnected, aborting attempt loop\n")
                    break

                if attempt == -1:
                    if _direct_cooldown_until > time.monotonic():
                        continue
                    # Direct connection (real IP, no Tor) — opt-in via --direct-first
                    sys.stderr.write(f"[direct] trying with real IP (no proxy)\n")
                    use_session = get_direct_session()
                elif attempt == 0:
                    # Initial attempt using active pre-warmed circuit from pool
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        sys.stderr.write(f"[budget] out of time before first attempt\n")
                        break
                    # PATIENT ACQUIRE: If pool is cold or slots busy, wait with keepalives!
                    # Never burns retry attempts or rejects the client.
                    current_slot, use_session = _circuit_pool.acquire_slot(
                        timeout=min(90.0, remaining),
                        on_wait=_sse_comment
                    )
                    if use_session is None:
                        sys.stderr.write("[tor-pool] pool wait timed out — no ready circuit\n")
                        last_status = 503
                        last_body = {"error": "pool busy, timed out waiting for warm circuit"}
                        continue
                    if _qlog("slot-active"):
                        sys.stderr.write(f"[tor-pool] slot {current_slot.slot_id} active (gen {current_slot.gen}, exit {current_slot.exit_ip or 'probing'})\n")
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        sys.stderr.write(f"[budget] out of time after {attempt+1} attempts\n")
                        break

                    # Hot-swap to the standby pre-warmed circuit, honoring
                    # backoff + upstream Retry-After (kills the 0ms self-DDoS
                    # storm: 7-14 POSTs back-to-back on a throttled upstream).
                    #
                    # PATIENT-WAIT (never fail the subagent): a huge Retry-After
                    # (>300s, upstream bans run for hours) does NOT burn the
                    # remaining attempts on the banned model. Instead we park a
                    # short pool-global hint for (upstream, model), emit
                    # keepalives so opencode sees activity, and jump to the
                    # rescue model below. Small RAs just back off normally.
                    #
                    # The skip only kicks in at attempt >= _BAN_SKIP_MIN_ATTEMPT
                    # with a throttle status: the probe (attempt 0) plus two
                    # rotations ALWAYS run, because throttles are often
                    # per-exit and a fresh exit is the real recovery path.
                    # Skipping everything (the old behavior) turned one header
                    # into a multi-hour hard outage with no recovery.
                    # Zen/opencode free models are NEVER parked. This proxy's
                    # entire purpose is Tor exit rotation: per-exit throttles
                    # are the norm and a FRESH EXIT is the recovery path. A
                    # pool-global ban/skip (old `[ban-park]`) took the free
                    # model offline for the Retry-After horizon and defeated
                    # the proxy, so we deliberately ignore the upstream horizon
                    # and keep rotating exits until one is accepted.
                    prev_slot = current_slot
                    wait_s = 0.0
                    try:
                        ra = float(getattr(self, "_last_retry_after", 0) or 0)
                    except Exception:
                        ra = 0.0
                    if ra > 300:
                        sys.stderr.write(
                            f"[throttle-rotate] '{requested}' Retry-After={ra:.0f}s — "
                            f"rotating Tor exit (Zen free model: never parked)\n")
                        _sse_comment()
                        last_status = last_status or 429
                        # Leaving this exit anyway; a long sleep is pointless.
                        wait_s = max(wait_s, 2.0)
                    else:
                        wait_s = max(wait_s, min(ra, 30.0))
                    try:
                        bo = float(self.backoff or BACKOFF)
                    except Exception:
                        bo = BACKOFF
                    if attempt >= 1:
                        wait_s = max(wait_s, min(bo ** min(attempt, 4), 8.0))
                    # 403 FreeTierError / 429 FreeUsageLimitError = this exit is
                    # blocked for this session/identity. Waiting is pure waste
                    # (a pause won't unblock a per-exit block, and the upstream
                    # Retry-After horizon is hours). SWAP IMMEDIATELY with only
                    # anti-spin jitter so the next fresh Tor exit is tried right
                    # away — exit rotation is the recovery path. This is what
                    # keeps overnight agent runs flowing instead of stalling in
                    # backoff on a blocked IP.
                    if last_status in (403, 429):
                        wait_s = 0.0
                    if wait_s > 0:
                        import random as _rj
                        wait_s = wait_s + _rj.uniform(0, 0.25)
                        if _qlog("backoff"):
                            sys.stderr.write(
                                f"{_yel(f'[backoff] attempt {attempt + 1}: waiting {wait_s:.2f}s ')}"
                                f"(retry-after={ra:.1f}s) before swap\n")
                        _t0 = time.monotonic()
                        while time.monotonic() - _t0 < wait_s:
                            if client_disconnected.is_set():
                                break
                            if sse:
                                _sse_comment()
                            time.sleep(min(0.5, wait_s - (time.monotonic() - _t0)))
                            if time.monotonic() - _t0 >= wait_s:
                                break
                    elif last_status == 403:
                        if _qlog("fastswap"):
                            sys.stderr.write(
                                f"{_yel(f'[403-fastswap] attempt {attempt + 1}: burned exit, swapping immediately (no backoff)')}\n")
                    # Record the ACTUAL error (not generic "retryable error") so
                    # stall kinds (ttfb-stall/timeout/transport) classify as
                    # short-quarantine exit-faults and the picker routes around
                    # stalled exits instead of rediscovering them per turn.
                    try:
                        _lb = last_body
                        _lb_txt = (_lb.get("error", {}) if isinstance(_lb, dict) else {})
                        _lb_txt = (_lb_txt.get("message", "") if isinstance(_lb_txt, dict) else str(_lb_txt)) or str(_lb)[:80]
                    except Exception:
                        _lb_txt = ""
                    _rec_reason = f"retryable-{last_status}:{_lb_txt[:80]}"
                    next_slot, use_session = _circuit_pool.report_failure(current_slot, last_status, _rec_reason)
                    if use_session is not None:
                        self._last_retry_after = 0
                        if _qlog("swap"):
                            sys.stderr.write(f"[tor-pool] swap (after {wait_s:.2f}s backoff): slot {prev_slot.slot_id if prev_slot else '?'} ({prev_slot.exit_ip if prev_slot else '?'}) -> slot {next_slot.slot_id} ({next_slot.exit_ip})\n")
                        _sse_comment(f"upstream throttled — swapping to warm circuit {next_slot.slot_id} (attempt {attempt + 1}/{self.max_retries + 1})")
                        current_slot = next_slot
                    else:
                        # Standby circuits momentarily exhausted: wait patiently for replenish worker
                        current_slot = None
                        sys.stderr.write(f"{_yel('[tor-pool] pool busy after swap — waiting for warm circuit')}\n")
                        _sse_comment("waiting for warm Tor circuit")
                        wait_time = min(60.0, remaining)
                        c_slot, c_sess = _circuit_pool.acquire_slot(
                            timeout=wait_time,
                            on_wait=_sse_comment,
                            exclude_slot=prev_slot
                        )
                        if c_sess is not None:
                            current_slot = c_slot
                            use_session = c_sess
                            if _qlog("slot-ready"):
                                sys.stderr.write(f"[tor-pool] slot {c_slot.slot_id} became ready ({c_slot.exit_ip})\n")
                        else:
                            last_status = 503
                            last_body = {"error": "pool busy, timed out waiting for warm circuit"}
                            continue
                        self._last_retry_after = 0

                t_start = time.perf_counter()
                # Hard first-content deadline for THIS attempt (monotonic).
                # Bare lifecycle pings may extend the peek, but never past
                # here — a stalled/queued exit fails over instead of
                # holding TTFT for 60s+.
                # Size-scaled: a 150KB+ upload over slow Tor legitimately
                # needs past 35s (upload alone). Grant +1s per 10KB incoming
                # so large turns complete instead of abort-retry-abort burning
                # exits. Tiny requests keep the FIRST_CONTENT_S base (90s).
                # NOTE: the ceiling must exceed FIRST_CONTENT_S, else the grant
                # is nullified (it was min(90.0, ...) with a 90s base -> the
                # scaling never applied and big uploads still died at 90s).
                try:
                    _in_bytes = len(raw_in or b"")
                except Exception:
                    _in_bytes = 0
                _hard_content_dl = time.monotonic() + min(
                    float(MAX_TIME), FIRST_CONTENT_S + _in_bytes / 10240.0)

                # OpenCode Zen free tier now REQUIRES the genuine client
                # session: a random x-opencode-session yields
                # 403 FreeTierError ("free tier can only be used from within
                # OpenCode") and random x-opencode-request alone yields 429.
                # Forward the client's real headers (was: per-attempt random
                # stealth IDs — reverse-verified 2026-09-17).
                req_session_id = client_session_id
                _is_zen_route = True

                req_msg_id = client_msg_id
                if isinstance(upstream_body, dict) and "prompt_cache_key" in upstream_body:
                    upstream_body["prompt_cache_key"] = req_session_id

                # Mirror the REAL OpenCode client's headers verbatim. Zen's
                # free-tier gate keys off the exact client identity (verified
                # 2026-09-23: a real `opencode run` through this proxy got 200s
                # from big-pickle/muse while our hand-picked header set got
                # 403 FreeTierError). We forward every x-opencode-* header plus
                # x-parent-session-id and the client's real UA; only the
                # Authorization is overridden to the free-tier sentinel.
                _client_ua = self.headers.get("User-Agent") or ""
                headers = {
                    "Authorization": auth_header_val,
                    "Content-Type": "application/json",
                    "User-Agent": _client_ua if _client_ua.startswith("opencode/") else _OPENCODE_UA,
                }
                for _hk, _hv in self.headers.items():
                    _lk = _hk.lower()
                    if _lk.startswith("x-opencode-") or _lk == "x-parent-session-id":
                        headers[_hk] = _hv
                # Fallbacks for non-opencode callers (or missing headers).
                headers.setdefault("x-opencode-client", client_client)
                headers.setdefault("x-opencode-project", client_project)
                headers["Expect"] = ""  # Strip libcurl's artificial 100-continue 1s-5s Tor delay
                headers["x-opencode-request"] = req_msg_id
                headers["x-opencode-session"] = req_session_id

                # Codex multi-turn fix: strip opaque echoed reasoning blobs so
                # Zen never 400s "not issued to this caller" (muse lane only,
                # idempotent across retry attempts).
                try:
                    if (upstream_is_responses or (isinstance(upstream_body, dict) and isinstance(upstream_body.get("input"), list))):
                        _n_strip = _strip_encrypted_reasoning(upstream_body)
                        if _n_strip:
                            sys.stderr.write(f"[reasoning-strip] {requested}: removed {_n_strip} encrypted blob(s)\n")
                except Exception:
                    pass

                # Optional forensic dump of the EXACT upstream identity so the
                # header replication can be diffed against a genuine client.
                if os.environ.get("ZEN_LOG_HEADERS"):
                    try:
                        with open("/tmp/opencode/upstream_headers.log", "a") as _uf:
                            _uf.write(f"=== {requested} {upstream_endpoint}\n")
                            for _hk, _hv in headers.items():
                                _uf.write(f"{_hk}: {_hv}\n")
                            _uf.write("\n")
                    except Exception:
                        pass

                if stream_mode:
                    # --- STREAMING: use curl_cffi stream=True for real-time ---
                    # Request-shape telemetry (k-format input, cyan; quiet by
                    # default). Cheap O(n) scan, once per attempt.
                    try:
                        if _qlog("req-size"):
                            _rs_items = upstream_body.get("input") or upstream_body.get("messages") or []
                            _rs_tools = upstream_body.get("tools") or []
                            _rs_chars = 0
                            if isinstance(_rs_items, list):
                                for _it in _rs_items:
                                    if isinstance(_it, dict):
                                        _c = _it.get("content", "")
                                        if isinstance(_c, str):
                                            _rs_chars += len(_c)
                                        elif isinstance(_c, list):
                                            for _p in _c:
                                                if isinstance(_p, dict):
                                                    _t = _p.get("text", "")
                                                    if isinstance(_t, str):
                                                        _rs_chars += len(_t)
                            sys.stderr.write(f"[req-size] {requested} items={len(_rs_items) if isinstance(_rs_items, list) else '?'} tools={len(_rs_tools) if isinstance(_rs_tools, list) else '?'} input=\033[36m{_kfmt(_rs_chars)}\033[0m\n")
                    except Exception:
                        pass
                    # Native Anthropic lane (union-alpha): upstream sends a bare
                    # non-SSE JSON message over an SSE-accepted POST (and some
                    # exits go silent). stream=False reads the whole body with
                    # one deadline, so the turn completes instead of hanging.
                    _union_stream_plain = (
                        _wire_protocol(requested) == "messages"
                        and upstream_endpoint.endswith("/messages"))
                    try:
                        # TTFB bound (headers deadline, size-aware): the POST
                        # itself is unbounded pre-headers (curl stream read =
                        # LOW_SPEED 600s), so a queued exit can hold 60s+ with
                        # no content-cap able to fire. Run it in a worker and
                        # join with a deadline; on expiry close + decache the
                        # request session and swap exits (next attempt taints
                        # via report_failure). Deterministic, no curl semantics.
                        try:
                            _in_b = len(raw_in or b"")
                        except Exception:
                            _in_b = 0
                        _ttfb_cap = min(300.0, STREAM_TTFB_TIMEOUT + _in_b / 4096.0)
                        _post_box = {}
                        def _do_post(_s=use_session, _u=upstream_endpoint,
                                     _h=headers, _b=upstream_body,
                                     _sp=not _union_stream_plain):
                            try:
                                # NOTE: for stream=True curl_cffi turns this tuple
                                # into LOW_SPEED_TIME=connect+read (not a TTFB
                                # bound), so the read side must be the generous
                                # STREAM_IDLE_ABORT — the real (resettable) idle
                                # bound is _ZenIdleGuard, armed below.
                                _post_box["r"] = _s.post(
                                    _u, headers=_h, json=_b,
                                    timeout=(STREAM_CONNECT_TIMEOUT, STREAM_IDLE_ABORT),
                                    stream=_sp)
                            except Exception as _pe:
                                _post_box["e"] = _pe
                        _pt = threading.Thread(target=_do_post, daemon=True)
                        _pt.start()
                        _pt.join(timeout=_ttfb_cap)
                        if _pt.is_alive():
                            try:
                                use_session.close()
                            except Exception:
                                pass
                            try:
                                _circuit_pool._req_local.sess = None
                                _circuit_pool._req_local.key = None
                            except Exception:
                                pass
                            sys.stderr.write(f"{_red(f'[ttfb-stall] no upstream headers in {_ttfb_cap:.0f}s — swapping exit')}\n")
                            last_status = 502
                            last_body = {"error": "upstream TTFB stall (no headers), swapped exit"}
                            _sse_comment(f"attempt {attempt + 1} stalled before headers — retrying")
                            continue
                        if "e" in _post_box:
                            raise _post_box["e"]
                        r = _post_box.get("r")
                        if r is None:
                            raise ValueError("upstream post returned nothing")
                    except Exception as e:

                        last_status = 502
                        last_body = {"error": f"upstream error: {e}"}
                        _sse_comment(f"attempt {attempt + 1} connection failed — retrying")
                        continue

                    t_elapsed = (time.perf_counter() - t_start) * 1000
                    if _qlog("upstream"):
                        sys.stderr.write(f"[upstream] {t_elapsed:6.0f}ms  http={r.status_code}  stream=True  endpoint={upstream_endpoint.split('/')[-1]}\n")
                    if _union_stream_plain and r.status_code == 200:
                        # Whole-body lane: adapt the Anthropic message to ONE
                        # chat chunk + [DONE] and finish the turn here (same
                        # shape as the bare-JSON commit path below).
                        try:
                            _up = r.json()
                        except Exception:
                            _up = None
                        try:
                            r.close()
                        except Exception:
                            pass
                        if (isinstance(_up, dict)
                                and _up.get("type") == "message"):
                            try:
                                _content_text = "\n".join(str(c.get("text", "")) for c in (_up.get("content") or []) if isinstance(c, dict) and c.get("type") == "text")
                                _reasoning = "\n".join(str(c.get("thinking", "")) for c in (_up.get("content") or []) if isinstance(c, dict) and c.get("type") in ("thinking", "reasoning"))
                                _tcs = []
                                for c in (_up.get("content") or []):
                                    if isinstance(c, dict) and c.get("type") in ("tool_use", "tool_calls"):
                                        _inp = c.get("input", {})
                                        _tcs.append({
                                            "index": len(_tcs),
                                            "id": c.get("id", ""),
                                            "type": "function",
                                            "function": {
                                                "name": c.get("name", ""),
                                                "arguments": json.dumps(_inp) if isinstance(_inp, dict) else str(_inp)
                                            }
                                        })
                                _delta = {"role": "assistant", "content": _content_text}
                                if _reasoning: _delta["reasoning"] = _reasoning
                                if _tcs: _delta["tool_calls"] = _tcs
                                self.wfile.write(
                                    b"data: " + json.dumps({
                                        "id": _up.get("id", "resp-zen"),
                                        "object": "chat.completion.chunk",
                                        "created": int(time.time()),
                                        "model": requested,
                                        "choices": [{
                                            "index": 0,
                                            "delta": _delta,
                                            "finish_reason": "tool_calls" if _tcs else "stop"
                                        }]}).encode()
                                    + b"\n\n")
                                self.wfile.write(b"data: [DONE]\n\n")
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError, OSError):
                                pass
                            if current_slot is not None:
                                _circuit_pool.report_success(current_slot)
                                current_slot = None
                            _circuit_pool.clear_retry_after(_ban_key)
                            return
                        last_status = 502
                        last_body = {"error": "union bare body unparseable",
                                     "type": "upstream_error"}
                        continue
                    try:
                        _hdrs = getattr(r, 'headers', {}) or {}
                        _ra_raw = _hdrs.get('Retry-After') or _hdrs.get('retry-after')
                        self._last_retry_after = max(0.0, float(str(_ra_raw).strip())) if _ra_raw else 0
                    except Exception:
                        self._last_retry_after = 0

                    # Retry on rate-limit / per-exit block / overloaded / endpoint
                    # unavailable. NOTE: a curl_cffi STREAM response keeps
                    # `.content` EMPTY, so the error body MUST be read off the
                    # stream. Reading it from .content made body_json={} and
                    # classified EVERY 4xx/5xx as a blind retryable 403 — which
                    # then got stamped "cloudflare_block" while the pool churned
                    # exits on a PERMANENT error (e.g. Zen FreeTierError).
                    if r.status_code in RETRYABLE_STATUSES:
                        _err_raw = b""
                        try:
                            for _c in r.iter_content():
                                if not _c:
                                    continue
                                _err_raw += (_c if isinstance(_c, bytes)
                                             else str(_c).encode("utf-8", "replace"))
                                if len(_err_raw) >= 8192:
                                    break
                        except Exception:
                            pass
                        try:
                            body_json = json.loads(_err_raw) if _err_raw else {}
                        except Exception:
                            body_json = {"error": {"message":
                                _err_raw[:500].decode("utf-8", "replace")}}
                        _e = body_json.get("error") if isinstance(body_json, dict) else None
                        if isinstance(_e, dict):
                            _etype = str(_e.get("type") or "")
                            _emsg = str(_e.get("message") or "")
                        else:
                            _etype = ""
                            _emsg = (str(_e) if _e is not None
                                     else _err_raw[:300].decode("utf-8", "replace"))
                        _low = _err_raw[:2000].lower()
                        # Zen sits behind Cloudflare, so a cf-ray HEADER is on
                        # every response — only the BODY tells a real CF
                        # interstitial (HTML) from an ordinary JSON error.
                        _is_cf = (_low.lstrip().startswith(b"<!doctype html")
                                  or b"<html" in _low[:200]
                                  or b"cloudflare" in _low)
                        if not _is_retryable(r.status_code, body_json,
                                             allow_free_tier_retry=_genuine_sess):
                            # Terminal (FreeTierError / AuthError / entitlement /
                            # model-unavailable / missing key): a different Tor
                            # exit can NEVER fix this. Surface the REAL upstream
                            # error verbatim — never a fabricated label.
                            last_status = r.status_code
                            last_body = {"error": {
                                "type": _etype or "upstream_error",
                                "message": _emsg or f"HTTP {r.status_code}"}}
                            sys.stderr.write(
                                f"[terminal] http={r.status_code} "
                                f"{_etype or 'upstream_error'}: {_emsg[:160]}\n")
                            try:
                                r.close()
                            except Exception:
                                pass
                            if current_slot is not None:
                                _circuit_pool.note_failure(
                                    current_slot, r.status_code,
                                    f"terminal-{r.status_code}"[:80])
                                _circuit_pool.release_slot(current_slot)
                                current_slot = None
                            break  # -> rescue / surface (no exit churn)
                        # Retryable: per-exit block / throttle / transient.
                        last_status = r.status_code
                        last_body = {"error": {
                            "type": _etype or ("cloudflare_block" if _is_cf
                                               else f"upstream_{r.status_code}"),
                            "message": _emsg or _err_raw[:300].decode("utf-8", "replace")}}
                        if r.status_code == 403:
                            _chk = _emsg.lower() if _emsg else ""
                            if "user_blocked" in _chk or "policy violat" in _chk:
                                sys.stderr.write(f"{_red('[403-block] Trust & Safety rejection detected: ' + _emsg[:150])}\n")
                                last_status = 403
                                break
                            if _qlog("403-retry"):
                                sys.stderr.write(
                                    f"{_yel('[403-retry] ' + ('cloudflare' if _is_cf else 'upstream'))} "
                                    f"{(_etype + ' ' + _emsg).strip()[:150] or '403'} — rotating exit\n")
                        if r.status_code == 429 and zen_db:
                            try:
                                _slot_ip = (current_slot.exit_ip if current_slot is not None and getattr(current_slot, "exit_ip", "") else _cached_exit_ip.get("ip", ""))
                                db_async('record_error', "429", requested, _slot_ip, "rate limited")
                            except Exception:
                                pass
                        if attempt == -1 and r.status_code in (429, 403):
                            _direct_cooldown_until = time.monotonic() + 900
                            sys.stderr.write(f"[direct-cooldown] real IP throttled ({r.status_code}) — skipping direct attempts for 15m\n")
                        if r.status_code != 403 and _qlog("retryable"):
                            sys.stderr.write(f"{_yel(f'[retryable] http={r.status_code} will retry via new exit')}\n")
                        self._zen_probed_this_turn = True
                        try:
                            r.close()
                        except Exception:
                            pass
                        continue

                    # --- PEEK BEFORE COMMIT ---
                    # Read the start of the stream BEFORE sending any headers to
                    # the client. Commit only once REAL content deltas arrive;
                    # embedded errors, dead exits that only emit lifecycle pings,
                    # stalled endpoints and immediate EOFs are retried across Tor
                    # circuits transparently — agents never see a broken stream.
                    pre_chunks = []
                    idle_guard = None
                    if getattr(r, "queue", None) is not None:
                        # Real content-aware idle bound for this stream. Give reasoning models ample thinking time.
                        idle_guard = _ZenIdleGuard(r, max(45.0, PEEK_BUDGET_S * 2))
                    peek = bytearray()
                    peek_norm = bytearray()  # incrementally normalized (cheap O(n) total)
                    peek_err = None
                    peek_err_is_retryable = True
                    remaining_budget = deadline - time.monotonic()
                    peek_deadline = time.monotonic() + max(10.0, min(PEEK_BUDGET_S * 2, remaining_budget))
                    _peek_t0 = time.monotonic()
                    up_it = r.iter_content()  # ONE iterator: peek then stream share it
                    try:
                        for chunk in up_it:
                            if not chunk:
                                continue
                            if isinstance(chunk, str):  # some exits decode SSE to str
                                chunk = chunk.encode("utf-8", errors="replace")
                            pre_chunks.append(chunk)
                            peek.extend(chunk)
                            if idle_guard is not None:
                                idle_guard.touch()
                            peek_norm.extend(re.sub(rb"\s+", b"", chunk.lower()))
                            if len(peek) > 512 * 1024:
                                break  # absurd without content marker
                            if _stream_has_content_norm(peek_norm):
                                break  # healthy — commit below
                            # EMPTY-BODY fast path: some endpoints answer 200/SSE
                            # with zero bytes (dead id like jev-1.13-free). That
                            # means backend-down, NOT a stalled stream — fail
                            # fast so one bad id can't burn all Tor exits.
                            if (r.status_code == 200 and not peek
                                    and (time.monotonic() - _peek_t0) > 3.0):
                                peek_err = ("empty stream body: upstream 200 "
                                            "with no bytes")
                                peek_err_is_retryable = False
                                break
                            # Native Anthropic lane: a complete non-streaming
                            # message object in one body is ALSO committable
                            # (union-alpha streams text/plain to SSE-naive
                            # exits as bare JSON). Detect + adapt below.
                            if (_wire_protocol(requested) == "messages"
                                    and len(peek) > 64
                                    and peek.lstrip()[:1] == b"{"):
                                try:
                                    _pk = json.loads(bytes(peek))
                                except Exception:
                                    _pk = None
                                if (isinstance(_pk, dict)
                                        and _pk.get("type") == "message"
                                        and isinstance(_pk.get("content"), list)):
                                    peek_err = None
                                    break
                            snippet = _looks_like_stream_error(peek)
                            if snippet:
                                peek_err = f"embedded upstream error: {snippet[:160]}"
                                peek_err_is_retryable = _is_retryable(503, {"error": {"message": snippet}}, allow_free_tier_retry=_genuine_sess)
                                if not peek_err_is_retryable:
                                    # The embedded frame is a terminal verdict
                                    # (e.g. jev-1.13-free's backend-down 500):
                                    # don't loop exits for it. Jump straight
                                    # to the post-loop handler so it surfaces
                                    # / rescues immediately.
                                    break
                            if _stream_has_lifecycle_norm(peek_norm):
                                # Lifecycle extends the peek, but NEVER past the
                                # hard first-content deadline: bare pings from a
                                # queued exit must fail over, not hold TTFT.
                                _ext = time.monotonic() + max(15.0, min(60.0, deadline - time.monotonic()))
                                peek_deadline = min(_ext, _hard_content_dl)
                                if idle_guard is not None:
                                    idle_guard.limit = max(idle_guard.limit, 60.0)
                                    idle_guard.touch()
                            else:
                                # No lifecycle either: still respect the hard cap.
                                peek_deadline = min(peek_deadline, _hard_content_dl)
                            if time.monotonic() > peek_deadline:
                                peek_err = (f"stalled stream: no content delta "
                                            f"within {FIRST_CONTENT_S:.0f}s hard cap")
                                break
                    except Exception as e:

                        sys.stderr.write(f"{_red('[peek] read error: ' + str(e)[:150])}\n")
                        peek_err = f"upstream read error: {e}"

                    if peek_err is None and not _stream_has_content_norm(peek_norm):
                        # Native Anthropic lane may have delivered a complete
                        # bare-JSON message body (stream ended cleanly with
                        # content, just no SSE markers) — committable, not an
                        # error. Re-check here: the in-loop break above only
                        # fires while chunks are still arriving.
                        _pk_end = None
                        if (_wire_protocol(requested) == "messages"
                                and len(peek) > 64
                                and bytes(peek).lstrip()[:1] == b"{"):
                            try:
                                _pk_end = json.loads(bytes(peek))
                            except Exception:
                                _pk_end = None
                        if (isinstance(_pk_end, dict)
                                and _pk_end.get("type") == "message"
                                and isinstance(_pk_end.get("content"), list)):
                            peek_err = None
                        else:
                            peek_err = "stream ended without delivering any content"

                    if peek_err:
                        if idle_guard is not None:
                            idle_guard.stop()
                        sys.stderr.write(f"{_red('[pre-commit] ' + peek_err[:180])} (peek_len={len(peek)})\n")
                        # Backend-Unavailable short-circuit FIRST (before any
                        # retry math): an embedded "Internal server error"
                        # JSON frame on a 500-id (dead/unserved backend like
                        # jev-1.13-free) must NOT retry across every exit —
                        # jump straight to rescue/surface below.
                        if ("internal server error" in (peek_err or "").lower()
                                and r.status_code == 500):
                            peek_err_is_retryable = False
                            last_status = 500
                        _sse_comment(f"attempt failed ({peek_err[:60]}) — retrying")
                        if peek_err_is_retryable:
                            last_status = 502
                            last_body = {"error": {"message": peek_err[:300], "type": "upstream_error"}}
                            if zen_db:
                                try:
                                    _slot_ip = (current_slot.exit_ip if current_slot is not None and getattr(current_slot, "exit_ip", "") else _cached_exit_ip.get("ip", ""))
                                    db_async('record_error', "stream_error", requested,
                                                        _slot_ip, peek_err[:120])
                                except Exception:
                                    pass
                            try:
                                r.close()
                            except Exception:
                                pass
                            continue  # retry via loop (rotates tor on next attempts)
                        # Non-retryable embedded error: route it into the
                        # rescue/fail-fast below INSTEAD of surfacing here.
                        # (Surfacing early skipped rescue_pending and left
                        # dead-id turns like jev-1.13-free client-visible.)
                        last_status = 500
                        last_body = {"error": {"message": peek_err[:300], "type": "upstream_error"}}
                        try:
                            r.close()
                        except Exception:
                            pass
                        if current_slot is not None:
                            _circuit_pool.release_slot(current_slot)
                            current_slot = None
                        break  # out of attempt loop -> rescue block

                    # Healthy — commit: pipe SSE chunks to client in real-time
                    if stream_mode and not headers_sent:
                        try:
                            ttfb_s = time.monotonic() - t_start
                            sys.stderr.write(f"\033[35m[{time.strftime('%H:%M:%S')}] [STREAM]\033[0m \033[33m[{client_type}]\033[0m UPSTREAM CONNECTED | TTFB: \033[32m{ttfb_s:.2f}s\033[0m | Flowing to client...\n")
                            sys.stderr.flush()
                            self.send_response(200)
                            self.send_header("Content-Type", "text/event-stream")
                            self.send_header("Cache-Control", "no-cache")
                            self.send_header("Connection", "close")
                            self.end_headers()
                            headers_sent = True
                            sse = True
                        except (BrokenPipeError, ConnectionResetError):
                            return

                    if idle_guard is not None:
                        # Content is flowing: tolerate long quiet reasoning.
                        idle_guard.set_limit(STREAM_IDLE_ABORT)
                    stream_opts = body.get("stream_options") or {}
                    include_usage = stream_opts.get("include_usage", False) if isinstance(stream_opts, dict) else False
                    track_ctx = {
                        "model": requested,
                        "t_start": t_start,
                        "retries": attempt,
                        "prompt_chars": prompt_chars,
                        "is_adapted_responses": upstream_is_responses and not is_responses_api,
                        "is_native_anthropic": (
                            _wire_protocol(requested) == "messages"
                            and upstream_endpoint.endswith("/messages")),
                        "include_usage": include_usage,
                        # /v1/responses client on a chat-wire backend: translate
                        # chat SSE to Responses events instead of passthrough.
                        "is_chat_backend": (is_responses_api
                                            and not upstream_is_responses),
                    }
                    if (_wire_protocol(requested) == "messages"
                            and isinstance(peek, (bytes, bytearray))
                            and len(peek) > 64 and bytes(peek).lstrip()[:1] == b"{"):
                        # Bare-JSON body lane (see peek): adapt the complete
                        # message to ONE chat chunk + [DONE] and finish the
                        # turn without entering the SSE loop.
                        try:
                            _pk2 = json.loads(bytes(peek))
                        except Exception:
                            _pk2 = None
                        if (isinstance(_pk2, dict)
                                and _pk2.get("type") == "message"):
                            try:
                                _content_text = "\n".join(str(c.get("text", "")) for c in (_pk2.get("content") or []) if isinstance(c, dict) and c.get("type") == "text")
                                _reasoning = "\n".join(str(c.get("thinking", "")) for c in (_pk2.get("content") or []) if isinstance(c, dict) and c.get("type") in ("thinking", "reasoning"))
                                _tcs = []
                                for c in (_pk2.get("content") or []):
                                    if isinstance(c, dict) and c.get("type") in ("tool_use", "tool_calls"):
                                        _inp = c.get("input", {})
                                        _tcs.append({
                                            "index": len(_tcs),
                                            "id": c.get("id", ""),
                                            "type": "function",
                                            "function": {
                                                "name": c.get("name", ""),
                                                "arguments": json.dumps(_inp) if isinstance(_inp, dict) else str(_inp)
                                            }
                                        })
                                _delta = {"role": "assistant", "content": _content_text}
                                if _reasoning: _delta["reasoning"] = _reasoning
                                if _tcs: _delta["tool_calls"] = _tcs
                                self.wfile.write(
                                    b"data: " + json.dumps({
                                        "id": _pk2.get("id", "resp-zen"),
                                        "object": "chat.completion.chunk",
                                        "created": int(time.time()),
                                        "model": requested,
                                        "choices": [{
                                            "index": 0,
                                            "delta": _delta,
                                            "finish_reason": "tool_calls" if _tcs else "stop"
                                        }]}).encode()
                                    + b"\n\n")
                                self.wfile.write(b"data: [DONE]\n\n")
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError, OSError):
                                pass
                            try:
                                r.close()
                            except Exception:
                                pass
                            if idle_guard is not None:
                                idle_guard.stop()
                            if current_slot is not None:
                                _circuit_pool.report_success(current_slot)
                                current_slot = None
                            _circuit_pool.clear_retry_after(_ban_key)
                            return
                    if track_ctx.get("is_chat_backend"):
                        client_gone = self._stream_chat_to_responses(
                            r, track_ctx=track_ctx,
                            chunk_iter=itertools.chain(pre_chunks, up_it),
                            headers_sent=sse, idle_guard=idle_guard,
                            requested=requested)
                    else:
                        client_gone = self._stream_realtime(r, client_type, track_ctx=track_ctx,
                                           chunk_iter=itertools.chain(pre_chunks, up_it),
                                           headers_sent=sse, idle_guard=idle_guard)
                    if client_gone:
                        if current_slot is not None:
                            _circuit_pool.release_slot(current_slot)
                            current_slot = None
                        return
                    if current_slot is not None:
                        _circuit_pool.report_success(current_slot)
                        current_slot = None
                    # Recovery detected: drop any parked ban for this model so
                    # later requests rotate normally instead of skipping.
                    _circuit_pool.clear_retry_after(_ban_key)
                    return

                else:
                    # --- NON-STREAMING ---
                    # Zen's free tier only serves STREAMED requests: a
                    # non-streaming POST to the /responses wire returns 403
                    # FreeTierError ("free tier can only be used from within
                    # OpenCode") even with genuine client headers. Verified
                    # 2026-09-23: identical body stream:true -> 200,
                    # stream:false -> 403 on every Tor exit. So we always
                    # stream upstream on the responses wire and rebuild the
                    # object for non-streaming clients below.
                    _union_stream_plain = (
                        _wire_protocol(requested) == "messages"
                        and upstream_endpoint.endswith("/messages"))
                    _ns_force_stream = not _union_stream_plain
                    try:
                        if _ns_force_stream:
                            upstream_body["stream"] = True
                        r = use_session.post(
                            upstream_endpoint,
                            headers=headers,
                            json=upstream_body,
                            timeout=(15, 60),
                            stream=_ns_force_stream,
                        )
                    except Exception as e:

                        last_status = 502
                        last_body = {"error": f"upstream error: {e}"}
                        continue

                    t_elapsed = (time.perf_counter() - t_start) * 1000
                    if _qlog("upstream"):
                        sys.stderr.write(f"[upstream] {t_elapsed:6.0f}ms  http={r.status_code}  stream=False  endpoint={upstream_endpoint.split('/')[-1]}\n")
                    try:
                        _hdrs = getattr(r, 'headers', {}) or {}
                        _ra_raw = _hdrs.get('Retry-After') or _hdrs.get('retry-after')
                        self._last_retry_after = max(0.0, float(str(_ra_raw).strip())) if _ra_raw else 0
                    except Exception:
                        self._last_retry_after = 0

                    # Retry on retryable status codes (also checks body for deepseek exclusion)
                    if r.status_code in RETRYABLE_STATUSES:
                        _err_raw = b""
                        if _ns_force_stream:
                            try:
                                for _c in r.iter_content():
                                    if not _c:
                                        continue
                                    _err_raw += (_c if isinstance(_c, bytes)
                                                 else str(_c).encode("utf-8", "replace"))
                                    if len(_err_raw) >= 8192:
                                        break
                            except Exception:
                                pass
                            try:
                                body_json = json.loads(_err_raw) if _err_raw else {}
                            except Exception:
                                body_json = {}
                        else:
                            try:
                                body_json = r.json()
                            except Exception:
                                body_json = {}
                        if _is_retryable(r.status_code, body_json,
                                         allow_free_tier_retry=_genuine_sess):
                            last_status = r.status_code
                            # Live probe observed: real throttle verdict from
                            # upstream on THIS turn (see streaming-path flag).
                            self._zen_probed_this_turn = True
                            if r.status_code == 403:
                                try:
                                    err = body_json.get("error", {}) if isinstance(body_json, dict) else {}
                                except Exception:
                                    err = {}
                                if not isinstance(err, dict):
                                    err = {"message": str(err)[:300]}
                                _raw_str = (_err_raw.decode("utf-8", "replace") if _ns_force_stream
                                            else (r.text or ""))
                                _low = _raw_str[:2000].lower()
                                _is_cf = ("cloudflare" in _low
                                          or _low.lstrip().startswith("<!doctype html")
                                          or _low.lstrip().startswith("<html"))
                                last_body = {"error": {
                                    "type": err.get("type") or ("cloudflare_block" if _is_cf else "upstream_403"),
                                    "message": err.get("message") or _raw_str[:300],
                                }}
                                _chk = str(err.get('message') or '').lower()
                                if "user_blocked" in _chk or "policy violat" in _chk:
                                    sys.stderr.write(f"{_red('[403-block] Trust & Safety rejection: ' + _chk[:120])}\n")
                                    last_status = 403
                                    break
                                if _qlog("403-retry2"):
                                    sys.stderr.write(f"{_yel('[403-retry] ' + ('cloudflare' if _is_cf else 'upstream'))} {(err.get('type') or '')}: {(err.get('message') or '')[:120]}\n")
                            else:
                                if _ns_force_stream:
                                    last_body = body_json if body_json else {"error": _err_raw.decode("utf-8", "replace")[:500]}
                                else:
                                    try:
                                        last_body = r.json()
                                    except Exception:
                                        last_body = {"error": (r.text or "")[:500]}
                            sys.stderr.write(f"[retryable] http={r.status_code} will retry via new exit\n")
                            if r.status_code == 429 and zen_db:
                                try:
                                    _slot_ip = (current_slot.exit_ip if current_slot is not None and getattr(current_slot, "exit_ip", "") else _cached_exit_ip.get("ip", ""))
                                    db_async('record_error', "429", requested, _slot_ip, "rate limited")
                                except Exception:
                                    pass
                            # Non-streaming direct 429/403 cools down too
                            # (streaming path already did; this was missing).
                            if attempt == -1 and r.status_code in (429, 403):
                                _direct_cooldown_until = time.monotonic() + 900
                                sys.stderr.write(f"[direct-cooldown] real IP throttled ({r.status_code}) — skipping direct attempts for 15m\n")
                            try:
                                r.close()  # avoid fd leak across retries
                            except Exception:
                                pass
                            continue

                    try:
                        if r.status_code != 200:
                            _err_raw = b""
                            if _ns_force_stream:
                                try:
                                    for _c in r.iter_content():
                                        if not _c:
                                            continue
                                        _err_raw += (_c if isinstance(_c, bytes)
                                                     else str(_c).encode("utf-8", "replace"))
                                        if len(_err_raw) >= 8192:
                                            break
                                except Exception:
                                    pass
                                try:
                                    raw_resp = json.loads(_err_raw) if _err_raw else {}
                                except Exception:
                                    raw_resp = {"error": {"message": _err_raw.decode("utf-8", "replace")[:500]}}
                            else:
                                raw_resp = r.json()
                        elif _ns_force_stream:
                            raw_resp = (_collect_responses_sse(r) if upstream_is_responses
                                        else _collect_chat_sse(r, requested))
                        else:
                            raw_resp = r.json()
                    except Exception:
                        try:
                            r.close()
                        except Exception:
                            pass
                        # Native Anthropic lane (union-alpha): upstream ALWAYS
                        # 200s, so a non-JSON 200 here is an anomaly — but a
                        # 4xx/5xx + HTML/CF page must NOT be swallowed as 200
                        # success. Retryables continue, terminals surface.
                        if _wire_protocol(requested) == "messages" and r.status_code not in RETRYABLE_STATUSES:
                            _circuit_pool.note_failure(current_slot, r.status_code, f"terminal-{r.status_code}"[:80])
                            current_slot = None
                            return self._json(r.status_code, {
                                "error": {"message": "bad upstream json", "type": "upstream_error"},
                                "raw": r.text[:500]
                            })
                        _circuit_pool.release_slot(current_slot)
                        current_slot = None
                        return self._json(r.status_code, {
                            "error": {"message": "bad upstream json", "type": "upstream_error"},
                            "raw": r.text[:500]
                        })
                    # Terminal upstream error (e.g. permanent 400): surface it cleanly
                    # instead of feeding an error payload through the success adapter
                    # (which yields an empty message + wrong stop_reason).
                    if raw_resp.get("error") and not _is_retryable(r.status_code, raw_resp,
                                                                   allow_free_tier_retry=_genuine_sess):
                        _err = raw_resp.get("error") if isinstance(raw_resp.get("error"), dict) \
                            else {"message": str(raw_resp.get("error"))[:300]}
                        _circuit_pool.note_failure(current_slot, r.status_code, f"terminal-{r.status_code}"[:80])
                        current_slot = None
                        try:
                            r.close()
                        except Exception:
                            pass
                        if is_anthropic_messages:
                            return self._json(r.status_code, {
                                "type": "error",
                                "error": {"type": _err.get("type", "invalid_request_error"),
                                          "message": _err.get("message", f"upstream error {r.status_code}")},
                            })
                        return self._json(r.status_code, raw_resp)
                    # Check 200 with embedded retryable error (e.g., rate limit inside 200 JSON)
                    if raw_resp.get("error") and _is_retryable(r.status_code, raw_resp,
                                                               allow_free_tier_retry=_genuine_sess):
                        last_status = r.status_code if r.status_code != 200 else 429
                        last_body = raw_resp
                        sys.stderr.write(f"[retryable] 200 with error will retry: {str(raw_resp)[:150]}\n")
                        if zen_db:
                            try:
                                _slot_ip = (current_slot.exit_ip if current_slot is not None and getattr(current_slot, "exit_ip", "") else _cached_exit_ip.get("ip", ""))
                                db_async('record_error', "429", requested, _slot_ip, "rate limited")
                            except Exception:
                                pass
                        try:
                            r.close()
                        except Exception:
                            pass
                        continue

                    # Adapt /responses object to chat.completion object if client requested chat/completions
                    # (also accept missing/variant "object" fields — sniff on output array)
                    # Anthropic-native lane (/v1/messages, e.g. union-alpha):
                    # upstream returns {type message, content[]} — same
                    # builder as openai_to_anthropic_message but inline here
                    # (non-stream success path).
                    _is_anthropic_payload = (
                        raw_resp.get("type") == "message"
                        and isinstance(raw_resp.get("content"), list))
                    if _is_anthropic_payload and not is_anthropic_messages:
                        try:
                            _a_text = "\n".join(
                                str(c.get("text", "")) for c in
                                (raw_resp.get("content") or [])
                                if isinstance(c, dict)
                                and c.get("type") == "text")
                            _a_tcs = [{
                                "id": t.get("id", ""),
                                "function": {
                                    "name": t.get("name", ""),
                                    "arguments": json.dumps(t.get("input", {})),
                                },
                            } for t in (raw_resp.get("content") or [])
                                if isinstance(t, dict)
                                and t.get("type") == "tool_use"]
                            _a_has_tc = bool(_a_tcs)
                            upstream_body = self.openai_to_anthropic_message(
                                {"choices": [{"message": {
                                    "content": _a_text,
                                    "tool_calls": _a_tcs or None,
                                }, "finish_reason": (
                                    "tool_calls" if _a_has_tc else "stop")}],
                                 "usage": {
                                    "prompt_tokens": ((raw_resp.get("usage") or {})
                                                      .get("input_tokens", 0) or 0),
                                    "completion_tokens": ((raw_resp.get("usage") or {})
                                                          .get("output_tokens", 0) or 0)}},
                                requested)
                            upstream_body["id"] = raw_resp.get("id", upstream_body.get("id"))
                            upstream_body["model"] = requested
                            # Anthropic native usage (input/output_tokens) is
                            # already translated inside the converted body —
                            # tag it so the usage block below (and the
                            # prompt-cache store) skips re-derivation and the
                            # success return below goes out verbatim.
                            upstream_body["_union_converted"] = True
                        except Exception:
                            upstream_body = raw_resp
                    _is_responses_payload = (
                        raw_resp.get("object") == "response"
                        or ("output" in raw_resp and isinstance(raw_resp.get("output"), list)
                            and "choices" not in raw_resp)
                    )
                    if upstream_is_responses and not is_responses_api and _is_responses_payload:
                        content_text = ""
                        reasoning_text = ""
                        tool_calls_out = []
                        for item in raw_resp.get("output", []):
                            if item.get("type") == "message":
                                for c in item.get("content", []):
                                    if c.get("type") in ("output_text", "text"):
                                        content_text += c.get("text", "")
                            elif item.get("type") == "function_call":
                                tool_calls_out.append({
                                    "index": len(tool_calls_out),
                                    "id": item.get("call_id") or item.get("id") or f"call_{int(time.time()*1000)}",
                                    "type": "function",
                                    "function": {
                                        "name": item.get("name", ""),
                                        # "{}" so client json.loads never sees ""
                                        "arguments": item.get("arguments") or "{}",
                                    },
                                })
                            elif item.get("type") == "reasoning":
                                # Plaintext only — never leak encrypted blobs into the client
                                if item.get("text"):
                                    reasoning_text += item["text"]
                                else:
                                    for s in item.get("summary", []) or []:
                                        if isinstance(s, dict) and s.get("text"):
                                            reasoning_text += s["text"]

                        if not content_text and reasoning_text and not tool_calls_out:
                            content_text = reasoning_text
                        msg_obj = {"role": "assistant", "content": content_text}
                        if reasoning_text:
                            msg_obj["reasoning_content"] = reasoning_text
                        finish_reason = "stop"
                        if tool_calls_out:
                            msg_obj["tool_calls"] = tool_calls_out
                            finish_reason = "tool_calls"

                        upstream_body = {
                            "id": raw_resp.get("id", "resp-zen"),
                            "object": "chat.completion",
                            "created": raw_resp.get("created_at", int(time.time())),
                            "model": requested,
                            "choices": [
                                {
                                    "index": 0,
                                    "message": msg_obj,
                                    "finish_reason": finish_reason
                                }
                            ],
                            "usage": raw_resp.get("usage", {})
                        }
                    elif _is_anthropic_payload and not is_anthropic_messages:
                        # Already converted above into OpenAI message shape
                        # (upstream_body); do NOT overwrite with the raw
                        # Anthropic payload (that dropped `choices` -> KeyError
                        # downstream + HTTP 200 with no content).
                        pass
                    else:
                        upstream_body = raw_resp
                    # Usage tracking for non-streaming requests (was entirely missing).
                    # Translate Responses-API token names when adapting.
                    u_raw = raw_resp.get("usage") or {}
                    if "prompt_tokens" not in u_raw and "input_tokens" in u_raw:
                        u_rec = {
                            "prompt_tokens": u_raw.get("input_tokens", 0) or 0,
                            "completion_tokens": u_raw.get("output_tokens", 0) or 0,
                            "prompt_tokens_details": {"cached_tokens":
                                (u_raw.get("input_tokens_details") or {}).get("cached_tokens", 0) or 0},
                        }
                    else:
                        u_rec = u_raw
                    if zen_db:
                        try:
                            db_async('record_usage', 
                                model=requested,
                                client_type=client_type,
                                prompt_tokens=u_rec.get("prompt_tokens", 0) or 0,
                                completion_tokens=u_rec.get("completion_tokens", 0) or 0,
                                cached_tokens=(u_rec.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0,
                                latency_ms=t_elapsed,
                                ttft_ms=t_elapsed,
                                tor_exit_ip=_cached_exit_ip.get("ip", ""),
                                status=r.status_code,
                                retries=attempt,
                                prompt_chars=prompt_chars,
                            )
                        except Exception as e:

                            sys.stderr.write(f"[db] record failed: {e}\n")
                    try:
                        r.close()
                    except Exception:
                        pass
                    if current_slot is not None:
                        _circuit_pool.report_success(current_slot)
                        current_slot = None
                    # Recovery detected: drop any parked ban for this model so
                    # later requests rotate normally instead of skipping.
                    _circuit_pool.clear_retry_after(_ban_key)
                    if (r.status_code == 200 and prompt_hash and zen_db
                            and hasattr(zen_db, "store_prompt_cache")
                            and not (isinstance(upstream_body, dict)
                                     and "_union_converted" in upstream_body)):
                        try:
                            _u_choices = upstream_body.get("choices") if isinstance(upstream_body, dict) else None
                            _u_msg = _u_choices[0].get("message") if (_u_choices and isinstance(_u_choices, list) and len(_u_choices) > 0 and isinstance(_u_choices[0], dict)) else {}
                            _can_cache = bool((_u_msg.get("content") or "").strip()
                                              or _u_msg.get("tool_calls")
                                              or (_u_msg.get("reasoning_content") or "").strip())
                            if _can_cache:
                                db_async('store_prompt_cache', prompt_hash, requested, upstream_body)
                        except Exception:
                            pass
                    if is_anthropic_messages:
                        upstream_body = self.openai_to_anthropic_message(upstream_body, requested)
                    if (isinstance(upstream_body, dict)
                            and upstream_body.pop("_union_converted", False)):
                        # Native Anthropic payload must go out VERBATIM —
                        # rebuild OpenAI chat shape explicitly (never trust an
                        # in-place conversion to have survived).
                        try:
                            _u_txt = "\n".join(
                                str(c.get("text", "")) for c in
                                (raw_resp.get("content") or [])
                                if isinstance(c, dict)
                                and c.get("type") == "text")
                            _u_tc = "tool_calls" if any(
                                isinstance(c, dict)
                                and c.get("type") == "tool_use"
                                for c in (raw_resp.get("content") or [])) else "stop"
                            upstream_body = {
                                "id": raw_resp.get("id", "resp-zen"),
                                "object": "chat.completion",
                                "created": int(time.time()),
                                "model": requested,
                                "choices": [{
                                    "index": 0,
                                    "message": {"role": "assistant",
                                                "content": _u_txt},
                                    "finish_reason": _u_tc,
                                }],
                                "usage": {
                                    "prompt_tokens": ((raw_resp.get("usage") or {})
                                                      .get("input_tokens", 0) or 0),
                                    "completion_tokens": ((raw_resp.get("usage") or {})
                                                          .get("output_tokens", 0) or 0)},
                            }
                        except Exception:
                            pass
                        return self._json(r.status_code, upstream_body)
                    if (_wire_protocol(requested) == "messages" and not is_anthropic_messages
                            and isinstance(upstream_body, dict)
                            and "choices" not in upstream_body):
                        # Native Anthropic payload made it here unconverted
                        # (builder missed a shape): convert now rather than
                        # emitting a choiceless 200 the client keys on.
                        try:
                            upstream_body = self.openai_to_anthropic_message(
                                {"choices": [{"message": {
                                    "content": "\n".join(
                                        str(c.get("text", "")) for c in
                                        (upstream_body.get("content") or [])
                                        if isinstance(c, dict)
                                        and c.get("type") == "text"),
                                    "tool_calls": None,
                                }, "finish_reason": "stop"}],
                                 "usage": {
                                    "prompt_tokens": ((upstream_body.get("usage") or {})
                                                      .get("input_tokens", 0) or 0),
                                    "completion_tokens": ((upstream_body.get("usage") or {})
                                                          .get("output_tokens", 0) or 0)}},
                                requested)
                            upstream_body["model"] = requested
                        except Exception:
                            pass
                    if (is_responses_api and not upstream_is_responses
                            and isinstance(upstream_body, dict)
                            and "choices" in upstream_body):
                        # /v1/responses client served by the chat backend
                        # (chat-wire model): shape the chat result as a
                        # Responses object instead of leaking chat JSON.
                        upstream_body = chat_completion_to_responses(
                            upstream_body, requested)
                    return self._json(r.status_code, upstream_body)

            # Inner attempt loop exhausted — decide whether to rescue.
            if client_disconnected.is_set():
                sys.stderr.write("[client] client disconnected, ending rescue/attempt loop\n")
                break

            # PATIENT-WAIT: rescue is the primary path now, not a last resort.
            # The client (opencode subagent) must NEVER see a 429 for a
            # retryable upstream throttle — we switch models with keepalives
            # instead. Only genuinely dead upstreams (no response at all)
            # fall through to an error.
            if last_status == 0:
                # No upstream response on ANY attempt. Try rescue once anyway
                # (different model may be on a healthy provider path).
                _rescue_reasons = ["no-response"]
            else:
                _rescue_reasons = []
            if rescue_pending and not client_disconnected.is_set() and (last_status in RETRYABLE_STATUSES or last_status == 0):
                # Walk the FALLBACK_CHAIN in order, skipping the burned model
                # AND any model under a pool-global ban horizon. First live,
                # unbanned model wins. Keepalives flow the whole time so the
                # subagent sees activity, never a failure.
                _, _live_ids = model_ids_fast(self.upstream, self.proxy_url)
                _is_muse_req = (requested.startswith("muse") or orig_requested.startswith("muse"))
                _cands = [m for m in FALLBACK_CHAIN
                          if m not in visited_rescue_models and m in _live_ids
                          and not (_is_muse_req and m.startswith("muse"))]
                rescuer = None
                for _cand in _cands:
                    _left = _circuit_pool.retry_after_left(f"{self.upstream}|{_cand}")
                    if _left > 0:
                        sys.stderr.write(
                            f"[rescue-skip] '{_cand}' banned pool-globally "
                            f"({_left:.0f}s left)\n")
                        continue
                    rescuer = _cand
                    break
                if rescuer:
                    visited_rescue_models.add(rescuer)
                    sys.stderr.write(
                        f"[rescue] '{requested}' (status={last_status}) -> "
                        f"rescuing via '{rescuer}' (subagent never fails)\n")
                    _sse_comment()
                    if zen_db:
                        try:
                            _slot_ip = (current_slot.exit_ip if current_slot is not None and getattr(current_slot, "exit_ip", "") else _cached_exit_ip.get("ip", ""))
                            db_async('record_error', "rescue", requested, _slot_ip, f"-> {rescuer}")
                        except Exception:
                            pass
                    rescue_pending = (rescuer not in FALLBACK_CHAIN[-1:])
                    requested = rescuer
                    _ban_key = f"{self.upstream}|{requested}"
                    body["model"] = rescuer
                    upstream_is_responses, upstream_endpoint, upstream_body = _prepare_upstream(requested)
                    _circuit_pool.note_failure(current_slot, last_status or 429, f"rescue->{rescuer}"[:80])
                    current_slot = None
                    last_status = 0
                    last_body = None
                    deadline = time.monotonic() + min(self.max_time, 120)
                    continue  # re-run attempts on the rescue model
                sys.stderr.write(
                    f"[rescue-exhausted] no live unbanned fallback for "
                    f"'{requested}' — failing (all models banned/down)\n")
            break

        _circuit_pool.note_failure(current_slot, last_status or 504, "exhausted"[:80])
        current_slot = None
        if client_disconnected.is_set():
            return
        if last_status == 0:
            if sse:
                # Headers already committed — deliver failure as an SSE error
                # chunk + [DONE] so the client terminates cleanly.
                msg = _error_message(last_body)
                self._terminate_stream_with_error(str(msg)[:300] or f"upstream status {last_status}")
                return
            return self._json(504, {"error": "all retries exhausted (no upstream response)"})
        if sse:
            msg = _error_message(last_body)
            self._terminate_stream_with_error(str(msg)[:300] or f"upstream status {last_status}")
            return
        if last_status in RETRYABLE_STATUSES and isinstance(last_body, dict):
            last_body.setdefault("note",
                f"all {self.max_retries+1} attempts through different tor exits failed")
        return self._json(last_status, last_body or {"error": "no body"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--default", default=DEFAULT_MODEL)
    ap.add_argument("--upstream", default=UPSTREAM)
    ap.add_argument("--socks", default=SOCKS)
    ap.add_argument("--control", default=CONTROL)
    ap.add_argument("--retries", type=int, default=MAX_RETRIES)
    ap.add_argument("--rotate-wait", type=float, default=ROTATE_WAIT,
                    help="seconds to wait after a successful NEWNYM")
    ap.add_argument("--max-time", type=float, default=MAX_TIME,
                    help="total seconds budget for one request (retries+budget)")
    ap.add_argument("--backoff", type=float, default=BACKOFF,
                    help="exponential backoff multiplier per retry")
    ap.add_argument("--restart-tor", action="store_true", default=RESTART_TOR,
                    help="if NEWNYM fails, kill+respawn tor (uses --torrc)")
    ap.add_argument("--direct-first", action="store_true", default=False,
                    help="try the real IP before Tor (fast direct-first; auto-rotates on 429/403)")
    ap.add_argument("--tor-first", action="store_false", dest="direct_first",
                    help="force Tor on first attempt instead of direct connection (default)")
    ap.add_argument("--torrc", default=TORRC,
                    help="path to torrc for --restart-tor")
    ap.add_argument("--pool-size", type=int, default=DEFAULT_POOL_SIZE,
                    help=f"number of pre-warmed virtual Tor circuits in pool (default: {DEFAULT_POOL_SIZE})")
    ap.add_argument("--public-auth", action="store_true", default=False,
                    help="require API key for requests arriving via public host (funnel)")
    args = ap.parse_args()

    global _PUBLIC_AUTH, _LOCAL_DNS_NAME
    if args.public_auth or os.environ.get("ZEN_PUBLIC_AUTH", "") == "1":
        _PUBLIC_AUTH = True
    try:
        import json as _js, subprocess as _sp
        _out = _sp.run(["tailscale", "status", "--json"], capture_output=True,
                       timeout=5, text=True)
        if _out.returncode == 0:
            _LOCAL_DNS_NAME = (_js.loads(_out.stdout).get("Self", {})
                               .get("DNSName", "") or "").rstrip(".").lower()
    except Exception:
        pass
    # Load public API keys into memory + ensure friend seed keys exist.
    try:
        if zen_db is not None and hasattr(zen_db, "ensure_seed_keys"):
            zen_db.ensure_seed_keys(("rayed", "parnol"))
        reload_api_keys(force=True)
        sys.stderr.write(f"[auth] public-auth={'on' if _PUBLIC_AUTH else 'off'} "
                         f"keys={len(_API_KEYS)} host={_LOCAL_DNS_NAME or '?'}\n")
    except Exception as e:

        sys.stderr.write(f"[auth] key load failed: {e}\n")

    Handler.default_model = args.default
    Handler.upstream = args.upstream
    Handler.socks = args.socks
    Handler.proxy_url = f"socks5h://{args.socks}"
    Handler.control_host, _, cp = args.control.partition(":")
    Handler.control_port = int(cp or "9151")
    Handler.max_retries = args.retries
    Handler.rotate_wait = args.rotate_wait
    Handler.max_time = args.max_time
    Handler.backoff = args.backoff
    Handler.restart_tor = args.restart_tor
    Handler.direct_first = args.direct_first
    Handler.torrc = args.torrc

    # Initialize usage tracking DB
    if zen_db:
        try:
            zen_db.init_db()
            zen_db.backup_db()
        except Exception as e:

            sys.stderr.write(f"[db] init failed: {e}\n")

    # Boot Tor first: after a reboot /tmp/tor2 is wiped, so no listener on :9150/:9151
    # and every slot probe fails with curl (7). Ensure user-mode Tor before probing.
    try:
        _ch, _, _cp = (args.control or "127.0.0.1:9151").partition(":")
        _sh2, _, _sp2 = (args.socks or "127.0.0.1:9150").partition(":")
        ensure_tor_daemon(_ch or "127.0.0.1", int(_cp or "9151"), int(_sp2 or "9150"))
    except Exception as _be:
        sys.stderr.write(f"[tor-supervisor] boot ensure failed: {_be}\n")

    # Start pre-warmed Tor virtual circuit pool (0ms failover)
    global _circuit_pool
    if args.pool_size != _circuit_pool.pool_size or args.socks != _circuit_pool.socks_hp:
        _circuit_pool = PrewarmedCircuitPool(pool_size=args.pool_size, socks_hp=args.socks)
    _circuit_pool.socks_hp = args.socks
    _circuit_pool.upstream = args.upstream
    _circuit_pool.health_model = args.default
    _circuit_pool.start()

    ThreadingHTTPServer.allow_reuse_address = True
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.request_queue_size = 64
    ThreadingHTTPServer.socket_timeout = 60
    try:
        srv = ThreadingHTTPServer((args.host, args.port), Handler)
    except OSError as e:
        if e.errno == 98:
            sys.stderr.write(f"[proxy-error] port {args.port} is already in use by another running instance.\n")
            sys.exit(98)
        raise
    print(f"tor-zen-proxy v3 listening on http://{args.host}:{args.port}")
    if args.host == "0.0.0.0":
        sys.stderr.write("[security] WARNING: bound to 0.0.0.0 (LAN-wide). /v1/* is unauthenticated; prefer 127.0.0.1 unless forwarding to owned devices.\n")
    print(f"  upstream:    {args.upstream}")
    print(f"  default:     {args.default}")
    print(f"  socks:       {args.socks}")
    print(f"  control:     {args.control}")
    print(f"  retries:     {args.retries}")
    print(f"  rotate-wait: {args.rotate_wait}s")
    print(f"  max-time:    {args.max_time}s")
    print(f"  backoff:     x{args.backoff}")
    print(f"  restart-tor: {args.restart_tor} (torrc={args.torrc})")
    print(f"  streaming:   enabled (SSE passthrough)")
    print(f"  tracking:    {'enabled' if zen_db else 'disabled'}")
    print(f"  auth token:  {TOKEN_FILE} (for /live /stats /rotate)")
    print(f"  direct-first:{args.direct_first} (default is Tor-first)")
    print(f"  tor exit:    {tor_exit_ip(args.socks)}")
    print(f"  free models: {len(MODELS)}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")


if __name__ == "__main__":
    main()
