import assert from "node:assert/strict";
import { onRequestOptions as optionsCompletions, onRequestPost as postCompletions } from "../../functions/v1/chat/completions.js";
import { onRequestOptions as optionsMessages, onRequestPost as postMessages } from "../../functions/v1/messages.js";
import { onRequestOptions as optionsModels, onRequestGet as getModels } from "../../functions/v1/models.js";
import { onRequestOptions as optionsDeduct, onRequestPost as postDeduct } from "../../functions/api/internal/deduct.js";
import { onRequestOptions as optionsAliasCompletions } from "../../functions/v1/v1/chat/completions.js";
import { onRequestOptions as optionsAliasMessages } from "../../functions/v1/v1/messages.js";
import { onRequestOptions as optionsAliasModels } from "../../functions/v1/v1/models.js";
import { onRequest as middleware } from "../../functions/_middleware.js";

async function runCorsOptionsSuite() {
  console.log("===============================================================");
  console.log("  ADVERSARIAL SUITE 1: CORS PREFLIGHT OPTIONS & HEADER AUDIT  ");
  console.log("===============================================================");

  let testCount = 0;
  let passCount = 0;

  function recordPass(msg) {
    testCount++;
    passCount++;
    console.log(`[PASS] (${testCount}) ${msg}`);
  }

  // -------------------------------------------------------------
  // Test 1: Direct onRequestOptions across all 7 route handlers
  // -------------------------------------------------------------
  const routeHandlers = [
    { name: "/v1/chat/completions", handler: optionsCompletions },
    { name: "/v1/messages", handler: optionsMessages },
    { name: "/v1/models", handler: optionsModels },
    { name: "/api/internal/deduct", handler: optionsDeduct },
    { name: "/v1/v1/chat/completions (alias)", handler: optionsAliasCompletions },
    { name: "/v1/v1/messages (alias)", handler: optionsAliasMessages },
    { name: "/v1/v1/models (alias)", handler: optionsAliasModels },
  ];

  for (const { name, handler } of routeHandlers) {
    const res = await handler();
    assert.equal(res.status, 204, `${name} must return 204 No Content`);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*", `${name} must allow origin *`);
    assert.ok(res.headers.get("Access-Control-Allow-Methods").includes("OPTIONS"), `${name} methods must include OPTIONS`);
    assert.ok(res.headers.get("Access-Control-Allow-Headers").includes("Content-Type"), `${name} headers must include Content-Type`);
    assert.ok(res.headers.get("Access-Control-Max-Age"), `${name} must specify Max-Age`);
    // Verify 204 response body is null or empty
    const body = await res.text();
    assert.equal(body, "", `${name} 204 response body must be empty`);
    recordPass(`Direct onRequestOptions on ${name} verified (status 204, CORS headers, empty body)`);
  }

  // -------------------------------------------------------------
  // Test 2: Global _middleware.js OPTIONS interception across all routes
  // -------------------------------------------------------------
  const testPaths = [
    "/",
    "/v1/chat/completions",
    "/v1/messages",
    "/v1/models",
    "/v1/v1/chat/completions",
    "/v1/v1/messages",
    "/v1/v1/models",
    "/api/internal/deduct",
    "/api/unknown/endpoint",
    "/v1/nested/very/deep/resource",
    "/strange%20path?param=1",
  ];

  for (const path of testPaths) {
    let nextCalled = false;
    const req = new Request(`http://localhost${path}`, {
      method: "OPTIONS",
      headers: {
        "Origin": "https://client-frontend.vercel.app",
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "authorization, content-type, anthropic-version, x-api-key",
      },
    });

    const res = await middleware({
      request: req,
      next: async () => {
        nextCalled = true;
        return new Response("Not Handled", { status: 404 });
      },
    });

    assert.equal(nextCalled, false, `OPTIONS to ${path} must be intercepted by middleware without calling next()`);
    assert.equal(res.status, 204, `OPTIONS to ${path} must return 204`);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*", `CORS origin must be * on ${path}`);
    const allowHeaders = res.headers.get("Access-Control-Allow-Headers");
    assert.ok(allowHeaders.includes("Authorization"), "Access-Control-Allow-Headers must include Authorization");
    assert.ok(allowHeaders.includes("anthropic-version"), "Access-Control-Allow-Headers must include anthropic-version");
    assert.ok(allowHeaders.includes("x-api-key"), "Access-Control-Allow-Headers must include x-api-key");
    assert.ok(allowHeaders.includes("Content-Type"), "Access-Control-Allow-Headers must include Content-Type");
    assert.equal(res.headers.get("Access-Control-Max-Age"), "86400", "Max-Age must be 86400");
    recordPass(`_middleware.js intercepted preflight OPTIONS on '${path}' with status 204`);
  }

  // -------------------------------------------------------------
  // Test 3: Downstream header preservation and anti-duplication
  // -------------------------------------------------------------
  {
    // Case A: Downstream sets Access-Control-Allow-Origin: *
    const req = new Request("http://localhost/v1/test", { method: "GET" });
    const resA = await middleware({
      request: req,
      next: async () => new Response("OK", {
        status: 200,
        headers: { "Access-Control-Allow-Origin": "*" },
      }),
    });
    // Header should not be doubled like "*, *"
    assert.equal(resA.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("Middleware does not duplicate Access-Control-Allow-Origin when already present");

    // Case B: Downstream omits CORS header
    const resB = await middleware({
      request: req,
      next: async () => new Response("OK", {
        status: 200,
        headers: { "Content-Type": "text/plain" },
      }),
    });
    assert.equal(resB.headers.get("Access-Control-Allow-Origin"), "*");
    assert.equal(resB.headers.get("Content-Type"), "text/plain");
    recordPass("Middleware injects Access-Control-Allow-Origin when omitted downstream");
  }

  // -------------------------------------------------------------
  // Test 4: Error responses across all endpoints guarantee CORS header
  // -------------------------------------------------------------
  {
    // 401 on completions (missing key)
    const reqNoKey = new Request("http://localhost/v1/chat/completions", { method: "POST" });
    const res401Comp = await postCompletions({ request: reqNoKey, env: {} });
    assert.equal(res401Comp.status, 401);
    assert.equal(res401Comp.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("401 Missing Key on /v1/chat/completions has CORS header");

    // 401 on messages (missing key)
    const res401Msg = await postMessages({ request: reqNoKey, env: {} });
    assert.equal(res401Msg.status, 401);
    assert.equal(res401Msg.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("401 Missing Key on /v1/messages has CORS header");

    // 401 on messages (invalid key)
    const reqBadKey = new Request("http://localhost/v1/messages", {
      method: "POST",
      headers: { "Authorization": "Bearer totally-invalid-key" },
    });
    const resBadKey = await postMessages({ request: reqBadKey, env: {} });
    assert.equal(resBadKey.status, 401);
    assert.equal(resBadKey.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("401 Invalid Key on /v1/messages has CORS header");

    // 402 on completions (exhausted balance)
    const reqEmptyBal = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": "Bearer empty-balance-key" },
    });
    const res402 = await postCompletions({ request: reqEmptyBal, env: {} });
    assert.equal(res402.status, 402);
    assert.equal(res402.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("402 Exhausted Balance on /v1/chat/completions has CORS header");

    // 403 on internal deduct (wrong secret)
    const reqBadSec = new Request("http://localhost/api/internal/deduct", {
      method: "POST",
      headers: { "X-Internal-Secret": "invalid-secret" },
    });
    const res403 = await postDeduct({ request: reqBadSec, env: {} });
    assert.equal(res403.status, 403);
    assert.equal(res403.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("403 Forbidden on /api/internal/deduct has CORS header");

    // 502 on completions (backend unreachable)
    const originalFetch = globalThis.fetch;
    try {
      globalThis.fetch = async () => {
        throw new Error("Connection refused ECONNREFUSED 127.0.0.1:8080");
      };
      const reqFetchFail = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o" }),
      });
      const res502Comp = await postCompletions({ request: reqFetchFail, env: {} });
      assert.equal(res502Comp.status, 502);
      assert.equal(res502Comp.headers.get("Access-Control-Allow-Origin"), "*");
      recordPass("502 Backend Unreachable on /v1/chat/completions has CORS header");

      // 502 on messages (backend unreachable)
      const reqFetchFailMsg = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "claude-3-5-sonnet" }),
      });
      const res502Msg = await postMessages({ request: reqFetchFailMsg, env: {} });
      assert.equal(res502Msg.status, 502);
      assert.equal(res502Msg.headers.get("Access-Control-Allow-Origin"), "*");
      recordPass("502 Backend Unreachable on /v1/messages has CORS header");

      // 502 on models (backend unreachable)
      const reqFetchFailModels = new Request("http://localhost/v1/models", { method: "GET" });
      const res502Models = await getModels({ request: reqFetchFailModels, env: {} });
      assert.equal(res502Models.status, 502);
      assert.equal(res502Models.headers.get("Access-Control-Allow-Origin"), "*");
      recordPass("502 Backend Unreachable on /v1/models has CORS header");
    } finally {
      globalThis.fetch = originalFetch;
    }
  }

  // -------------------------------------------------------------
  // Test 5: Pipeline composition: Request passing through _middleware to handler
  // -------------------------------------------------------------
  {
    // Simulate Cloudflare Pages Functions pipeline: _middleware(context) -> handler(context)
    const pipeline = async (path, method, headers = {}, body = null, env = {}) => {
      const req = new Request(`http://localhost${path}`, {
        method,
        headers,
        body: (method === "POST" && body) ? JSON.stringify(body) : null,
      });

      return await middleware({
        request: req,
        env,
        next: async () => {
          if (path === "/v1/chat/completions") {
            return await postCompletions({ request: req, env });
          } else if (path === "/v1/messages") {
            return await postMessages({ request: req, env });
          }
          return new Response("Not Found", { status: 404 });
        },
      });
    };

    // Test OPTIONS through pipeline
    const preflightRes = await pipeline("/v1/chat/completions", "OPTIONS", {
      "Origin": "https://example.com",
      "Access-Control-Request-Method": "POST",
    });
    assert.equal(preflightRes.status, 204);
    assert.equal(preflightRes.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("Pipeline preflight OPTIONS to /v1/chat/completions returns 204 No Content with CORS");

    // Test POST 401 through pipeline
    const authFailRes = await pipeline("/v1/chat/completions", "POST", {});
    assert.equal(authFailRes.status, 401);
    assert.equal(authFailRes.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("Pipeline POST (no auth) to /v1/chat/completions returns 401 with CORS");
  }

  console.log("---------------------------------------------------------------");
  console.log(`SUITE 1 RESULTS: ${passCount} / ${testCount} tests passed (100%)`);
  console.log("===============================================================\n");
}

runCorsOptionsSuite().catch((err) => {
  console.error("SUITE 1 CRITICAL FAILURE:", err);
  process.exit(1);
});
