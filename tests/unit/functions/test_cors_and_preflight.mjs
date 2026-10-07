import assert from "node:assert/strict";
import { onRequestOptions as optionsCompletions, onRequestPost as postCompletions } from "../../../functions/v1/chat/completions.js";
import { onRequestOptions as optionsMessages, onRequestPost as postMessages } from "../../../functions/v1/messages.js";
import { onRequestOptions as optionsModels, onRequestGet as getModels } from "../../../functions/v1/models.js";
import { onRequestOptions as optionsDeduct, onRequestPost as postDeduct } from "../../../functions/api/internal/deduct.js";
import { onRequest as middleware } from "../../../functions/_middleware.js";

async function runTests() {
  console.log("--- Testing CORS & Preflight Across Endpoints ---");

  // Test 1: OPTIONS on completions.js
  {
    const res = await optionsCompletions();
    assert.equal(res.status, 204);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    assert.ok(res.headers.get("Access-Control-Allow-Methods").includes("OPTIONS"));
    assert.ok(res.headers.get("Access-Control-Allow-Headers").includes("Authorization"));
    console.log("✓ Test 1 Passed: completions.js onRequestOptions returns 204 No Content with CORS");
  }

  // Test 2: OPTIONS on messages.js
  {
    const res = await optionsMessages();
    assert.equal(res.status, 204);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    assert.ok(res.headers.get("Access-Control-Allow-Methods").includes("OPTIONS"));
    console.log("✓ Test 2 Passed: messages.js onRequestOptions returns 204 with CORS");
  }

  // Test 3: OPTIONS on models.js
  {
    const res = await optionsModels();
    assert.equal(res.status, 204);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    console.log("✓ Test 3 Passed: models.js onRequestOptions returns 204 with CORS");
  }

  // Test 4: OPTIONS on deduct.js
  {
    const res = await optionsDeduct();
    assert.equal(res.status, 204);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    console.log("✓ Test 4 Passed: deduct.js onRequestOptions returns 204 with CORS");
  }

  // Test 5: Global _middleware.js handles OPTIONS preflight
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      method: "OPTIONS",
      headers: { "Origin": "http://client.example.com" }
    });
    const res = await middleware({ request: req, next: () => assert.fail("Should not call next for OPTIONS") });
    assert.equal(res.status, 204);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    console.log("✓ Test 5 Passed: _middleware.js intercepts OPTIONS preflight globally");
  }

  // Test 6: Global _middleware.js injects Access-Control-Allow-Origin if missing
  {
    const req = new Request("http://localhost/something", { method: "GET" });
    const res = await middleware({
      request: req,
      next: async () => new Response("OK", { status: 200, headers: { "Content-Type": "text/plain" } })
    });
    assert.equal(res.status, 200);
    assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
    console.log("✓ Test 6 Passed: _middleware.js injects CORS header onto downstream responses");
  }

  // Test 7: Error responses include Access-Control-Allow-Origin
  {
    // 401 Missing Key on completions
    const reqNoKey = new Request("http://localhost/v1/chat/completions", { method: "POST" });
    const resNoKey = await postCompletions({ request: reqNoKey, env: {} });
    assert.equal(resNoKey.status, 401);
    assert.equal(resNoKey.headers.get("Access-Control-Allow-Origin"), "*");

    // 401 Missing Key on messages
    const resNoKeyMsg = await postMessages({ request: reqNoKey, env: {} });
    assert.equal(resNoKeyMsg.status, 401);
    assert.equal(resNoKeyMsg.headers.get("Access-Control-Allow-Origin"), "*");

    // 403 Forbidden on deduct
    const reqDeduct = new Request("http://localhost/api/internal/deduct", {
      method: "POST",
      headers: { "X-Internal-Secret": "wrong-secret" }
    });
    const resDeduct = await postDeduct({ request: reqDeduct, env: {} });
    assert.equal(resDeduct.status, 403);
    assert.equal(resDeduct.headers.get("Access-Control-Allow-Origin"), "*");

    console.log("✓ Test 7 Passed: Error responses (401, 403) all carry Access-Control-Allow-Origin: *");
  }

  console.log("All CORS & Preflight Tests Passed Successfully!\n");
}

runTests();
