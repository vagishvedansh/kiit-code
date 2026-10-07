import assert from "node:assert/strict";
import * as aliasChat from "../../../functions/v1/v1/chat/completions.js";
import * as directChat from "../../../functions/v1/chat/completions.js";
import * as aliasMessages from "../../../functions/v1/v1/messages.js";
import * as directMessages from "../../../functions/v1/messages.js";
import * as aliasModels from "../../../functions/v1/v1/models.js";
import * as directModels from "../../../functions/v1/models.js";

async function runTests() {
  console.log("--- Testing Route Aliases (/v1/v1/*) ---");

  // Test 1: chat completions alias exports
  assert.equal(typeof aliasChat.onRequestPost, "function");
  assert.equal(typeof aliasChat.onRequestOptions, "function");
  assert.equal(aliasChat.onRequestPost, directChat.onRequestPost);
  assert.equal(aliasChat.onRequestOptions, directChat.onRequestOptions);
  console.log("✓ Test 1 Passed: /v1/v1/chat/completions re-exports onRequestPost and onRequestOptions from /v1/chat/completions");

  // Test 2: messages alias exports
  assert.equal(typeof aliasMessages.onRequestPost, "function");
  assert.equal(typeof aliasMessages.onRequestOptions, "function");
  assert.equal(aliasMessages.onRequestPost, directMessages.onRequestPost);
  assert.equal(aliasMessages.onRequestOptions, directMessages.onRequestOptions);
  console.log("✓ Test 2 Passed: /v1/v1/messages re-exports handlers from /v1/messages");

  // Test 3: models alias exports
  assert.equal(typeof aliasModels.onRequestGet, "function");
  assert.equal(typeof aliasModels.onRequestOptions, "function");
  assert.equal(aliasModels.onRequestGet, directModels.onRequestGet);
  assert.equal(aliasModels.onRequestOptions, directModels.onRequestOptions);
  console.log("✓ Test 3 Passed: /v1/v1/models re-exports handlers from /v1/models");

  // Test 4: Functional verification on alias handler
  const req = new Request("http://localhost/v1/v1/chat/completions", {
    method: "OPTIONS"
  });
  const res = await aliasChat.onRequestOptions({ request: req });
  assert.equal(res.status, 204);
  assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
  console.log("✓ Test 4 Passed: Calling alias handler produces correct HTTP 204 preflight");

  console.log("All Alias Tests Passed Successfully!\n");
}

runTests();
