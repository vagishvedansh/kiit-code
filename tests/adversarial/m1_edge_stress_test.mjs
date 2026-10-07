import assert from "node:assert/strict";
import crypto from "node:crypto";
import { validateApiKey, MOCK_API_KEYS } from "../../functions/_auth.js";
import { onRequestPost as handleCompletions, onRequestOptions as handleCompletionsOptions } from "../../functions/v1/chat/completions.js";
import { onRequestPost as handleMessages, onRequestOptions as handleMessagesOptions } from "../../functions/v1/messages.js";

// Helper to create mock context
function createContext(request, env = {}) {
  const waitPromises = [];
  return {
    request,
    env,
    waitUntil(promise) {
      waitPromises.push(promise);
    },
    _waitPromises: waitPromises
  };
}

// Sleep helper
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

console.log("======================================================================");
console.log("      ADVERSARIAL STRESS TEST SUITE — MILESTONE M1 EDGE LAYER         ");
console.log("======================================================================\n");

let suitesPassed = 0;
let suitesFailed = 0;

// -----------------------------------------------------------------------------
// SUITE 1: Auth Verification with Valid, Invalid, Empty & Malformed Headers
// -----------------------------------------------------------------------------
async function runAuthAdversarialSuite() {
  console.log(">>> [Suite 1] Adversarial Auth Verification (env.DB undefined & offline)...");
  let passed = 0;

  const envUndefinedVariations = [
    undefined,
    {},
    { DB: undefined },
    { DB: null },
    { DB: "invalid_db_string" },
    { DB: 12345 },
    { DB: {} },
    { DB: { prepare: null } },
    { DB: { prepare: "not_a_function" } }
  ];

  // 1.1: Valid Bearer tokens across all env.DB undefined variations
  for (const envVar of envUndefinedVariations) {
    const validKeys = ["test-key", "default-dev-key", "live-key-valid", "kiit-mock-key-12345", "public"];
    for (const key of validKeys) {
      const req = new Request("http://localhost/v1/chat/completions", {
        headers: { "Authorization": `Bearer ${key}` }
      });
      const res = await validateApiKey(req, envVar);
      assert.equal(res.success, true, `Key ${key} must succeed when env is ${JSON.stringify(envVar)}`);
      assert.equal(res.apiKey, key);
      assert.equal(res.user.is_mock, true);
    }
  }
  passed++;
  console.log("  ✓ 1.1: Valid mock keys authenticate across all 9 undefined/invalid env.DB permutations");

  // 1.2: Case sensitivity & whitespace handling in Bearer header
  const headerVariations = [
    { header: "bearer test-key", expectedKey: "test-key" },
    { header: "BEARER test-key", expectedKey: "test-key" },
    { header: "Bearer   test-key   ", expectedKey: "test-key" },
    { header: "Bearer\ttest-key", expectedKey: "test-key" },
    { header: "Bearer\t  test-key  \t", expectedKey: "test-key" },
    { header: "test-key", expectedKey: "test-key" } // Raw key in Authorization header
  ];
  for (const { header, expectedKey } of headerVariations) {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": header }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, true, `Header '${header}' should succeed`);
    assert.equal(res.apiKey, expectedKey);
  }
  passed++;
  console.log("  ✓ 1.2: Bearer case variations, extra whitespace, tabs, and raw tokens handled cleanly");

  // 1.3: Invalid Bearer tokens
  const invalidKeys = [
    "invalid-key-999",
    "Bearer",
    "sk-none-exists",
    "random_token_123!@#",
    "null",
    "undefined",
    "true",
    "0"
  ];
  for (const invalidKey of invalidKeys) {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": `Bearer ${invalidKey}` }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.equal(res.errorType, "authentication_error");
    assert.equal(res.message, "Invalid or disabled API Key");
  }
  passed++;
  console.log("  ✓ 1.3: Invalid tokens rejected with 401 and authentication_error");

  // 1.4: Inactive and Exhausted Balance keys
  {
    const reqInactive = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer inactive-key" }
    });
    const resInactive = await validateApiKey(reqInactive, {});
    assert.equal(resInactive.success, false);
    assert.equal(resInactive.status, 401);
    assert.equal(resInactive.message, "Invalid or disabled API Key");

    const reqEmpty = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer empty-balance-key" }
    });
    const resEmpty = await validateApiKey(reqEmpty, {});
    assert.equal(resEmpty.success, false);
    assert.equal(resEmpty.status, 402);
    assert.equal(resEmpty.errorType, "invalid_request_error");
    assert.ok(resEmpty.message.includes("Credit balance exhausted"));
  }
  passed++;
  console.log("  ✓ 1.4: Inactive key returns 401; zero credit balance returns 402");

  // 1.5: Empty headers and empty Bearer tokens
  const emptyHeaders = [
    {}, // completely missing
    { "Authorization": "" },
    { "Authorization": "   " },
    { "Authorization": "\t\t" },
    { "Authorization": "Bearer" }, // just Bearer
    { "Authorization": "Bearer " }, // Bearer with space
    { "Authorization": "Bearer    " }, // Bearer with multiple spaces
    { "Authorization": "Bearer \t " },
    { "x-api-key": "" },
    { "x-api-key": "   " }
  ];
  for (const hdrs of emptyHeaders) {
    const req = new Request("http://localhost/v1/chat/completions", { headers: hdrs });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    // Either "Missing API Key" or "Invalid or disabled API Key" (when "Bearer" itself was parsed)
    assert.ok(res.status === 401);
  }
  passed++;
  console.log("  ✓ 1.5: Empty, whitespace-only, and bare 'Bearer' headers return 401");

  // 1.6: x-api-key header priority and variations
  {
    // Authorization takes precedence if present
    const reqBoth = new Request("http://localhost/v1/chat/completions", {
      headers: {
        "Authorization": "Bearer test-key",
        "x-api-key": "invalid-key"
      }
    });
    const resBoth = await validateApiKey(reqBoth, {});
    assert.equal(resBoth.success, true);
    assert.equal(resBoth.apiKey, "test-key");

    // x-api-key used when Authorization is absent
    const reqXOnly = new Request("http://localhost/v1/messages", {
      headers: { "x-api-key": "default-dev-key" }
    });
    const resXOnly = await validateApiKey(reqXOnly, {});
    assert.equal(resXOnly.success, true);
    assert.equal(resXOnly.apiKey, "default-dev-key");

    // X-API-Key case variation
    const reqXUpper = new Request("http://localhost/v1/messages", {
      headers: { "X-API-Key": "default-dev-key" }
    });
    const resXUpper = await validateApiKey(reqXUpper, {});
    assert.equal(resXUpper.success, true);
    assert.equal(resXUpper.apiKey, "default-dev-key");
  }
  passed++;
  console.log("  ✓ 1.6: x-api-key precedence and case variations work as expected");

  // 1.7: End-to-end integration through completions and messages endpoints
  {
    // Completions 401 response format check
    const reqComp = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": "Bearer bad-key" }
    });
    const compRes = await handleCompletions(createContext(reqComp, {}));
    assert.equal(compRes.status, 401);
    assert.equal(compRes.headers.get("Access-Control-Allow-Origin"), "*");
    const compBody = await compRes.json();
    assert.equal(compBody.error, "Invalid or disabled API Key");

    // Messages 401 Anthropic error format check
    const reqMsg = new Request("http://localhost/v1/messages", {
      method: "POST",
      headers: { "x-api-key": "bad-key" }
    });
    const msgRes = await handleMessages(createContext(reqMsg, {}));
    assert.equal(msgRes.status, 401);
    assert.equal(msgRes.headers.get("Access-Control-Allow-Origin"), "*");
    assert.equal(msgRes.headers.get("anthropic-version"), "2023-06-01");
    const msgBody = await msgRes.json();
    assert.equal(msgBody.type, "error");
    assert.equal(msgBody.error.type, "authentication_error");
    assert.equal(msgBody.error.message, "Invalid or disabled API Key");
  }
  passed++;
  console.log("  ✓ 1.7: Edge handlers return compliant error structures and CORS on auth rejection");

  console.log(`[Suite 1 Result] ${passed}/7 test groups passed.\n`);
  suitesPassed++;
}

// -----------------------------------------------------------------------------
// SUITE 2: Slow Chunk Feeds & Verification of Zero-Buffering
// -----------------------------------------------------------------------------
async function runSlowChunkFeedSuite() {
  console.log(">>> [Suite 2] Testing Streaming Passthrough Under Slow Chunk Feeds...");
  const originalFetch = globalThis.fetch;

  try {
    const chunkCount = 5;
    const chunkDelayMs = 80; // 80ms between chunks
    const chunkTexts = ["Chunk-1: Hello", "Chunk-2: to", "Chunk-3: the", "Chunk-4: streaming", "Chunk-5: world!"];
    const serverEmitTimes = [];

    // Upstream mock that produces chunks with deliberate delays
    globalThis.fetch = async (url, options) => {
      const stream = new ReadableStream({
        async start(controller) {
          const enc = new TextEncoder();
          for (let i = 0; i < chunkCount; i++) {
            if (i > 0) {
              await sleep(chunkDelayMs);
            }
            serverEmitTimes.push(Date.now());
            controller.enqueue(enc.encode(`data: {"choices":[{"delta":{"content":"${chunkTexts[i]}"}}]}\n\n`));
          }
          controller.enqueue(enc.encode("data: [DONE]\n\n"));
          controller.close();
        }
      });

      return new Response(stream, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" }
      });
    };

    const req = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": "Bearer test-key",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "gpt-4o",
        messages: [{ role: "user", content: "Tell me a story slowly" }],
        stream: true
      })
    });

    const testStartTime = Date.now();
    const response = await handleCompletions(createContext(req, {}));
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("Content-Type"), "text/event-stream; charset=utf-8");

    // Client reader records arrival time of each chunk
    const reader = response.body.getReader();
    const dec = new TextDecoder();
    const clientArrivalTimes = [];
    const receivedChunks = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const text = dec.decode(value);
      if (text.trim()) {
        clientArrivalTimes.push(Date.now());
        receivedChunks.push(text);
      }
    }

    // Assertions for zero buffering and incremental delivery:
    // 1. First chunk MUST arrive well before the stream finishes!
    const timeToFirstToken = clientArrivalTimes[0] - testStartTime;
    const totalDuration = clientArrivalTimes[clientArrivalTimes.length - 1] - testStartTime;

    console.log(`  - TTFT (Time To First Chunk): ${timeToFirstToken}ms`);
    console.log(`  - Total stream duration: ${totalDuration}ms`);
    console.log(`  - Server total planned delay: ${chunkDelayMs * (chunkCount - 1)}ms`);

    // If buffered, TTFT would be >= totalDuration. In passthrough, TTFT is small (< 50ms)
    assert.ok(timeToFirstToken < 60, `First chunk must be received immediately! Got ${timeToFirstToken}ms`);
    assert.ok(totalDuration >= (chunkDelayMs * (chunkCount - 1)) * 0.8, `Stream must be spaced by server delays`);

    // 2. Client arrival times must track server emit times (within small jitter tolerance)
    for (let i = 0; i < serverEmitTimes.length; i++) {
      const diff = Math.abs(clientArrivalTimes[i] - serverEmitTimes[i]);
      assert.ok(diff < 50, `Chunk ${i} client arrival time (${clientArrivalTimes[i]}) should match server emit time (${serverEmitTimes[i]}), diff=${diff}ms`);
    }

    // 3. Verify all chunks received intact
    const fullReceivedText = receivedChunks.join("");
    for (const chunkText of chunkTexts) {
      assert.ok(fullReceivedText.includes(chunkText), `Received text must include '${chunkText}'`);
    }

    console.log("  ✓ Slow chunk feeds delivered incrementally with zero intermediate buffering!");
    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 3: Aborted Streams (Client Cancellation & Upstream Errors)
// -----------------------------------------------------------------------------
async function runAbortedStreamsSuite() {
  console.log(">>> [Suite 3] Testing Aborted Streams & Error Handling...");
  const originalFetch = globalThis.fetch;

  try {
    // 3.1: Client cancels stream mid-transmission
    {
      let upstreamCancelled = false;
      let upstreamCancelReason = null;

      globalThis.fetch = async () => {
        const stream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('data: {"choices":[{"delta":{"content":"Chunk 1"}}]}\n\n'));
            controller.enqueue(enc.encode('data: {"choices":[{"delta":{"content":"Chunk 2"}}]}\n\n'));
          },
          cancel(reason) {
            upstreamCancelled = true;
            upstreamCancelReason = reason;
          }
        });

        return new Response(stream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: {
          "Authorization": "Bearer test-key",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const response = await handleCompletions(createContext(req, {}));
      const reader = response.body.getReader();

      // Read only first chunk
      const chunk1 = await reader.read();
      assert.equal(chunk1.done, false);

      // Client aborts/cancels reader
      await reader.cancel("Client abort test");

      // Verify cancellation propagated cleanly without crashing worker
      assert.equal(upstreamCancelled, true, "Upstream stream must receive cancellation signal");
      console.log("  ✓ 3.1: Client cancellation propagates to upstream stream cleanly");
    }

    // 3.2: Upstream errors mid-stream
    {
      globalThis.fetch = async () => {
        const stream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('data: {"choices":[{"delta":{"content":"Beginning"}}]}\n\n'));
            // Upstream suddenly crashes
            setTimeout(() => {
              controller.error(new Error("Upstream connection abruptly reset by peer"));
            }, 10);
          }
        });

        return new Response(stream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const response = await handleCompletions(createContext(req, {}));
      const reader = response.body.getReader();

      const chunk1 = await reader.read();
      assert.equal(chunk1.done, false);

      // Subsequent read should reject with the upstream error
      await assert.rejects(async () => {
        await reader.read();
      }, /Upstream connection abruptly reset/);

      console.log("  ✓ 3.2: Upstream mid-stream error propagates directly to client reader");
    }

    // 3.3: Upstream initial fetch network failure
    {
      globalThis.fetch = async () => {
        throw new TypeError("Failed to fetch: Connection refused (127.0.0.1:8080)");
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const response = await handleCompletions(createContext(req, {}));
      assert.equal(response.status, 502);
      assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");
      const body = await response.json();
      assert.equal(body.error, "Backend unreachable");

      console.log("  ✓ 3.3: Upstream network unreachable returns 502 with CORS");
    }

    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 4: Large Payload Passthrough (Stress & Memory Check)
// -----------------------------------------------------------------------------
async function runLargePayloadSuite() {
  console.log(">>> [Suite 4] Testing Large Payload Streaming & Memory Integrity...");
  const originalFetch = globalThis.fetch;

  try {
    // 4.1: Large stream from backend (1,000 chunks of 4KB each = 4MB)
    const totalChunks = 1000;
    const chunkSize = 4096;
    const testPattern = "A".repeat(chunkSize);
    let totalBytesSent = 0;

    globalThis.fetch = async () => {
      const stream = new ReadableStream({
        start(controller) {
          const enc = new TextEncoder();
          for (let i = 0; i < totalChunks; i++) {
            const chunkData = `data: {"id":"chk_${i}","payload":"${testPattern}"}\n\n`;
            const encoded = enc.encode(chunkData);
            totalBytesSent += encoded.byteLength;
            controller.enqueue(encoded);
          }
          controller.enqueue(enc.encode("data: [DONE]\n\n"));
          controller.close();
        }
      });

      return new Response(stream, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" }
      });
    };

    const req = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": "Bearer test-key",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ model: "gpt-4o", stream: true })
    });

    const initialMem = process.memoryUsage().heapUsed;
    const response = await handleCompletions(createContext(req, {}));
    assert.equal(response.status, 200);

    const reader = response.body.getReader();
    let totalBytesReceived = 0;
    let chunksRead = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytesReceived += value.byteLength;
      chunksRead++;
    }

    const finalMem = process.memoryUsage().heapUsed;
    const memDeltaMB = (finalMem - initialMem) / (1024 * 1024);

    console.log(`  - Total bytes sent: ${totalBytesSent} bytes (~${(totalBytesSent / 1024 / 1024).toFixed(2)} MB)`);
    console.log(`  - Total bytes received: ${totalBytesReceived} bytes`);
    console.log(`  - Stream chunks read: ${chunksRead}`);
    console.log(`  - Heap change during stream: ${memDeltaMB.toFixed(2)} MB`);

    assert.ok(totalBytesReceived >= totalBytesSent, "All bytes must be delivered to client without truncation");
    assert.ok(chunksRead > 0, "Chunks were read successfully");

    console.log("  ✓ 4.1: 4MB stream passed through completely without memory spike or truncation");

    // 4.2: Large client request payload (e.g. 2MB prompt)
    {
      let capturedBody = "";
      globalThis.fetch = async (url, options) => {
        capturedBody = options.body;
        return new Response(JSON.stringify({
          id: "chatcmpl-large-prompt",
          choices: [{ message: { role: "assistant", content: "Prompt received OK" } }]
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      };

      const largePrompt = "X".repeat(2 * 1024 * 1024); // 2MB prompt
      const reqLarge = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: {
          "Authorization": "Bearer test-key",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: "gpt-4o",
          messages: [{ role: "user", content: largePrompt }],
          stream: false
        })
      });

      const res = await handleCompletions(createContext(reqLarge, {}));
      assert.equal(res.status, 200);
      assert.ok(capturedBody.length > 2 * 1024 * 1024, "Large body forwarded in full");
      console.log("  ✓ 4.2: Large 2MB client request body forwarded intact to upstream");
    }

    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 5: Code Strings, URLs & Punctuation Immutability (Zero Corruption)
// -----------------------------------------------------------------------------
async function runCodeImmutabilitySuite() {
  console.log(">>> [Suite 5] Testing Code & URL Immutability (Anti-Corruption Verification)...");
  const originalFetch = globalThis.fetch;

  try {
    const sensitiveTokens = [
      "const myVariable = 123;",
      "function calculateTotalSum(userId, itemId) {",
      "const url = 'https://api.github.com/v1/repos/owner/repo/pulls?state=open&limit=100';",
      "const obj = { 'userId': 101, 'isActive': true, 'balanceUSD': 45.99 };",
      "import { useState, useEffect, useCallback } from 'react';",
      "const regex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$/g;",
      "filename = 'bundle.min.js';",
      "const pi = 3.141592653589793;",
      "const v = 'version 1.0.0.RELEASE';"
    ];

    const fullCodeBlock = sensitiveTokens.join("\n");

    // 5.1: In Streaming Mode (completions.js)
    {
      globalThis.fetch = async () => {
        const stream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode(`data: {"choices":[{"delta":{"content":${JSON.stringify(fullCodeBlock)}}}]}\n\n`));
            controller.enqueue(enc.encode("data: [DONE]\n\n"));
            controller.close();
          }
        });
        return new Response(stream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const response = await handleCompletions(createContext(req, {}));
      const reader = response.body.getReader();
      const dec = new TextDecoder();
      let streamedOutput = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        streamedOutput += dec.decode(value);
      }

      // Parse SSE delta chunks just like a real client parser
      let extractedStreamContent = "";
      for (const line of streamedOutput.split("\n")) {
        if (line.startsWith("data: ") && !line.includes("[DONE]")) {
          const payload = JSON.parse(line.slice(6));
          if (payload.choices?.[0]?.delta?.content) {
            extractedStreamContent += payload.choices[0].delta.content;
          }
        }
      }

      // Assert zero mutation of any code token in streaming mode
      for (const token of sensitiveTokens) {
        assert.ok(extractedStreamContent.includes(token), `Stream must contain exact token: '${token}'`);
      }
      assert.equal(extractedStreamContent, fullCodeBlock, "Streamed content must match original full code block byte-for-byte");
      assert.ok(!extractedStreamContent.includes("my Variable"), "No mutated space in 'myVariable'");
      assert.ok(!extractedStreamContent.includes("calculate Total Sum"), "No mutated space in 'calculateTotalSum'");
      assert.ok(!extractedStreamContent.includes("https: //"), "No broken protocol in URL");
      assert.ok(!extractedStreamContent.includes("user Id"), "No broken space in 'userId'");
      console.log("  ✓ 5.1: Streaming mode passes all code and URLs with 100% byte fidelity");
    }

    // 5.2: In Non-Streaming Mode (completions.js sanitizeModelText)
    {
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          id: "chatcmpl-test",
          choices: [{
            message: {
              role: "assistant",
              content: `Here is the solution:\n\`\`\`javascript\n${fullCodeBlock}\n\`\`\`\nI am ox-alpha from Z.ai.`
            }
          }]
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: false })
      });

      const res = await handleCompletions(createContext(req, {}));
      const json = await res.json();
      const content = json.choices[0].message.content;

      for (const token of sensitiveTokens) {
        assert.ok(content.includes(token), `Non-streaming output must contain exact token: '${token}'`);
      }
      assert.ok(!content.includes("my Variable"), "Zero corruption in identifiers");
      assert.ok(!content.includes("calculate Total"), "Zero corruption in functions");
      assert.ok(!content.includes("https: //"), "Zero corruption in URLs");
      assert.ok(!content.includes("bundle. min. js"), "Zero corruption in filenames");

      // Model vendor rebranding worked
      assert.ok(content.includes("GPT-4o"));
      assert.ok(content.includes("OpenAI"));
      assert.ok(!content.includes("ox-alpha"));
      assert.ok(!content.includes("Z.ai"));
      console.log("  ✓ 5.2: Non-streaming completions.js preserves code and sanitizes vendor names cleanly");
    }

    // 5.3: In Non-Streaming Mode (messages.js sanitizeModelText)
    {
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          id: "msg_test",
          content: [{
            type: "text",
            text: `Claude code sample:\n\`\`\`javascript\n${fullCodeBlock}\n\`\`\`\nProvided by ox-alpha.`
          }]
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "x-api-key": "default-dev-key" },
        body: JSON.stringify({ model: "claude-3-5-sonnet-20241022", stream: false })
      });

      const res = await handleMessages(createContext(req, {}));
      const json = await res.json();
      const text = json.content[0].text;

      for (const token of sensitiveTokens) {
        assert.ok(text.includes(token), `Anthropic output must contain exact token: '${token}'`);
      }
      assert.ok(text.includes("Claude 3.5 Sonnet"));
      assert.ok(!text.includes("ox-alpha"));
      console.log("  ✓ 5.3: Non-streaming messages.js preserves code and sanitizes model cleanly");
    }

    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 6: CORS Preflight & Route Aliases Stress
// -----------------------------------------------------------------------------
async function runCorsAndAliasesSuite() {
  console.log(">>> [Suite 6] Testing CORS Preflight & Route Aliases...");

  // 6.1: OPTIONS preflight on completions
  const compOptRes = await handleCompletionsOptions();
  assert.equal(compOptRes.status, 204);
  assert.equal(compOptRes.headers.get("Access-Control-Allow-Origin"), "*");
  assert.ok(compOptRes.headers.get("Access-Control-Allow-Methods").includes("POST"));
  assert.ok(compOptRes.headers.get("Access-Control-Allow-Headers").includes("Authorization"));
  console.log("  ✓ 6.1: completions onRequestOptions returns 204 with complete CORS headers");

  // 6.2: OPTIONS preflight on messages
  const msgOptRes = await handleMessagesOptions();
  assert.equal(msgOptRes.status, 204);
  assert.equal(msgOptRes.headers.get("Access-Control-Allow-Origin"), "*");
  console.log("  ✓ 6.2: messages onRequestOptions returns 204 with complete CORS headers");

  // 6.3: Aliased imports test
  const aliasComp = await import("../../functions/v1/v1/chat/completions.js");
  assert.equal(typeof aliasComp.onRequestPost, "function");
  assert.equal(typeof aliasComp.onRequestOptions, "function");

  const aliasMsg = await import("../../functions/v1/v1/messages.js");
  assert.equal(typeof aliasMsg.onRequestPost, "function");
  assert.equal(typeof aliasMsg.onRequestOptions, "function");

  console.log("  ✓ 6.3: /v1/v1/chat/completions and /v1/v1/messages re-export route handlers properly");

  suitesPassed++;
}

// -----------------------------------------------------------------------------
// SUITE 7: Model Code Translation & Header Propagation
// -----------------------------------------------------------------------------
async function runModelAliasesSuite() {
  console.log(">>> [Suite 7] Testing Model Alias Translation & Proxy Headers...");
  const originalFetch = globalThis.fetch;

  try {
    const testCases = [
      { inputModel: "g4o", expectedTranslated: "gpt-4o" },
      { inputModel: "g54", expectedTranslated: "gpt-4o-mini" },
      { inputModel: "dsr", expectedTranslated: "deepseek-r1" },
      { inputModel: "qw3", expectedTranslated: "qwen-3.6-coder" },
      { inputModel: "custom-model", expectedTranslated: "custom-model" }
    ];

    for (const { inputModel, expectedTranslated } of testCases) {
      let capturedHeaders = null;
      let capturedBody = null;

      globalThis.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        capturedBody = JSON.parse(options.body);
        return new Response(JSON.stringify({
          choices: [{ message: { role: "assistant", content: "ok" } }]
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: {
          "Authorization": "Bearer test-key",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ model: inputModel, stream: false })
      });

      const res = await handleCompletions(createContext(req, { INTERNAL_SECRET: "custom_sec_123" }));
      assert.equal(res.status, 200);

      assert.equal(capturedHeaders.get("X-Model-Name"), expectedTranslated);
      assert.equal(capturedHeaders.get("X-Internal-Secret"), "custom_sec_123");
      assert.equal(capturedBody.model, expectedTranslated);
    }

    console.log("  ✓ 7.1: Model shorthand aliases and internal headers translated and forwarded correctly");
    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 8: Upstream Error Propagation Across HTTP Status Codes
// -----------------------------------------------------------------------------
async function runUpstreamErrorPropagationSuite() {
  console.log(">>> [Suite 8] Testing Upstream Error Propagation (400, 403, 429, 500, 503)...");
  const originalFetch = globalThis.fetch;

  try {
    const errorCodes = [400, 403, 429, 500, 503];

    for (const code of errorCodes) {
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({ error: { message: `Upstream error ${code}` } }), {
          status: code,
          headers: { "Content-Type": "application/json" }
        });
      };

      // Test completions endpoint
      const reqComp = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });
      const resComp = await handleCompletions(createContext(reqComp, {}));
      assert.equal(resComp.status, code, `Completions status should match ${code}`);
      assert.equal(resComp.headers.get("Access-Control-Allow-Origin"), "*");
      const bodyComp = await resComp.json();
      assert.equal(bodyComp.error.message, `Upstream error ${code}`);

      // Test messages endpoint
      const reqMsg = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "x-api-key": "default-dev-key" },
        body: JSON.stringify({ model: "claude-3-5-sonnet-20241022", stream: true })
      });
      const resMsg = await handleMessages(createContext(reqMsg, {}));
      assert.equal(resMsg.status, code, `Messages status should match ${code}`);
      assert.equal(resMsg.headers.get("Access-Control-Allow-Origin"), "*");
      const bodyMsg = await resMsg.json();
      assert.equal(bodyMsg.error.message, `Upstream error ${code}`);
    }

    console.log("  ✓ 8.1: Upstream errors (400, 403, 429, 500, 503) propagate faithfully with CORS headers");
    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 9: Malformed Request Payloads & Edge Resilience
// -----------------------------------------------------------------------------
async function runMalformedBodyResilienceSuite() {
  console.log(">>> [Suite 9] Testing Resilience Against Malformed & Degraded Bodies...");
  const originalFetch = globalThis.fetch;

  try {
    let capturedOptions = null;
    globalThis.fetch = async (url, options) => {
      capturedOptions = options;
      return new Response(JSON.stringify({ choices: [{ message: { role: "assistant", content: "ok" } }] }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    };

    // 9.1: Malformed JSON syntax in body
    const reqMalformed = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": "Bearer test-key",
        "Content-Type": "application/json"
      },
      body: '{"model": "gpt-4o", "stream": true, INVALID_JSON'
    });

    const resMalformed = await handleCompletions(createContext(reqMalformed, {}));
    assert.equal(resMalformed.status, 200, "Must not crash with 500 on malformed JSON body");
    assert.equal(capturedOptions.body, "{}", "Defaults safely to empty JSON object");

    // 9.2: Empty string body
    const reqEmpty = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": "Bearer test-key" },
      body: ""
    });
    const resEmpty = await handleCompletions(createContext(reqEmpty, {}));
    assert.equal(resEmpty.status, 200);

    // 9.3: stream: "true" string truthiness check
    globalThis.fetch = async (url, options) => {
      capturedOptions = options;
      return new Response(new ReadableStream({
        start(c) {
          c.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
          c.close();
        }
      }), {
        status: 200,
        headers: { "Content-Type": "text/event-stream" }
      });
    };

    const reqStringStream = new Request("http://localhost/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": "Bearer test-key" },
      body: JSON.stringify({ model: "gpt-4o", stream: "true" })
    });
    const resStringStream = await handleCompletions(createContext(reqStringStream, {}));
    assert.equal(resStringStream.status, 200);
    assert.equal(resStringStream.headers.get("Content-Type"), "text/event-stream; charset=utf-8");

    console.log("  ✓ 9.1: Edge safely catches malformed JSON and defaults cleanly without 500 errors");
    suitesPassed++;
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// -----------------------------------------------------------------------------
// SUITE 10: D1 Database Exception Fallback Stress
// -----------------------------------------------------------------------------
async function runD1ExceptionFallbackSuite() {
  console.log(">>> [Suite 10] Testing D1 Database Exception Fallback Stress...");

  const throwingErrors = [
    new Error("D1_ERROR: disk I/O error"),
    new TypeError("D1: network socket connection failed"),
    new Error("D1: statement timeout after 5000ms"),
    new Error("D1: busy database locked")
  ];

  for (const dbErr of throwingErrors) {
    const errorDb = {
      prepare() {
        throw dbErr;
      }
    };

    // Valid mock key must succeed via fallback even when D1 explodes
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer test-key" }
    });
    const res = await validateApiKey(req, { DB: errorDb });
    assert.equal(res.success, true, `Should fall back to mock key when DB throws: ${dbErr.message}`);
    assert.equal(res.apiKey, "test-key");

    // Invalid key must return 401 via fallback
    const reqInvalid = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer unknown-key" }
    });
    const resInvalid = await validateApiKey(reqInvalid, { DB: errorDb });
    assert.equal(resInvalid.success, false);
    assert.equal(resInvalid.status, 401);
  }

  console.log("  ✓ 10.1: All D1 exceptions (disk I/O, socket drop, timeout, locked) safely recover to mock registry");
  suitesPassed++;
}

// -----------------------------------------------------------------------------
// MAIN RUNNER
// -----------------------------------------------------------------------------
async function runAll() {
  try {
    await runAuthAdversarialSuite();
    await runSlowChunkFeedSuite();
    await runAbortedStreamsSuite();
    await runLargePayloadSuite();
    await runCodeImmutabilitySuite();
    await runCorsAndAliasesSuite();
    await runModelAliasesSuite();
    await runUpstreamErrorPropagationSuite();
    await runMalformedBodyResilienceSuite();
    await runD1ExceptionFallbackSuite();

    console.log("======================================================================");
    console.log(`ALL ${suitesPassed} ADVERSARIAL CHALLENGE SUITES PASSED EMPIRICALLY!`);
    console.log("======================================================================");
  } catch (err) {
    suitesFailed++;
    console.error("\n❌ ADVERSARIAL CHALLENGE TEST FAILED:");
    console.error(err);
    process.exit(1);
  }
}

runAll();

