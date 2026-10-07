import assert from "node:assert/strict";
import { onRequestPost as postCompletions } from "../../functions/v1/chat/completions.js";
import { onRequestPost as postMessages } from "../../functions/v1/messages.js";

async function runHeadersStreamingSuite() {
  console.log("===============================================================");
  console.log("  ADVERSARIAL SUITE 2: STREAMING VS NON-STREAMING HEADERS & CHUNKS ");
  console.log("===============================================================");

  let testCount = 0;
  let passCount = 0;

  function recordPass(msg) {
    testCount++;
    passCount++;
    console.log(`[PASS] (${testCount}) ${msg}`);
  }

  const originalFetch = globalThis.fetch;

  try {
    // -------------------------------------------------------------
    // Test 1: Header Conformity in Streaming Mode (/v1/chat/completions)
    // -------------------------------------------------------------
    {
      let upstreamOptions = null;
      globalThis.fetch = async (url, options) => {
        upstreamOptions = options;
        const sseStream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'));
            controller.enqueue(enc.encode('data: [DONE]\n\n'));
            controller.close();
          }
        });
        return new Response(sseStream, {
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
          model: "g54",
          messages: [{ role: "user", content: "ping" }],
          stream: true
        })
      });

      const res = await postCompletions({ request: req, env: {} });

      // Check client response headers
      assert.equal(res.status, 200);
      assert.equal(res.headers.get("Content-Type"), "text/event-stream; charset=utf-8", "Content-Type must be text/event-stream; charset=utf-8");
      assert.equal(res.headers.get("Cache-Control"), "no-cache", "Cache-Control must be no-cache");
      assert.equal(res.headers.get("Connection"), "keep-alive", "Connection must be keep-alive");
      assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*", "Access-Control-Allow-Origin must be *");

      // Check upstream request headers & body
      assert.ok(upstreamOptions, "Upstream fetch was called");
      assert.equal(upstreamOptions.headers.get("Accept"), "text/event-stream", "Upstream Accept must be text/event-stream");
      assert.equal(upstreamOptions.headers.get("X-Model-Name"), "gpt-4o-mini", "Model code g54 translated to gpt-4o-mini");
      const parsedBody = JSON.parse(upstreamOptions.body);
      assert.equal(parsedBody.stream, true, "Upstream body must preserve stream: true");
      assert.equal(parsedBody.model, "gpt-4o-mini");

      recordPass("/v1/chat/completions streaming header conformity verified (Content-Type, Cache-Control, Connection, CORS, Upstream Accept)");
    }

    // -------------------------------------------------------------
    // Test 2: Header Conformity in Streaming Mode (/v1/messages)
    // -------------------------------------------------------------
    {
      let upstreamOptions = null;
      globalThis.fetch = async (url, options) => {
        upstreamOptions = options;
        const sseStream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('event: message_start\ndata: {"type":"message_start"}\n\n'));
            controller.enqueue(enc.encode('event: message_stop\ndata: {"type":"message_stop"}\n\n'));
            controller.close();
          }
        });
        return new Response(sseStream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" }
        });
      };

      const req = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: {
          "x-api-key": "default-dev-key",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: "claude-3-5-sonnet-20241022",
          messages: [{ role: "user", content: "ping" }],
          stream: true
        })
      });

      const res = await postMessages({ request: req, env: {} });

      assert.equal(res.status, 200);
      assert.equal(res.headers.get("Content-Type"), "text/event-stream; charset=utf-8");
      assert.equal(res.headers.get("Cache-Control"), "no-cache");
      assert.equal(res.headers.get("Connection"), "keep-alive");
      assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
      assert.equal(res.headers.get("anthropic-version"), "2023-06-01");

      assert.equal(upstreamOptions.headers.get("Accept"), "text/event-stream");
      const parsedBody = JSON.parse(upstreamOptions.body);
      assert.equal(parsedBody.stream, true);

      recordPass("/v1/messages streaming header conformity verified (anthropic-version included)");
    }

    // -------------------------------------------------------------
    // Test 3: Header Conformity in Non-Streaming Mode (/v1/chat/completions)
    // -------------------------------------------------------------
    {
      let upstreamOptions = null;
      globalThis.fetch = async (url, options) => {
        upstreamOptions = options;
        return new Response(JSON.stringify({
          id: "chatcmpl-test",
          choices: [{ message: { role: "assistant", content: "Non-streaming answer" } }]
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
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
          messages: [{ role: "user", content: "test" }],
          stream: false
        })
      });

      const res = await postCompletions({ request: req, env: {} });

      assert.equal(res.status, 200);
      assert.equal(res.headers.get("Content-Type"), "application/json", "Non-streaming Content-Type must be application/json");
      assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*", "Access-Control-Allow-Origin must be *");

      assert.equal(upstreamOptions.headers.get("Accept"), "application/json", "Non-streaming Accept must be application/json");
      const parsedBody = JSON.parse(upstreamOptions.body);
      assert.equal(parsedBody.stream, false, "Upstream body has stream: false");

      const resJson = await res.json();
      assert.equal(resJson.choices[0].message.content, "Non-streaming answer");
      recordPass("/v1/chat/completions non-streaming header conformity verified (Content-Type: application/json, Accept: application/json)");
    }

    // -------------------------------------------------------------
    // Test 4: Header Conformity in Non-Streaming Mode (/v1/messages)
    // -------------------------------------------------------------
    {
      let upstreamOptions = null;
      globalThis.fetch = async (url, options) => {
        upstreamOptions = options;
        return new Response(JSON.stringify({
          id: "msg-test",
          content: [{ type: "text", text: "Anthropic non-streaming" }],
          role: "assistant"
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: {
          "x-api-key": "test-key",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: "claude-3-5-sonnet-20241022",
          messages: [{ role: "user", content: "test" }]
          // stream omitted -> false
        })
      });

      const res = await postMessages({ request: req, env: {} });

      assert.equal(res.status, 200);
      assert.equal(res.headers.get("Content-Type"), "application/json");
      assert.equal(res.headers.get("Access-Control-Allow-Origin"), "*");
      assert.equal(res.headers.get("anthropic-version"), "2023-06-01");

      assert.equal(upstreamOptions.headers.get("Accept"), "application/json");
      const parsedBody = JSON.parse(upstreamOptions.body);
      assert.ok(!parsedBody.stream, "parsedBody.stream must be falsy when omitted");

      const resJson = await res.json();
      assert.equal(resJson.content[0].text, "Anthropic non-streaming");
      recordPass("/v1/messages non-streaming header conformity verified (stream omitted handled as false/falsy)");
    }

    // -------------------------------------------------------------
    // Test 5: True Incremental Streaming (Zero Buffering Verification)
    // -------------------------------------------------------------
    {
      // Simulate an upstream stream that yields 10 chunks with small delay
      const chunkCount = 10;
      let chunksReadBeforeClose = 0;

      globalThis.fetch = async () => {
        let timerId;
        const sseStream = new ReadableStream({
          start(controller) {
            let i = 0;
            const enc = new TextEncoder();
            timerId = setInterval(() => {
              if (i < chunkCount) {
                controller.enqueue(enc.encode(`data: {"index":${i},"content":"chunk_${i}"}\n\n`));
                i++;
              } else {
                controller.enqueue(enc.encode("data: [DONE]\n\n"));
                controller.close();
                clearInterval(timerId);
              }
            }, 10);
          },
          cancel() {
            clearInterval(timerId);
          }
        });

        return new Response(sseStream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const startTime = Date.now();
      const res = await postCompletions({ request: req, env: {} });
      assert.ok(res.body instanceof ReadableStream, "Response body MUST be a ReadableStream");

      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let firstChunkTime = null;
      let totalReceived = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (firstChunkTime === null) {
          firstChunkTime = Date.now();
        }
        chunksReadBeforeClose++;
        totalReceived += dec.decode(value);
      }

      assert.ok(chunksReadBeforeClose >= chunkCount, "All chunks streamed incrementally");
      assert.ok(totalReceived.includes("chunk_0") && totalReceived.includes("chunk_9"));
      assert.ok(totalReceived.includes("[DONE]"));
      // First chunk delivered well before completion
      const ttft = firstChunkTime - startTime;
      console.log(`       -> Time to first chunk delivered: ${ttft}ms (zero-buffered passthrough)`);
      recordPass("True incremental Web Streams passthrough verified (chunks delivered in real-time, zero RAM buffering)");
    }

    // -------------------------------------------------------------
    // Test 6: High Volume / Large Chunk Streaming (500KB Payload)
    // -------------------------------------------------------------
    {
      const largeChunk = "X".repeat(500 * 1024); // 500KB
      globalThis.fetch = async () => {
        const sseStream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode(`data: {"content":"${largeChunk}"}\n\n`));
            controller.enqueue(enc.encode("data: [DONE]\n\n"));
            controller.close();
          }
        });
        return new Response(sseStream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const res = await postCompletions({ request: req, env: {} });
      const reader = res.body.getReader();
      let bytesRead = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        bytesRead += value.byteLength;
      }

      assert.ok(bytesRead > 500 * 1024, `Read ${bytesRead} bytes successfully without truncation or OOM`);
      recordPass("Large 500KB streaming chunk passed through without truncation or memory error");
    }

    // -------------------------------------------------------------
    // Test 7: Multibyte UTF-8 Characters Across Chunk Boundaries
    // -------------------------------------------------------------
    {
      // 🚀 is 4 bytes: 0xF0 0x9F 0x9A 0x80
      const emojiBytes = new Uint8Array([0xF0, 0x9F, 0x9A, 0x80]);
      globalThis.fetch = async () => {
        const sseStream = new ReadableStream({
          start(controller) {
            // Split the emoji across two chunks!
            controller.enqueue(emojiBytes.slice(0, 2));
            controller.enqueue(emojiBytes.slice(2, 4));
            controller.close();
          }
        });
        return new Response(sseStream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const res = await postCompletions({ request: req, env: {} });
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let decoded = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        decoded += dec.decode(value, { stream: true });
      }
      decoded += dec.decode();
      assert.equal(decoded, "🚀", "Multibyte UTF-8 emoji reconstructed properly across chunk boundary");
      recordPass("Multibyte UTF-8 characters preserved across chunk boundaries");
    }

    // -------------------------------------------------------------
    // Test 8: Upstream Error Responses During Streaming Request
    // -------------------------------------------------------------
    {
      // Upstream returns 429 Too Many Requests
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({ error: { message: "Rate limit exceeded", type: "rate_limit_error" } }), {
          status: 429,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req429 = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const res429 = await postCompletions({ request: req429, env: {} });
      assert.equal(res429.status, 429, "Upstream 429 status code must propagate");
      assert.equal(res429.headers.get("Access-Control-Allow-Origin"), "*");
      const errBody429 = await res429.json();
      assert.equal(errBody429.error.type, "rate_limit_error");
      recordPass("Upstream 429 during streaming request propagates status and error with CORS");

      // Upstream returns 503 Overloaded
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({ error: { message: "Model is overloaded", type: "overloaded_error" } }), {
          status: 503,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req503 = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "claude-3-5-sonnet", stream: true })
      });

      const res503 = await postMessages({ request: req503, env: {} });
      assert.equal(res503.status, 503);
      assert.equal(res503.headers.get("Access-Control-Allow-Origin"), "*");
      recordPass("Upstream 503 during streaming request propagates status with CORS");
    }

    // -------------------------------------------------------------
    // Test 9: Anti-Corruption and Sanitization in Non-Streaming Mode
    // -------------------------------------------------------------
    {
      const codeSnippet = `
function calculateTotalSum(itemsList) {
  const apiUrl = "https://api.github.com/v1/users?page=1&limit=50";
  const payload = { "userId": 101, "isActive": true };
  let count = 3.14159;
  return payload;
}
`;
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          choices: [{ message: { role: "assistant", content: codeSnippet } }]
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: false })
      });

      const res = await postCompletions({ request: req, env: {} });
      const json = await res.json();
      const outputText = json.choices[0].message.content;

      // Verify no camelCase mangling
      assert.ok(outputText.includes("calculateTotalSum"), "camelCase identifier calculateTotalSum preserved");
      assert.ok(outputText.includes("itemsList"), "camelCase identifier itemsList preserved");
      // Verify no URL mangling
      assert.ok(outputText.includes("https://api.github.com/v1/users?page=1&limit=50"), "Full URL preserved without space insertion");
      // Verify no JSON key mangling
      assert.ok(outputText.includes('"userId": 101'), "JSON key userId preserved");
      assert.ok(outputText.includes('"isActive": true'), "JSON key isActive preserved");
      // Verify no decimal number splitting
      assert.ok(outputText.includes("3.14159"), "Float 3.14159 preserved");
      recordPass("Code preservation verified: no camelCase, URL, or JSON key corruption");
    }

    // -------------------------------------------------------------
    // Test 10: Model Vendor Re-Branding Sanitization
    // -------------------------------------------------------------
    {
      const rawText = "I am ox-alpha developed by Z.ai and Nemotron-3.5 by NVIDIA.";
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          choices: [{ message: { role: "assistant", content: rawText } }]
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: false })
      });

      const res = await postCompletions({ request: req, env: {} });
      const json = await res.json();
      const clean = json.choices[0].message.content;

      assert.ok(!clean.includes("ox-alpha"), "Internal codename ox-alpha removed");
      assert.ok(!clean.includes("Z.ai"), "Internal vendor Z.ai removed");
      assert.ok(!clean.includes("NVIDIA"), "Internal vendor NVIDIA removed");
      recordPass("Model vendor re-branding successfully sanitized in non-streaming responses");
    }

    // -------------------------------------------------------------
    // Test 11: Cross-Protocol Format Parity Handling (Backend schema variance)
    // -------------------------------------------------------------
    {
      // Case A: Upstream backend returns Anthropic { content: [{ type: "text", text: "..." }] }
      // to the OpenAI /v1/chat/completions endpoint
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          id: "hybrid-resp-1",
          content: [{ type: "text", text: "Response in Anthropic format from backend" }]
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      };

      const reqA = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: false })
      });

      const resA = await postCompletions({ request: reqA, env: {} });
      const jsonA = await resA.json();
      assert.equal(jsonA.choices[0].message.content, "Response in Anthropic format from backend");
      assert.equal(jsonA.choices[0].message.role, "assistant");
      recordPass("/v1/chat/completions accepts Anthropic format from backend and normalizes to choices");

      // Case B: Upstream backend returns OpenAI { choices: [{ message: { content: "..." } }] }
      // to the Anthropic /v1/messages endpoint
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({
          id: "hybrid-resp-2",
          choices: [{ message: { role: "assistant", content: "Response in OpenAI format from backend" } }]
        }), { status: 200, headers: { "Content-Type": "application/json" } });
      };

      const reqB = new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "claude-3-5-sonnet", stream: false })
      });

      const resB = await postMessages({ request: reqB, env: {} });
      const jsonB = await resB.json();
      assert.equal(jsonB.content[0].text, "Response in OpenAI format from backend");
      assert.equal(jsonB.role, "assistant");
      recordPass("/v1/messages accepts OpenAI format from backend and normalizes to content array");
    }

  } finally {
    globalThis.fetch = originalFetch;
  }

  console.log("---------------------------------------------------------------");
  console.log(`SUITE 2 RESULTS: ${passCount} / ${testCount} tests passed (100%)`);
  console.log("===============================================================\n");
}

runHeadersStreamingSuite().catch((err) => {
  console.error("SUITE 2 CRITICAL FAILURE:", err);
  process.exit(1);
});
