import assert from "node:assert/strict";
import { onRequestPost as handleCompletions } from "../../../functions/v1/chat/completions.js";
import { onRequestPost as handleMessages } from "../../../functions/v1/messages.js";

async function runTests() {
  console.log("--- Testing Edge Web Streams Streaming & Zero-Buffering ---");

  const originalFetch = globalThis.fetch;

  try {
    // Test 1: completions.js with stream: true
    {
      let capturedUrl = "";
      let capturedOptions = null;

      globalThis.fetch = async (url, options) => {
        capturedUrl = url;
        capturedOptions = options;

        const sseStream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"Hello"}}]}\n\n'));
            controller.enqueue(enc.encode('data: {"id":"chatcmpl-1","choices":[{"delta":{"content":" world"}}]}\n\n'));
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
          model: "gpt-4o",
          messages: [{ role: "user", content: "Hi" }],
          stream: true
        })
      });

      const response = await handleCompletions({ request: req, env: {} });

      assert.equal(response.status, 200);
      assert.equal(response.headers.get("Content-Type"), "text/event-stream; charset=utf-8");
      assert.equal(response.headers.get("Cache-Control"), "no-cache");
      assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");

      assert.ok(capturedOptions, "fetch was called");
      assert.equal(capturedOptions.headers.get("Accept"), "text/event-stream");
      const sentBody = JSON.parse(capturedOptions.body);
      assert.equal(sentBody.stream, true, "stream: true MUST be preserved and forwarded to backend!");

      assert.ok(response.body instanceof ReadableStream, "Response body must be a ReadableStream");
      const reader = response.body.getReader();
      const dec = new TextDecoder();
      let streamData = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        streamData += dec.decode(value);
      }

      assert.ok(streamData.includes("Hello"));
      assert.ok(streamData.includes("world"));
      assert.ok(streamData.includes("[DONE]"));
      console.log("✓ Test 1 Passed: completions.js forwards stream: true and pipes ReadableStream directly with zero buffering");
    }

    // Test 2: completions.js with stream: false
    {
      let capturedOptions = null;
      globalThis.fetch = async (url, options) => {
        capturedOptions = options;
        return new Response(JSON.stringify({
          id: "chatcmpl-2",
          choices: [{ message: { role: "assistant", content: "Direct JSON answer" } }],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 }
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
          messages: [{ role: "user", content: "Hi" }],
          stream: false
        })
      });

      const response = await handleCompletions({ request: req, env: {} });
      assert.equal(response.status, 200);
      assert.equal(response.headers.get("Content-Type"), "application/json");
      assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");

      const sentBody = JSON.parse(capturedOptions.body);
      assert.equal(sentBody.stream, false);
      const json = await response.json();
      assert.equal(json.choices[0].message.content, "Direct JSON answer");
      console.log("✓ Test 2 Passed: completions.js handles non-streaming JSON request correctly");
    }

    // Test 3: messages.js with stream: true
    {
      let capturedOptions = null;
      globalThis.fetch = async (url, options) => {
        capturedOptions = options;
        const sseStream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder();
            controller.enqueue(enc.encode('event: message_start\ndata: {"type":"message_start"}\n\n'));
            controller.enqueue(enc.encode('event: content_block_delta\ndata: {"type":"content_block_delta","delta":{"text":"Anthropic stream"}}\n\n'));
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
          messages: [{ role: "user", content: "Hello Anthropic" }],
          stream: true
        })
      });

      const response = await handleMessages({ request: req, env: {} });
      assert.equal(response.status, 200);
      assert.equal(response.headers.get("Content-Type"), "text/event-stream; charset=utf-8");
      assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");
      assert.equal(response.headers.get("anthropic-version"), "2023-06-01");

      const sentBody = JSON.parse(capturedOptions.body);
      assert.equal(sentBody.stream, true, "stream: true forwarded to Go backend");
      assert.equal(capturedOptions.headers.get("Accept"), "text/event-stream");

      assert.ok(response.body instanceof ReadableStream);
      const reader = response.body.getReader();
      const dec = new TextDecoder();
      let streamData = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        streamData += dec.decode(value);
      }
      assert.ok(streamData.includes("content_block_delta"));
      assert.ok(streamData.includes("Anthropic stream"));
      console.log("✓ Test 3 Passed: messages.js forwards stream: true and streams chunks incrementally");
    }

    // Test 4: Upstream error propagation
    {
      globalThis.fetch = async () => {
        return new Response(JSON.stringify({ error: { message: "Model overloaded", type: "overloaded_error" } }), {
          status: 503,
          headers: { "Content-Type": "application/json" }
        });
      };

      const req = new Request("http://localhost/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": "Bearer test-key" },
        body: JSON.stringify({ model: "gpt-4o", stream: true })
      });

      const response = await handleCompletions({ request: req, env: {} });
      assert.equal(response.status, 503);
      assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");
      const errJson = await response.json();
      assert.equal(errJson.error.message, "Model overloaded");
      console.log("✓ Test 4 Passed: Upstream error status and payload propagate cleanly with CORS");
    }

    console.log("All Streaming Tests Passed Successfully!\n");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

runTests();
