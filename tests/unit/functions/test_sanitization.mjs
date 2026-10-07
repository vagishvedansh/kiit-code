import assert from "node:assert/strict";
import { onRequestPost as handleCompletions } from "../../../functions/v1/chat/completions.js";
import { onRequestPost as handleMessages } from "../../../functions/v1/messages.js";

async function runTests() {
  console.log("--- Testing Sanitization & Anti-Corruption (Anti-fixMissingSpaces) ---");

  const originalFetch = globalThis.fetch;

  try {
    const codeSnippet = `function calculateTotalSum(userId, itemId) {
  const url = "https://api.github.com/v1/users";
  const config = { "userId": 101, "timeoutMs": 5000 };
  const filename = "app.min.js";
  return 42;
}`;

    // Test with completions.js
    globalThis.fetch = async () => {
      return new Response(JSON.stringify({
        id: "chatcmpl-test",
        choices: [{
          message: {
            role: "assistant",
            content: `Here is the code:\n\`\`\`javascript\n${codeSnippet}\n\`\`\`\nI am ox-alpha from Z.ai.`
          }
        }]
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
        messages: [{ role: "user", content: "give code" }],
        stream: false
      })
    });

    const res = await handleCompletions({ request: req, env: {} });
    const data = await res.json();
    const content = data.choices[0].message.content;

    // Verify code is NOT corrupted
    assert.ok(content.includes("calculateTotalSum"), "camelCase must NOT be split into 'calculate Total Sum'!");
    assert.ok(!content.includes("calculate Total Sum"), "No corrupted spacing in identifiers");

    assert.ok(content.includes("https://api.github.com/v1/users"), "URLs must NOT be split into 'https: //api. github. com/v 1'!");
    assert.ok(!content.includes("https: //"), "No broken protocol in URL");

    assert.ok(content.includes('"userId": 101'), "JSON properties must NOT have corrupted spaces");
    assert.ok(content.includes("app.min.js"), "File extensions must NOT be split into 'app. min. js'!");

    // Verify vendor replacement still works cleanly
    assert.ok(content.includes("GPT-4o"), "ox-alpha should be replaced with proper model name");
    assert.ok(content.includes("OpenAI"), "Z.ai should be replaced with OpenAI");
    assert.ok(!content.includes("ox-alpha"), "ox-alpha must be sanitized");
    assert.ok(!content.includes("Z.ai"), "Z.ai must be sanitized");

    console.log("✓ Test 1 Passed: completions.js preserves code and URLs perfectly without regex word-splitting corruption");

    // Test with messages.js
    globalThis.fetch = async () => {
      return new Response(JSON.stringify({
        id: "msg_test",
        content: [{
          type: "text",
          text: `Solution:\n${codeSnippet}\nModel: ox-alpha developed by Z.ai.`
        }]
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    };

    const reqAnthropic = new Request("http://localhost/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": "test-key",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "claude-3-5-sonnet-20241022",
        messages: [{ role: "user", content: "give code" }],
        stream: false
      })
    });

    const resAnthropic = await handleMessages({ request: reqAnthropic, env: {} });
    const dataAnthropic = await resAnthropic.json();
    const contentAnthropic = dataAnthropic.content[0].text;

    assert.ok(contentAnthropic.includes("calculateTotalSum"), "Anthropic content preserves camelCase");
    assert.ok(contentAnthropic.includes("https://api.github.com/v1/users"), "Anthropic content preserves URLs");
    assert.ok(contentAnthropic.includes("app.min.js"), "Anthropic content preserves filenames");
    assert.ok(!contentAnthropic.includes("calculate Total Sum"), "No corrupted spacing");
    assert.ok(contentAnthropic.includes("Claude 3.5 Sonnet"), "Model rebranded properly");

    console.log("✓ Test 2 Passed: messages.js preserves code and URLs perfectly without corruption");
    console.log("All Sanitization Tests Passed Successfully!\n");
  } finally {
    globalThis.fetch = originalFetch;
  }
}

runTests();
