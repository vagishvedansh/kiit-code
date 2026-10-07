import { validateApiKey, MOCK_API_KEYS } from "../../../functions/_auth.js";
import assert from "node:assert/strict";

async function runTests() {
  console.log("--- Testing Auth Logic (_auth.js) ---");

  // Test 1: Missing API key
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: {}
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.equal(res.message, "Missing API Key");
    console.log("✓ Test 1 Passed: Missing key returns 401 Missing API Key");
  }

  // Test 2: Fallback mock table with test-key (env.DB is undefined)
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer test-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, true);
    assert.equal(res.apiKey, "test-key");
    assert.equal(res.user.is_mock, true);
    console.log("✓ Test 2 Passed: test-key succeeds via fallback mock table when env.DB is undefined");
  }

  // Test 3: Fallback mock table with default-dev-key
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer default-dev-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, true);
    assert.equal(res.apiKey, "default-dev-key");
    assert.equal(res.user.is_mock, true);
    console.log("✓ Test 3 Passed: default-dev-key succeeds via fallback mock table");
  }

  // Test 4: Invalid key when env.DB is undefined
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer invalid-unknown-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.equal(res.message, "Invalid or disabled API Key");
    console.log("✓ Test 4 Passed: Unknown key returns 401 when DB is undefined");
  }

  // Test 5: x-api-key header support
  {
    const req = new Request("http://localhost/v1/messages", {
      headers: { "x-api-key": "test-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, true);
    assert.equal(res.apiKey, "test-key");
    console.log("✓ Test 5 Passed: x-api-key header works for auth");
  }

  // Test 6: D1 Database mock object - active key with credit
  {
    const mockDb = {
      prepare(query) {
        return {
          bind(key) {
            return {
              async first() {
                if (key === "d1-live-key") {
                  return { credit_balance: 50.0, is_active: 1, key_id: "key-123" };
                }
                return null;
              }
            };
          }
        };
      }
    };
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer d1-live-key" }
    });
    const res = await validateApiKey(req, { DB: mockDb });
    assert.equal(res.success, true);
    assert.equal(res.user.key_id, "key-123");
    assert.equal(res.user.credit_balance, 50.0);
    console.log("✓ Test 6 Passed: D1 database query authenticates active key");
  }

  // Test 7: D1 Database - disabled key returns 401
  {
    const mockDb = {
      prepare(query) {
        return {
          bind(key) {
            return {
              async first() {
                return { credit_balance: 50.0, is_active: 0, key_id: "key-disabled" };
              }
            };
          }
        };
      }
    };
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer disabled-key" }
    });
    const res = await validateApiKey(req, { DB: mockDb });
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.equal(res.message, "Invalid or disabled API Key");
    console.log("✓ Test 7 Passed: Disabled D1 key returns 401");
  }

  // Test 8: D1 Database - exhausted credit returns 402
  {
    const mockDb = {
      prepare(query) {
        return {
          bind(key) {
            return {
              async first() {
                return { credit_balance: 0.0, is_active: 1, key_id: "key-broke" };
              }
            };
          }
        };
      }
    };
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer broke-key" }
    });
    const res = await validateApiKey(req, { DB: mockDb });
    assert.equal(res.success, false);
    assert.equal(res.status, 402);
    assert.equal(res.message, "Credit balance exhausted ($0.00 remaining).");
    console.log("✓ Test 8 Passed: Exhausted balance returns 402");
  }

  // Test 9: D1 Database offline / throwing error falls back gracefully to mock table
  {
    const throwingDb = {
      prepare() {
        throw new Error("D1 connection refused: database offline");
      }
    };
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer test-key" }
    });
    const res = await validateApiKey(req, { DB: throwingDb });
    assert.equal(res.success, true);
    assert.equal(res.apiKey, "test-key");
    assert.equal(res.user.is_mock, true);
    console.log("✓ Test 9 Passed: D1 offline error falls back safely to mock key table (no TypeError / 500)");
  }

  // Test 10: Mock table inactive-key returns 401 when DB is absent
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer inactive-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.equal(res.message, "Invalid or disabled API Key");
    console.log("✓ Test 10 Passed: inactive-key in mock table returns 401");
  }

  // Test 11: Mock table empty-balance-key returns 402 when DB is absent
  {
    const req = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer empty-balance-key" }
    });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 402);
    assert.equal(res.message, "Credit balance exhausted ($0.00 remaining).");
    console.log("✓ Test 11 Passed: empty-balance-key in mock table returns 402");
  }

  console.log("All Auth Tests Passed Successfully!\n");
}

runTests();
