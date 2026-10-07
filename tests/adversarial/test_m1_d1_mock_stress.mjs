import assert from "node:assert/strict";
import { validateApiKey, MOCK_API_KEYS } from "../../functions/_auth.js";
import { onRequestPost as postDeduct } from "../../functions/api/internal/deduct.js";

async function runD1MockStressSuite() {
  console.log("===============================================================");
  console.log("  ADVERSARIAL SUITE 3: MOCK TABLE VS SIMULATED D1 STRESS TEST  ");
  console.log("===============================================================");

  let testCount = 0;
  let passCount = 0;

  function recordPass(msg) {
    testCount++;
    passCount++;
    console.log(`[PASS] (${testCount}) ${msg}`);
  }

  // -------------------------------------------------------------
  // Test 1: Exhaustive Mock Table Verification (All 7 keys)
  // -------------------------------------------------------------
  const expectedMockKeys = [
    { key: "test-key", status: 200, active: 1, balance: 1000.0, expectSuccess: true },
    { key: "default-dev-key", status: 200, active: 1, balance: 1000.0, expectSuccess: true },
    { key: "live-key-valid", status: 200, active: 1, balance: 1000.0, expectSuccess: true },
    { key: "kiit-mock-key-12345", status: 200, active: 1, balance: 1000.0, expectSuccess: true },
    { key: "public", status: 200, active: 1, balance: 1000.0, expectSuccess: true },
    { key: "inactive-key", status: 401, active: 0, balance: 1000.0, expectSuccess: false, errType: "authentication_error" },
    { key: "empty-balance-key", status: 402, active: 1, balance: 0.0, expectSuccess: false, errType: "invalid_request_error" },
  ];

  for (const item of expectedMockKeys) {
    // Test with Bearer auth
    const reqBearer = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": `Bearer ${item.key}` }
    });
    const resBearer = await validateApiKey(reqBearer, {});
    assert.equal(resBearer.success, item.expectSuccess);
    if (item.expectSuccess) {
      assert.equal(resBearer.user.is_mock, true);
      assert.equal(resBearer.user.credit_balance, item.balance);
    } else {
      assert.equal(resBearer.status, item.status);
      assert.equal(resBearer.errorType, item.errType);
    }

    // Test with x-api-key
    const reqXApi = new Request("http://localhost/v1/messages", {
      headers: { "x-api-key": item.key }
    });
    const resXApi = await validateApiKey(reqXApi, {});
    assert.equal(resXApi.success, item.expectSuccess);

    // Test with raw Authorization without Bearer prefix
    const reqRaw = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": item.key }
    });
    const resRaw = await validateApiKey(reqRaw, {});
    assert.equal(resRaw.success, item.expectSuccess);

    // Test with whitespace padding
    const reqPadded = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": `Bearer   ${item.key}  ` }
    });
    const resPadded = await validateApiKey(reqPadded, {});
    assert.equal(resPadded.success, item.expectSuccess);

    recordPass(`Mock key '${item.key}' verified across all header formats (expected success=${item.expectSuccess}, status=${item.status})`);
  }

  // -------------------------------------------------------------
  // Test 2: Missing & Malformed Header Variations
  // -------------------------------------------------------------
  const malformedHeaders = [
    { headers: {}, desc: "No headers" },
    { headers: { "Authorization": "" }, desc: "Empty Authorization" },
    { headers: { "Authorization": "   " }, desc: "Whitespace Authorization" },
    { headers: { "Authorization": "Bearer" }, desc: "Bearer without key" },
    { headers: { "Authorization": "Bearer    " }, desc: "Bearer with only spaces" },
    { headers: { "x-api-key": "" }, desc: "Empty x-api-key" },
    { headers: { "x-api-key": "   " }, desc: "Whitespace x-api-key" },
  ];

  for (const tc of malformedHeaders) {
    const req = new Request("http://localhost/v1/chat/completions", { headers: tc.headers });
    const res = await validateApiKey(req, {});
    assert.equal(res.success, false);
    assert.equal(res.status, 401);
    assert.ok(res.message.includes("API Key"), "Error message should mention API Key");
    recordPass(`Malformed header case '${tc.desc}' safely returns 401 (message: ${res.message})`);
  }

  // -------------------------------------------------------------
  // Test 3: Simulated D1 Database Operations
  // -------------------------------------------------------------
  {
    // D1 with active record
    const mockDbActive = {
      prepare(sql) {
        return {
          bind(key) {
            return {
              async first() {
                if (key === "prod-key-xyz") {
                  return { credit_balance: 250.75, is_active: 1, key_id: "key-prod-1" };
                }
                return null;
              }
            };
          }
        };
      }
    };

    const reqActive = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer prod-key-xyz" }
    });
    const resActive = await validateApiKey(reqActive, { DB: mockDbActive });
    assert.equal(resActive.success, true);
    assert.equal(resActive.user.key_id, "key-prod-1");
    assert.equal(resActive.user.credit_balance, 250.75);
    recordPass("Simulated D1 active user with positive balance returns success");

    // D1 with disabled key (is_active: 0)
    const mockDbDisabled = {
      prepare() {
        return {
          bind() {
            return {
              async first() {
                return { credit_balance: 100.0, is_active: 0, key_id: "key-disabled" };
              }
            };
          }
        };
      }
    };
    const reqDisabled = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer prod-key-disabled" }
    });
    const resDisabled = await validateApiKey(reqDisabled, { DB: mockDbDisabled });
    assert.equal(resDisabled.success, false);
    assert.equal(resDisabled.status, 401);
    assert.equal(resDisabled.message, "Invalid or disabled API Key");
    recordPass("Simulated D1 disabled key (is_active: 0) returns 401");

    // D1 with zero balance
    const mockDbZero = {
      prepare() {
        return {
          bind() {
            return {
              async first() {
                return { credit_balance: 0.0, is_active: 1, key_id: "key-zero" };
              }
            };
          }
        };
      }
    };
    const reqZero = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer prod-key-zero" }
    });
    const resZero = await validateApiKey(reqZero, { DB: mockDbZero });
    assert.equal(resZero.success, false);
    assert.equal(resZero.status, 402);
    assert.equal(resZero.message, "Credit balance exhausted ($0.00 remaining).");
    recordPass("Simulated D1 zero balance returns 402 Payment Required");

    // D1 with negative balance
    const mockDbNeg = {
      prepare() {
        return {
          bind() {
            return {
              async first() {
                return { credit_balance: -15.20, is_active: 1, key_id: "key-neg" };
              }
            };
          }
        };
      }
    };
    const reqNeg = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer prod-key-neg" }
    });
    const resNeg = await validateApiKey(reqNeg, { DB: mockDbNeg });
    assert.equal(resNeg.success, false);
    assert.equal(resNeg.status, 402);
    recordPass("Simulated D1 negative balance returns 402 Payment Required");

    // D1 key not found -> Fallback to mock table in hybrid environment
    const reqFallback = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer test-key" }
    });
    // mockDbActive returns null for "test-key"
    const resFallback = await validateApiKey(reqFallback, { DB: mockDbActive });
    assert.equal(resFallback.success, true);
    assert.equal(resFallback.user.is_mock, true);
    recordPass("Key absent from D1 gracefully falls back to mock key registry in hybrid mode");

    // Key not found in D1 AND not found in mock table
    const reqUnknown = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": "Bearer non-existent-in-d1-or-mock" }
    });
    const resUnknown = await validateApiKey(reqUnknown, { DB: mockDbActive });
    assert.equal(resUnknown.success, false);
    assert.equal(resUnknown.status, 401);
    recordPass("Key absent from both D1 and mock table returns 401 Invalid or disabled API Key");
  }

  // -------------------------------------------------------------
  // Test 4: Fault Injection & Resilience (D1 Crashes & Outages)
  // -------------------------------------------------------------
  {
    // Case A: env.DB is null
    const resNullDb = await validateApiKey(
      new Request("http://localhost/v1/chat/completions", { headers: { "Authorization": "Bearer test-key" } }),
      { DB: null }
    );
    assert.equal(resNullDb.success, true);
    recordPass("Fault Injection: env.DB = null handled safely via fallback");

    // Case B: env.DB is an invalid object without prepare
    const resInvalidDb = await validateApiKey(
      new Request("http://localhost/v1/chat/completions", { headers: { "Authorization": "Bearer test-key" } }),
      { DB: { notPrepare: 123 } }
    );
    assert.equal(resInvalidDb.success, true);
    recordPass("Fault Injection: env.DB without prepare() handled safely via fallback");

    // Case C: env.DB.prepare throws synchronous error
    const syncThrowDb = {
      prepare() {
        throw new Error("SQLite error: table api_keys is locked");
      }
    };
    const resSyncThrow = await validateApiKey(
      new Request("http://localhost/v1/chat/completions", { headers: { "Authorization": "Bearer test-key" } }),
      { DB: syncThrowDb }
    );
    assert.equal(resSyncThrow.success, true);
    recordPass("Fault Injection: Synchronous D1 prepare() exception caught and recovered via mock fallback");

    // Case D: env.DB.prepare().bind().first() throws asynchronous Promise rejection
    const asyncRejectDb = {
      prepare() {
        return {
          bind() {
            return {
              async first() {
                throw new Error("Cloudflare D1 network timeout: connection reset by peer");
              }
            };
          }
        };
      }
    };
    const resAsyncReject = await validateApiKey(
      new Request("http://localhost/v1/chat/completions", { headers: { "Authorization": "Bearer test-key" } }),
      { DB: asyncRejectDb }
    );
    assert.equal(resAsyncReject.success, true);
    recordPass("Fault Injection: Asynchronous D1 Promise rejection caught and recovered via mock fallback");
  }

  // -------------------------------------------------------------
  // Test 5: High Concurrency Stress Test (6,000 Operations Total)
  // -------------------------------------------------------------
  {
    const CONCURRENCY = 2000;

    // Subtest 5A: 2,000 Concurrent Mock Table Validations
    console.log(`       -> Launching ${CONCURRENCY} concurrent Mock Table authentications...`);
    const t0 = Date.now();
    const mockPromises = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const key = i % 2 === 0 ? "test-key" : "default-dev-key";
      const req = new Request("http://localhost/v1/chat/completions", {
        headers: { "Authorization": `Bearer ${key}` }
      });
      mockPromises.push(validateApiKey(req, {}));
    }
    const mockResults = await Promise.all(mockPromises);
    const mockDuration = Date.now() - t0;
    const allMockSuccess = mockResults.every(r => r.success === true);
    assert.ok(allMockSuccess, "All 2,000 concurrent mock authentications must succeed");
    console.log(`       -> Completed ${CONCURRENCY} mock authentications in ${mockDuration}ms (${(CONCURRENCY / (mockDuration / 1000)).toFixed(0)} ops/sec)`);
    recordPass(`Stress test: 2,000 concurrent mock authentications passed with 100% success`);

    // Subtest 5B: 2,000 Concurrent Simulated D1 Validations
    const mockDbStress = {
      prepare() {
        return {
          bind(key) {
            return {
              async first() {
                // Simulate small async microtask
                return { credit_balance: 500.0, is_active: 1, key_id: `key-${key}` };
              }
            };
          }
        };
      }
    };

    console.log(`       -> Launching ${CONCURRENCY} concurrent simulated D1 authentications...`);
    const t1 = Date.now();
    const d1Promises = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const req = new Request("http://localhost/v1/chat/completions", {
        headers: { "Authorization": `Bearer live-prod-key-${i}` }
      });
      d1Promises.push(validateApiKey(req, { DB: mockDbStress }));
    }
    const d1Results = await Promise.all(d1Promises);
    const d1Duration = Date.now() - t1;
    const allD1Success = d1Results.every(r => r.success === true);
    assert.ok(allD1Success, "All 2,000 concurrent D1 authentications must succeed");
    console.log(`       -> Completed ${CONCURRENCY} D1 authentications in ${d1Duration}ms (${(CONCURRENCY / (d1Duration / 1000)).toFixed(0)} ops/sec)`);
    recordPass(`Stress test: 2,000 concurrent simulated D1 authentications passed with 100% success`);

    // Subtest 5C: 2,000 Concurrent Flapping / Failing D1 Validations (Intermittent Chaos)
    const flappingDb = {
      prepare() {
        return {
          bind(key) {
            return {
              async first() {
                if (Math.random() < 0.5) {
                  throw new Error("Chaos injected D1 connection failure");
                }
                return { credit_balance: 100.0, is_active: 1, key_id: "key-flapping" };
              }
            };
          }
        };
      }
    };

    console.log(`       -> Launching ${CONCURRENCY} concurrent chaos flapping D1 authentications...`);
    const t2 = Date.now();
    const chaosPromises = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const req = new Request("http://localhost/v1/chat/completions", {
        headers: { "Authorization": "Bearer test-key" }
      });
      chaosPromises.push(validateApiKey(req, { DB: flappingDb }));
    }
    const chaosResults = await Promise.all(chaosPromises);
    const chaosDuration = Date.now() - t2;
    const allChaosHandled = chaosResults.every(r => r.success === true);
    assert.ok(allChaosHandled, "All 2,000 chaotic flapping authentications must resolve without crashing");
    console.log(`       -> Completed ${CONCURRENCY} chaos flapping authentications in ${chaosDuration}ms (100% resilient)`);
    recordPass(`Stress test: 2,000 chaotic flapping D1 authentications passed with 100% resilience`);
  }

  // -------------------------------------------------------------
  // Test 6: SQL Injection & Adversarial Key Payloads
  // -------------------------------------------------------------
  {
    const sqlInjections = [
      "' OR '1'='1",
      "' OR 1=1 --",
      "'; DROP TABLE users; --",
      "admin'--",
      "1' UNION SELECT 1, 'mock', 1000 --",
      `" OR "a"="a`,
    ];

    let capturedBoundKey = null;
    const mockDbSql = {
      prepare(sql) {
        return {
          bind(key) {
            capturedBoundKey = key;
            return {
              async first() {
                // Return null since injected key won't match any row
                return null;
              }
            };
          }
        };
      }
    };

    for (const sqli of sqlInjections) {
      const req = new Request("http://localhost/v1/chat/completions", {
        headers: { "Authorization": `Bearer ${sqli}` }
      });
      const res = await validateApiKey(req, { DB: mockDbSql });
      assert.equal(res.success, false);
      assert.equal(res.status, 401);
      assert.equal(capturedBoundKey, sqli, "Bound key must match input verbatim without string concatenation in SQL");
      recordPass(`SQL injection payload '${sqli}' safely parameterized and rejected with 401`);
    }

    // Very large key payload (10KB)
    const largeKey = "K".repeat(10240);
    const reqLarge = new Request("http://localhost/v1/chat/completions", {
      headers: { "Authorization": `Bearer ${largeKey}` }
    });
    const resLarge = await validateApiKey(reqLarge, {});
    assert.equal(resLarge.success, false);
    assert.equal(resLarge.status, 401);
    recordPass("10KB oversized API key rejected with 401 without buffer overflow or crash");
  }

  // -------------------------------------------------------------
  // Test 7: Dedicated /api/internal/deduct Endpoint Stress
  // -------------------------------------------------------------
  {
    // Test 7A: Invalid secret -> 403 Forbidden with CORS
    const reqBadSecret = new Request("http://localhost/api/internal/deduct", {
      method: "POST",
      headers: { "X-Internal-Secret": "wrong-secret", "Content-Type": "application/json" },
      body: JSON.stringify({ key_value: "test-key", cost: 0.05 })
    });
    const resBadSecret = await postDeduct({ request: reqBadSecret, env: { INTERNAL_SECRET: "my-secret" } });
    assert.equal(resBadSecret.status, 403);
    assert.equal(resBadSecret.headers.get("Access-Control-Allow-Origin"), "*");
    recordPass("/api/internal/deduct with invalid secret returns 403 Forbidden with CORS");

    // Test 7B: Missing DB -> returns 200 mocked: true
    const reqMockDeduct = new Request("http://localhost/api/internal/deduct", {
      method: "POST",
      headers: { "X-Internal-Secret": "kiit_proxy_sec_998877", "Content-Type": "application/json" },
      body: JSON.stringify({ key_value: "test-key", cost: 0.05 })
    });
    const resMockDeduct = await postDeduct({ request: reqMockDeduct, env: {} });
    assert.equal(resMockDeduct.status, 200);
    const mockJson = await resMockDeduct.json();
    assert.equal(mockJson.mocked, true);
    recordPass("/api/internal/deduct with undefined DB safely returns 200 mocked: true");

    // Test 7C: Live D1 with valid key -> executes batch update
    let batchStatements = [];
    const mockDbDeduct = {
      prepare(sql) {
        return {
          bind(...args) {
            return {
              sql,
              args,
              async first() {
                if (args[0] === "valid-key") {
                  return { id: "key-123", user_id: "user-456" };
                }
                return null;
              },
              async run() {
                return { success: true };
              }
            };
          }
        };
      },
      async batch(stmts) {
        batchStatements = stmts;
      }
    };

    const reqValidDeduct = new Request("http://localhost/api/internal/deduct", {
      method: "POST",
      headers: { "X-Internal-Secret": "kiit_proxy_sec_998877", "Content-Type": "application/json" },
      body: JSON.stringify({
        key_value: "valid-key",
        model: "gpt-4o",
        prompt_tokens: 100,
        completion_tokens: 50,
        cost: 0.0015
      })
    });

    const resValidDeduct = await postDeduct({
      request: reqValidDeduct,
      env: { DB: mockDbDeduct, INTERNAL_SECRET: "kiit_proxy_sec_998877" }
    });

    assert.equal(resValidDeduct.status, 200);
    assert.equal(batchStatements.length, 2, "Batch must execute 2 statements (credit update + usage log)");
    recordPass("/api/internal/deduct performs atomic batch credit deduction and usage logging");
  }

  console.log("---------------------------------------------------------------");
  console.log(`SUITE 3 RESULTS: ${passCount} / ${testCount} tests passed (100%)`);
  console.log("===============================================================\n");
}

runD1MockStressSuite().catch((err) => {
  console.error("SUITE 3 CRITICAL FAILURE:", err);
  process.exit(1);
});
