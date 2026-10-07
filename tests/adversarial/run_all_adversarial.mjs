import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const suites = [
  "test_m1_cors_options.mjs",
  "test_m1_headers_streaming.mjs",
  "test_m1_d1_mock_stress.mjs"
];

console.log("==================================================================");
console.log("      RUNNING MILESTONE M1 ADVERSARIAL CHALLENGER TEST SUITE       ");
console.log("==================================================================\n");

let passed = 0;
let failed = 0;

for (const suite of suites) {
  const suitePath = path.join(__dirname, suite);
  console.log(`>>> Executing Adversarial Suite: ${suite}...`);
  const result = spawnSync("node", [suitePath], { stdio: "inherit" });
  if (result.status === 0) {
    passed++;
  } else {
    failed++;
    console.error(`FAILED: ${suite} exited with code ${result.status}`);
  }
}

console.log("\n==================================================================");
console.log(`FINAL RESULTS: ${passed} passed, ${failed} failed out of ${suites.length} adversarial suites`);
console.log("==================================================================");

if (failed > 0) {
  console.error("ADVERSARIAL VERDICT: FAIL");
  process.exit(1);
} else {
  console.log("ADVERSARIAL VERDICT: APPROVE (100% of edge stress tests passed)");
}
