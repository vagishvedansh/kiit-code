import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const testFiles = [
  "test_auth.mjs",
  "test_streaming.mjs",
  "test_sanitization.mjs",
  "test_cors_and_preflight.mjs",
  "test_aliases.mjs"
];

console.log("==================================================");
console.log("       RUNNING MILESTONE M1 VERIFICATION SUITE     ");
console.log("==================================================\n");

let passed = 0;
let failed = 0;

for (const file of testFiles) {
  const filePath = path.join(__dirname, file);
  console.log(`>>> Executing ${file}...`);
  const result = spawnSync("node", [filePath], { stdio: "inherit" });
  if (result.status === 0) {
    passed++;
  } else {
    failed++;
    console.error(`FAILED: ${file} exited with code ${result.status}`);
  }
}

console.log("==================================================");
console.log(`Results: ${passed} passed, ${failed} failed out of ${testFiles.length} suites`);
console.log("==================================================");

if (failed > 0) {
  process.exit(1);
} else {
  console.log("ALL TESTS PASSED WITH 100% SUCCESS!");
}
