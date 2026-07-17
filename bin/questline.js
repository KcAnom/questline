#!/usr/bin/env node
// Wrapper: re-exec node with TypeScript transform support (node >= 22),
// forwarding signals so `kill <wrapper-pid>` reaches the actual CLI.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

const entry = join(dirname(fileURLToPath(import.meta.url)), "..", "src", "main.ts");
const child = spawn(process.execPath, ["--experimental-transform-types", "--disable-warning=ExperimentalWarning", entry, ...process.argv.slice(2)], {
  stdio: "inherit",
});
for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(sig, () => { try { child.kill(sig); } catch { /* already gone */ } });
}
child.on("close", (code) => process.exit(code ?? 1));
