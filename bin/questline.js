#!/usr/bin/env node
// Published installs run the compiled dist; a repo checkout without dist
// falls back to running the TypeScript source directly.
import { existsSync } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dist = join(root, "dist", "main.js");
if (existsSync(dist)) {
  // node:sqlite works fine for our use — hide its experimental warning.
  const emitWarning = process.emitWarning.bind(process);
  process.emitWarning = (warning, ...args) => {
    if (String(warning).includes("SQLite is an experimental feature")) return;
    emitWarning(warning, ...args);
  };
  await import(dist);
} else {
  const child = spawn(process.execPath, ["--experimental-transform-types", "--disable-warning=ExperimentalWarning", join(root, "src", "main.ts"), ...process.argv.slice(2)], { stdio: "inherit" });
  for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    process.on(sig, () => { try { child.kill(sig); } catch { /* gone */ } });
  }
  child.on("close", (code) => process.exit(code ?? 1));
}
