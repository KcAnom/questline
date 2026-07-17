/**
 * Executor: runs one claimed quest by spawning a CLI agent (claude, pi,
 * codex — anything with a print mode) in the quest's project directory.
 * stdout is the result; exit code 0 = done, anything else = failed. The
 * quest lease is heartbeated by the runtime while this runs; a timeout
 * kills the child and fails the attempt.
 */
import { spawn } from "node:child_process";
import type { ExecutorConfig } from "./config.ts";
import type { QuestRecord } from "./store.ts";

export interface ExecutionResult {
  ok: boolean;
  output: string;
  exitCode: number | null;
  durationMs: number;
  timedOut: boolean;
}

function substitute(template: string, quest: QuestRecord): string {
  return template
    .replaceAll("{task}", quest.context ? `${quest.task}\n\nContext:\n${quest.context}` : quest.task)
    .replaceAll("{context}", quest.context ?? "")
    .replaceAll("{project}", quest.project)
    .replaceAll("{role}", quest.role);
}

export function executeQuest(quest: QuestRecord, executor: ExecutorConfig, signal?: AbortSignal): Promise<ExecutionResult> {
  const [cmd, ...args] = executor.command.map((part) => substitute(part, quest));
  const timeoutMs = executor.timeoutMs ?? 15 * 60_000;
  const startedAt = Date.now();
  return new Promise((resolvePromise) => {
    const child = spawn(cmd, args, {
      cwd: quest.project,
      stdio: ["ignore", "pipe", "pipe"],
      env: process.env,
    });
    let out = "";
    let err = "";
    let timedOut = false;
    let settled = false;
    const settle = (ok: boolean, exitCode: number | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const output = ok ? out.trim() : `${out.trim()}\n${err.trim()}`.trim() || `exit code ${exitCode}`;
      resolvePromise({ ok, output, exitCode, durationMs: Date.now() - startedAt, timedOut });
    };
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);
    (timer as { unref?: () => void }).unref?.();
    signal?.addEventListener("abort", () => child.kill("SIGTERM"), { once: true });
    child.stdout.on("data", (d: Buffer) => { out += d.toString(); if (out.length > 4_000_000) out = out.slice(-2_000_000); });
    child.stderr.on("data", (d: Buffer) => { err += d.toString(); if (err.length > 400_000) err = err.slice(-200_000); });
    child.on("error", (e) => { err += String(e); settle(false, null); });
    child.on("close", (code) => settle(code === 0 && !timedOut, code));
  });
}
