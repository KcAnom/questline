/**
 * Executor: runs one claimed quest by spawning a CLI agent (claude, pi,
 * codex — anything with a print mode) in the quest's project directory.
 */
import { spawn } from "node:child_process";
import type { ExecutorConfig } from "./config.ts";
import type { QuestArtifactInput, QuestRecord, QuestTelemetry } from "./store.ts";
import { StreamCollector } from "./artifacts.ts";

export type ExecutionStatus = "succeeded" | "failed" | "timed-out" | "aborted" | "protocol-error";

export interface ExecutionResult {
  ok: boolean;
  status: ExecutionStatus;
  output: string;
  exitCode: number | null;
  durationMs: number;
  timedOut: boolean;
  aborted: boolean;
  artifacts: QuestArtifactInput[];
  telemetry?: QuestTelemetry;
}

export interface ExecuteQuestOptions {
  signal?: AbortSignal;
  artifactDir?: string;
  agentRunId?: string;
  onTelemetry?: (telemetry: QuestTelemetry) => void;
}

function substitute(template: string, quest: QuestRecord): string {
  return template
    .replaceAll("{task}", quest.context ? `${quest.task}\n\nContext:\n${quest.context}` : quest.task)
    .replaceAll("{context}", quest.context ?? "")
    .replaceAll("{project}", quest.project)
    .replaceAll("{role}", quest.role)
    .replaceAll("{id}", quest.id)
    .replaceAll("{name}", quest.name);
}

function killProcessTree(child: ReturnType<typeof spawn>, signal: NodeJS.Signals): void {
  if (!child.pid) return;
  if (process.platform !== "win32") {
    try { process.kill(-child.pid, signal); return; } catch { /* fall back */ }
  }
  try { child.kill(signal); } catch { /* already dead */ }
}

function validateTelemetry(raw: unknown): QuestTelemetry {
  const t = raw as QuestTelemetry;
  const out: QuestTelemetry = {};
  if (t.model !== undefined) out.model = String(t.model).slice(0, 256);
  if (t.provider !== undefined) out.provider = String(t.provider).slice(0, 128);
  if (t.tier !== undefined) out.tier = String(t.tier).slice(0, 128);
  if (t.lastActivity !== undefined) out.lastActivity = String(t.lastActivity).slice(0, 1000);
  if (t.turns !== undefined) {
    if (!Number.isInteger(t.turns) || t.turns < 0) throw new Error("telemetry.turns must be a nonnegative integer");
    out.turns = t.turns;
  }
  if (t.tokens !== undefined) {
    if (!Number.isInteger(t.tokens) || t.tokens < 0) throw new Error("telemetry.tokens must be a nonnegative integer");
    out.tokens = t.tokens;
  }
  if (t.costUsd !== undefined) {
    if (typeof t.costUsd !== "number" || !Number.isFinite(t.costUsd) || t.costUsd < 0) throw new Error("telemetry.costUsd must be nonnegative");
    out.costUsd = t.costUsd;
  }
  return out;
}

interface ProtocolState {
  buffer: string;
  resultSeen: boolean;
  resultOk?: boolean;
  resultError?: string;
  protocolError?: string;
  telemetry: QuestTelemetry;
}

function mergeTelemetry(base: QuestTelemetry, patch: QuestTelemetry): QuestTelemetry {
  return { ...base, ...patch };
}

function parseProtocolLine(line: string, collector: StreamCollector, state: ProtocolState, onTelemetry?: (t: QuestTelemetry) => void): void {
  if (!line.trim()) return;
  if (line.length > 1024 * 1024) throw new Error("protocol line exceeds 1MiB");
  if (state.resultSeen) throw new Error("protocol event after result");
  const envelope = JSON.parse(line) as Record<string, unknown>;
  if (envelope.protocol !== "questline/1") throw new Error("unsupported protocol envelope");
  if (envelope.type === "output") {
    if (typeof envelope.text !== "string") throw new Error("output.text must be a string");
    collector.write(envelope.text);
  } else if (envelope.type === "telemetry") {
    const telemetry = validateTelemetry(envelope);
    state.telemetry = mergeTelemetry(state.telemetry, telemetry);
    onTelemetry?.(telemetry);
  } else if (envelope.type === "result") {
    if (typeof envelope.ok !== "boolean") throw new Error("result.ok must be boolean");
    state.resultSeen = true;
    state.resultOk = envelope.ok;
    if (envelope.error !== undefined) state.resultError = String(envelope.error);
  } else {
    throw new Error(`unknown protocol event type ${String(envelope.type)}`);
  }
}

export function executeQuest(quest: QuestRecord, executor: ExecutorConfig, signalOrOptions?: AbortSignal | ExecuteQuestOptions): Promise<ExecutionResult> {
  const options: ExecuteQuestOptions = signalOrOptions instanceof AbortSignal ? { signal: signalOrOptions } : (signalOrOptions ?? {});
  const [cmd, ...args] = executor.command.map((part) => substitute(part, quest));
  const timeoutMs = executor.timeoutMs ?? 15 * 60_000;
  const terminateGraceMs = executor.terminateGraceMs ?? 5_000;
  const protocol = executor.protocol ?? "text";
  const startedAt = Date.now();
  const agentRunId = options.agentRunId ?? quest.agentRunId ?? "manual";
  const artifactDir = options.artifactDir ?? "~/.questline/artifacts";
  const stdout = new StreamCollector({ artifactDir, questId: quest.id, attempt: quest.attempts, agentRunId, kind: "stdout", maxInlineBytes: executor.maxInlineOutputBytes ?? 256 * 1024, previewBytes: executor.outputPreviewBytes ?? 16 * 1024 });
  const stderr = new StreamCollector({ artifactDir, questId: quest.id, attempt: quest.attempts, agentRunId, kind: "stderr", maxInlineBytes: executor.maxInlineOutputBytes ?? 256 * 1024, previewBytes: executor.outputPreviewBytes ?? 16 * 1024 });

  return new Promise((resolvePromise) => {
    if (options.signal?.aborted) {
      resolvePromise({ ok: false, status: "aborted", output: "aborted before start", exitCode: null, durationMs: 0, timedOut: false, aborted: true, artifacts: [] });
      return;
    }

    const child = spawn(cmd, args, {
      cwd: quest.project,
      stdio: ["ignore", "pipe", "pipe"],
      env: process.env,
      detached: process.platform !== "win32",
    });
    let timedOut = false;
    let aborted = false;
    let settled = false;
    let exitCode: number | null = null;
    let escalation: ReturnType<typeof setTimeout> | undefined;
    let protocolState: ProtocolState = { buffer: "", resultSeen: false, telemetry: {} };

    const terminate = (kind: "timeout" | "abort") => {
      if (settled) return;
      if (kind === "timeout") timedOut = true;
      else aborted = true;
      killProcessTree(child, "SIGTERM");
      escalation = setTimeout(() => killProcessTree(child, "SIGKILL"), terminateGraceMs);
      escalation.unref?.();
    };

    const timer = setTimeout(() => terminate("timeout"), timeoutMs);
    timer.unref?.();
    const abortListener = () => terminate("abort");
    options.signal?.addEventListener("abort", abortListener, { once: true });

    const finish = async () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (escalation) clearTimeout(escalation);
      options.signal?.removeEventListener("abort", abortListener);
      try {
        if (protocolState.protocolError) throw new Error(protocolState.protocolError);
        if (protocol === "questline-jsonl" && protocolState.buffer.length) {
          parseProtocolLine(protocolState.buffer, stdout, protocolState, options.onTelemetry);
          protocolState.buffer = "";
        }
        if (protocol === "questline-jsonl" && !protocolState.resultSeen) throw new Error("protocol result event missing");
        const outResult = await stdout.finish();
        const errResult = await stderr.finish();
        const artifacts = [outResult.artifact, errResult.artifact].filter(Boolean) as QuestArtifactInput[];
        const durationMs = Date.now() - startedAt;
        if (aborted || options.signal?.aborted) {
          resolvePromise({ ok: false, status: "aborted", output: outResult.text || errResult.text || "aborted", exitCode, durationMs, timedOut: false, aborted: true, artifacts, telemetry: protocolState.telemetry });
        } else if (timedOut) {
          resolvePromise({ ok: false, status: "timed-out", output: `${outResult.text}\n${errResult.text}`.trim() || `timed out after ${durationMs}ms`, exitCode, durationMs, timedOut: true, aborted: false, artifacts, telemetry: protocolState.telemetry });
        } else if (protocol === "questline-jsonl") {
          const ok = protocolState.resultOk === true && exitCode === 0;
          const output = ok ? outResult.text.trim() : (protocolState.resultError || `${outResult.text}\n${errResult.text}`.trim() || `exit code ${exitCode}`);
          resolvePromise({ ok, status: ok ? "succeeded" : "failed", output, exitCode, durationMs, timedOut: false, aborted: false, artifacts, telemetry: protocolState.telemetry });
        } else {
          const ok = exitCode === 0;
          const output = ok ? outResult.text.trim() : `${outResult.text.trim()}\n${errResult.text.trim()}`.trim() || `exit code ${exitCode}`;
          resolvePromise({ ok, status: ok ? "succeeded" : "failed", output, exitCode, durationMs, timedOut: false, aborted: false, artifacts, telemetry: protocolState.telemetry });
        }
      } catch (err) {
        stdout.cleanup();
        stderr.cleanup();
        resolvePromise({ ok: false, status: "protocol-error", output: String(err), exitCode, durationMs: Date.now() - startedAt, timedOut, aborted, artifacts: [], telemetry: protocolState.telemetry });
      }
    };

    child.stdout.on("data", (d: Buffer) => {
      if (protocol === "questline-jsonl") {
        try {
          protocolState.buffer += d.toString("utf8");
          let idx: number;
          while ((idx = protocolState.buffer.indexOf("\n")) >= 0) {
            const line = protocolState.buffer.slice(0, idx).replace(/\r$/, "");
            protocolState.buffer = protocolState.buffer.slice(idx + 1);
            parseProtocolLine(line, stdout, protocolState, options.onTelemetry);
          }
          if (protocolState.buffer.length > 1024 * 1024) throw new Error("protocol line exceeds 1MiB");
        } catch (err) {
          protocolState.protocolError = String(err);
          terminate("abort");
        }
      } else {
        stdout.write(d);
      }
    });
    child.stderr.on("data", (d: Buffer) => { stderr.write(d); });
    child.on("error", (e) => { stderr.write(String(e)); });
    child.on("close", (code) => {
      exitCode = code;
      if (protocolState.protocolError) stderr.write(protocolState.protocolError);
      void finish();
    });
  });
}
