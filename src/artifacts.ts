import { createWriteStream, existsSync, mkdirSync, renameSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
import type { QuestArtifactInput } from "./store.ts";

function expandHome(p: string): string {
  if (p === "~") return homedir();
  if (p.startsWith("~/")) return join(homedir(), p.slice(2));
  return p;
}

function appendBounded(bufs: Buffer[], chunk: Buffer, maxBytes: number, fromTail: boolean): Buffer[] {
  bufs.push(chunk);
  let total = bufs.reduce((n, b) => n + b.length, 0);
  while (total > maxBytes && bufs.length) {
    if (fromTail) {
      const first = bufs[0]!;
      const over = total - maxBytes;
      if (over >= first.length) { bufs.shift(); total -= first.length; }
      else { bufs[0] = first.subarray(over); total -= over; }
    } else {
      const last = bufs[bufs.length - 1]!;
      const over = total - maxBytes;
      if (over >= last.length) { bufs.pop(); total -= last.length; }
      else { bufs[bufs.length - 1] = last.subarray(0, last.length - over); total -= over; }
    }
  }
  return bufs;
}

export interface StreamCollectorOptions {
  artifactDir: string;
  questId: string;
  attempt: number;
  agentRunId: string;
  kind: string;
  maxInlineBytes: number;
  previewBytes: number;
}

export interface StreamCollectorResult {
  text: string;
  bytes: number;
  artifact?: QuestArtifactInput;
  truncated: boolean;
}

export class StreamCollector {
  private inline: Buffer[] = [];
  private head: Buffer[] = [];
  private tail: Buffer[] = [];
  private bytes = 0;
  private stream?: ReturnType<typeof createWriteStream>;
  private tmpPath?: string;
  private finalPath?: string;
  private writeError?: Error;

  constructor(private readonly options: StreamCollectorOptions) {
    this.options.maxInlineBytes = Math.max(1024, options.maxInlineBytes);
    this.options.previewBytes = Math.max(128, options.previewBytes);
  }

  write(data: Buffer | string): void {
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!chunk.length) return;
    this.bytes += chunk.length;
    const headLimit = Math.ceil(this.options.previewBytes / 2);
    const tailLimit = Math.floor(this.options.previewBytes / 2);
    if (Buffer.concat(this.head).length < headLimit) appendBounded(this.head, chunk, headLimit, false);
    appendBounded(this.tail, chunk, tailLimit, true);

    if (!this.stream && this.bytes > this.options.maxInlineBytes) this.openArtifact();
    if (this.stream) this.stream.write(chunk);
    else appendBounded(this.inline, chunk, this.options.maxInlineBytes, false);
  }

  async finish(): Promise<StreamCollectorResult> {
    if (this.stream) {
      await new Promise<void>((resolveDone, reject) => {
        this.stream!.end(() => this.writeError ? reject(this.writeError) : resolveDone());
      });
      if (this.tmpPath && this.finalPath) renameSync(this.tmpPath, this.finalPath);
      const head = Buffer.concat(this.head).toString("utf8");
      const tail = Buffer.concat(this.tail).toString("utf8");
      const omitted = Math.max(0, this.bytes - Buffer.byteLength(head) - Buffer.byteLength(tail));
      const text = omitted > 0
        ? `${head}\n\n… omitted ${omitted} bytes; full output: ${this.finalPath} …\n\n${tail}`
        : `${head}${tail}`;
      return { text, bytes: this.bytes, truncated: true, artifact: { kind: this.options.kind, path: this.finalPath!, bytes: this.bytes } };
    }
    return { text: Buffer.concat(this.inline).toString("utf8"), bytes: this.bytes, truncated: false };
  }

  cleanup(): void {
    if (this.stream) this.stream.destroy();
    for (const p of [this.tmpPath, this.finalPath]) if (p && existsSync(p)) rmSync(p, { force: true });
  }

  private openArtifact(): void {
    const root = resolve(expandHome(this.options.artifactDir));
    const dir = join(root, this.options.questId);
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    const safeRun = this.options.agentRunId.replace(/[^a-zA-Z0-9_.-]/g, "_");
    this.finalPath = join(dir, `attempt-${this.options.attempt}-${safeRun}.${this.options.kind}.log`);
    this.tmpPath = `${this.finalPath}.tmp-${process.pid}-${Date.now()}`;
    this.stream = createWriteStream(this.tmpPath, { mode: 0o600 });
    this.stream.on("error", (err) => { this.writeError = err; });
    for (const b of this.inline) this.stream.write(b);
    this.inline = [];
  }
}
