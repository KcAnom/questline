import { createWriteStream, existsSync, mkdirSync, renameSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
function expandHome(p) {
    if (p === "~")
        return homedir();
    if (p.startsWith("~/"))
        return join(homedir(), p.slice(2));
    return p;
}
function appendBounded(bufs, chunk, maxBytes, fromTail) {
    bufs.push(chunk);
    let total = bufs.reduce((n, b) => n + b.length, 0);
    while (total > maxBytes && bufs.length) {
        if (fromTail) {
            const first = bufs[0];
            const over = total - maxBytes;
            if (over >= first.length) {
                bufs.shift();
                total -= first.length;
            }
            else {
                bufs[0] = first.subarray(over);
                total -= over;
            }
        }
        else {
            const last = bufs[bufs.length - 1];
            const over = total - maxBytes;
            if (over >= last.length) {
                bufs.pop();
                total -= last.length;
            }
            else {
                bufs[bufs.length - 1] = last.subarray(0, last.length - over);
                total -= over;
            }
        }
    }
    return bufs;
}
export class StreamCollector {
    options;
    inline = [];
    head = [];
    tail = [];
    bytes = 0;
    stream;
    tmpPath;
    finalPath;
    writeError;
    constructor(options) {
        this.options = options;
        this.options.maxInlineBytes = Math.max(1024, options.maxInlineBytes);
        this.options.previewBytes = Math.max(128, options.previewBytes);
    }
    write(data) {
        const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (!chunk.length)
            return;
        this.bytes += chunk.length;
        const headLimit = Math.ceil(this.options.previewBytes / 2);
        const tailLimit = Math.floor(this.options.previewBytes / 2);
        if (Buffer.concat(this.head).length < headLimit)
            appendBounded(this.head, chunk, headLimit, false);
        appendBounded(this.tail, chunk, tailLimit, true);
        if (!this.stream && this.bytes > this.options.maxInlineBytes)
            this.openArtifact();
        if (this.stream)
            this.stream.write(chunk);
        else
            appendBounded(this.inline, chunk, this.options.maxInlineBytes, false);
    }
    async finish() {
        if (this.stream) {
            await new Promise((resolveDone, reject) => {
                this.stream.end(() => this.writeError ? reject(this.writeError) : resolveDone());
            });
            if (this.tmpPath && this.finalPath)
                renameSync(this.tmpPath, this.finalPath);
            const head = Buffer.concat(this.head).toString("utf8");
            const tail = Buffer.concat(this.tail).toString("utf8");
            const omitted = Math.max(0, this.bytes - Buffer.byteLength(head) - Buffer.byteLength(tail));
            const text = omitted > 0
                ? `${head}\n\n… omitted ${omitted} bytes; full output: ${this.finalPath} …\n\n${tail}`
                : `${head}${tail}`;
            return { text, bytes: this.bytes, truncated: true, artifact: { kind: this.options.kind, path: this.finalPath, bytes: this.bytes } };
        }
        return { text: Buffer.concat(this.inline).toString("utf8"), bytes: this.bytes, truncated: false };
    }
    cleanup() {
        if (this.stream)
            this.stream.destroy();
        for (const p of [this.tmpPath, this.finalPath])
            if (p && existsSync(p))
                rmSync(p, { force: true });
    }
    openArtifact() {
        const root = resolve(expandHome(this.options.artifactDir));
        const dir = join(root, this.options.questId);
        mkdirSync(dir, { recursive: true, mode: 0o700 });
        const safeRun = this.options.agentRunId.replace(/[^a-zA-Z0-9_.-]/g, "_");
        this.finalPath = join(dir, `attempt-${this.options.attempt}-${safeRun}.${this.options.kind}.log`);
        this.tmpPath = `${this.finalPath}.tmp-${process.pid}-${Date.now()}`;
        this.stream = createWriteStream(this.tmpPath, { mode: 0o600 });
        this.stream.on("error", (err) => { this.writeError = err; });
        for (const b of this.inline)
            this.stream.write(b);
        this.inline = [];
    }
}
