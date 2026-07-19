# ⚒ questline

A durable work queue, CLI/TUI, SDK, and small HTTP API for CLI coding agents — Claude Code, pi, codex, or anything with a print mode. Queue tasks, run them through your agent, and keep state in SQLite even if a terminal, daemon, or machine crashes.

## Execution guarantees

Questline is **at-least-once**, not exactly-once. Database state transitions are fenced by versioned leases, so a stale worker cannot overwrite a quest after another session reclaims it. External side effects performed by the agent process are not fenced by SQLite; make agent tasks idempotent or protect them with your own locks when they touch external systems.

- **Leases with fencing** — every claim gets a generation token; stale complete/fail callbacks are rejected.
- **Heartbeats + reclamation** — if a worker stops heartbeating, another session may reclaim the quest.
- **Lease-loss cancellation** — local executors receive an `AbortSignal`; spawned process groups are terminated so reclaimed quests do not keep producing side effects.
- **Graceful shutdown** — daemon/TUI/CLI shutdown sends SIGTERM to child process groups, waits a grace window, escalates to SIGKILL, then marks unfinished owned quests interrupted.
- **Retries are bounded** with exponential backoff. Retryable execution failures do not fail dependents; terminal `failed`/`cancelled` dependencies do.

## Install

```bash
npm install -g questline     # needs node >= 22
```

## Quick start

```bash
cd your-project
questline add build "Write unit tests for src/parser.ts" --name tests
questline run                # claims + executes via your agent, prints result preview
questline dash               # TUI: ↑↓ select · ←→ filter · / search · ⏎ details · r/R/x
```

Queue several and let the daemon drain them:

```bash
questline add explore "Map this codebase" --priority 5
questline add build "Fix the flaky test" --max-attempts 3
questline add review "Review the diff" --depends q-abc12345
questline daemon             # runs everything, respecting priority/deps/backoff
```

## The executor: bring your own agent

By default quests run through **Claude Code**: `claude -p "<task>"`. Configure any agent in `~/.questline/config.json` (global) or `<project>/.questline.json` (per project). Config files are JSONC: comments and trailing commas are accepted, but invalid config now fails loudly with file/line details.

```jsonc
{
  "executor": { "command": ["claude", "-p", "{task}"], "timeoutMs": 900000 },
  "executors": {
    "review": { "command": ["claude", "-p", "--model", "opus", "{task}"] }
  }
}
```

Placeholders: `{task}` (includes context), `{context}`, `{project}`, `{role}`, `{id}`, `{name}`. The command runs with the quest's project as cwd; stdout is the stored result in text mode; exit 0 = done, non-zero or timeout = failed (and retried if attempts remain).

### Structured JSONL executor protocol

Set `"protocol": "questline-jsonl"` on an executor to emit machine-readable output and telemetry. Each stdout line is one JSON envelope:

```json
{"protocol":"questline/1","type":"output","text":"partial result\n"}
{"protocol":"questline/1","type":"telemetry","provider":"anthropic","model":"claude-sonnet","turns":2,"tokens":1234,"costUsd":0.08,"lastActivity":"editing"}
{"protocol":"questline/1","type":"result","ok":true}
```

Telemetry values are cumulative. Stderr remains diagnostic text. Malformed JSONL, unknown event types, duplicate/missing result events, or non-zero exits fail the attempt.

## Output artifacts

Large stdout/stderr is streamed to files instead of being silently truncated. SQLite stores a bounded head/tail preview plus artifact metadata. Defaults:

```jsonc
{
  "artifactDir": "~/.questline/artifacts",
  "executor": {
    "maxInlineOutputBytes": 262144,
    "outputPreviewBytes": 16384,
    "terminateGraceMs": 5000
  }
}
```

`questline show <id>` displays attempt telemetry, recent events, result/error previews, and full artifact paths.

## Commands

| Command | Summary |
|---|---|
| `add <role> "task"` | `--name` `--priority N` `--context TEXT` `--depends id1,id2` `--max-attempts N` `--not-before ISO` `--dedupe-key KEY` `--retain` `--group ID` |
| `list` / `show <id>` | queue table (`--json`) / full detail with telemetry, events, artifacts, and preview |
| `run [id]` | claim next eligible (or a specific quest) and execute now |
| `daemon` | drain continuously; Ctrl-C terminates workers and releases cleanly |
| `dash` | interactive TUI |
| `cancel` / `retry` / `consume <id>` | cancel pending/running work / requeue failed work / release a retained result's dedupe key |
| `pause` / `resume` | durable project queue pause; running quests continue, new claims and recurrence materialization stop |
| `group create/list/show/cancel/retry` | first-class quest groups |
| `pipeline list/run <name>` | enqueue configured DAG pipelines atomically |
| `recurring add/list/pause/resume/remove` | cron-style recurring quests |
| `events` / `runs <id>` | audit journal / per-attempt telemetry |
| `health [--json] [--all-projects]` | database integrity plus project health, dependency issues, stale schedulers, overdue work |
| `export` / `import` / `backup` | JSON archive round-trip and SQLite `VACUUM INTO` backup |
| `api --port N` | local HTTP API: `/health`, `/quests`, `/pause`, `/resume` |
| `completion` | shell completion snippet |

## Dependencies

Dependencies are AND-gates: every dependency must be `done` before a quest can run. Questline rejects nonexistent dependencies, cross-project dependencies, self-dependencies, and dependency cycles. If a dependency becomes terminally `failed` or `cancelled`, queued/interrupted dependents fail immediately with `failureKind: "dependency"`. Requeueing the root revives dependency-failed descendants; independently failed descendants stay failed.

## Pipelines

Pipelines live in config and are enqueued as one group in a single transaction:

```jsonc
{
  "pipelines": {
    "plan-build-review": {
      "steps": {
        "plan": { "role": "plan", "task": "Plan {project}" },
        "build": { "role": "build", "task": "Implement the plan", "dependsOn": ["plan"] },
        "review": { "role": "review", "task": "Review the result", "dependsOn": ["build"] }
      }
    }
  }
}
```

Run with `questline pipeline run plan-build-review`.

## Recurring quests

```bash
questline recurring add nightly-review \
  --cron "0 2 * * *" \
  --timezone America/New_York \
  --role review \
  --task "Review today's diff" \
  --catch-up one
questline daemon
```

Cron expressions are standard five-field (`minute hour dom month dow`). Next-run times are computed with **IANA timezones and DST** via `cron-parser` (civil time in the given zone; default `UTC`). The scheduler materializes due occurrences while it owns the scheduler lease. Supported catch-up modes: `one`, `all`, `skip`.

## Config reference

```jsonc
{
  "dbPath": "~/.questline/quests.sqlite",
  "artifactDir": "~/.questline/artifacts",
  "maxHistory": 500,
  "leaseTtlMs": 120000,
  "heartbeatMs": 30000,
  "maxAttempts": 1,
  "backoffBaseMs": 30000,
  "scheduler": {
    "pollMs": 5000,
    "maxConcurrent": 2,
    "roleConcurrency": { "review": 1 }
  },
  "executor": {
    "command": ["claude", "-p", "{task}"],
    "timeoutMs": 900000,
    "protocol": "text",
    "terminateGraceMs": 5000,
    "maxInlineOutputBytes": 262144,
    "outputPreviewBytes": 16384
  },
  "notifications": {
    "command": ["osascript", "-e", "display notification \"{name}: {state}\" with title \"questline\""]
  }
}
```

Validation rejects unknown keys, invalid numbers, empty executor commands, and heartbeat intervals greater than or equal to the lease TTL.

Quests are **per-project** (keyed by the directory you run commands in), stored in one SQLite database (WAL). Compatible with the [pi-fairy-tales](https://github.com/KcAnom/pi-fairy-tales) quest journal — set `dbPath` to `~/.pi/agent/fairy-tales-quests.sqlite` and questline and pi share one queue.

## SDK

Questline exports a stable ESM SDK; no `dist/*` imports are required:

```js
import {
  QuestStore,
  QuestRuntime,
  QuestScheduler,
  executeQuest,
  loadConfig,
  startHttpApi
} from "questline";
```

Published packages include `main`, `exports`, and TypeScript declarations.

## License

MIT
