# ⚒ questforge

A durable, crash-safe work queue and TUI for CLI coding agents — Claude Code, pi, codex, or anything with a print mode. Queue tasks, run them through your agent, and never lose work to a crashed session.

- **Leases with fencing** — every claim gets a versioned lease; a dead worker's stale writes are rejected, so work never runs twice or gets clobbered.
- **Heartbeats + reclamation** — if a process dies, its lease expires and any other session picks the work up (at-least-once).
- **Bounded retries** with exponential backoff, **priorities**, **dependencies** (a quest waits for the quests it depends on), **not-before scheduling**, and **idempotent dedupe keys**.
- **Per-attempt telemetry** kept in SQLite: who ran it, how long, what happened.
- **Daemon** mode drains the queue continuously (one scheduler per project, enforced by a scheduler lease — run it on two machines against a shared disk and only one drains).
- **Zero-dependency TUI** dashboard: filters, search, details, run/retry/cancel.

## Install

```bash
npm install -g questforge     # needs node >= 22
```

## Quick start

```bash
cd your-project
questforge add build "Write unit tests for src/parser.ts" --name tests
questforge run                # claims + executes via your agent, prints result
questforge dash               # TUI: ↑↓ select · ←→ filter · / search · ⏎ details · r/R/x
```

Queue several and let the daemon drain them:

```bash
questforge add explore "Map this codebase" --priority 5
questforge add build "Fix the flaky test" --max-attempts 3
questforge add review "Review the diff" --depends q-abc12345
questforge daemon             # runs everything, respecting priority/deps/backoff
```

## The executor: bring your own agent

By default quests run through **Claude Code**: `claude -p "<task>"`. Configure any agent in `~/.questforge/config.json` (global) or `<project>/.questforge.json` (per project):

```jsonc
{
  "executor": { "command": ["claude", "-p", "{task}"], "timeoutMs": 900000 },
  // or: { "command": ["pi", "--mode", "text", "{task}"] }
  // or: { "command": ["codex", "exec", "{task}"] }
  "executors": {                       // optional per-role override
    "review": { "command": ["claude", "-p", "--model", "opus", "{task}"] }
  }
}
```

Placeholders: `{task}` (includes context), `{context}`, `{project}`, `{role}`. The command runs with the quest's project as cwd; stdout is the stored result; exit 0 = done, non-zero or timeout = failed (and retried if the quest has attempts left).

## Commands

| | |
|---|---|
| `add <role> "task"` | `--name` `--priority N` `--context TEXT` `--depends id1,id2` `--max-attempts N` `--not-before ISO` `--dedupe-key KEY` `--retain` |
| `list` / `show <id>` | queue table (`--json`) / full detail with result and telemetry |
| `run [id]` | claim next eligible (or a specific quest) and execute now |
| `daemon` | drain continuously; Ctrl-C releases cleanly |
| `dash` | interactive TUI |
| `cancel` / `retry` / `consume <id>` | cancel queued work / requeue failed work with one more attempt / release a retained result's dedupe key |
| `events` / `runs <id>` | audit journal / per-attempt telemetry |
| `health` | DB integrity + queue counts |

## Config reference

```jsonc
{
  "dbPath": "~/.questforge/quests.sqlite",  // point at pi's journal to share its queue
  "maxHistory": 500,
  "leaseTtlMs": 120000,
  "heartbeatMs": 30000,
  "maxAttempts": 1,                          // default per-quest retry budget
  "backoffBaseMs": 30000,
  "scheduler": { "pollMs": 5000, "maxConcurrent": 2 },
  "executor": { "command": ["claude", "-p", "{task}"], "timeoutMs": 900000 }
}
```

Quests are **per-project** (keyed by the directory you run commands in), stored in one SQLite database (WAL). Compatible with the [pi-fairy-tales](https://github.com/KcAnom/pi-fairy-tales) quest journal — set `dbPath` to `~/.pi/agent/fairy-tales-quests.sqlite` and questforge and pi share one queue.

## License

MIT
