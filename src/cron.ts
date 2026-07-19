/**
 * Cron helpers with IANA timezone + DST awareness via cron-parser.
 * Expressions are standard five-field cron: minute hour dom month dow.
 */
import { CronExpressionParser } from "cron-parser";

export type RecurrenceCatchUp = "one" | "all" | "skip";

function validateTimezone(timezone: string): void {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: timezone }).format(new Date());
  } catch {
    throw new Error(`invalid timezone ${timezone}`);
  }
}

function parse(expr: string, timezone: string, currentDate?: Date) {
  const fields = expr.trim().split(/\s+/);
  if (fields.length !== 5) throw new Error("cron expression must have exactly five fields");
  // cron-parser accepts 5-field expressions; force currentDate for deterministic next().
  return CronExpressionParser.parse(expr.trim(), {
    currentDate: currentDate ?? new Date(),
    tz: timezone,
  });
}

export function validateCron(expr: string, timezone = "UTC"): void {
  validateTimezone(timezone);
  // Force a next() so invalid fields fail early.
  parse(expr, timezone, new Date(0)).next();
}

/**
 * First occurrence strictly after `afterMs` in the given IANA timezone.
 * DST transitions are handled by cron-parser (civil time in `timezone`).
 */
export function nextCronTime(expr: string, afterMs: number, timezone = "UTC"): number {
  validateTimezone(timezone);
  // currentDate is inclusive of that instant in cron-parser; advance 1ms so we
  // always return a time strictly after `afterMs`.
  const interval = parse(expr, timezone, new Date(afterMs + 1));
  const next = interval.next().toDate().getTime();
  if (!Number.isFinite(next) || next <= afterMs) {
    throw new Error("could not find next cron occurrence");
  }
  return next;
}
