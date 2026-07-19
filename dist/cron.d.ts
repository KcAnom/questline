export type RecurrenceCatchUp = "one" | "all" | "skip";
export declare function validateCron(expr: string, timezone?: string): void;
/**
 * First occurrence strictly after `afterMs` in the given IANA timezone.
 * DST transitions are handled by cron-parser (civil time in `timezone`).
 */
export declare function nextCronTime(expr: string, afterMs: number, timezone?: string): number;
//# sourceMappingURL=cron.d.ts.map