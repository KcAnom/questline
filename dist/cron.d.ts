export type RecurrenceCatchUp = "one" | "all" | "skip";
export declare function validateCron(expr: string, timezone?: string): void;
export declare function nextCronTime(expr: string, afterMs: number, timezone?: string): number;
//# sourceMappingURL=cron.d.ts.map