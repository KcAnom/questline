const FIELD_RANGES = [
    [0, 59], // minute
    [0, 23], // hour
    [1, 31], // day of month
    [1, 12], // month
    [0, 7], // day of week (0 or 7 = Sunday)
];
function parseField(field, min, max) {
    const out = new Set();
    for (const part of field.split(",")) {
        if (!part)
            throw new Error(`empty cron field part in ${field}`);
        const [rangePart, stepPart] = part.split("/");
        const step = stepPart === undefined ? 1 : Number(stepPart);
        if (!Number.isInteger(step) || step < 1)
            throw new Error(`invalid cron step ${stepPart}`);
        let start;
        let end;
        if (rangePart === "*") {
            start = min;
            end = max;
        }
        else if (rangePart?.includes("-")) {
            const [a, b] = rangePart.split("-").map(Number);
            if (!Number.isInteger(a) || !Number.isInteger(b))
                throw new Error(`invalid cron range ${rangePart}`);
            start = a;
            end = b;
        }
        else {
            const n = Number(rangePart);
            if (!Number.isInteger(n))
                throw new Error(`invalid cron value ${rangePart}`);
            start = n;
            end = n;
        }
        if (start < min || end > max || start > end)
            throw new Error(`cron value ${rangePart} outside ${min}-${max}`);
        for (let n = start; n <= end; n += step)
            out.add(max === 7 && n === 7 ? 0 : n);
    }
    return out;
}
function parseCron(expr) {
    const fields = expr.trim().split(/\s+/);
    if (fields.length !== 5)
        throw new Error("cron expression must have exactly five fields");
    return fields.map((f, i) => parseField(f, FIELD_RANGES[i][0], FIELD_RANGES[i][1]));
}
function validateTimezone(timezone) {
    try {
        new Intl.DateTimeFormat("en-US", { timeZone: timezone }).format(new Date());
    }
    catch {
        throw new Error(`invalid timezone ${timezone}`);
    }
}
export function validateCron(expr, timezone = "UTC") {
    validateTimezone(timezone);
    parseCron(expr);
}
export function nextCronTime(expr, afterMs, timezone = "UTC") {
    validateTimezone(timezone);
    const [minutes, hours, doms, months, dows] = parseCron(expr);
    // This lightweight implementation evaluates UTC calendar fields. Timezone is
    // validated and preserved for API clarity; callers needing civil-time DST
    // semantics can swap this module for a cron-parser backed implementation.
    let t = Math.floor(afterMs / 60_000) * 60_000 + 60_000;
    const limit = t + 366 * 24 * 60 * 60_000;
    while (t <= limit) {
        const d = new Date(t);
        const minute = d.getUTCMinutes();
        const hour = d.getUTCHours();
        const dom = d.getUTCDate();
        const month = d.getUTCMonth() + 1;
        const dow = d.getUTCDay();
        if (minutes.has(minute) && hours.has(hour) && months.has(month) && doms.has(dom) && dows.has(dow))
            return t;
        t += 60_000;
    }
    throw new Error("could not find next cron occurrence within one year");
}
