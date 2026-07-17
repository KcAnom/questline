/** Small formatting helpers (shared by CLI and TUI). */
export function fmtDuration(ms) {
    if (ms < 0)
        ms = 0;
    const s = Math.floor(ms / 1000);
    if (s < 60)
        return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60)
        return `${m}m${s % 60 ? ` ${s % 60}s` : ""}`;
    const h = Math.floor(m / 60);
    return `${h}h${m % 60 ? ` ${m % 60}m` : ""}`;
}
export function fmtTokens(n) {
    if (n >= 1_000_000)
        return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000)
        return `${Math.round(n / 1_000)}k`;
    return String(n);
}
export function fmtUsd(usd) {
    return `$${usd.toFixed(usd >= 1 ? 2 : usd >= 0.01 ? 3 : 4)}`;
}
export function shortModelId(id) {
    const seg = id.split("-").pop() ?? id;
    return seg && seg !== id && !/^\d/.test(seg) ? seg : id;
}
