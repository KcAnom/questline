import type { QuestRecord, QuestRunRecord, QuestStore } from "./store.ts";
export interface DashTheme {
    accent(s: string): string;
    dim(s: string): string;
    text(s: string): string;
    muted(s: string): string;
    success(s: string): string;
    error(s: string): string;
    warning(s: string): string;
    bold(s: string): string;
}
export declare const PLAIN_THEME: DashTheme;
export declare const ANSI_THEME: DashTheme;
export declare const QUEST_FILTERS: readonly ["all", "active", "failed", "done", "cancelled"];
export type QuestFilter = (typeof QUEST_FILTERS)[number];
export declare function filterQuests(rows: QuestRecord[], filter: QuestFilter, search: string): QuestRecord[];
export declare function questRowText(q: QuestRecord, now?: number): string;
export declare function questTableText(rows: QuestRecord[], filter?: QuestFilter, search?: string): string;
export declare function questDetailLines(q: QuestRecord, runs: QuestRunRecord[], events: Array<{
    event: string;
    at: number;
    data: unknown;
}>, artifacts?: Array<{
    kind: string;
    attempt: number;
    path: string;
    bytes: number;
}>): string[];
export interface DashboardActions {
    run?: (id: string) => string | undefined;
    retry?: (id: string) => string | undefined;
    cancel?: (id: string) => string | undefined;
}
export interface DashboardOptions {
    store: QuestStore;
    project: string;
    theme?: DashTheme;
    actions?: DashboardActions;
    onQuit?: () => void;
    rows?: () => number;
    now?: () => number;
}
export interface DashboardComponent {
    render(width: number): string[];
    handleInput(data: string): void;
    readonly state: {
        filter: QuestFilter;
        search: string;
        searchMode: boolean;
        selected: number;
        detail: boolean;
        status?: string;
    };
}
export declare function dashboard(opts: DashboardOptions): DashboardComponent;
//# sourceMappingURL=dashboard.d.ts.map