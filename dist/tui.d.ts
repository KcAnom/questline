/**
 * Minimal zero-dependency TUI host: alternate screen, raw stdin, a repaint
 * loop, and clean teardown on quit or signals. Feeds raw key bytes to the
 * dashboard component and repaints on every key plus a 1s live-refresh tick.
 */
import type { DashboardComponent } from "./dashboard.ts";
export declare function runTui(component: DashboardComponent, opts: {
    onQuit: () => void;
}): {
    stop: () => void;
};
//# sourceMappingURL=tui.d.ts.map