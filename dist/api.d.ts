import { type Server } from "node:http";
import type { QuestStore } from "./store.ts";
export interface HttpApiOptions {
    store: QuestStore;
    project: string;
    port?: number;
    host?: string;
}
export interface HttpApiHandle {
    server: Server;
    url: string;
    close(): Promise<void>;
}
export declare function startHttpApi(options: HttpApiOptions): Promise<HttpApiHandle>;
//# sourceMappingURL=api.d.ts.map