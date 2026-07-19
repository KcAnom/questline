import { createServer } from "node:http";
async function readJson(req) {
    const chunks = [];
    for await (const chunk of req)
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    if (!chunks.length)
        return {};
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
function send(res, status, body) {
    res.statusCode = status;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify(body, null, 2));
}
export async function startHttpApi(options) {
    const { store, project } = options;
    const server = createServer(async (req, res) => {
        try {
            const url = new URL(req.url ?? "/", "http://localhost");
            const parts = url.pathname.split("/").filter(Boolean);
            if (req.method === "GET" && url.pathname === "/health")
                return send(res, 200, store.health(project));
            if (req.method === "GET" && url.pathname === "/quests")
                return send(res, 200, store.list(project, Number(url.searchParams.get("limit") ?? 100)));
            if (req.method === "POST" && url.pathname === "/quests") {
                const body = await readJson(req);
                if (typeof body.role !== "string" || typeof body.task !== "string")
                    return send(res, 400, { error: "role and task are required" });
                const q = store.enqueue({ ...body, project, role: body.role, task: body.task });
                return send(res, 201, q);
            }
            if (req.method === "POST" && url.pathname === "/pause") {
                const body = await readJson(req);
                return send(res, 200, store.pauseProject(project, typeof body.reason === "string" ? body.reason : undefined));
            }
            if (req.method === "POST" && url.pathname === "/resume")
                return send(res, 200, store.resumeProject(project));
            if (parts[0] === "quests" && parts[1]) {
                const id = parts[1];
                if (req.method === "GET" && parts.length === 2) {
                    const q = store.get(id);
                    if (!q || q.project !== project)
                        return send(res, 404, { error: "not found" });
                    return send(res, 200, { quest: q, runs: store.runs(id), events: store.events(id), artifacts: store.artifacts(id) });
                }
                if (req.method === "POST" && parts[2] === "cancel")
                    return send(res, store.cancel(id, project) ? 200 : 409, { ok: store.get(id)?.state === "cancelled" });
                if (req.method === "POST" && parts[2] === "retry")
                    return send(res, store.requeue(id, project) ? 200 : 409, { ok: store.get(id)?.state === "queued" });
            }
            send(res, 404, { error: "not found" });
        }
        catch (err) {
            send(res, 500, { error: String(err) });
        }
    });
    const host = options.host ?? "127.0.0.1";
    const port = options.port ?? 0;
    await new Promise((resolveListen) => server.listen(port, host, resolveListen));
    const addr = server.address();
    const actualPort = typeof addr === "object" && addr ? addr.port : port;
    return {
        server,
        url: `http://${host}:${actualPort}`,
        close: () => new Promise((resolveClose) => server.close(() => resolveClose())),
    };
}
