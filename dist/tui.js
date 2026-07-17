const ALT_ON = "\x1b[?1049h\x1b[?25l";
const ALT_OFF = "\x1b[?1049l\x1b[?25h";
export function runTui(component, opts) {
    const out = process.stdout;
    const inp = process.stdin;
    let stopped = false;
    const paint = () => {
        if (stopped)
            return;
        // Some PTYs (script, CI) report 0 columns/rows — fall back, never blank.
        const width = out.columns && out.columns > 10 ? out.columns : 100;
        const height = out.rows && out.rows > 4 ? out.rows : 30;
        const lines = component.render(width).slice(0, height);
        out.write(`\x1b[H\x1b[2J${lines.join("\r\n")}`);
    };
    const stop = () => {
        if (stopped)
            return;
        stopped = true;
        clearInterval(tick);
        inp.off("data", onData);
        if (inp.isTTY)
            inp.setRawMode(false);
        inp.pause();
        out.write(ALT_OFF);
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
        opts.onQuit();
    };
    const onData = (buf) => {
        const data = buf.toString("utf8");
        if (data === "\x03") {
            stop();
            return;
        } // ctrl-c always exits
        component.handleInput(data);
        if (!stopped)
            paint();
    };
    out.write(ALT_ON);
    if (inp.isTTY)
        inp.setRawMode(true);
    inp.resume();
    inp.on("data", onData);
    process.on("SIGINT", stop);
    process.on("SIGTERM", stop);
    const tick = setInterval(paint, 1000);
    paint();
    return { stop };
}
