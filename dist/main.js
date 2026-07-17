import { main } from "./cli.js";
main(process.argv.slice(2)).then((code) => { process.exitCode = code; }, (err) => { console.error(String(err)); process.exitCode = 1; });
