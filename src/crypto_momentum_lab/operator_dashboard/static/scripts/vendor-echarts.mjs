import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const staticRoot = resolve(scriptDirectory, "..");
const source = resolve(staticRoot, "node_modules/echarts/dist/echarts.min.js");
const destination = resolve(staticRoot, "vendor/echarts.min.js");

await mkdir(dirname(destination), { recursive: true });
await copyFile(source, destination);
