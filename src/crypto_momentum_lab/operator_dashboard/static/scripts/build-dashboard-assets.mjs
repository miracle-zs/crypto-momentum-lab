import { build } from "esbuild";
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const staticRoot = resolve(scriptDirectory, "..");
const assetsRoot = resolve(staticRoot, "assets");
const vendorRoot = resolve(staticRoot, "vendor");

function digest(contents) {
  return createHash("sha256").update(contents).digest("hex").slice(0, 16);
}

async function bundle(entryPoint, prefix, { globalName } = {}) {
  const temporary = resolve(assetsRoot, `.${prefix}.tmp.js`);
  await build({
    bundle: true,
    entryPoints: [entryPoint],
    format: "iife",
    globalName,
    minify: true,
    outfile: temporary,
    platform: "browser",
    target: ["es2022"],
  });
  const contents = await readFile(temporary);
  const filename = `${prefix}-${digest(contents)}.js`;
  await rename(temporary, resolve(assetsRoot, filename));
  return { contents, filename };
}

function replaceAssetReference(index, pattern, replacement) {
  if (!pattern.test(index)) {
    throw new Error(`Dashboard index is missing asset reference: ${pattern}`);
  }
  return index.replace(pattern, replacement);
}

await mkdir(assetsRoot, { recursive: true });
await mkdir(vendorRoot, { recursive: true });
for (const filename of await readdir(assetsRoot)) {
  if (/^(?:dashboard|echarts)-[a-f0-9]{16}\.(?:css|js)$/.test(filename)) {
    await rm(resolve(assetsRoot, filename));
  }
}

const echarts = await bundle(
  resolve(scriptDirectory, "echarts-runtime.js"),
  "echarts",
  { globalName: "CmlEchartsRuntime" },
);
await writeFile(resolve(vendorRoot, "echarts.min.js"), echarts.contents);

const dashboard = await bundle(resolve(staticRoot, "dashboard.js"), "dashboard");
const cssContents = await readFile(resolve(staticRoot, "dashboard.css"));
const cssFilename = `dashboard-${digest(cssContents)}.css`;
await writeFile(resolve(assetsRoot, cssFilename), cssContents);

let index = await readFile(resolve(staticRoot, "index.html"), "utf8");
index = replaceAssetReference(
  index,
  /static\/(?:assets\/)?dashboard-[a-f0-9]+\.css(?:\?v=[^"']+)?|static\/dashboard\.css(?:\?v=[^"']+)?/,
  `static/assets/${cssFilename}`,
);
index = replaceAssetReference(
  index,
  /static\/(?:assets\/)?echarts-[a-f0-9]+\.js(?:\?v=[^"']+)?|static\/vendor\/echarts\.min\.js(?:\?v=[^"']+)?/,
  `static/assets/${echarts.filename}`,
);
index = replaceAssetReference(
  index,
  /static\/(?:assets\/)?dashboard-[a-f0-9]+\.js(?:\?v=[^"']+)?|static\/dashboard\.js(?:\?v=[^"']+)?/,
  `static/assets/${dashboard.filename}`,
);
await writeFile(resolve(staticRoot, "index.html"), index);

console.log(`Built ${dashboard.filename}, ${echarts.filename}, and ${cssFilename}`);
