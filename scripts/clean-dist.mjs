import { rmSync } from "node:fs";
import { basename, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const outputDirectory = resolve(repositoryRoot, "dist");

if (basename(outputDirectory) !== "dist" || outputDirectory === repositoryRoot) {
  throw new Error(`refusing to clean unexpected output directory: ${outputDirectory}`);
}

rmSync(outputDirectory, { recursive: true, force: true });
