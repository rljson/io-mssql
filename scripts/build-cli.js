// Bundles src/cli.ts + all dependencies into a single dist/cli.cjs file
// using esbuild. Run via: node scripts/build-cli.js

import { build } from 'esbuild';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '..');

mkdirSync(resolve(root, 'dist'), { recursive: true });

await build({
  entryPoints: [resolve(root, 'src', 'cli.ts')],
  bundle: true,
  platform: 'node',
  target: ['node22'],
  outfile: resolve(root, 'dist', 'cli.cjs'),
  format: 'cjs',
  minify: false,
  sourcemap: false,
  define: {
    'process.env.NODE_ENV': '"production"',
  },
});

console.log('Bundle written → dist/cli.cjs');
