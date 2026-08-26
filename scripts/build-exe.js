// Creates dist/io-mssql.exe using Node.js Single Executable Application (SEA).
// Requires: dist/cli.cjs already built (run build-cli.js first).
// Run via: node scripts/build-exe.js

import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root      = resolve(__dirname, '..');
const dist      = resolve(root,  'dist');
const cliBundle = resolve(dist,  'cli.cjs');
const blobFile  = resolve(root,  'sea-prep.blob');
const seaCfgFile= resolve(root,  'sea-config.json');
const exeFile   = resolve(dist,  'io-mssql.exe');

// ── 1. Write SEA config ────────────────────────────────────────────────────
writeFileSync(seaCfgFile, JSON.stringify({
  main:   cliBundle,
  output: blobFile,
  disableExperimentalSEAWarning: true,
}, null, 2));

// ── 2. Generate blob ───────────────────────────────────────────────────────
console.log('Generating SEA blob…');
run('node', ['--experimental-sea-config', seaCfgFile]);

// ── 3. Copy node.exe → dist/io-mssql.exe ─────────────────────────────────
console.log(`Copying ${process.execPath} → ${exeFile}`);
copyFileSync(process.execPath, exeFile);

// ── 4. Remove code signature (Windows — signtool may not be present) ──────
const st = spawnSync('signtool', ['remove', '/s', exeFile], { stdio: 'pipe' });
if (st.error) {
  console.log('(signtool not found — skipping signature removal)');
}

// ── 5. Inject blob via postject ────────────────────────────────────────────
console.log('Injecting blob…');
const { inject } = await import('postject');
await inject(
  exeFile,
  'NODE_SEA_BLOB',
  readFileSync(blobFile),
  { sentinelFuse: 'NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2' },
);

// ── Cleanup ────────────────────────────────────────────────────────────────
rmSync(seaCfgFile, { force: true });
rmSync(blobFile,   { force: true });

console.log(`\nExe ready → ${exeFile}`);

// ── helper ─────────────────────────────────────────────────────────────────
function run(cmd, args) {
  const result = spawnSync(cmd, args, { stdio: 'inherit' });
  if (result.status !== 0) {
    throw new Error(`${cmd} ${args.join(' ')} exited with code ${result.status}`);
  }
}
