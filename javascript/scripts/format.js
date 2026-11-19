// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// The formatter (root CLAUDE.md, JavaScript Binding Conventions, Build,
// packaging and release, Q74): Prettier, with prettier.config.cjs, over the
// client's TypeScript and JavaScript. `make format-check` (part of
// `lint-javascript`, so of the root `verify`) fails on a file Prettier would
// change; `make format` rewrites them.
//
// A generated file (its header says "GENERATED, DO NOT EDIT") is left out:
// its generator's output is the one form `cargo xtask check-generated`
// accepts. The local-only bench/ and the build outputs are not the client's
// sources.
//
// Usage: node scripts/format.js check|write

const fs = require('node:fs');
const path = require('node:path');
const prettier = require('prettier');

const ROOT = path.join(__dirname, '..');
const DIRS = ['src', 'test', 'scripts', 'verifiable_clients', 'grpc-server', 'tools', 'examples'];
const EXTENSIONS = new Set(['.ts', '.js', '.cjs', '.mjs']);
const SKIPPED = new Set(['node_modules', 'dist', 'build']);

/**
 * Whether `source` is a generated file.
 *
 * @param {string} source the file's text
 * @returns {boolean} whether its header says it is generated
 */
function isGenerated(source) {
  return source.split('\n', 20).some((line) => line.includes('GENERATED, DO NOT EDIT'));
}

/**
 * The files the formatter covers, relative to the client's directory, sorted.
 *
 * @returns {string[]} the files
 */
function files() {
  const found = fs.readdirSync(ROOT).filter((name) => EXTENSIONS.has(path.extname(name)));
  const walk = (dir) => {
    for (const entry of fs.readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
      const rel = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (!SKIPPED.has(entry.name)) walk(rel);
      } else if (EXTENSIONS.has(path.extname(entry.name))) {
        found.push(rel);
      }
    }
  };
  for (const dir of DIRS) walk(dir);
  return found.sort();
}

async function main(mode) {
  if (mode !== 'check' && mode !== 'write') {
    console.error('Usage: node scripts/format.js check|write');
    return 2;
  }
  const unformatted = [];
  let checked = 0;
  for (const file of files()) {
    const filepath = path.join(ROOT, file);
    const source = fs.readFileSync(filepath, 'utf8');
    if (isGenerated(source)) continue;
    checked++;
    const options = { ...(await prettier.resolveConfig(filepath)), filepath };
    if (await prettier.check(source, options)) continue;
    unformatted.push(file);
    if (mode === 'write') fs.writeFileSync(filepath, await prettier.format(source, options));
  }
  if (mode === 'write') {
    console.log(`format: ${unformatted.length} of ${checked} file(s) rewritten`);
    return 0;
  }
  if (unformatted.length === 0) {
    console.log(`✅ format-check: ${checked} file(s) formatted`);
    return 0;
  }
  for (const file of unformatted) console.error(`format-check: ${file} is not formatted`);
  console.error(`format-check: ${unformatted.length} of ${checked} file(s) not formatted: run \`make -C javascript format\``);
  return 1;
}

main(process.argv[2]).then(
  (status) => {
    process.exitCode = status;
  },
  (error) => {
    console.error(error);
    process.exitCode = 1;
  },
);
