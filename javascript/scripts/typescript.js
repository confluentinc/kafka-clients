// Copyright 2025 Confluent Inc.
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

// Runs the TypeScript compiler for the typing tests (test/typing), which
// type-check a consumer's code against the compiled declarations in dist/ in
// several consumer configurations. The compiler is a devDependency: `TSC`
// names the command (default node_modules/.bin/tsc) and `TS_TYPE_ROOTS` the
// directory holding @types/node (default node_modules/@types). The tsconfig
// files are written under build/, which is git-ignored. (The library build
// itself is `make build-ts`, with javascript/tsconfig.json.)

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');

/** The command that runs tsc. */
function tscCommand() {
  return process.env.TSC || path.join(ROOT, 'node_modules', '.bin', 'tsc');
}

/** The typeRoots holding @types/node. */
function typeRoots() {
  return path.resolve(ROOT, process.env.TS_TYPE_ROOTS || path.join('node_modules', '@types'));
}

/** Runs tsc with `args`; returns {status, output}. */
function runTsc(args) {
  const command = [tscCommand(), ...args.map((a) => JSON.stringify(a))].join(' ');
  const r = spawnSync(command, { cwd: ROOT, encoding: 'utf8', shell: true });
  return { status: r.status, output: `${r.stdout || ''}${r.stderr || ''}` };
}

/** Whether tsc can be run at all. */
function tscAvailable() {
  return runTsc(['--version']).status === 0;
}

/** Writes `config` as `<dir>/tsconfig.json` (dir created) and returns its path. */
function writeConfig(dir, config) {
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, 'tsconfig.json');
  fs.writeFileSync(file, `${JSON.stringify(config, null, 2)}\n`);
  return file;
}

module.exports = { ROOT, tscCommand, typeRoots, runTsc, tscAvailable, writeConfig };
