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

// The installed package without its platform package, as an install with
// --omit=optional, or a lockfile made on another OS, leaves it
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Build, packaging and release,
// Q73: "A missing platform package fails at load with an error that names the
// package and the usual causes"). In the project that installed the tarballs
// (.semaphore/test-npm.sh), the platform package's folder is moved aside, a
// fresh Node.js requires the client there, and its error must name the
// platform package, the usual causes and both remedies; the folder is put
// back whatever happens. The main package has no development build, so
// nothing else can load.
//
// Usage: node missing-platform.js --project <dir> --platform <platform>
// It exits non-zero when the error is not the one expected.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const MAIN = '@confluentinc/kafka-javascript';

/**
 * @param {string[]} argv the arguments
 * @returns {{ project: string, platform: string }} the project and the platform
 */
function parseArgs(argv) {
  /** @type {Record<string, string>} */
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    if (!['--project', '--platform'].includes(argv[i]) || argv[i + 1] === undefined) {
      throw new Error('usage: node missing-platform.js --project <dir> --platform <platform>');
    }
    args[argv[i].slice(2)] = argv[i + 1];
  }
  if (!args.project || !args.platform) throw new Error('--project and --platform are required');
  return { project: fs.realpathSync(args.project), platform: args.platform };
}

function main() {
  const { project, platform } = parseArgs(process.argv.slice(2));
  const platformPackage = `${MAIN}-${platform}`;
  const installed = path.join(project, 'node_modules', ...platformPackage.split('/'));
  const aside = path.join(project, 'node_modules', '@confluentinc', `.${path.basename(installed)}-aside`);
  assert.ok(fs.existsSync(path.join(installed, 'package.json')), `${platformPackage} is not installed in ${project}`);
  fs.renameSync(installed, aside);
  try {
    const r = spawnSync(process.execPath, ['--no-warnings', '-e', `require(${JSON.stringify(`${MAIN}/producer`)})`], {
      cwd: project,
      encoding: 'utf8',
    });
    assert.notEqual(r.status, 0, `requiring ${MAIN}/producer without ${platformPackage} succeeded`);
    const out = r.stderr;
    for (const expected of [
      `cannot load the native addon of ${MAIN} for platform ${platform}: `,
      `${platformPackage} is not installed or does not load, `,
      'The usual causes: the optional dependencies were left out at install (--omit=optional), or the lockfile was made on another OS',
      `Install ${MAIN} on a supported platform, with its optional dependencies`,
      'Or build it from source with `make -C javascript build`',
    ]) {
      assert.ok(out.includes(expected), `the error does not say ${JSON.stringify(expected)}:\n${out}`);
    }
    console.log(`without ${platformPackage}, requiring ${MAIN} fails naming it, the usual causes and both remedies`);
  } finally {
    fs.renameSync(aside, installed);
  }
}

try {
  main();
} catch (error) {
  console.error('missing-platform:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
