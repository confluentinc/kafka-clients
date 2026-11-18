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

// The unit and typing tests against the installed tarballs, not the
// repository's build: a file the published `files` list leaves out, or a
// path that only works in the repository, then fails a real test. In the
// project that installed the tarballs (.semaphore/test-npm.sh) it builds a
// copy of the repository, `<project>/repo`:
//   - javascript/ holds the published package as it was installed (its
//     package.json, the published one, and dist/, copied from
//     node_modules/@confluentinc/kafka-javascript), and copies of the code
//     the tests run (test/, scripts/, grpc-server/, verifiable_clients/,
//     examples/, jest.config.cjs, tsconfig.json), so Node resolves the
//     package's own name (its self-reference) to that dist/;
//   - everything else, the docs and the sources the tests read as data,
//     src/ among them, is a symbolic link to the repository's, as are the
//     repository's other top-level folders (kafka/, rust/, ...), and
//     javascript/node_modules is the repository's devDependencies (Jest,
//     TypeScript), which hold no copy of the package;
//   - there is no native/: the addon is the one of the installed platform
//     package, which Node finds in <project>/node_modules.
// Then Jest runs the unit and typing projects there. One test is left out,
// as it asserts the repository's own build: "this checkout loads its
// development build, as an instance of its own".
//
// Usage: node installed-suites.js --project <dir>
// It exits with Jest's status.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const MAIN = '@confluentinc/kafka-javascript';
const JS = path.resolve(__dirname, '..', '..');
const REPO = path.dirname(JS);
// Copied: code that resolves the package from where it lies, and Jest's
// configuration, whose rootDir is its own folder.
const COPIED = ['test', 'scripts', 'grpc-server', 'verifiable_clients', 'examples', 'jest.config.cjs', 'tsconfig.json'];
// Neither copied nor linked: the repository's build and its addon.
const LEFT_OUT = ['dist', 'native', 'build', 'node_modules', 'package.json', 'package-lock.json'];
const REPOSITORY_ONLY = ['this checkout loads its development build, as an instance of its own'];

/**
 * @param {string[]} argv the arguments
 * @returns {string} the project
 */
function parseArgs(argv) {
  if (argv.length !== 2 || argv[0] !== '--project') throw new Error('usage: node installed-suites.js --project <dir>');
  return fs.realpathSync(argv[1]);
}

/**
 * A regular expression for Jest's --testNamePattern matching every test but those named.
 *
 * @param {string[]} names the tests left out
 * @returns {string} the pattern
 */
function allBut(names) {
  const escaped = names.map((n) => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  return `^(?!(${escaped.join('|')})$)`;
}

function main() {
  const project = parseArgs(process.argv.slice(2));
  const installed = fs.realpathSync(path.join(project, 'node_modules', ...MAIN.split('/')));
  const jest = path.join(JS, 'node_modules', 'jest', 'bin', 'jest.js');
  assert.ok(fs.existsSync(jest), `${jest} is missing: run \`npm ci\` in javascript/ first`);

  const root = path.join(project, 'repo');
  const suite = path.join(root, 'javascript');
  fs.rmSync(root, { recursive: true, force: true });
  fs.mkdirSync(suite, { recursive: true });
  for (const entry of fs.readdirSync(REPO)) {
    if (entry !== 'javascript' && entry !== '.git') fs.symlinkSync(path.join(REPO, entry), path.join(root, entry));
  }
  for (const entry of fs.readdirSync(JS)) {
    if (LEFT_OUT.includes(entry)) continue;
    const from = path.join(JS, entry);
    if (COPIED.includes(entry)) fs.cpSync(from, path.join(suite, entry), { recursive: true, dereference: true });
    else fs.symlinkSync(from, path.join(suite, entry));
  }
  fs.symlinkSync(path.join(JS, 'node_modules'), path.join(suite, 'node_modules'));
  fs.copyFileSync(path.join(installed, 'package.json'), path.join(suite, 'package.json'));
  fs.cpSync(path.join(installed, 'dist'), path.join(suite, 'dist'), { recursive: true });
  fs.mkdirSync(path.join(suite, 'build'));

  console.log(`the unit and typing tests, in ${suite}, against ${MAIN} as installed in ${project}`);
  console.log(`left out, as it asserts the repository's own build: ${REPOSITORY_ONLY.join(', ')}`);
  const r = spawnSync(
    process.execPath,
    [jest, '--ci', '--selectProjects', 'unit', 'typing', '--testNamePattern', allBut(REPOSITORY_ONLY)],
    { cwd: suite, stdio: 'inherit' },
  );
  process.exitCode = r.status ?? 1;
}

try {
  main();
} catch (error) {
  console.error('installed-suites:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
