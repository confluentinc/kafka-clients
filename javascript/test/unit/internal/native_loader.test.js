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

// The addon loader: the platform key and the order the addon is looked for
// in, driven with a stand-in `require` and process, so no platform package
// and no other platform is needed.

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const {
  ADDON_FILE,
  SUPPORTED_PLATFORMS,
  addonCandidates,
  linuxLibc,
  loadAddon,
  platformKey,
} = require('../../../dist/internal/native_loader');

/** A process report whose header has `glibcVersionRuntime` (or not), recording excludeNetwork at the call. */
function reportOf(header, seen = []) {
  return {
    excludeNetwork: false,
    getReport() {
      seen.push(this.excludeNetwork);
      return { header };
    },
  };
}

test('the platform key is <platform>-<arch>, with the C library on Linux', () => {
  assert.equal(platformKey({ platform: 'darwin', arch: 'arm64' }), 'darwin-arm64');
  assert.equal(platformKey({ platform: 'darwin', arch: 'x64' }), 'darwin-x64');
  assert.equal(platformKey({ platform: 'win32', arch: 'x64' }), 'win32-x64');
  assert.equal(platformKey({ platform: 'linux', arch: 'x64' }, () => 'gnu'), 'linux-x64-gnu');
  assert.equal(platformKey({ platform: 'linux', arch: 'arm64' }, () => 'gnu'), 'linux-arm64-gnu');
  assert.equal(platformKey({ platform: 'linux', arch: 'x64' }, () => 'musl'), 'linux-x64-musl');
  let asked = false;
  platformKey({ platform: 'darwin', arch: 'arm64' }, () => {
    asked = true;
    return 'gnu';
  });
  assert.equal(asked, false, 'the C library is read on Linux only');
});

test('the supported platforms are the preview\'s, one folder each in npm/', () => {
  assert.deepEqual([...SUPPORTED_PLATFORMS], ['linux-x64-gnu', 'darwin-arm64']);
  const npmDir = require('node:path').join(__dirname, '..', '..', '..', 'npm');
  const folders = require('node:fs').readdirSync(npmDir, { withFileTypes: true }).filter((e) => e.isDirectory()).map((e) => e.name);
  assert.deepEqual(folders.sort(), [...SUPPORTED_PLATFORMS].sort());
});

test('glibc is read from the process report with the network section excluded, then restored', () => {
  const seen = [];
  const report = reportOf({ glibcVersionRuntime: '2.28' }, seen);
  const noLib = () => assert.fail('/lib is not read when the report answers');
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64', report }, noLib), 'gnu');
  assert.deepEqual(seen, [true]);
  assert.equal(report.excludeNetwork, false);
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64', report: reportOf({}) }, noLib), 'musl');
});

test('without a report, musl is a /lib/ld-musl-* loader, and anything else glibc', () => {
  const failing = {
    excludeNetwork: true,
    getReport() {
      throw new Error('no report');
    },
  };
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64', report: failing }, () => ['ld-musl-x86_64.so.1']), 'musl');
  assert.equal(failing.excludeNetwork, true, 'excludeNetwork is restored after a failing report');
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64', report: failing }, () => ['ld-linux-x86-64.so.2']), 'gnu');
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64' }, () => []), 'gnu');
  const unreadable = () => {
    throw new Error('EACCES');
  };
  assert.equal(linuxLibc({ platform: 'linux', arch: 'x64' }, unreadable), 'gnu');
});

test('the platform package comes first, then the development build', () => {
  assert.deepEqual(addonCandidates('linux-x64-gnu', '/pkg'), [
    `@confluentinc/kafka-javascript-linux-x64-gnu/${ADDON_FILE}`,
    path.join('/pkg', 'native', 'build', 'Release', ADDON_FILE),
  ]);
});

test('the platform package is loaded when it loads, and the development build is not tried', () => {
  const tried = [];
  const loaded = loadAddon(
    (id) => {
      tried.push(id);
      return { from: id };
    },
    'darwin-arm64',
    '/pkg',
  );
  assert.deepEqual(tried, [`@confluentinc/kafka-javascript-darwin-arm64/${ADDON_FILE}`]);
  assert.deepEqual(loaded, { addon: { from: tried[0] }, path: tried[0] });
});

test('the development build is loaded when the platform package does not load', () => {
  const tried = [];
  const dev = path.join('/pkg', 'native', 'build', 'Release', ADDON_FILE);
  const loaded = loadAddon(
    (id) => {
      tried.push(id);
      if (id !== dev) throw Object.assign(new Error(`Cannot find module '${id}'`), { code: 'MODULE_NOT_FOUND' });
      return { from: id };
    },
    'linux-x64-gnu',
    '/pkg',
  );
  assert.deepEqual(tried, [`@confluentinc/kafka-javascript-linux-x64-gnu/${ADDON_FILE}`, dev]);
  assert.equal(loaded.path, dev);
});

test('when nothing loads, the error names the platform package, the usual causes, the supported ones and both remedies', () => {
  const failures = [new Error('first'), new Error('second')];
  let i = 0;
  const fail = () => {
    throw failures[i++];
  };
  assert.throws(
    () => loadAddon(fail, 'linux-x64-gnu', '/pkg'),
    (e) => {
      assert.match(e.message, /^cannot load the native addon of @confluentinc\/kafka-javascript for platform linux-x64-gnu: /);
      assert.match(e.message, /@confluentinc\/kafka-javascript-linux-x64-gnu is not installed or does not load, /);
      assert.match(e.message, /the development build \/pkg\/native\/build\/Release\/confluent-kafka-javascript\.node does not load either/);
      assert.match(
        e.message,
        /The usual causes: the optional dependencies were left out at install \(--omit=optional\), or the lockfile was made on another OS, so it has no entry for this platform package\. /,
      );
      assert.match(e.message, /Install @confluentinc\/kafka-javascript on a supported platform, with its optional dependencies/);
      assert.match(e.message, /give npm that machine's `--os`, `--cpu` and `--libc`/);
      assert.match(e.message, /Or build it from source with `make -C javascript build`/);
      assert.match(e.message, /Supported platforms: linux-x64-gnu, darwin-arm64$/);
      assert.ok(e.cause instanceof AggregateError);
      assert.deepEqual(e.cause.errors, failures);
      return true;
    },
  );
  i = 0;
  // Not in the preview: linux-arm64 has no platform package (npm/).
  assert.throws(
    () => loadAddon(fail, 'linux-arm64-gnu', '/pkg'),
    /there is no @confluentinc\/kafka-javascript-linux-arm64-gnu, the platform is not supported/,
  );
  i = 0;
  assert.throws(
    () => loadAddon(fail, 'win32-x64', '/pkg'),
    (e) => {
      assert.match(e.message, /there is no @confluentinc\/kafka-javascript-win32-x64, the platform is not supported/);
      assert.doesNotMatch(e.message, /The usual causes/);
      assert.match(e.message, /Or build it from source with `make -C javascript build`/);
      assert.match(e.message, /Supported platforms: linux-x64-gnu, darwin-arm64$/);
      return true;
    },
  );
});

test('this checkout loads its development build', () => {
  // The unit tests run against native/build/Release/ (`make -C javascript build`).
  const loaded = Object.keys(require.cache).filter((p) => p.endsWith(ADDON_FILE));
  require('../../../dist/internal/native');
  const now = Object.keys(require.cache).filter((p) => p.endsWith(ADDON_FILE));
  assert.ok(loaded.length <= 1);
  assert.deepEqual(now, [path.join(__dirname, '..', '..', '..', 'native', 'build', 'Release', ADDON_FILE)]);
});
