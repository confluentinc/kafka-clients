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

// The release scripts' registry checks, without the registry:
// .semaphore/check-published-npm.py's own self-test (its version ordering and
// dist-tag rules), and release/publish.sh's read-back of the dist-tags, run
// from a fake bundle against a stub `npm` whose packages are all published
// already (the path a rerun takes; publishing itself asks for a one-time
// password on a terminal).

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const REPO = path.join(__dirname, '..', '..', '..');
const PYTHON = process.env.PYTHON || 'python3';

test("check-published-npm.py's version ordering and dist-tag rules", () => {
  const r = spawnSync(PYTHON, [path.join(REPO, '.semaphore', 'check-published-npm.py'), 'self-test'], {
    encoding: 'utf8',
  });
  assert.equal(r.error, undefined, `cannot run ${PYTHON}: ${r.error}`);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'check-published-npm: self-test passed\n');
});

const MAIN = '@confluentinc/kafka-javascript';
const VERSION = '4.4.0-beta.1';
const PLATFORMS = ['linux-x64-gnu', 'linux-arm64-gnu', 'darwin-arm64'];

// The stub: `npm whoami`, `npm view <pkg>@<v> dist.integrity` and `npm view
// <pkg> dist-tags --json`, answered from files under $STUB_NPM_STATE; any
// other call (a publish) fails. `sleep` returns at once, so the read-back's
// retries take no time.
const STUB_NPM = `#!/bin/sh
state="$STUB_NPM_STATE"
case "$1" in
  whoami) echo tester ;;
  view)
    case "$2" in ?*@*) pkg="\${2%@*}" ;; *) pkg="$2" ;; esac
    file="$state/$(printf %s "$pkg" | tr '/@' '__')"
    case "$3" in
      dist.integrity) [ -f "$file.integrity" ] && cat "$file.integrity" || { echo "npm error code E404" >&2; exit 1; } ;;
      dist-tags) cat "$file.tags" ;;
      *) echo "stub npm: view $3" >&2; exit 1 ;;
    esac ;;
  *) echo "stub npm: $1 not expected" >&2; exit 1 ;;
esac
`;

/** Runs publish.sh in a fresh bundle whose packages are published with `tags(name)`. */
function runPublish(tags) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'publish-sh-'));
  try {
    const bundle = path.join(dir, 'bundle');
    const bin = path.join(dir, 'bin');
    const state = path.join(dir, 'state');
    for (const d of [bundle, bin, state]) fs.mkdirSync(d);
    fs.copyFileSync(path.join(__dirname, '..', '..', 'release', 'publish.sh'), path.join(bundle, 'publish.sh'));
    const sums = [];
    const packages = [...PLATFORMS.map((p) => [`${MAIN}-${p}`, `confluentinc-kafka-javascript-${p}-${VERSION}.tgz`]),
      [MAIN, `confluentinc-kafka-javascript-${VERSION}.tgz`]];
    for (const [name, file] of packages) {
      const data = Buffer.from(`tarball of ${name}`);
      fs.writeFileSync(path.join(bundle, file), data);
      sums.push(`${crypto.createHash('sha256').update(data).digest('hex')}  ${file}`);
      const key = path.join(state, name.replace(/[/@]/g, '_'));
      fs.writeFileSync(`${key}.integrity`, `sha512-${crypto.createHash('sha512').update(data).digest('base64')}`);
      fs.writeFileSync(`${key}.tags`, JSON.stringify(tags(name)));
    }
    fs.writeFileSync(path.join(bundle, 'SHA256SUMS'), `${sums.join('\n')}\n`);
    fs.writeFileSync(path.join(bin, 'npm'), STUB_NPM, { mode: 0o755 });
    fs.writeFileSync(path.join(bin, 'sleep'), '#!/bin/sh\n', { mode: 0o755 });
    return spawnSync('bash', [path.join(bundle, 'publish.sh')], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}${path.delimiter}${process.env.PATH}`, STUB_NPM_STATE: state },
    });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

const GOOD = (name) => (name === MAIN ? { latest: '1.6.0', alpha: '1.0.0-alpha.1', preview: VERSION } : { latest: VERSION, preview: VERSION });

test('publish.sh accepts a rerun whose dist-tags are right', () => {
  const r = runPublish(GOOD);
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /Every package of 4\.4\.0-beta\.1 is already published\./);
  assert.match(r.stdout, new RegExp(`${MAIN}: preview is ${VERSION.replace(/\./g, '\\.')}\n`));
  assert.match(r.stdout, /is published under the preview dist-tag, and latest is on the earlier client/);
});

test('publish.sh refuses when latest moved to this client', () => {
  const r = runPublish((name) => (name === MAIN ? { latest: VERSION, preview: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.equal(
    r.stderr,
    `publish.sh: the latest dist-tag of ${MAIN} is ${VERSION}, but it must stay on the earlier client, below 4.0.0: ` +
      `point it back with \`npm dist-tag add ${MAIN}@<its version> latest\`\n`,
  );
});

test('publish.sh refuses when rc or alpha moved to this client', () => {
  const r = runPublish((name) => (name === MAIN ? { latest: '1.6.0', rc: '4.4.0-rc.1', preview: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.match(r.stderr, /^publish\.sh: the rc dist-tag of @confluentinc\/kafka-javascript is 4\.4\.0-rc\.1, but it must stay/);
});

test('publish.sh refuses when a package was published without the preview tag', () => {
  const platform = `${MAIN}-${PLATFORMS[1]}`;
  const r = runPublish((name) => (name === platform ? { latest: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.equal(r.stderr, `publish.sh: the preview dist-tag of ${platform} is not set, not ${VERSION}\n`);
});
