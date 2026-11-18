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

// The release scripts, without the registry: .semaphore/check-published-npm.py's
// own self-test (its version ordering and dist-tag rules), check-npm.py's
// dist-tag rule (a pre-release under beta only, never latest; 1.x fixes
// under legacy), and .semaphore/publish-npm.sh, the Semaphore publish job, run
// over a fake recorded release against a stub `npm`: a first publish, a rerun,
// and its read-back of the dist-tags.

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
const PLATFORMS = ['linux-x64-gnu', 'darwin-arm64'];

test("check-npm.py dist-tag: beta for a pre-release, never latest; latest for a release; legacy for 1.x", () => {
  const run = (...args) =>
    spawnSync(PYTHON, [path.join(REPO, '.semaphore', 'check-npm.py'), 'dist-tag', ...args], { encoding: 'utf8', cwd: REPO });
  for (const [version, tag] of [['4.4.0-beta.1', 'beta'], ['4.4.0-rc.1', 'beta'], ['4.5.0', 'latest'], ['1.6.1', 'legacy']]) {
    const r = run(version);
    assert.equal(r.status, 0, r.stderr);
    assert.equal(r.stdout, `${tag}\n`, version);
    assert.equal(run(version, '--tag', tag).status, 0, `${version} --tag ${tag}`);
  }
  const latest = run('4.4.0-beta.1', '--tag', 'latest');
  assert.equal(latest.status, 1);
  assert.equal(
    latest.stderr,
    "check-npm: refusing the dist-tag 'latest': 4.4.0-beta.1 is a pre-release, which goes out under beta only, never latest\n",
  );
  const legacy = run('1.6.1', '--tag', 'latest');
  assert.equal(legacy.status, 1);
  assert.match(legacy.stderr, /1\.6\.1 is a fix of the earlier client \(below 4\.0\.0\), which goes out under legacy/);
  assert.equal(run('4.4', '--tag', 'beta').status, 1);
});

// The stub: `npm whoami`, `npm view <pkg>@<v> dist.integrity`, `npm view <pkg>
// dist-tags --json` and `npm publish <tarball> ...`, answered from files
// under $STUB_NPM_STATE. A publish of a tarball moves its package's
// `.pending-*` files into place, as the registry would, and logs its
// arguments; it fails without --provenance=false or without a token in the
// user config. `sleep` returns at once, so the read-back's retries take no
// time.
const STUB_NPM = `#!/bin/sh
state="$STUB_NPM_STATE"
case "$1" in
  whoami)
    grep -q '_authToken=\${NPM_TOKEN}' "$NPM_CONFIG_USERCONFIG" || { echo "npm error code ENEEDAUTH" >&2; exit 1; }
    [ -n "$NPM_TOKEN" ] || { echo "npm error code ENEEDAUTH" >&2; exit 1; }
    echo tester ;;
  view)
    case "$2" in ?*@*) pkg="\${2%@*}" ;; *) pkg="$2" ;; esac
    file="$state/$(printf %s "$pkg" | tr '/@' '__')"
    case "$3" in
      dist.integrity) [ -f "$file.integrity" ] && cat "$file.integrity" || { echo "npm error code E404" >&2; exit 1; } ;;
      dist-tags) cat "$file.tags" ;;
      *) echo "stub npm: view $3" >&2; exit 1 ;;
    esac ;;
  publish)
    echo "$*" >> "$state/calls"
    case " $* " in *" --provenance=false "*) ;; *) echo "stub npm: publish without --provenance=false" >&2; exit 1 ;; esac
    key="$(cat "$state/tarball_$(basename "$2")")"
    mv "$key.pending-integrity" "$key.integrity" && mv "$key.pending-tags" "$key.tags" ;;
  *) echo "stub npm: $1 not expected" >&2; exit 1 ;;
esac
`;

/**
 * Runs publish-npm.sh over a fresh recorded release of `version`, whose
 * packages are published with `tags(name)` (`published` true) or published by
 * the run, ending with those tags (`published` false), with `token` as
 * NPM_TOKEN (`null`: unset; `undefined` would take the default). Returns the
 * result and the stub's logged publish calls.
 */
function runPublish(tags, { version = VERSION, published = true, token = 'npm_token', check = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'publish-npm-'));
  try {
    const release = path.join(dir, 'release');
    const bin = path.join(dir, 'bin');
    const state = path.join(dir, 'state');
    for (const d of [release, bin, state]) fs.mkdirSync(d);
    const sums = [];
    const packages = [...PLATFORMS.map((p) => [`${MAIN}-${p}`, `confluentinc-kafka-javascript-${p}-${version}.tgz`]),
      [MAIN, `confluentinc-kafka-javascript-${version}.tgz`]];
    for (const [name, file] of packages) {
      const data = Buffer.from(`tarball of ${name}`);
      fs.writeFileSync(path.join(release, file), data);
      sums.push(`${crypto.createHash('sha256').update(data).digest('hex')}  ${file}`);
      const key = path.join(state, name.replace(/[/@]/g, '_'));
      const suffix = published ? '' : '.pending-';
      fs.writeFileSync(`${key}${suffix || '.'}integrity`, `sha512-${crypto.createHash('sha512').update(data).digest('base64')}`);
      fs.writeFileSync(`${key}${suffix || '.'}tags`, JSON.stringify(tags(name)));
      fs.writeFileSync(path.join(state, `tarball_${file}`), key);
    }
    fs.writeFileSync(path.join(release, 'SHA256SUMS'), `${sums.sort((a, b) => (a.split('  ')[1] < b.split('  ')[1] ? -1 : 1)).join('\n')}\n`);
    fs.writeFileSync(path.join(bin, 'npm'), STUB_NPM, { mode: 0o755 });
    fs.writeFileSync(path.join(bin, 'sleep'), '#!/bin/sh\n', { mode: 0o755 });
    const env = {
      ...process.env,
      PATH: `${bin}${path.delimiter}${process.env.PATH}`,
      STUB_NPM_STATE: state,
      SEMAPHORE_GIT_TAG_NAME: `javascript/v${version}`,
    };
    if (token === null) delete env.NPM_TOKEN;
    else env.NPM_TOKEN = token;
    const args = [path.join(REPO, '.semaphore', 'publish-npm.sh'), ...(check ? ['--check'] : []), release];
    const r = spawnSync('bash', args, { encoding: 'utf8', cwd: REPO, env });
    const callsFile = path.join(state, 'calls');
    const calls = fs.existsSync(callsFile) ? fs.readFileSync(callsFile, 'utf8').trim().split('\n') : [];
    return { ...r, calls };
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

const GOOD = (name) => (name === MAIN ? { latest: '1.6.0', alpha: '1.0.0-alpha.1', beta: VERSION } : { latest: VERSION, beta: VERSION });

test('publish-npm.sh publishes the platform packages, then the main package, under beta, with no provenance', () => {
  const r = runPublish(GOOD, { published: false });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(
    r.calls.map((c) => c.split(' ')[1]),
    [...PLATFORMS.map((p) => `confluentinc-kafka-javascript-${p}-${VERSION}.tgz`), `confluentinc-kafka-javascript-${VERSION}.tgz`],
  );
  for (const call of r.calls) {
    assert.match(call, / --access public --tag beta --provenance=false --registry https:\/\/registry\.npmjs\.org\/ /);
    assert.doesNotMatch(call, /--provenance( |$)|--otp/);
  }
  assert.match(r.stdout, new RegExp(`${MAIN}: beta is ${VERSION.replace(/\./g, '\\.')}\n`));
  assert.match(r.stdout, /is published under the beta dist-tag\.\n$/);
});

test('publish-npm.sh accepts a rerun whose dist-tags are right, and publishes nothing again', () => {
  const r = runPublish(GOOD);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.calls, []);
  assert.match(r.stdout, /Every package of 4\.4\.0-beta\.1 is already published\./);
});

test('publish-npm.sh refuses when latest moved to this client', () => {
  const r = runPublish((name) => (name === MAIN ? { latest: VERSION, beta: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.equal(
    r.stderr,
    `publish-npm: the latest dist-tag of ${MAIN} is ${VERSION}, but it must stay on the earlier client, below 4.0.0: ` +
      `point it back with \`npm dist-tag add ${MAIN}@<its version> latest\`\n`,
  );
});

test('publish-npm.sh refuses when rc or alpha moved to this client', () => {
  const r = runPublish((name) => (name === MAIN ? { latest: '1.6.0', rc: '4.4.0-rc.1', beta: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.match(r.stderr, /^publish-npm: the rc dist-tag of @confluentinc\/kafka-javascript is 4\.4\.0-rc\.1, but it must stay/);
});

test('publish-npm.sh refuses when a package was published without the beta tag', () => {
  const platform = `${MAIN}-${PLATFORMS[1]}`;
  const r = runPublish((name) => (name === platform ? { latest: VERSION } : GOOD(name)));
  assert.equal(r.status, 1);
  assert.equal(r.stderr, `publish-npm: the beta dist-tag of ${platform} is not set, not ${VERSION}\n`);
});

test('publish-npm.sh puts a release on latest', () => {
  const r = runPublish((name) => ({ latest: '4.5.0' }), { version: '4.5.0', published: false });
  assert.equal(r.status, 0, r.stderr);
  for (const call of r.calls) assert.match(call, / --tag latest --provenance=false /);
});

test('publish-npm.sh refuses a 1.x version, whose fixes go out under legacy from the earlier client', () => {
  const r = runPublish(GOOD, { version: '1.6.1' });
  assert.equal(r.status, 1);
  assert.match(r.stderr, /^publish-npm: 1\.6\.1 is below 4\.0\.0; the versions of @confluentinc\/kafka-javascript below 4 are the earlier client's/);
  assert.deepEqual(r.calls, []);
});

test('publish-npm.sh --check makes every token-free check, reading the registry anonymously, and stops before the token', () => {
  const r = runPublish(GOOD, { token: null, check: true });
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.stdout, /OK\n/);
  assert.match(r.stdout, /already published with this tarball; skipping it\n/);
  assert.match(r.stdout, /Every check that needs no token passed\.\n$/);
  assert.doesNotMatch(r.stdout, /Checking the token/);
  assert.deepEqual(r.calls, []);
});

test('publish-npm.sh needs the token, and checks it only after the tarballs', () => {
  const r = runPublish(GOOD, { token: null });
  assert.equal(r.status, 1);
  assert.match(r.stderr, /^publish-npm: NPM_TOKEN is not set/);
  assert.match(r.stdout, /OK\n/);
});
