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

// Checks @confluentinc/kafka-javascript as a user installs it, in a project
// that installed it: by .semaphore/test-npm.sh, from the release tarballs,
// before they are recorded, and by .semaphore/javascript-post-install.yml,
// from npmjs.com, after they are published. It is not part of the package and
// needs no build: it uses only the installed package and Node's own modules.
//
// Usage: node package-smoke.js --platform <platform> --version <version> [--project <dir>]
//   <dir> (default: the current directory) is the project whose node_modules
//   holds the packages. It checks that
//   - the platform package of <platform> is installed in the project, at
//     <version>, as is the main package, whose optionalDependencies name it at
//     that version;
//   - every entry point of the main package's `exports` loads;
//   - the addon loaded is the platform package's, and the main package has no
//     development build;
//   - a KafkaProducer can be created and closed, against an address where no
//     broker listens.
// It exits non-zero on the first failure.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const MAIN = '@confluentinc/kafka-javascript';
const ADDON = 'confluent-kafka-javascript.node';

function parseArgs(argv) {
  const args = { project: process.cwd() };
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i];
    const value = argv[i + 1];
    if (value === undefined || !['--platform', '--version', '--project'].includes(flag)) {
      throw new Error(`usage: node package-smoke.js --platform <platform> --version <version> [--project <dir>]`);
    }
    args[flag.slice(2)] = value;
  }
  if (args.platform === undefined || args.version === undefined) {
    throw new Error('--platform and --version are required');
  }
  return args;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

async function main() {
  const { platform, version, project } = parseArgs(process.argv.slice(2));
  const root = fs.realpathSync(project);
  // Resolves as a module of the project does, not as this file, which may be
  // in a checkout of the repository.
  const projectRequire = createRequire(path.join(root, 'package-smoke.js'));
  const platformPackage = `${MAIN}-${platform}`;
  const modules = path.join(root, 'node_modules');

  // The platform package and the main package are those of the project, at the version.
  const addonPath = fs.realpathSync(projectRequire.resolve(`${platformPackage}/${ADDON}`));
  const platformDir = path.join(modules, ...platformPackage.split('/'));
  assert.equal(addonPath, path.join(fs.realpathSync(platformDir), ADDON), `${platformPackage} is not the project's`);
  const mainDir = fs.realpathSync(path.join(modules, ...MAIN.split('/')));
  assert.equal(
    path.dirname(path.dirname(fs.realpathSync(projectRequire.resolve(MAIN)))),
    mainDir,
    `${MAIN} does not resolve to the project's`,
  );
  const mainManifest = readJson(path.join(mainDir, 'package.json'));
  const platformManifest = readJson(path.join(platformDir, 'package.json'));
  assert.equal(mainManifest.version, version, `${MAIN} is at ${mainManifest.version}`);
  assert.equal(platformManifest.version, version, `${platformPackage} is at ${platformManifest.version}`);
  assert.equal(mainManifest.optionalDependencies?.[platformPackage], version, `${MAIN} does not depend on ${platformPackage}@${version}`);
  assert.ok(!fs.existsSync(path.join(mainDir, 'build')), `${MAIN} carries a build/ directory`);

  // Every entry point loads.
  const entryPoints = Object.keys(mainManifest.exports);
  assert.ok(entryPoints.length > 1, `${MAIN} exports ${entryPoints.length} entry points`);
  for (const subpath of entryPoints) {
    const specifier = subpath === '.' ? MAIN : `${MAIN}/${subpath.slice(2)}`;
    const loaded = projectRequire(specifier);
    assert.ok(Object.keys(loaded).length > 0, `${specifier} exports nothing`);
    console.log(`loaded ${specifier}`);
  }

  // The one addon loaded is the platform package's.
  const addons = Object.keys(require.cache).filter((file) => file.endsWith('.node'));
  assert.deepEqual(addons.map((file) => fs.realpathSync(file)), [addonPath], `the addons loaded are ${addons}`);
  console.log(`the addon is ${addonPath}`);

  // A producer opens and closes; nothing listens at 127.0.0.1:9.
  const { KafkaProducer } = projectRequire(`${MAIN}/producer`);
  const producer = new KafkaProducer({
    'bootstrap.servers': '127.0.0.1:9',
    'max.block.ms': 1000,
    'request.timeout.ms': 500,
    'delivery.timeout.ms': 1000,
  });
  await producer.close();
  console.log(`${MAIN} ${version} works on ${platform}`);
}

const watchdog = setTimeout(() => {
  console.error('package-smoke: no result after 120 s');
  process.exit(1);
}, 120_000);
watchdog.unref();

main().catch((error) => {
  console.error('package-smoke:', error);
  process.exitCode = 1;
});
