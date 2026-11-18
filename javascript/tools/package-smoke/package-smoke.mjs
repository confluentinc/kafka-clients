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

// The ES module half of package-smoke.js: @confluentinc/kafka-javascript as
// an ES module app imports it, a CommonJS package that ES modules use through
// named imports (root CLAUDE.md, JavaScript Binding Conventions, Layout and
// modules, Q13). An ES module resolves a bare specifier from its own
// location, so this file is copied into the project that installed the
// package and run there (.semaphore/test-npm.sh, test-published-npm.sh):
//
//   cp package-smoke.mjs <project>/ && node <project>/package-smoke.mjs --version <version>
//
// It checks that
//   - every entry point of the main package's `exports` imports, and that
//     `import` and `require` give the same classes: each named export is the
//     very value `require` gives, and the default import is `require`'s object;
//   - a deep import fails with ERR_PACKAGE_PATH_NOT_EXPORTED, as for require;
//   - the addon loaded is the platform package's;
//   - a KafkaProducer, imported by name, is created, sent to and closed,
//     against an address where no broker listens.
// It exits non-zero on the first failure.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

import { ExecutionError } from '@confluentinc/kafka-javascript';
import { KafkaProducer, ProducerRecord } from '@confluentinc/kafka-javascript/producer';
import { Header } from '@confluentinc/kafka-javascript/common/header';
import * as producerNamespace from '@confluentinc/kafka-javascript/producer';

const MAIN = '@confluentinc/kafka-javascript';
const ADDON = 'confluent-kafka-javascript.node';
const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

function parseArgs(argv) {
  if (argv.length !== 2 || argv[0] !== '--version') throw new Error('usage: node package-smoke.mjs --version <version>');
  return { version: argv[1] };
}

async function main() {
  const { version } = parseArgs(process.argv.slice(2));
  const mainDir = fs.realpathSync(path.join(here, 'node_modules', ...MAIN.split('/')));
  const manifest = JSON.parse(fs.readFileSync(path.join(mainDir, 'package.json'), 'utf8'));
  assert.equal(manifest.version, version, `${MAIN} is at ${manifest.version}`);

  // Every entry point imports, and gives what require gives.
  const entryPoints = Object.keys(manifest.exports);
  assert.ok(entryPoints.length > 1, `${MAIN} exports ${entryPoints.length} entry points`);
  for (const subpath of entryPoints) {
    const specifier = subpath === '.' ? MAIN : `${MAIN}/${subpath.slice(2)}`;
    const namespace = await import(specifier);
    const required = require(specifier);
    assert.equal(namespace.default, required, `${specifier}: the default import is not require's object`);
    const names = Object.keys(required).filter((name) => name !== '__esModule');
    for (const name of names) {
      assert.ok(name in namespace, `${specifier}: ${name} is not a named export`);
      assert.equal(namespace[name], required[name], `${specifier}: import and require give two ${name}`);
    }
    // Node.js 23 and later also export the whole module.exports by the name
    // 'module.exports'.
    if ('module.exports' in namespace) {
      assert.equal(namespace['module.exports'], required, `${specifier}: 'module.exports' is not require's object`);
    }
    const own = ['default', '__esModule', 'module.exports'];
    const extra = Object.keys(namespace).filter((name) => !own.includes(name) && !names.includes(name));
    assert.deepEqual(extra, [], `${specifier}: named exports require does not give`);
    console.log(`imported ${specifier} (${names.length} names, the same as require's)`);
  }
  assert.equal(producerNamespace.KafkaProducer, KafkaProducer);
  assert.equal(KafkaProducer, require(`${MAIN}/producer`).KafkaProducer);

  // A deep import fails, as for require.
  for (const deep of [`${MAIN}/dist/producer/kafka_producer.js`, `${MAIN}/dist/internal/config.js`, `${MAIN}/producer/kafka_producer`]) {
    await assert.rejects(import(deep), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' }, deep);
  }
  console.log('deep imports fail');

  // The one addon loaded is the platform package's.
  const addons = Object.keys(require.cache).filter((file) => file.endsWith('.node'));
  assert.equal(addons.length, 1, `the addons loaded are ${addons}`);
  const platformDir = path.dirname(fs.realpathSync(addons[0]));
  assert.equal(path.basename(addons[0]), ADDON);
  const platformManifest = JSON.parse(fs.readFileSync(path.join(platformDir, 'package.json'), 'utf8'));
  assert.ok(platformManifest.name.startsWith(`${MAIN}-`), `the addon is ${addons[0]}`);
  assert.equal(manifest.optionalDependencies?.[platformManifest.name], version, `the addon is ${addons[0]}`);
  console.log(`the addon is ${platformManifest.name}'s`);

  // A producer opens, takes a record with headers and closes; nothing
  // listens at 127.0.0.1:9.
  const record = new ProducerRecord({
    topic: 'package-smoke-esm',
    partition: null,
    key: Buffer.from('k'),
    value: Buffer.from('v'),
    headers: [new Header({ key: 'h', value: Buffer.from('1') })],
  });
  const producer = new KafkaProducer({
    configs: {
      'bootstrap.servers': '127.0.0.1:9',
      'max.block.ms': 1000,
      'request.timeout.ms': 500,
      'delivery.timeout.ms': 1000,
    },
  });
  const sent = producer.send(record).then(
    async (future) => {
      try {
        await future.get();
        return 'delivered';
      } catch (error) {
        assert.ok(error instanceof ExecutionError, `get() rejected with ${error}`);
        return `not delivered: ${error.cause?.name}`;
      }
    },
    (error) => `refused: ${error.name}`,
  );
  await producer.close({ timeout: 0 });
  console.log(`the send ended: ${await sent}`);
  console.log(`${MAIN} ${version} works from an ES module`);
}

const watchdog = setTimeout(() => {
  console.error('package-smoke.mjs: no result after 120 s');
  process.exit(1);
}, 120_000);
watchdog.unref();

main().catch((error) => {
  console.error('package-smoke.mjs:', error);
  process.exitCode = 1;
});
