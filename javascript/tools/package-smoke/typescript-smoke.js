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

// A TypeScript consumer of @confluentinc/kafka-javascript as a user installs
// it (javascript/CLAUDE.md, JavaScript Binding Conventions, Layout and modules and
// Stability): in the project that installed the tarballs
// (.semaphore/test-npm.sh), `tsc --strict --noEmit` over a small consumer,
// with `skipLibCheck: false`, so the published declarations are checked as
// well as typed against:
//   - under `module: nodenext`, an ES module file (.mts) and a CommonJS one
//     (.cts), resolved through the published `exports`;
//   - under `module: commonjs` (node10 resolution, which ignores `exports`),
//     resolved through the published `typesVersions` and the folders behind
//     the import paths.
// Each consumer also holds misuses marked `@ts-expect-error`, which fail the
// check if the declarations stopped refusing them (an `any` would accept
// them). The compiler is the repository's TypeScript, run by path, and
// @types/node the repository's: nothing is read from a registry.
//
// Usage: node typescript-smoke.js --project <dir> --tsc <tsc.js> --type-roots <dir>
// It exits non-zero when a compilation fails.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

// The consumer: the README's calls, typed. `{{import}}` is replaced by each
// module form's import of the four paths.
const BODY = `
async function run(): Promise<void> {
  const producer = new KafkaProducer({
    configs: { 'bootstrap.servers': '127.0.0.1:9', 'linger.ms': 5 },
    keySerializer: stringSerializer(),
    valueSerializer: stringSerializer(),
  });
  const record = new ProducerRecord({
    topic: 'typescript-smoke',
    partition: null,
    key: 'k',
    value: 'v',
    headers: [new Header({ key: 'h', value: Buffer.from('1') })],
  });
  const future: KafkaFuture<RecordMetadata> = await producer.send({ record, callback: ({ metadata, exception }) => void [metadata, exception] });
  try {
    const metadata: RecordMetadata = await future.get({ timeout: 1000 });
    const offset: bigint = metadata.offset;
    const partition: number = metadata.partition;
    void [offset, partition];
  } catch (err) {
    const e = err instanceof ExecutionError ? err.cause : err;
    if (e instanceof RetriableError) void e.message;
  }
  // @ts-expect-error a record of number keys and values is not a ProducerRecord<string, string>, this producer's
  await producer.send(new ProducerRecord({ topic: 't', key: 1, value: 2 }));
  // @ts-expect-error send takes a record or one object of named parameters
  await producer.send(42);
  // @ts-expect-error the offset is a bigint
  const wrong: number = (await future.get()).offset;
  void wrong;
  await producer.flush();
  await producer.close({ timeout: 0 });
}
void run;
`;

const ESM_IMPORTS = `import { ExecutionError } from '@confluentinc/kafka-javascript';
import { KafkaFuture } from '@confluentinc/kafka-javascript/common';
import { RetriableError } from '@confluentinc/kafka-javascript/common/errors';
import { Header } from '@confluentinc/kafka-javascript/common/header';
import { stringSerializer } from '@confluentinc/kafka-javascript/common/serialization';
import { KafkaProducer, ProducerRecord, RecordMetadata } from '@confluentinc/kafka-javascript/producer';
`;

const CJS_IMPORTS = `import kafka = require('@confluentinc/kafka-javascript');
import common = require('@confluentinc/kafka-javascript/common');
import errors = require('@confluentinc/kafka-javascript/common/errors');
import header = require('@confluentinc/kafka-javascript/common/header');
import serialization = require('@confluentinc/kafka-javascript/common/serialization');
import producerPackage = require('@confluentinc/kafka-javascript/producer');
const { ExecutionError } = kafka;
type KafkaFuture<T> = common.KafkaFuture<T>;
const { RetriableError } = errors;
const { Header } = header;
const { stringSerializer } = serialization;
const { KafkaProducer, ProducerRecord } = producerPackage;
type RecordMetadata = producerPackage.RecordMetadata;
`;

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i];
    if (!['--project', '--tsc', '--type-roots'].includes(flag) || argv[i + 1] === undefined) {
      throw new Error('usage: node typescript-smoke.js --project <dir> --tsc <tsc.js> --type-roots <dir>');
    }
    args[flag.slice(2).replace('-r', 'R')] = path.resolve(argv[i + 1]);
  }
  if (!args.project || !args.tsc || !args.typeRoots) throw new Error('--project, --tsc and --type-roots are required');
  return args;
}

/** Writes the consumer files and their tsconfig.json into `dir`, and runs tsc over them. */
function compile(tsc, dir, compilerOptions, files) {
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  for (const [name, text] of Object.entries(files)) fs.writeFileSync(path.join(dir, name), text);
  const config = {
    compilerOptions: {
      strict: true,
      noEmit: true,
      skipLibCheck: false,
      target: 'es2022',
      lib: ['es2022'],
      types: ['node'],
      ...compilerOptions,
    },
    files: Object.keys(files),
  };
  fs.writeFileSync(path.join(dir, 'tsconfig.json'), `${JSON.stringify(config, null, 2)}\n`);
  const r = spawnSync(process.execPath, [tsc, '-p', path.join(dir, 'tsconfig.json'), '--pretty', 'false'], { cwd: dir, encoding: 'utf8' });
  assert.equal(r.status, 0, `tsc failed in ${dir}:\n${r.stdout}${r.stderr}`);
  console.log(`tsc --strict passed: ${path.basename(dir)} (${Object.keys(files).join(', ')})`);
}

function main() {
  const { project, tsc, typeRoots } = parseArgs(process.argv.slice(2));
  assert.ok(
    fs.existsSync(path.join(project, 'node_modules', '@confluentinc', 'kafka-javascript', 'package.json')),
    `${project} has no installed package`,
  );
  compile(
    tsc,
    path.join(project, 'typescript-nodenext'),
    { module: 'nodenext', moduleResolution: 'nodenext', typeRoots: [typeRoots] },
    {
      'consumer.mts': ESM_IMPORTS + BODY,
      'consumer.cts': CJS_IMPORTS + BODY,
    },
  );
  compile(
    tsc,
    path.join(project, 'typescript-commonjs'),
    { module: 'commonjs', moduleResolution: 'node10', typeRoots: [typeRoots] },
    {
      'consumer.ts': ESM_IMPORTS + BODY,
    },
  );
}

try {
  main();
} catch (error) {
  console.error('typescript-smoke:', error.message);
  process.exitCode = 1;
}
