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

// The memory probes have teeth: each mutant of the addon (or of dist/), made
// in a scratch copy of the package, makes the probes that guard what it
// breaks fail, while the unmutated copy passes them, as
// test/lint/mutation.test.js shows for the rule linter. A copy holds dist/,
// native/'s sources, package.json and a link to node_modules; its addon is
// built with node-gyp as `make build-addon` builds it (NODE_GYP and
// NODEDIR_FLAG, which `make test-memory-mutation` passes), linked to the
// checkout's libconfluent_kafka. Each probe is the checkout's own test, run
// by Jest with CONFLUENT_KAFKA_JS_PROBE_ROOT naming the copy
// (test/memory/_probe.js), so the probe loads the copy's dist/ and addon.
//
// The copies go under MEMORY_MUTATION_DIR, or else a fresh directory under
// the system's temporary directory (TMPDIR), and are removed after their
// test. The hand-over probe needs a broker: KAFKA_BOOTSTRAP_SERVERS, which
// `make test-memory-mutation` sets as `make test-integration` does.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const PACKAGE = path.join(__dirname, '..', '..');
const REPO = path.join(PACKAGE, '..');
const ADDON = path.join('native', 'build', 'Release', 'confluent-kafka-javascript.node');
const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP_SERVERS;
const PARENT = process.env.MEMORY_MUTATION_DIR || fs.mkdtempSync(path.join(os.tmpdir(), 'kafka-js-mutants-'));

/**
 * The node-gyp command and its --nodedir flag, as javascript/Makefile finds
 * them when `make test-memory-mutation` does not pass them: node-gyp on PATH,
 * else the copy bundled with npm; the running Node's headers when its prefix
 * ships them.
 *
 * @returns {{ gyp: string, nodedir: string }} the command and the flag (empty when not needed)
 */
function nodeGyp() {
  const prefix = path.dirname(path.dirname(fs.realpathSync(process.execPath)));
  const nodedir =
    process.env.NODEDIR_FLAG ?? (fs.existsSync(path.join(prefix, 'include', 'node', 'node_api.h')) ? `--nodedir=${prefix}` : '');
  if (process.env.NODE_GYP) return { gyp: process.env.NODE_GYP, nodedir };
  const onPath = spawnSync('sh', ['-c', 'command -v node-gyp'], { encoding: 'utf8' });
  if (onPath.status === 0 && onPath.stdout.trim() !== '') return { gyp: onPath.stdout.trim(), nodedir };
  const npm = spawnSync('sh', ['-c', 'command -v npm'], { encoding: 'utf8' }).stdout.trim();
  const candidates = [path.join(prefix, 'lib', 'node_modules', 'npm', 'node_modules', 'node-gyp', 'bin', 'node-gyp.js')];
  if (npm !== '') candidates.push(path.join(path.dirname(fs.realpathSync(npm)), '..', 'node_modules', 'node-gyp', 'bin', 'node-gyp.js'));
  const found = candidates.find((c) => fs.existsSync(c));
  assert.ok(
    found !== undefined,
    `node-gyp not found: put it on PATH or set NODE_GYP; npm's bundled copy is not at ${candidates.join(', ')}`,
  );
  return { gyp: `${JSON.stringify(process.execPath)} ${JSON.stringify(found)}`, nodedir };
}

/**
 * A scratch copy of the package, named `name`: dist/, native/'s sources,
 * package.json and a link to node_modules.
 *
 * @param {string} name the copy's directory name
 * @returns {string} its root
 */
function makeCopy(name) {
  const root = path.join(PARENT, name);
  fs.rmSync(root, { recursive: true, force: true });
  fs.mkdirSync(root, { recursive: true });
  fs.cpSync(path.join(PACKAGE, 'dist'), path.join(root, 'dist'), { recursive: true });
  fs.cpSync(path.join(PACKAGE, 'native'), path.join(root, 'native'), {
    recursive: true,
    filter: (source) => path.relative(path.join(PACKAGE, 'native'), source).split(path.sep)[0] !== 'build',
  });
  fs.copyFileSync(path.join(PACKAGE, 'package.json'), path.join(root, 'package.json'));
  fs.symlinkSync(path.join(PACKAGE, 'node_modules'), path.join(root, 'node_modules'), 'junction');
  return root;
}

/**
 * Replaces `from`, which must occur exactly once, by `to` in root-relative
 * file `rel`.
 *
 * @param {string} root the copy's root
 * @param {string} rel the file, relative to it
 * @param {string} from the text replaced
 * @param {string} to its replacement
 */
function edit(root, rel, from, to) {
  const file = path.join(root, rel);
  const text = fs.readFileSync(file, 'utf8');
  assert.equal(text.split(from).length - 1, 1, `${rel} does not have exactly one ${JSON.stringify(from)}`);
  fs.writeFileSync(file, text.replace(from, to));
}

/**
 * Builds the copy's addon as `make build-addon` does, against the checkout's
 * libconfluent_kafka and header.
 *
 * @param {string} root the copy's root
 */
function build(root) {
  const { gyp, nodedir } = nodeGyp();
  const r = spawnSync('sh', ['-c', `${gyp} rebuild ${nodedir}`], {
    cwd: path.join(root, 'native'),
    encoding: 'utf8',
    timeout: 600000,
    env: {
      ...process.env,
      CONFLUENT_KAFKA_LIB_DIR: process.env.CONFLUENT_KAFKA_LIB_DIR ?? path.join(REPO, 'rust', 'target', 'release'),
      CONFLUENT_KAFKA_INCLUDE_DIR: process.env.CONFLUENT_KAFKA_INCLUDE_DIR ?? path.join(REPO, 'rust', 'target', 'include'),
      CONFLUENT_KAFKA_LINK: 'dynamic',
    },
  });
  assert.equal(r.status, 0, `building the addon of ${root} failed:\n${r.stdout}\n${r.stderr}`);
}

/**
 * A memory probe: a test of the checkout's, chosen by file, project and name.
 *
 * @typedef {{ file: string, project: string, name: string, broker?: boolean }} Probe
 */

/** @type {Record<string, Probe>} */
const PROBES = {
  counts: { file: 'test/memory/native_counts.test.js', project: 'memory', name: 'every native handle a producer makes is freed once' },
  timedGets: { file: 'test/memory/native_counts.test.js', project: 'memory', name: 'a hundred thousand timed gets' },
  readBack: { file: 'test/memory/record_read_back.test.js', project: 'memory', name: 'a send hands the core' },
  headersModel: { file: 'test/memory/headers_model.test.js', project: 'memory', name: 'random sequences on a record' },
  headersUnderStress: { file: 'test/memory/gc_stress.test.js', project: 'memory', name: 'the headers model under' },
  churn: { file: 'test/memory/lifecycle.test.js', project: 'memory', name: 'producers made, used and closed by the hundred' },
  handOver: {
    file: 'test/integration/memory_probes.test.js',
    project: 'integration',
    name: 'a future dropped before its delivery',
    broker: true,
  },
};

/**
 * Runs `probe` with the copy at `root` as the package it loads: Jest's
 * outcome of the tests it selects.
 *
 * @param {string} root the copy's root
 * @param {Probe} probe the probe
 * @returns {{ passed: number, failed: number, output: string }} how many of its tests passed and failed, and Jest's output
 */
function probeOutcome(root, probe) {
  if (probe.broker) assert.ok(BOOTSTRAP, `${probe.file} needs a broker: set KAFKA_BOOTSTRAP_SERVERS (make test-memory-mutation does)`);
  const results = path.join(root, `jest-${Object.keys(PROBES).find((k) => PROBES[k] === probe)}.json`);
  const name = probe.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const r = spawnSync(
    process.execPath,
    [
      path.join(PACKAGE, 'node_modules', '.bin', 'jest'),
      '--ci',
      probe.file,
      '--selectProjects',
      probe.project,
      '--runInBand',
      '-t',
      name,
      '--json',
      `--outputFile=${results}`,
    ],
    { cwd: PACKAGE, encoding: 'utf8', timeout: 900000, env: { ...process.env, CONFLUENT_KAFKA_JS_PROBE_ROOT: root } },
  );
  const output = `${r.stdout}\n${r.stderr}`;
  assert.ok(fs.existsSync(results), `Jest wrote no results for ${probe.file} (status ${r.status}):\n${output}`);
  const json = JSON.parse(fs.readFileSync(results, 'utf8'));
  return { passed: json.numPassedTests, failed: json.numFailedTests, output };
}

/**
 * A mutant: what it breaks, the edits that make it in a copy (whether its
 * addon must be built, or the checkout's built one serves), and the probes
 * that must fail for it.
 *
 * @typedef {{ name: string, edits: [string, string, string][], native: boolean, probes: Probe[] }} Mutant
 */

/** @type {Mutant[]} */
const MUTANTS = [
  {
    name: "a dropped napi_delete_reference: a callback Promise's settle function is never released",
    native: true,
    edits: [
      [
        'native/producer.c',
        '    if (ctx->settle_ref != NULL) {\n        napi_delete_reference(env, ctx->settle_ref);\n        ctx->settle_ref = NULL;\n    }\n',
        '    if (ctx->settle_ref != NULL) {\n        ctx->settle_ref = NULL;\n    }\n',
      ],
    ],
    probes: [PROBES.counts],
  },
  {
    name: 'a record future destroyed twice: its context does not forget it once destroyed',
    native: true,
    edits: [
      [
        'native/producer.c',
        '    atomic_fetch_add(&stat_futures_destroyed, 1);\n    ctx->future = NULL;\n',
        '    atomic_fetch_add(&stat_futures_destroyed, 1);\n',
      ],
    ],
    probes: [PROBES.counts],
  },
  {
    name: 'the key and value Buffers released at the delivery instead of at the hand-over',
    native: true,
    edits: [
      [
        'native/producer.c',
        '    if (value != NULL) atomic_fetch_add(&stat_futures_held, 1);\n    send_ctx_release_buffers(env, ctx);\n',
        '    if (value != NULL) atomic_fetch_add(&stat_futures_held, 1);\n',
      ],
      [
        'native/producer.c',
        '    int deferred = 0;\n    mock_pending_unlink(ctx);\n',
        '    int deferred = 0;\n    send_ctx_release_buffers(env, ctx);\n    mock_pending_unlink(ctx);\n',
      ],
    ],
    probes: [PROBES.handOver],
  },
  {
    // The destroy and the count that records it go together.
    name: 'no record-future destroy: the context drops its native future without destroying it',
    native: true,
    edits: [
      [
        'native/producer.c',
        '    kafka_common_Error_t* ffi_err = NULL;\n    error_warn(env, !ctx->p->queue.finalizing, FFI_ERR(kafka_common_KafkaFuture_destroy_err(ctx->future, &ffi_err)));\n    atomic_fetch_add(&stat_futures_destroyed, 1);\n    ctx->future = NULL;\n',
        '    (void)env;\n    ctx->future = NULL;\n',
      ],
    ],
    probes: [PROBES.counts],
  },
  {
    name: 'no producer release: a wait on a record future keeps its unit of the producer',
    native: true,
    edits: [
      [
        'native/producer.c',
        "    send_ctx_maybe_destroy_future(env, ctx);\n    send_ctx_release(env, ctx);\n    cbq_call_end(env, &p->queue);\n    // The wait's unit of the struct, which holds the queue: released last.\n    producer_release(p);\n}\n\n// recordFutureGet(",
        "    send_ctx_maybe_destroy_future(env, ctx);\n    send_ctx_release(env, ctx);\n    cbq_call_end(env, &p->queue);\n    // The wait's unit of the struct, which holds the queue: released last.\n}\n\n// recordFutureGet(",
      ],
    ],
    probes: [PROBES.counts, PROBES.timedGets],
  },
  {
    name: "no producer release: the handle object's finalizer keeps its unit of the producer",
    native: true,
    edits: [['native/producer.c', '    if (!open) producer_release(p);\n', '    (void)open;\n']],
    probes: [PROBES.counts, PROBES.churn],
  },
  {
    name: 'no send-context release: a wait on a record future keeps its reference on the context',
    native: true,
    edits: [
      [
        'native/producer.c',
        "    send_ctx_maybe_destroy_future(env, ctx);\n    send_ctx_release(env, ctx);\n    cbq_call_end(env, &p->queue);\n    // The wait's unit of the struct, which holds the queue: released last.\n    producer_release(p);\n}\n\n// recordFutureGet(",
        "    send_ctx_maybe_destroy_future(env, ctx);\n    cbq_call_end(env, &p->queue);\n    // The wait's unit of the struct, which holds the queue: released last.\n    producer_release(p);\n}\n\n// recordFutureGet(",
      ],
    ],
    probes: [PROBES.counts],
  },
  {
    name: "a value Buffer that takes no reference on its header, whose bytes then go with the header's last other holder",
    native: true,
    edits: [['native/headers.c', '    if (status == napi_ok) {\n        box->refs++;\n', '    if (status == napi_ok) {\n']],
    probes: [PROBES.headersModel, PROBES.headersUnderStress],
  },
  {
    name: 'a key or value copied before the core reads it',
    native: true,
    edits: [
      [
        'native/record.c',
        '    out->bytes.data = (length == 0 || data == NULL) ? EMPTY_BYTES : (const uint8_t*)data;\n',
        '    out->bytes.data = (length == 0 || data == NULL) ? EMPTY_BYTES : (const uint8_t*)memcpy(malloc(length), data, length);\n',
      ],
    ],
    probes: [PROBES.readBack],
  },
  {
    // The use after free the allocator's poisoning once caught: the FFI drops
    // a handle's header views on every mutating call, refused or not.
    name: 'a refused change to read-only headers sent to their own handle, which frees the views held',
    native: false,
    edits: [
      ['dist/common/header/headers.js', '        return native.headersIsReadOnly(own) ? sealedEmpty() : own;\n', '        return own;\n'],
    ],
    probes: [PROBES.headersModel],
  },
];

/**
 * A copy with `mutant`'s edits (none for the unmutated copy) and its addon:
 * built from its own sources, or the checkout's when only dist/ changed.
 *
 * @param {string} name the copy's directory name
 * @param {Mutant | null} mutant the mutant, or null
 * @returns {string} the copy's root
 */
function prepared(name, mutant) {
  const root = makeCopy(name);
  for (const [rel, from, to] of mutant?.edits ?? []) edit(root, rel, from, to);
  if (mutant === null || mutant.native) {
    build(root);
  } else {
    fs.mkdirSync(path.dirname(path.join(root, ADDON)), { recursive: true });
    fs.copyFileSync(path.join(PACKAGE, ADDON), path.join(root, ADDON));
  }
  return root;
}

/** Every probe a mutant targets, once each. */
const TARGETED = [...new Set(MUTANTS.flatMap((m) => m.probes))];

/**
 * Runs `check` on a copy prepared for `mutant` under `name`, then removes the
 * copy (MEMORY_MUTATION_KEEP keeps it), whether it passed or not.
 *
 * @param {string} name the copy's directory name
 * @param {Mutant | null} mutant the mutant, or null
 * @param {(root: string) => void} check what to run on it
 */
function withCopy(name, mutant, check) {
  try {
    check(prepared(name, mutant));
  } finally {
    if (!process.env.MEMORY_MUTATION_KEEP) fs.rmSync(path.join(PARENT, name), { recursive: true, force: true });
  }
}

test('the unmutated copy passes every probe the mutants target', () => {
  withCopy('unmutated', null, (root) => {
    for (const probe of TARGETED) {
      const r = probeOutcome(root, probe);
      assert.ok(
        r.failed === 0 && r.passed > 0,
        `${probe.file} "${probe.name}" on the unmutated copy: ${r.passed} passed, ${r.failed} failed\n${r.output}`,
      );
    }
  });
}, 1800000);

MUTANTS.forEach((mutant, i) => {
  test(`a mutant makes its probes fail: ${mutant.name}`, () => {
    withCopy(`mutant-${i}`, mutant, (root) => {
      for (const probe of mutant.probes) {
        const r = probeOutcome(root, probe);
        assert.ok(r.failed > 0, `${probe.file} "${probe.name}" passed the mutant (${r.passed} passed)\n${r.output}`);
      }
    });
  }, 1800000);
});

afterAll(() => {
  if (!process.env.MEMORY_MUTATION_DIR && !process.env.MEMORY_MUTATION_KEEP) fs.rmSync(PARENT, { recursive: true, force: true });
});
