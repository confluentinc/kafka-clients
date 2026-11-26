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

// Runs every admin example (examples/admin/<rpc>.js) as a user does,
// `node examples/admin/<rpc>.js ...`, and checks its exit code and output.
// The broker-backed tests run only when KAFKA_BOOTSTRAP_SERVERS is set
// (`make -C javascript test-integration` starts a scratch broker for them):
// one plaintext broker, with no authorizer, no token secret and one log dir,
// whose node id is 1. What that broker refuses (ACLs and delegation tokens, a
// reassignment needing more brokers, a preferred election with nothing to
// elect, a replica move to another log dir) still runs, and the test checks the
// broker's error and exit code 1. The usage tests need no broker. The token
// examples also run against the broker of `make test-integration-sasl`, which
// has a token secret, as Rust's delegation_token_examples runs them.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

const ROOT = path.join(__dirname, '..', '..');
const EXAMPLES = path.join(ROOT, 'examples', 'admin');

// A string in every test that needs a broker: those are skipped when it is unset.
const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to administer';
if (SKIP) console.log(`skipping the broker-backed admin example tests: ${SKIP}`);

/** A name no other run uses. */
const RUN = `${process.pid}-${Date.now()}`;

/**
 * @typedef {{ code: number | null, stdout: string, stderr: string }} Output
 */

/**
 * Runs one example as a user does.
 *
 * @param {string} name the example, `examples/admin/<name>.js`
 * @param {string[]} args its arguments
 * @param {string} [bootstrap] KAFKA_BOOTSTRAP_SERVERS for it
 * @returns {Output} its exit code and output, the preview warning left out of stderr
 */
function run(name, args, bootstrap = BOOTSTRAP) {
  const r = spawnSync(process.execPath, [path.join('examples', 'admin', `${name}.js`), ...args], {
    cwd: ROOT,
    encoding: 'utf8',
    timeout: 120000,
    // The core's log, which RUST_LOG would turn on, goes to the example's
    // stderr, which the tests read: pinned off, as Rust's examples log nothing.
    env: { ...process.env, KAFKA_BOOTSTRAP_SERVERS: bootstrap, RUST_LOG: 'off' },
  });
  // The client's one ExperimentalWarning (the preview notice) and Node's hint after it.
  const stderr = r.stderr
    .split('\n')
    .filter((line) => !/ExperimentalWarning|--trace-warnings/.test(line))
    .join('\n');
  return { code: r.status, stdout: r.stdout, stderr };
}

/**
 * The example's stdout, asserting it succeeded.
 *
 * @param {Output} output the run
 * @returns {string} its stdout
 */
function ok(output) {
  assert.equal(output.code, 0, `example failed\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`);
  return output.stdout;
}

/**
 * The example's stderr, asserting it exited with `code`.
 *
 * @param {Output} output the run
 * @param {number} code the exit code expected
 * @returns {string} its stderr
 */
function failed(output, code) {
  assert.equal(output.code, code, `unexpected exit code\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`);
  return output.stderr;
}

/**
 * Runs an example until it succeeds with `expected` in its stdout: what one
 * call changed reaches every broker's metadata, and the group coordinator,
 * a little later.
 *
 * @param {string} name the example
 * @param {string[]} args its arguments
 * @param {string} expected the text awaited
 * @returns {Promise<string>} the stdout that held it
 */
async function runUntil(name, args, expected) {
  let output = run(name, args);
  for (let attempt = 0; attempt < 30 && !(output.code === 0 && output.stdout.includes(expected)); attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    output = run(name, args);
  }
  assert.ok(
    output.code === 0 && output.stdout.includes(expected),
    `${JSON.stringify(expected)} not in the output of ${name}: ${JSON.stringify(output)}`,
  );
  return output.stdout;
}

/**
 * The text of `text` between `prefix` and the next `end`.
 *
 * @param {string} text the output
 * @param {string} prefix what precedes the field
 * @param {string} end what ends it
 * @returns {string} the field
 */
function between(text, prefix, end) {
  const at = text.indexOf(prefix);
  assert.ok(at >= 0, `${JSON.stringify(prefix)} not in ${text}`);
  const rest = text.slice(at + prefix.length);
  const stop = rest.indexOf(end);
  return stop < 0 ? rest : rest.slice(0, stop);
}

/**
 * Produces `count` records to a partition and waits for each.
 *
 * @param {string} topic the topic
 * @param {number} partition the partition
 * @param {number} count how many records
 */
async function produce(topic, partition, count) {
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 0 } });
  try {
    for (let i = 0; i < count; i++) {
      await (await producer.send(new ProducerRecord({ topic, partition, key: null, value: Buffer.from(`value-${i}`) }))).get();
    }
  } finally {
    await producer.close();
  }
}

test('every admin RPC has its example, and only those', () => {
  // The same names as the Rust examples beside this package, so a command carries over between clients.
  const rust = fs
    .readdirSync(path.join(ROOT, '..', 'rust', 'examples', 'admin'))
    .filter((f) => f.endsWith('.rs'))
    .map((f) => f.slice(0, -3))
    .sort();
  const js = fs
    .readdirSync(EXAMPLES)
    .filter((f) => f.endsWith('.js') && f !== 'common.js')
    .map((f) => f.slice(0, -3))
    .sort();
  assert.equal(js.length, 45);
  assert.deepEqual(js, rust);
});

test('a usage error exits with code 2, --help with 0, and a secret is never repeated', () => {
  // A usage error is reported before any request is sent, so no broker is needed.
  const noBroker = 'localhost:1';
  let stderr = failed(run('create_topics', [], noBroker), 2);
  assert.ok(stderr.startsWith('no topic given\n\nusage: node examples/admin/create_topics.js '), stderr);
  stderr = failed(run('list_topics', ['--bogus'], noBroker), 2);
  assert.ok(stderr.startsWith('unknown option --bogus\n\n'), stderr);
  stderr = failed(run('list_topics', ['--internal=yes'], noBroker), 2);
  assert.ok(stderr.startsWith('--internal takes no value\n\n'), stderr);
  stderr = failed(run('list_groups', ['--states'], noBroker), 2);
  assert.ok(stderr.startsWith('--states needs a value: --states=...\n\n'), stderr);
  stderr = failed(run('create_partitions', ['t'], noBroker), 2);
  assert.ok(stderr.startsWith('expected arguments in groups of 2\n\n'), stderr);
  stderr = failed(run('create_partitions', ['t', 'x'], noBroker), 2);
  assert.ok(stderr.startsWith('invalid partition count: "x"\n\n'), stderr);
  stderr = failed(run('elect_leaders', ['bogus'], noBroker), 2);
  assert.ok(stderr.startsWith('invalid election type: "bogus", expected one of preferred, unclean\n\n'), stderr);
  const help = ok(run('list_topics', ['--help'], noBroker));
  assert.equal(help, 'usage: node examples/admin/list_topics.js [-X key=value]... [--internal]\n');
  // An argument that may be a secret is never repeated in a usage error.
  stderr = failed(run('list_topics', ['-X', 'hunter2'], noBroker), 2);
  assert.ok(stderr.startsWith('-X needs a key=value argument\n\n') && !stderr.includes('hunter2'), stderr);
  stderr = failed(run('alter_user_scram_credentials', ['UPSERT', 'alice', 'hunter2', '8192', 'secret'], noBroker), 2);
  assert.ok(stderr.startsWith('invalid mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512\n\n') && !stderr.includes('hunter2'), stderr);
  stderr = failed(run('renew_delegation_token', ['not-hex-secret'], noBroker), 2);
  assert.ok(stderr.startsWith('invalid hex string\n\n') && !stderr.includes('not-hex-secret'), stderr);
  // The numbers are read as Rust's examples read them.
  stderr = failed(run('alter_user_scram_credentials', ['UPSERT', 'alice', 'SCRAM-SHA-256', '99999999999', 'pw'], noBroker), 2);
  assert.ok(stderr.startsWith('invalid iterations, expected a number\n\n'), stderr);
  for (const value of ['0x10', ' 12 ', '1e', 'infinit']) {
    stderr = failed(run('alter_client_quotas', ['user=alice', `producer_byte_rate=${value}`], noBroker), 2);
    assert.ok(stderr.startsWith(`invalid quota value: ${JSON.stringify(value)}\n\n`), stderr);
  }
  // A set but empty KAFKA_BOOTSTRAP_SERVERS is passed on, as Rust's examples pass it.
  stderr = failed(run('list_topics', [], ''), 1);
  assert.equal(
    stderr,
    'Error: KafkaError: Failed to create new KafkaAdminClient (caused by: ConfigError: You must set either bootstrap.servers or bootstrap.controllers)\n',
  );
  // A configuration the client refuses fails the example with exit code 1.
  stderr = failed(run('list_topics', ['-X', 'request.timeout.ms=abc'], noBroker), 1);
  assert.ok(stderr.startsWith('Error: ConfigError: '), stderr);
}, 60000);

(SKIP ? test.skip : test)(
  'topics, partitions, records, configs and log dirs',
  async () => {
    const topic = `admin-examples-${RUN}`;
    let stdout = ok(run('create_topics', ['--validate-only', `${topic}:2:1`]));
    assert.ok(stdout.startsWith(`Topic ${topic} validated: id `) && stdout.endsWith(', 2 partitions, replication factor 1\n'), stdout);
    stdout = ok(run('create_topics', [`${topic}:2:1`]));
    assert.ok(stdout.startsWith(`Topic ${topic} created: id `) && stdout.endsWith(', 2 partitions, replication factor 1\n'), stdout);
    // Creating it again fails for that topic only, with exit code 1.
    let stderr = failed(run('create_topics', [topic]), 1);
    assert.ok(stderr.includes(`Failed to create topic ${topic}: TopicExistsError: Topic '${topic}' already exists.\n`), stderr);
    await runUntil('list_topics', [], `Topic ${topic} (id `);
    stdout = await runUntil('describe_topics', [topic], '  partition 1: leader 1, replicas [1], isr [1]');
    assert.ok(stdout.includes('  partition 0: leader 1, replicas [1], isr [1]\n'), stdout);
    stderr = failed(run('describe_topics', [topic, `${topic}-missing`]), 1);
    assert.ok(
      stderr.includes(
        `Failed to describe topic ${topic}-missing: UnknownTopicOrPartitionError: This server does not host this topic-partition.\n`,
      ),
      stderr,
    );
    assert.equal(ok(run('create_partitions', [topic, '3'])), `Topic ${topic} now has 3 partitions\n`);
    await produce(topic, 0, 10);
    stdout = ok(run('list_offsets', ['read_uncommitted', topic, '0', 'latest']));
    assert.ok(stdout.startsWith(`${topic}-0: offset 10, timestamp -1, leader epoch `), stdout);
    assert.equal(ok(run('delete_records', [topic, '0', '4'])), `Deleted records of partition ${topic}-0: low watermark 4\n`);
    stdout = ok(run('list_offsets', ['read_committed', topic, '0', 'earliest']));
    assert.ok(stdout.startsWith(`${topic}-0: offset 4, `), stdout);
    assert.equal(ok(run('incremental_alter_configs', ['topic', topic, 'retention.ms=SET:123456'])), `Altered configs of topic ${topic}\n`);
    stdout = await runUntil('describe_configs', ['topic', topic], '  retention.ms = 123456 [DYNAMIC_TOPIC_CONFIG]');
    assert.ok(stdout.startsWith(`Configs of topic ${topic}:\n`), stdout);
    stdout = ok(run('list_config_resources', ['topic']));
    assert.ok(stdout.split('\n').includes(`topic ${topic}`), stdout);
    // The single broker of a combined-mode cluster is node 1.
    stdout = ok(run('describe_log_dirs', ['1']));
    assert.ok(stdout.startsWith('Broker 1: 1 log dirs\n') && stdout.includes(`    replica ${topic}-0: size `), stdout);
    stdout = ok(run('describe_replica_log_dirs', [topic, '0', '1']));
    assert.ok(stdout.startsWith(`Replica ${topic}-0-1: log dir /`), stdout);
    // One log dir: the broker has no other to move the replica to.
    stderr = failed(run('alter_replica_log_dirs', [topic, '0', '1', '/nowhere']), 1);
    assert.ok(
      stderr.includes(
        `Failed to move replica ${topic}-0-1 to /nowhere: LogDirNotFoundError: The user-specified log directory is not found in the broker config.\n`,
      ),
      stderr,
    );
    // Each key prints in command-line order, as Rust's examples print, not in
    // the Result map's order.
    assert.equal(
      ok(run('delete_records', [topic, '1', '0', topic, '0', '4'])),
      `Deleted records of partition ${topic}-1: low watermark 0\nDeleted records of partition ${topic}-0: low watermark 4\n`,
    );
    // Listings sort by code units, as Rust's sort by bytes: an upper-case name
    // first, whatever the host's locale.
    ok(run('create_topics', [`${topic}-a`, `${topic}-Z`]));
    stdout = await runUntil('list_topics', [], `Topic ${topic}-a (id `);
    assert.ok(
      stdout.indexOf(`Topic ${topic}-Z (id `) >= 0 && stdout.indexOf(`Topic ${topic}-Z (id `) < stdout.indexOf(`Topic ${topic}-a (id `),
      stdout,
    );
    ok(run('delete_topics', [`${topic}-a`, `${topic}-Z`]));
    assert.equal(ok(run('delete_topics', [topic])), `Topic ${topic} deleted\n`);
  },
  300000,
);

(SKIP ? test.skip : test)(
  'the cluster, elections, reassignments and features',
  async () => {
    const topic = `admin-examples-elect-${RUN}`;
    let stdout = ok(run('describe_cluster', ['--authorized-operations']));
    assert.match(stdout, /^Cluster id: \S+\nController: 1 at \S+:\d+\nBroker 1 at \S+:\d+\nAuthorized operations: \S/);
    ok(run('create_topics', [`${topic}:1:1`]));
    await runUntil('describe_topics', [topic], '  partition 0: leader 1');
    // One replica, already the leader: no election is needed.
    let stderr = failed(run('elect_leaders', ['preferred', topic, '0']), 1);
    assert.ok(
      stderr.includes(`Failed to elect leader for ${topic}-0: ElectionNotNeededError: Leader election not needed for topic partition.\n`),
      stderr,
    );
    // One broker: a reassignment to a second one is refused, and there is nothing to cancel.
    stderr = failed(run('alter_partition_reassignments', [topic, '0', '1,2']), 1);
    assert.ok(
      stderr.includes(
        `Failed to reassign partition ${topic}-0 to [1,2]: InvalidReplicaAssignmentError: ` +
          'The manual partition assignment includes broker 2, but no such broker is registered.\n',
      ),
      stderr,
    );
    stderr = failed(run('alter_partition_reassignments', [topic, '0', 'cancel']), 1);
    assert.ok(
      stderr.includes(
        `Failed to cancel the reassignment of partition ${topic}-0: NoReassignmentInProgressError: No partition reassignment is in progress.\n`,
      ),
      stderr,
    );
    assert.equal(ok(run('list_partition_reassignments', [topic, '0'])), 'No ongoing reassignments\n');
    stdout = ok(run('describe_features', []));
    const maxLevel = between(between(stdout, 'Finalized feature metadata.version: ', '\n'), 'max level ', '\n');
    const supportedMax = Number(between(stdout, 'Supported feature metadata.version: ', '\n').split('max version ')[1]);
    assert.equal(
      ok(run('update_features', ['--validate-only', 'metadata.version', maxLevel, 'upgrade'])),
      `Feature metadata.version validated: max level ${maxLevel}\n`,
    );
    // No controller has this id. metadata.version 33 is the first to support
    // controller unregistration: a broker that cannot reach it does not offer
    // the RPC, and one finalized below it refuses the request.
    const unregisterLevel = 33;
    const expected =
      supportedMax < unregisterLevel
        ? 'UnsupportedVersionError: The node does not support UNREGISTER_CONTROLLER'
        : Number(maxLevel) < unregisterLevel
          ? 'The current MetadataVersion is too old to support controller unregistration.'
          : 'Controller ID 987654 is not currently registered.';
    stderr = failed(run('unregister_controller', ['987654']), 1);
    assert.ok(stderr.startsWith('Error: ') && stderr.includes(expected), stderr);
    ok(run('delete_topics', [topic]));
  },
  300000,
);

(SKIP ? test.skip : test)(
  'consumer groups',
  async () => {
    const topic = `admin-examples-group-${RUN}`;
    const group = `admin-examples-group-${RUN}`;
    ok(run('create_topics', [`${topic}:1:1`]));
    await runUntil('describe_topics', [topic], '  partition 0: leader 1');
    // Committing an offset creates the group, empty and of the classic type.
    assert.equal(ok(run('alter_consumer_group_offsets', [group, topic, '0', '5'])), `Group ${group}: ${topic}-0 offset set to 5\n`);
    assert.equal(
      ok(run('list_consumer_group_offsets', [group])),
      `Group ${group}: 1 partitions\n  ${topic}-0: offset 5, leader epoch none, metadata ""\n`,
    );
    await runUntil('list_groups', [], `Group ${group}: type Classic, state Empty, protocol "", simple true`);
    assert.equal(
      ok(run('describe_consumer_groups', [group])),
      `Group ${group}: type Classic, state Empty, assignor "", coordinator 1, simple true\n`,
    );
    assert.equal(
      ok(run('describe_classic_groups', [group])),
      `Group ${group}: state Empty, protocol "", protocol data "", coordinator 1, simple true\n`,
    );
    // The group has no member with this instance id.
    let stderr = failed(run('remove_members_from_consumer_group', [group, 'no-such-instance']), 1);
    assert.ok(
      stderr.includes(
        `Failed to remove member no-such-instance from group ${group}: UnknownMemberIdError: The coordinator is not aware of this member.\n`,
      ),
      stderr,
    );
    assert.equal(ok(run('delete_consumer_group_offsets', [group, topic, '0'])), `Group ${group}: ${topic}-0 offset deleted\n`);
    assert.equal(ok(run('delete_consumer_groups', [group])), `Group ${group} deleted\n`);
    stderr = failed(run('delete_consumer_groups', [group]), 1);
    assert.ok(stderr.includes(`Failed to delete group ${group}: GroupIdNotFoundError: The group id does not exist.\n`), stderr);
    // In command-line order, as Rust's example prints.
    stderr = failed(run('delete_consumer_groups', [`${group}-zeta`, `${group}-alpha`]), 1);
    assert.equal(
      stderr,
      `Failed to delete group ${group}-zeta: GroupIdNotFoundError: The group id does not exist.\n` +
        `Failed to delete group ${group}-alpha: GroupIdNotFoundError: The group id does not exist.\n`,
    );
    ok(run('delete_topics', [topic]));
  },
  300000,
);

(SKIP ? test.skip : test)(
  'ACLs and delegation tokens, which the broker refuses',
  async () => {
    const topic = `admin-examples-acl-${RUN}`;
    const binding = ['TOPIC', topic, 'LITERAL', 'User:alice', '*', 'READ', 'ALLOW'];
    const text = `TOPIC ${topic} LITERAL User:alice * READ ALLOW`;
    // No authorizer: every ACL call is refused.
    let stderr = failed(run('create_acls', binding), 1);
    assert.ok(stderr.includes(`Failed to create ACL ${text}: SecurityDisabledError: No Authorizer is configured.\n`), stderr);
    stderr = failed(run('describe_acls', binding), 1);
    assert.ok(stderr.includes('Error: SecurityDisabledError: No Authorizer is configured on the broker\n'), stderr);
    stderr = failed(run('delete_acls', binding), 1);
    assert.ok(stderr.includes(`Failed to delete ACLs matching ${text}: SecurityDisabledError: No Authorizer is configured.\n`), stderr);
    // A PLAINTEXT connection: the broker refuses every token request.
    const refused =
      'Error: UnsupportedByAuthenticationError: Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.\n';
    for (const [name, args] of /** @type {Array<[string, string[]]>} */ ([
      ['create_delegation_token', ['--renewer=User:renewer']],
      ['describe_delegation_token', []],
      ['renew_delegation_token', ['--period-ms=600000', '00ff']],
      ['expire_delegation_token', ['--period-ms=-1', '00ff']],
    ])) {
      stderr = failed(run(name, args), 1);
      assert.ok(stderr.includes(refused), `${name}: ${stderr}`);
    }
  },
  300000,
);

(SKIP ? test.skip : test)(
  'client quotas and SCRAM credentials',
  async () => {
    const user = `admin-examples-user-${RUN}`;
    const entity = `user=${user}`;
    assert.equal(
      ok(run('alter_client_quotas', ['--validate-only', entity, 'producer_byte_rate=1024'])),
      `Client quotas of ${entity} validated\n`,
    );
    assert.equal(ok(run('alter_client_quotas', [entity, 'producer_byte_rate=1024'])), `Client quotas of ${entity} altered\n`);
    await runUntil('describe_client_quotas', ['user', user], `${entity}: producer_byte_rate=1024\n1 entities match\n`);
    ok(run('alter_client_quotas', [entity, 'producer_byte_rate=REMOVE']));
    await runUntil('describe_client_quotas', ['user', user], '0 entities match');
    assert.equal(
      ok(
        run('alter_user_scram_credentials', [
          'UPSERT',
          user,
          'SCRAM-SHA-256',
          '8192',
          'secret',
          'UPSERT',
          `${user}-salted`,
          'scram-sha-512',
          '4096',
          'pw',
          'salt',
        ]),
      ),
      `Altered SCRAM credentials of user ${user}\nAltered SCRAM credentials of user ${user}-salted\n`,
    );
    await runUntil('describe_user_scram_credentials', [user], `User ${user}: SCRAM-SHA-256 iterations=8192\n`);
    const stdout = ok(run('describe_user_scram_credentials', []));
    assert.ok(stdout.includes(`User ${user}-salted: SCRAM-SHA-512 iterations=4096\n`), stdout);
    let stderr = failed(run('describe_user_scram_credentials', [`${user}-none`]), 1);
    assert.ok(
      stderr.includes(
        `Failed to describe SCRAM credentials of user ${user}-none: ResourceNotFoundError: Attempt to describe a user credential that does not exist: ${user}-none\n`,
      ),
      stderr,
    );
    ok(run('alter_user_scram_credentials', ['DELETE', user, 'SCRAM-SHA-256', 'DELETE', `${user}-salted`, 'SCRAM-SHA-512']));
    stderr = failed(run('alter_user_scram_credentials', ['DELETE', user, 'SCRAM-SHA-256']), 1);
    assert.ok(
      stderr.includes(
        `Failed to alter SCRAM credentials of user ${user}: ResourceNotFoundError: Attempt to delete a user credential that does not exist\n`,
      ),
      stderr,
    );
  },
  300000,
);

(SKIP ? test.skip : test)(
  'producers and transactions',
  async () => {
    const topic = `admin-examples-txn-${RUN}`;
    const transactionalId = `admin-examples-txn-${RUN}`;
    ok(run('create_topics', [`${topic}:1:1`]));
    await runUntil('describe_topics', [topic], '  partition 0: leader 1');
    // An idempotent producer: the partition's one active producer.
    await produce(topic, 0, 1);
    let stdout = ok(run('describe_producers', [topic, '0']));
    assert.ok(stdout.startsWith(`Partition ${topic}-0: 1 active producers\n  producer id `), stdout);
    const producerId = between(stdout, 'producer id ', ',');
    const producerEpoch = between(stdout, 'producer epoch ', ',');
    // The JavaScript producer has no transactions: fencing a transactional id
    // registers it, with no transaction open.
    let stderr = failed(run('describe_transactions', [transactionalId]), 1);
    assert.ok(
      stderr.includes(
        `Failed to describe transaction ${transactionalId}: TransactionalIdNotFoundError: ` +
          `DescribeTransactions request for transactionalId \`${transactionalId}\` failed because the ID could not be found\n`,
      ),
      stderr,
    );
    stdout = ok(run('fence_producers', [transactionalId]));
    assert.match(stdout, new RegExp(`^Fenced ${transactionalId}: producer id \\d+, epoch \\d+\\n$`));
    stdout = ok(run('describe_transactions', [transactionalId]));
    assert.match(
      stdout,
      new RegExp(
        `^Transaction ${transactionalId}: state Empty, coordinator 1, producer id \\d+, producer epoch \\d+, timeout \\d+ ms, start time none\\n$`,
      ),
    );
    stdout = await runUntil(
      'list_transactions',
      ['--states=empty', `--pattern=${transactionalId}`],
      `Transaction ${transactionalId}: producer id `,
    );
    assert.ok(stdout.endsWith(', state Empty\n'), stdout);
    assert.equal(ok(run('list_transactions', ['--states=ongoing', `--pattern=${transactionalId}`])), 'No transactions\n');
    // Writes an abort marker for the producer, which has no transaction open.
    assert.equal(
      ok(run('abort_transaction', [topic, '0', producerId, producerEpoch, '0'])),
      `Aborted transaction of producer id ${producerId}, producer epoch ${producerEpoch} on ${topic}-0\n`,
    );
    assert.equal(ok(run('force_terminate_transaction', [transactionalId])), `Transaction ${transactionalId} terminated\n`);
    ok(run('delete_topics', [topic]));
  },
  300000,
);

// ---- the token examples on the SASL broker --------------------------------------------
//
// Rust's delegation_token_examples (rust/tests/integration/admin_examples_test.rs),
// on the broker of `make -C javascript test-integration-sasl`, which has a
// token secret: its SASL_PLAINTEXT listener, PLAIN, as user admin.

const SASL_PLAINTEXT = process.env.KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS;
const SKIP_SASL = SASL_PLAINTEXT ? false : 'KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS is not set: no SASL broker with a token secret';
if (SKIP_SASL) console.log(`skipping the delegation-token example test: ${SKIP_SASL}`);

(SKIP_SASL ? test.skip : test)(
  'the delegation-token examples on a broker with a token secret: create, describe, renew and expire',
  async () => {
    const sasl = /** @type {string} */ (SASL_PLAINTEXT);
    const config = [
      '-X',
      'security.protocol=SASL_PLAINTEXT',
      '-X',
      'sasl.mechanism=PLAIN',
      '-X',
      'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
    ];
    /** @param {string} name @param {string[]} args */
    const runSasl = (name, args) => run(name, [...config, ...args], sasl);
    /** @param {string} name @param {string[]} args @param {string} expected */
    const runSaslUntil = async (name, args, expected) => {
      let output = runSasl(name, args);
      for (let attempt = 0; attempt < 30 && !(output.code === 0 && output.stdout.includes(expected)); attempt++) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        output = runSasl(name, args);
      }
      assert.ok(
        output.code === 0 && output.stdout.includes(expected),
        `${JSON.stringify(expected)} not in the output of ${name}: ${JSON.stringify(output)}`,
      );
      return output.stdout;
    };

    let stdout = ok(runSasl('create_delegation_token', ['--renewer=User:renewer']));
    const tokenId = between(stdout, 'Created delegation token ', '\n');
    const hmac = between(stdout, 'HMAC (hex): ', '\n');
    assert.ok(stdout.includes('Owner: User:admin'), stdout);
    assert.ok(stdout.includes('Renewers: User:renewer'), stdout);

    // The HMAC is the token's password: describing shows it only when asked.
    stdout = await runSaslUntil('describe_delegation_token', [], `Token ${tokenId}: `);
    assert.ok(!stdout.includes(hmac), stdout);
    stdout = ok(runSasl('describe_delegation_token', ['--show-hmac']));
    assert.ok(stdout.includes(`  HMAC (hex): ${hmac}`), stdout);
    // A malformed HMAC is not repeated back.
    const stderr = failed(runSasl('renew_delegation_token', ['not-hex-secret']), 2);
    assert.ok(!stderr.includes('not-hex-secret'), stderr);
    stdout = ok(runSasl('describe_delegation_token', ['User:nobody']));
    assert.ok(stdout.includes('No delegation tokens'), stdout);

    // Only the owner and the renewers may renew; the owner is the caller here.
    stdout = ok(runSasl('renew_delegation_token', ['--period-ms=600000', hmac]));
    assert.ok(stdout.includes('Delegation token renewed, expiry timestamp: '), stdout);
    stdout = ok(runSasl('expire_delegation_token', ['--period-ms=-1', hmac]));
    assert.ok(stdout.includes('Delegation token expired, expiry timestamp: '), stdout);
    await runSaslUntil('describe_delegation_token', [], 'No delegation tokens');
  },
  300000,
);
