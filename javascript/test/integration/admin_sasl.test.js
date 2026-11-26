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

// The admin client's ACL, authorization and delegation-token calls, ported
// from SaslSslAdminIntegrationTest, BaseAdminIntegrationTest#testAuthorizedOperations
// and DescribeAuthorizedOperationsTest, on the broker of
// `make -C javascript test-integration-sasl`: SASL/PLAIN, the
// StandardAuthorizer with admin a super user and alice holding only the ACLs a
// test gives her, and a delegation-token secret. The clients connect over its
// SASL_PLAINTEXT listener with PLAIN, as Rust's delegation_token_examples
// (rust/tests/integration/admin_examples_test.rs) does. Runs only when the
// SASL broker's variables are set.

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const {
  Admin,
  CreateAclsOptions,
  CreateDelegationTokenOptions,
  DescribeClusterOptions,
  DescribeDelegationTokenOptions,
  DescribeTopicsOptions,
  ExpireDelegationTokenOptions,
  NewTopic,
  RenewDelegationTokenOptions,
} = require('@confluentinc/kafka-javascript/admin');
const acl = require('@confluentinc/kafka-javascript/common/acl');
const {
  ClusterAuthorizationError,
  DelegationTokenExpiredError,
  DelegationTokenNotFoundError,
  InvalidRequestError,
  TopicAuthorizationError,
  UnknownTopicOrPartitionError,
} = require('@confluentinc/kafka-javascript/common/errors');
const res = require('@confluentinc/kafka-javascript/common/resource');
const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { DelegationToken, TokenInformation } = require('@confluentinc/kafka-javascript/common/security/token/delegation');

/** @typedef {import('@confluentinc/kafka-javascript/admin').Admin} AdminClient */
/** @typedef {import('@confluentinc/kafka-javascript/common/acl').AclBinding} AclBinding */
/** @typedef {import('@confluentinc/kafka-javascript/common/acl').AclBindingFilter} AclBindingFilter */

const SASL_PLAINTEXT = process.env.KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS;
const SKIP = SASL_PLAINTEXT ? false : 'KAFKA_SASL_PLAINTEXT_BOOTSTRAP_SERVERS is not set: no SASL broker';
if (SKIP) console.log(`skipping the SASL admin integration tests: ${SKIP}`);

/**
 * An admin client over SASL_PLAINTEXT with PLAIN, as `username`.
 *
 * @param {'admin' | 'alice'} username
 */
function saslAdmin(username) {
  return Admin.create({
    'bootstrap.servers': /** @type {string} */ (SASL_PLAINTEXT),
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.jaas.config': `org.apache.kafka.common.security.plain.PlainLoginModule required username="${username}" password="${username}-secret";`,
  });
}

let count = 0;
/**
 * A name no other run uses.
 *
 * @param {string} what
 */
const fresh = (what) => `js-sasl-admin-${what}-${process.pid}-${Date.now()}-${count++}`;

/** @param {number} ms */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * `value`, asserted present.
 *
 * @template T
 * @param {T} value
 * @returns {NonNullable<T>}
 */
function present(value) {
  assert.ok(value !== null && value !== undefined);
  return /** @type {NonNullable<T>} */ (value);
}

/**
 * The error a future fails with: the cause of its ExecutionError.
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown> | null | undefined} future
 * @returns {Promise<Error>}
 */
async function failureOf(future) {
  assert.ok(future, 'no future for the key');
  try {
    await future.get();
  } catch (error) {
    assert.ok(error instanceof ExecutionError, String(error));
    assert.ok(error.cause instanceof Error, String(error.cause));
    return error.cause;
  }
  assert.fail('the future succeeded');
}

/**
 * A binding.
 *
 * @param {import('@confluentinc/kafka-javascript/common/resource').ResourceType} resourceType
 * @param {string} name
 * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} patternType
 * @param {import('@confluentinc/kafka-javascript/common/acl').AclOperation} operation
 * @param {string} [principal]
 * @param {import('@confluentinc/kafka-javascript/common/acl').AclPermissionType} [permissionType]
 */
const binding = (resourceType, name, patternType, operation, principal = 'User:alice', permissionType = acl.AclPermissionType.ALLOW) =>
  new acl.AclBinding({
    pattern: new res.ResourcePattern({ resourceType, name, patternType }),
    entry: new acl.AccessControlEntry({ principal, host: '*', operation, permissionType }),
  });

/**
 * A filter on the resources of `type` named `name` by `patternType`, any entry.
 *
 * @param {import('@confluentinc/kafka-javascript/common/resource').ResourceType} resourceType
 * @param {string | null} name
 * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} patternType
 */
const filter = (resourceType, name, patternType) =>
  new acl.AclBindingFilter({
    patternFilter: new res.ResourcePatternFilter({ resourceType, name, patternType }),
    entryFilter: acl.AccessControlEntryFilter.ANY,
  });

/**
 * The bindings `f` matches, read until `accept` takes them: a binding reaches
 * describeAcls a little after createAcls returns.
 *
 * @param {AdminClient} admin
 * @param {AclBindingFilter} f
 * @param {(bindings: AclBinding[]) => boolean} accept
 * @returns {Promise<AclBinding[]>}
 */
async function aclsUntil(admin, f, accept) {
  for (let attempt = 0; ; attempt++) {
    const found = await admin.describeAcls(f).values().get();
    if (accept(found) || attempt >= 60) return found;
    await sleep(250);
  }
}

/**
 * Whether `bindings` are exactly `expected`, by Java's equals.
 *
 * @param {AclBinding[]} bindings
 * @param {AclBinding[]} expected
 */
const sameBindings = (bindings, expected) =>
  bindings.length === expected.length && expected.every((e) => bindings.some((b) => b.equals(e)));

// ---- ACLs ------------------------------------------------------------------------

// SaslSslAdminIntegrationTest#testAclOperations2, #testAclDescribe,
// #testAclDelete and #testLegacyAclOpsNeverAffectOrReturnPrefixed: bindings
// beyond a literal topic, read back with the MATCH and ANY pattern types and
// AclBindingFilter.ANY, a binding given twice, and one deleteAcls with several
// filters.
(SKIP ? test.skip : test)(
  'ACLs beyond TOPIC/LITERAL: PREFIXED topic, group and transactional id bindings, a cluster binding; MATCH, ANY and AclBindingFilter.ANY; several filters in one delete',
  async () => {
    const admin = saslAdmin('admin');
    const prefix = fresh('acl');
    const principal = `User:${fresh('principal')}`;
    const { ResourceType: T, PatternType: P } = res;
    const { AclOperation: O } = acl;
    const topicLiteral = binding(T.TOPIC, `${prefix}-topic`, P.LITERAL, O.READ, principal);
    const topicPrefixed = binding(T.TOPIC, prefix, P.PREFIXED, O.WRITE, principal);
    const group = binding(T.GROUP, prefix, P.PREFIXED, O.READ, principal);
    const transactional = binding(T.TRANSACTIONAL_ID, prefix, P.PREFIXED, O.WRITE, principal);
    const cluster = binding(T.CLUSTER, 'kafka-cluster', P.LITERAL, O.DESCRIBE, principal);
    const ours = [topicLiteral, topicPrefixed, group, transactional, cluster];
    /** @param {AclBinding[]} bindings */
    const mine = (bindings) => bindings.filter((b) => b.entry().principal() === principal);
    try {
      // A binding given twice is one key.
      const created = admin.createAcls({ acls: [...ours, topicPrefixed], options: new CreateAclsOptions() });
      assert.equal(created.values().size(), 5);
      assert.equal(await created.all().get(), null);
      for (const b of ours) assert.equal(await present(created.values().get(b)).get(), null);

      const all = await aclsUntil(admin, acl.AclBindingFilter.ANY, (found) => mine(found).length === 5);
      assert.ok(sameBindings(mine(all), ours), all.map(String).join('\n'));
      for (const b of ours) assert.ok(sameBindings(mine(await admin.describeAcls(b.toFilter()).values().get()), [b]), String(b));

      // MATCH: the literal topic, and the prefixed binding that covers it.
      const matching = await admin
        .describeAcls(filter(T.TOPIC, `${prefix}-topic`, P.MATCH))
        .values()
        .get();
      assert.ok(sameBindings(mine(matching), [topicLiteral, topicPrefixed]), matching.map(String).join('\n'));
      // ANY with no name: both topic bindings; LITERAL and PREFIXED one each.
      assert.ok(
        sameBindings(
          mine(
            await admin
              .describeAcls(filter(T.TOPIC, null, P.ANY))
              .values()
              .get(),
          ),
          [topicLiteral, topicPrefixed],
        ),
      );
      assert.ok(
        sameBindings(
          mine(
            await admin
              .describeAcls(filter(T.TOPIC, null, P.LITERAL))
              .values()
              .get(),
          ),
          [topicLiteral],
        ),
      );
      assert.ok(
        sameBindings(
          mine(
            await admin
              .describeAcls(filter(T.TOPIC, null, P.PREFIXED))
              .values()
              .get(),
          ),
          [topicPrefixed],
        ),
      );
      assert.ok(
        sameBindings(
          mine(
            await admin
              .describeAcls(filter(T.GROUP, null, P.PREFIXED))
              .values()
              .get(),
          ),
          [group],
        ),
      );
      assert.ok(
        sameBindings(
          mine(
            await admin
              .describeAcls(filter(T.TRANSACTIONAL_ID, `${prefix}-tx`, P.MATCH))
              .values()
              .get(),
          ),
          [transactional],
        ),
      );

      // Several filters in one call; one matches nothing.
      const nothing = filter(T.TOPIC, `${prefix}-none`, P.LITERAL);
      const groupFilter = filter(T.GROUP, prefix, P.PREFIXED);
      const deleted = admin.deleteAcls([topicLiteral.toFilter(), groupFilter, nothing]);
      assert.equal(deleted.values().size(), 3);
      const literalResults = await present(deleted.values().get(topicLiteral.toFilter())).get();
      assert.ok(sameBindings(mine(literalResults.values().map((r) => present(r.binding()))), [topicLiteral]));
      for (const r of literalResults.values()) assert.equal(r.exception(), null);
      const groupResults = await present(deleted.values().get(groupFilter)).get();
      assert.ok(sameBindings(mine(groupResults.values().map((r) => present(r.binding()))), [group]));
      assert.deepEqual((await present(deleted.values().get(nothing)).get()).values(), []);
      assert.ok(sameBindings(mine(await deleted.all().get()), [topicLiteral, group]));
      const left = await aclsUntil(admin, acl.AclBindingFilter.ANY, (found) => mine(found).length === 3);
      assert.ok(sameBindings(mine(left), [topicPrefixed, transactional, cluster]));
    } finally {
      await admin
        .deleteAcls(ours.map((b) => b.toFilter()))
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  120_000,
);

// SaslSslAdminIntegrationTest#testAttemptToCreateInvalidAcls: Java asserts
// the class; the texts are the broker's.
(SKIP ? test.skip : test)(
  'testAttemptToCreateInvalidAcls: a CLUSTER named foobar and a TOPIC with an empty name fail their own keys',
  async () => {
    const admin = saslAdmin('admin');
    const clusterAcl = binding(res.ResourceType.CLUSTER, 'foobar', res.PatternType.LITERAL, acl.AclOperation.READ, 'User:ANONYMOUS');
    const emptyName = binding(res.ResourceType.TOPIC, '', res.PatternType.LITERAL, acl.AclOperation.READ, 'User:ANONYMOUS');
    try {
      const result = admin.createAcls({ acls: [clusterAcl, emptyName], options: new CreateAclsOptions() });
      assert.equal(result.values().size(), 2);
      const clusterError = await failureOf(result.values().get(clusterAcl));
      assert.equal(clusterError.constructor, InvalidRequestError);
      assert.equal(clusterError.message, 'The only valid name for the CLUSTER resource is kafka-cluster');
      const emptyError = await failureOf(result.values().get(emptyName));
      assert.equal(emptyError.constructor, InvalidRequestError);
      assert.equal(emptyError.message, 'Invalid empty resource name');
    } finally {
      await admin.close();
    }
  },
  60_000,
);

// SaslSslAdminIntegrationTest#testAclAuthorizationDenied and
// #testCreateTopicsResponseMetadataAndConfig, BaseAdminIntegrationTest
// #testAuthorizedOperations and DescribeAuthorizedOperationsTest: alice, with
// no cluster ACL, may neither describe nor create ACLs; with CREATE, DESCRIBE
// and DESCRIBE_CONFIGS on her topics and DESCRIBE_CONFIGS denied on one, she
// creates both, but that one's metadata, config and id are refused.
(SKIP ? test.skip : test)(
  'as alice: the ACL calls without cluster DESCRIBE and ALTER; createTopics with DESCRIBE_CONFIGS denied; her authorized operations',
  async () => {
    const admin = saslAdmin('admin');
    const alice = saslAdmin('alice');
    const prefix = fresh('alice');
    const [allowed, denied] = [`${prefix}-1`, `${prefix}-2`];
    const { ResourceType: T, PatternType: P } = res;
    const { AclOperation: O } = acl;
    const grants = [
      binding(T.TOPIC, prefix, P.PREFIXED, O.CREATE),
      binding(T.TOPIC, prefix, P.PREFIXED, O.DESCRIBE),
      binding(T.TOPIC, prefix, P.PREFIXED, O.DESCRIBE_CONFIGS),
      binding(T.TOPIC, denied, P.LITERAL, O.DESCRIBE_CONFIGS, 'User:alice', acl.AclPermissionType.DENY),
    ];
    try {
      // The broker's message names the request and its connection; Java's
      // test asserts the class.
      const describeError = await failureOf(alice.describeAcls(acl.AclBindingFilter.ANY).values());
      assert.equal(describeError.constructor, ClusterAuthorizationError, String(describeError));
      assert.match(describeError.message, /needs DESCRIBE permission\.$/);
      const probe = binding(T.TOPIC, `${prefix}-probe`, P.LITERAL, O.READ);
      const createError = await failureOf(alice.createAcls([probe]).all());
      assert.equal(createError.constructor, ClusterAuthorizationError, String(createError));
      assert.match(createError.message, /needs ALTER permission\.$/);
      const deleteError = await failureOf(alice.deleteAcls([probe.toFilter()]).all());
      assert.equal(deleteError.constructor, ClusterAuthorizationError, String(deleteError));

      assert.equal(await admin.createAcls(grants).all().get(), null);
      await aclsUntil(admin, filter(T.TOPIC, denied, P.MATCH), (found) => found.length === 4);

      let result = alice.createTopics([
        new NewTopic({ name: allowed, numPartitions: 2, replicationFactor: 1 }),
        new NewTopic({ name: denied, numPartitions: null, replicationFactor: null }),
      ]);
      // A refusal racing the ACLs' propagation: created again once they reached the broker.
      for (let attempt = 0; attempt < 20; attempt++) {
        const failure = await result
          .all()
          .get()
          .then(
            () => null,
            (/** @type {unknown} */ e) => e,
          );
        if (failure === null) break;
        assert.ok(failure instanceof ExecutionError && failure.cause instanceof TopicAuthorizationError, String(failure));
        await sleep(500);
        result = alice.createTopics([
          new NewTopic({ name: allowed, numPartitions: 2, replicationFactor: 1 }),
          new NewTopic({ name: denied, numPartitions: null, replicationFactor: null }),
        ]);
      }
      assert.equal(await result.all().get(), null);
      assert.equal(await result.numPartitions(allowed).get(), 2);
      assert.equal(await result.replicationFactor(allowed).get(), 1);
      assert.ok(present(await result.config(allowed).get()).get('cleanup.policy'));
      assert.ok(await result.topicId(allowed).get());
      for (const future of [
        result.numPartitions(denied),
        result.replicationFactor(denied),
        result.config(denied),
        result.topicId(denied),
      ]) {
        const cause = await failureOf(future);
        assert.equal(cause.constructor, TopicAuthorizationError, String(cause));
        assert.equal(cause.message, 'Topic authorization failed.');
      }

      // Exactly her operations on the topic, and none on the cluster.
      /** @type {Set<import('@confluentinc/kafka-javascript/common/acl').AclOperation> | null} */
      let operations = null;
      for (let attempt = 0; attempt < 40 && operations === null; attempt++) {
        try {
          const described = await present(
            alice
              .describeTopics({ topicNames: [allowed], options: new DescribeTopicsOptions({ includeAuthorizedOperations: true }) })
              .allTopicNames(),
          ).get();
          operations = new Set(present(present(described.get(allowed)).authorizedOperations));
        } catch (error) {
          assert.ok(error instanceof ExecutionError && error.cause instanceof UnknownTopicOrPartitionError, String(error));
          await sleep(250);
        }
      }
      assert.deepEqual([...present(operations)].map(String).sort(), ['CREATE', 'DESCRIBE', 'DESCRIBE_CONFIGS']);
      const clusterOperations = await alice
        .describeCluster(new DescribeClusterOptions({ includeAuthorizedOperations: true }))
        .authorizedOperations()
        .get();
      assert.deepEqual([...present(clusterOperations)], []);
    } finally {
      await admin
        .deleteTopics([allowed, denied])
        .all()
        .get()
        .catch(() => {});
      await admin
        .deleteAcls(grants.map((b) => b.toFilter()))
        .all()
        .get()
        .catch(() => {});
      await alice.close();
      await admin.close();
    }
  },
  120_000,
);

// ---- delegation tokens -----------------------------------------------------------

/**
 * The tokens `options` describes, read until `accept` takes them: a token
 * reaches describeDelegationToken a little after it is created or expired.
 *
 * @param {AdminClient} admin
 * @param {DescribeDelegationTokenOptions} options
 * @param {(tokens: DelegationToken[]) => boolean} accept
 * @returns {Promise<DelegationToken[]>}
 */
async function tokensUntil(admin, options, accept) {
  for (let attempt = 0; ; attempt++) {
    const tokens = await admin.describeDelegationToken(options).delegationTokens().get();
    if (accept(tokens) || attempt >= 40) return tokens;
    await sleep(250);
  }
}

// SaslSslAdminIntegrationTest#testExpireDelegationToken,
// #testExpireDelegationTokenWithOptionExpireTimePeriodMs and the other token
// tests, set up as Rust's delegation_token_examples sets them up.
(SKIP ? test.skip : test)(
  'a delegation-token round trip: create, describe, renew and expire; a token past its maxLifetimeMs; an empty HMAC',
  async () => {
    const admin = saslAdmin('admin');
    const owner = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'admin' });
    const renewer = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: 'renewer' });
    const nobody = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: fresh('nobody') });
    try {
      const before = Date.now();
      const token = await admin
        .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [renewer], maxLifetimeMs: 3_600_000 }))
        .delegationToken()
        .get();
      assert.ok(token instanceof DelegationToken);
      const info = token.tokenInfo();
      assert.ok(info instanceof TokenInformation);
      assert.equal(typeof info.tokenId(), 'string');
      assert.ok(info.tokenId().length > 0);
      assert.ok(info.owner().equals(owner), String(info.owner()));
      assert.equal(info.ownerAsString(), 'User:admin');
      assert.ok(info.tokenRequester().equals(owner));
      assert.equal(info.tokenRequesterAsString(), 'User:admin');
      assert.equal(info.renewers().length, 1);
      assert.ok(info.renewers()[0].equals(renewer));
      assert.deepEqual(info.renewersAsString(), ['User:renewer']);
      assert.ok(info.issueTimestamp() >= BigInt(before - 60_000), String(info.issueTimestamp()));
      assert.equal(info.maxTimestamp(), info.issueTimestamp() + 3_600_000n);
      assert.ok(info.expiryTimestamp() <= info.maxTimestamp());
      assert.equal(info.ownerOrRenewer(owner), true);
      assert.equal(info.ownerOrRenewer(renewer), true);
      assert.equal(info.ownerOrRenewer(nobody), false);
      assert.ok(token.hmac().length > 0);
      assert.equal(token.hmacAsBase64String(), token.hmac().toString('base64'));
      assert.equal(String(token).includes('hmac=[*******]'), true, String(token));

      // Every token (owners null), the HMAC included; no owner ([]) and an
      // owner with no token: none.
      const all = await tokensUntil(admin, new DescribeDelegationTokenOptions(), (tokens) => tokens.some((t) => t.equals(token)));
      const described = present(all.find((t) => t.tokenInfo().tokenId() === info.tokenId()));
      assert.ok(described.equals(token));
      assert.equal(described.hashCode(), token.hashCode());
      assert.deepEqual(described.hmac(), token.hmac());
      assert.deepEqual(
        await admin
          .describeDelegationToken(new DescribeDelegationTokenOptions({ owners: [] }))
          .delegationTokens()
          .get(),
        [],
      );
      assert.deepEqual(
        await admin
          .describeDelegationToken(new DescribeDelegationTokenOptions({ owners: [nobody] }))
          .delegationTokens()
          .get(),
        [],
      );
      const byOwner = await admin
        .describeDelegationToken(new DescribeDelegationTokenOptions({ owners: [owner] }))
        .delegationTokens()
        .get();
      assert.ok(byOwner.some((t) => t.equals(token)));

      const renewed = await admin
        .renewDelegationToken({ hmac: token.hmac(), options: new RenewDelegationTokenOptions({ renewTimePeriodMs: 600_000 }) })
        .expiryTimestamp()
        .get();
      assert.equal(typeof renewed, 'bigint');
      assert.ok(renewed > BigInt(before) && renewed <= info.maxTimestamp(), String(renewed));
      const expired = await admin
        .expireDelegationToken({ hmac: token.hmac(), options: new ExpireDelegationTokenOptions({ expiryTimePeriodMs: -1 }) })
        .expiryTimestamp()
        .get();
      assert.ok(expired <= BigInt(Date.now()), String(expired));
      const after = await tokensUntil(admin, new DescribeDelegationTokenOptions(), (tokens) => !tokens.some((t) => t.equals(token)));
      assert.ok(!after.some((t) => t.equals(token)));

      // A token past its maxLifetimeMs.
      const short = await admin
        .createDelegationToken(new CreateDelegationTokenOptions({ maxLifetimeMs: 1000 }))
        .delegationToken()
        .get();
      await sleep(1500);
      const renewError = await failureOf(admin.renewDelegationToken(short.hmac()).expiryTimestamp());
      assert.equal(renewError.constructor, DelegationTokenExpiredError, String(renewError));
      assert.equal(renewError.message, 'Delegation Token is expired.');
      const expireError = await failureOf(
        admin
          .expireDelegationToken({ hmac: short.hmac(), options: new ExpireDelegationTokenOptions({ expiryTimePeriodMs: 1 }) })
          .expiryTimestamp(),
      );
      assert.equal(expireError.constructor, DelegationTokenExpiredError, String(expireError));
      assert.equal(expireError.message, 'Delegation Token is expired.');
      // Expiring it at once is no error.
      assert.equal(
        typeof (await admin
          .expireDelegationToken({ hmac: short.hmac(), options: new ExpireDelegationTokenOptions({ expiryTimePeriodMs: -1 }) })
          .expiryTimestamp()
          .get()),
        'bigint',
      );

      const emptyError = await failureOf(admin.expireDelegationToken(new Uint8Array(0)).expiryTimestamp());
      assert.equal(emptyError.constructor, DelegationTokenNotFoundError, String(emptyError));
      assert.equal(emptyError.message, 'Delegation Token is not found on server.');
    } finally {
      await admin.close();
    }
  },
  120_000,
);

// ---- non-ASCII names -------------------------------------------------------------

// The .NET string tests: an ACL resource name and principal, and a token
// renewer, outside ASCII, read back exactly.
(SKIP ? test.skip : test)(
  'a non-ASCII ACL resource name and principal, and a non-ASCII token renewer, read back exactly',
  async () => {
    const admin = saslAdmin('admin');
    const name = `grüße-日本-😀-${process.pid}-${Date.now()}`;
    const principal = `User:ünïcødé-${Date.now()}`;
    const b = binding(res.ResourceType.TOPIC, name, res.PatternType.LITERAL, acl.AclOperation.READ, principal);
    try {
      const created = admin.createAcls([b]);
      assert.equal(await present(created.values().get(b)).get(), null);
      const found = await aclsUntil(admin, b.toFilter(), (bindings) => bindings.length === 1);
      assert.equal(found.length, 1);
      assert.equal(found[0].pattern().name(), name);
      assert.equal(found[0].entry().principal(), principal);
      assert.ok(found[0].equals(b));
      const deleted = admin.deleteAcls([b.toFilter()]);
      assert.ok(
        sameBindings(
          (await present(deleted.values().get(b.toFilter())).get()).values().map((r) => present(r.binding())),
          [b],
        ),
      );

      const renewer = new KafkaPrincipal({ principalType: KafkaPrincipal.USER_TYPE, name: `rénewer-日本-${Date.now()}` });
      const token = await admin
        .createDelegationToken(new CreateDelegationTokenOptions({ renewers: [renewer] }))
        .delegationToken()
        .get();
      assert.ok(token.tokenInfo().renewers()[0].equals(renewer));
      const described = await tokensUntil(admin, new DescribeDelegationTokenOptions(), (tokens) => tokens.some((t) => t.equals(token)));
      const read = present(described.find((t) => t.equals(token)));
      assert.equal(read.tokenInfo().renewers()[0].getName(), renewer.getName());
      await admin
        .expireDelegationToken({ hmac: token.hmac(), options: new ExpireDelegationTokenOptions({ expiryTimePeriodMs: -1 }) })
        .expiryTimestamp()
        .get();
    } finally {
      await admin
        .deleteAcls([b.toFilter()])
        .all()
        .get()
        .catch(() => {});
      await admin.close();
    }
  },
  60_000,
);
