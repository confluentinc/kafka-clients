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

// Broker-backed admin client test: runs only when KAFKA_BOOTSTRAP_SERVERS is
// set (`make -C javascript test-integration` starts a scratch broker for it).

const assert = require('node:assert/strict');

const { Admin, DescribeClusterOptions, ListTopicsOptions, TopicListing } = require('@confluentinc/kafka-javascript/admin');
const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');
const { Node } = require('@confluentinc/kafka-javascript/common');

// A string in every test that runs: the tests are skipped when it is unset.
const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to administer';
if (SKIP) console.log(`skipping the integration tests: ${SKIP}`);

(SKIP ? test.skip : test)(
  'describeCluster: the brokers, the controller, the cluster id and the authorized operations',
  async () => {
    const admin = Admin.create({ 'bootstrap.servers': BOOTSTRAP });
    try {
      const result = admin.describeCluster();
      const nodes = await result.nodes().get();
      assert.ok(nodes.length >= 1);
      for (const n of nodes) assert.ok(n instanceof Node && n.id >= 0 && n.port > 0, String(n));
      const controller = await result.controller().get();
      assert.ok(controller instanceof Node);
      assert.ok(nodes.some((n) => n.id === controller.id));
      const clusterId = await result.clusterId().get();
      assert.equal(typeof clusterId, 'string');
      assert.ok(clusterId.length > 0);
      // Not requested: Java's null.
      assert.equal(await result.authorizedOperations().get(), null);
      const withOperations = admin.describeCluster(new DescribeClusterOptions({ includeAuthorizedOperations: true }));
      const operations = await withOperations.authorizedOperations().get();
      assert.ok(operations instanceof Set);
      for (const op of operations) assert.ok(op instanceof AclOperation);
      assert.equal(await withOperations.clusterId().get(), clusterId);
    } finally {
      await admin.close();
    }
  },
  60000,
);

(SKIP ? test.skip : test)(
  'listTopics: the internal topics only when asked for',
  async () => {
    const admin = Admin.create({ configs: { 'bootstrap.servers': BOOTSTRAP } });
    try {
      // __consumer_offsets exists once a group has committed; whatever the
      // broker has, the listing without internal topics holds none.
      const listings = await admin.listTopics().namesToListings().get();
      assert.ok(listings instanceof Map);
      for (const [name, listing] of listings) {
        assert.ok(listing instanceof TopicListing);
        assert.equal(listing.name(), name);
        assert.equal(listing.isInternal(), false);
        assert.ok(listing.topicId() !== null);
      }
      const all = admin.listTopics(new ListTopicsOptions({ listInternal: true }));
      const names = await all.names().get();
      for (const name of listings.keys()) assert.ok(names.has(name));
      assert.equal((await all.listings().get()).length, names.size);
    } finally {
      await admin.close();
    }
  },
  60000,
);
