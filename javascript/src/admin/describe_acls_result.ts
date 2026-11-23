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

// Java's org.apache.kafka.clients.admin.DescribeAclsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). Java's `Collection<AclBinding>` is an
// array, as Java's response builds a list (Types).

import { JavaForms } from '../internal/args';
import type { AclBinding } from '../common/acl/acl_binding';
import type { KafkaFuture } from '../common/kafka_future';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });

/**
 * The result of the `KafkaAdminClient.describeAcls(filter)` call.
 *
 * Instances come from the admin client's `describeAcls()`.
 */
export class DescribeAclsResult {
  readonly #future: KafkaFuture<AclBinding[]>;

  private constructor(future: KafkaFuture<AclBinding[]>) {
    this.#future = future;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param future - the future of the ACLs
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<AclBinding[]>): DescribeAclsResult {
    return new DescribeAclsResult(future);
  }

  /**
   * Return a future containing the ACLs requested.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): KafkaFuture<AclBinding[]> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return this.#future;
  }
}
