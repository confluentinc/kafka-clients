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

// Java's org.apache.kafka.clients.admin.CreateAclsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java's map keyed by AclBinding is an
// AclBindingMap (Q25); `all()` is Java's `allOf` over its futures.

import { JavaForms } from '../internal/args';
import { AclBindingMap } from '../common/acl/acl_binding_map';
import { KafkaFuture } from '../common/kafka_future';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.createAcls(acls)` call.
 *
 * Instances come from the admin client's `createAcls()`.
 */
export class CreateAclsResult {
  readonly #futures: AclBindingMap<KafkaFuture<void>>;

  private constructor(futures: AclBindingMap<KafkaFuture<void>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by binding
   * @returns the result
   * @internal
   */
  static internalCreate(futures: AclBindingMap<KafkaFuture<void>>): CreateAclsResult {
    return new CreateAclsResult(futures);
  }

  /**
   * Return a map from ACL bindings to futures which can be used to check the
   * status of the creation of each ACL binding.
   *
   * @returns a new map of the futures, by binding
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): AclBindingMap<KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return new AclBindingMap(this.#futures);
  }

  /**
   * Return a future which succeeds only if all the ACL creations succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}
