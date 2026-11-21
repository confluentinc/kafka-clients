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

// Java's org.apache.kafka.clients.admin.DeleteAclsResult, with its nested
// FilterResult and FilterResults, classes of the namespace merged with it
// (root CLAUDE.md, JavaScript Binding Conventions, Names, Q11). The three Java
// constructors are package-private: private constructors here, the client
// building them through `internalCreate` (Class family, Q16). Java's map
// keyed by AclBindingFilter is an AclBindingFilterMap (Q25); `all()` is
// Java's `allOf` then `thenApply` over its futures.

import { JavaForms } from '../internal/args';
import { AclBindingFilterMap } from '../common/acl/acl_binding_filter_map';
import type { AclBinding } from '../common/acl/acl_binding';
import type { ApiError } from '../common/errors/api_error';
import { KafkaError } from '../common/kafka_error';
import { KafkaFuture } from '../common/kafka_future';

const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const BINDING_FORMS = new JavaForms('binding', [[]], { reader: true });
const ERROR_FORMS = new JavaForms('exception', [[]], { reader: true });

/**
 * The result of the `Admin.deleteAcls(filters)` call.
 *
 * Instances come from the admin client's `deleteAcls()`.
 */
export class DeleteAclsResult {
  readonly #futures: AclBindingFilterMap<KafkaFuture<DeleteAclsResult.FilterResults>>;

  private constructor(futures: AclBindingFilterMap<KafkaFuture<DeleteAclsResult.FilterResults>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by filter
   * @returns the result
   * @internal
   */
  static internalCreate(futures: AclBindingFilterMap<KafkaFuture<DeleteAclsResult.FilterResults>>): DeleteAclsResult {
    return new DeleteAclsResult(futures);
  }

  /**
   * Return a map from acl filters to futures which can be used to check the
   * status of the deletions by each filter.
   *
   * @returns a new map of the futures, by filter
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): AclBindingFilterMap<KafkaFuture<DeleteAclsResult.FilterResults>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return new AclBindingFilterMap(this.#futures);
  }

  /**
   * Return a future which succeeds only if all the ACLs deletions succeed,
   * and which contains all the deleted ACLs. Note that it if the filters
   * don't match any ACLs, this is not considered an error.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<AclBinding[]> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = [...this.#futures.values()];
    return KafkaFuture.allOf(futures).thenApply(() => {
      const acls: AclBinding[] = [];
      for (const value of futures) {
        // allOf succeeded, so each future is done: getNow reads its value.
        let results: DeleteAclsResult.FilterResults;
        try {
          results = value.getNow({ valueIfAbsent: null }) as DeleteAclsResult.FilterResults;
        } catch (e) {
          // This should be unreachable, since the future returned by
          // KafkaFuture#allOf should have failed if any Future failed.
          throw new KafkaError({ message: 'DeleteAclsResult#all: internal error', cause: e });
        }
        for (const result of results.values()) {
          const error = result.exception();
          if (error !== null) throw error;
          acls.push(result.binding() as AclBinding);
        }
      }
      return acls;
    });
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested classes DeleteAclsResult.FilterResult and FilterResults, reached through the class name as in Java
export namespace DeleteAclsResult {
  /**
   * A class containing either the deleted ACL binding or an exception if the
   * delete failed.
   */
  export class FilterResult {
    readonly #binding: AclBinding | null;
    readonly #error: ApiError | null;

    private constructor(binding: AclBinding | null, error: ApiError | null) {
      this.#binding = binding;
      this.#error = error;
    }

    /**
     * Java's package-private constructor. Called by the admin client.
     *
     * @param binding - the deleted binding, or null
     * @param error - the error the deletion failed with, or null
     * @returns the result
     * @internal
     */
    static internalCreate(binding: AclBinding | null, error: ApiError | null): FilterResult {
      return new FilterResult(binding, error);
    }

    /**
     * Return the deleted ACL binding or null if there was an error.
     *
     * @returns the binding, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    binding(): AclBinding | null {
      if (arguments.length !== 0) BINDING_FORMS.match(undefined, arguments.length);
      return this.#binding;
    }

    /**
     * Return an exception if the ACL delete was not successful or null if it
     * was.
     *
     * @returns the error, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    exception(): ApiError | null {
      if (arguments.length !== 0) ERROR_FORMS.match(undefined, arguments.length);
      return this.#error;
    }

    /**
     * The result as `JSON.stringify` writes it: `{ binding, exception }`,
     * the error as its text.
     *
     * @returns the result's fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return {
        binding: this.#binding === null ? null : this.#binding.toJSON(),
        exception: this.#error === null ? null : String(this.#error),
      };
    }
  }

  /**
   * A class containing the results of the delete ACLs operation.
   */
  export class FilterResults {
    readonly #values: readonly FilterResult[];

    private constructor(values: readonly FilterResult[]) {
      this.#values = values;
    }

    /**
     * Java's package-private constructor. Called by the admin client.
     *
     * @param values - the results, in the response's order
     * @returns the results
     * @internal
     */
    static internalCreate(values: readonly FilterResult[]): FilterResults {
      return new FilterResults(values);
    }

    /**
     * Return a list of delete ACLs results for a given filter.
     *
     * @returns a new array of the results
     * @throws `TypeError` when it is given an argument: it only reads
     */
    values(): FilterResult[] {
      if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
      return [...this.#values];
    }

    /**
     * The results as `JSON.stringify` writes them: an array of each
     * result's `toJSON()`.
     *
     * @returns the results
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): unknown[] {
      return this.#values.map((v) => v.toJSON());
    }
  }
}
