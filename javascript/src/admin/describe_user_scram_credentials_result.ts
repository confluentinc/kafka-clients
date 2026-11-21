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

// Java's org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult.
// Its Java constructor is package-private and takes the response's future;
// as Python's Result for the admin client, this one takes the core's own
// futures of `all()`, `users()` and `description(userName)`, each read when
// asked for (Python's Futures rule names this Result), so each call hands out
// a new future. A private constructor, the client building it through
// `internalOfClient` (javascript/CLAUDE.md, JavaScript Binding Conventions, Class
// family, Q16). Java's package-private constructor, over the future of the
// response's per-user results, is `internalCreate`, with Java's composition
// (as R2's C-future Results keep Java's constructor beside the client's path).

import { JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import { ResourceNotFoundError } from '../common/errors/resource_not_found_error';
import type { ScramCredentialInfo } from './scram_credential_info';
import { UserScramCredentialsDescription } from './user_scram_credentials_description';

// One user's result in the response, Java's
// `DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult`:
// its error is null for Java's `Errors.NONE` and a `ResourceNotFoundError` for
// `RESOURCE_NOT_FOUND`.
type UserResult = {
  readonly user: string;
  readonly error: Error | null;
  readonly credentialInfos: ReadonlyArray<ScramCredentialInfo>;
};

// Java's `errorCode() != RESOURCE_NOT_FOUND.code()`: the class itself, as the
// code is the class's own.
const isNotFound = (error: Error | null): boolean => error !== null && Object.getPrototypeOf(error) === ResourceNotFoundError.prototype;

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const USERS_FORMS = new JavaForms('users', [[]], { reader: true });
const DESCRIPTION_FORMS = new JavaForms('description', [['userName']], {
  bareTypes: { userName: (value) => value === null || typeof value === 'string' },
});

/**
 * The result of the `Admin.describeUserScramCredentials()` call.
 *
 * Instances come from the admin client's `describeUserScramCredentials()`.
 */
export class DescribeUserScramCredentialsResult {
  readonly #all: () => KafkaFuture<Map<string, UserScramCredentialsDescription>>;
  readonly #users: () => KafkaFuture<string[]>;
  readonly #description: (userName: string | null) => KafkaFuture<UserScramCredentialsDescription>;

  private constructor(
    all: () => KafkaFuture<Map<string, UserScramCredentialsDescription>>,
    users: () => KafkaFuture<string[]>,
    description: (userName: string | null) => KafkaFuture<UserScramCredentialsDescription>,
  ) {
    this.#all = all;
    this.#users = users;
    this.#description = description;
  }

  /**
   * The result over the core's own futures, each read when asked for.
   * Called by the admin client.
   *
   * @param all - the core's `all()`
   * @param users - the core's `users()`
   * @param description - the core's `description(userName)`
   * @returns the result
   * @internal
   */
  static internalOfClient(
    all: () => KafkaFuture<Map<string, UserScramCredentialsDescription>>,
    users: () => KafkaFuture<string[]>,
    description: (userName: string | null) => KafkaFuture<UserScramCredentialsDescription>,
  ): DescribeUserScramCredentialsResult {
    return new DescribeUserScramCredentialsResult(all, users, description);
  }

  /**
   * Java's package-private constructor, over the future of the response's
   * per-user results, with Java's composition.
   *
   * @param dataFuture - the future of the per-user results
   * @returns the result
   * @internal
   */
  static internalCreate(dataFuture: KafkaFuture<ReadonlyArray<UserResult>>): DescribeUserScramCredentialsResult {
    const describe = (r: UserResult): UserScramCredentialsDescription =>
      new UserScramCredentialsDescription({ name: r.user, credentialInfos: [...r.credentialInfos] });
    const derive = <T>(compose: (data: ReadonlyArray<UserResult>) => { value: T; error: Error | null }): KafkaFuture<T> => {
      const retval = KafkaFuture.internalCreate<T>(false);
      dataFuture.whenComplete((data, throwable) => {
        if (throwable !== null && throwable !== undefined) {
          retval.internalComplete(null as unknown as T, throwable);
        } else {
          const { value, error } = compose(data as ReadonlyArray<UserResult>);
          retval.internalComplete(value, error);
        }
      });
      return retval;
    };
    const all = (): KafkaFuture<Map<string, UserScramCredentialsDescription>> =>
      derive((data) => {
        const failed = data.find((r) => r.error !== null && !isNotFound(r.error));
        if (failed !== undefined) return { value: null as unknown as Map<string, UserScramCredentialsDescription>, error: failed.error };
        return { value: new Map(data.map((r) => [r.user, describe(r)])), error: null };
      });
    const users = (): KafkaFuture<string[]> =>
      derive((data) => ({ value: data.filter((r) => !isNotFound(r.error)).map((r) => r.user), error: null }));
    const description = (userName: string | null): KafkaFuture<UserScramCredentialsDescription> =>
      derive((data) => {
        const userResult = data.find((r) => r.user === userName);
        const none = null as unknown as UserScramCredentialsDescription;
        if (userResult === undefined) {
          return { value: none, error: new ResourceNotFoundError(`No such user: ${String(userName)}`) };
        }
        // RESOURCE_NOT_FOUND is included here
        if (userResult.error !== null) return { value: none, error: userResult.error };
        return { value: describe(userResult), error: null };
      });
    return new DescribeUserScramCredentialsResult(all, users, description);
  }

  /**
   * A future for the results of all described users with map keys (one per
   * user) being consistent with the contents of the list returned by
   * `users()`. The future will complete successfully only if all such user
   * descriptions complete successfully.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<Map<string, UserScramCredentialsDescription>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.#all();
  }

  /**
   * A future indicating the distinct users that meet the request criteria
   * and that have at least one credential. The future will not complete
   * successfully if the user is not authorized to perform the describe
   * operation; otherwise, it will complete successfully as long as the list
   * of users with credentials can be successfully determined within some
   * hard-coded timeout period. Note that the returned list will not include
   * users that do not exist/have no credentials: a request to describe an
   * explicit list of users, none of which existed/had a credential, will
   * result in a future that returns an empty list being returned here. A
   * returned list will include users that have a credential but that could
   * not be described.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  users(): KafkaFuture<string[]> {
    if (arguments.length !== 0) USERS_FORMS.match(undefined, arguments.length);
    return this.#users();
  }

  /**
   * A future indicating the description results for the given user. The
   * future will complete with an error if the future returned by `users()`
   * completes with an error. Note that if the given user does not exist in
   * the list of described users then the returned future will complete with
   * a `ResourceNotFoundError`.
   *
   * @param userName - the name of the user description being requested, on
   *   its own or as `{ userName }`
   * @returns the future
   * @throws `TypeError` when the name is neither a string nor null
   */
  description(userName: string | null): KafkaFuture<UserScramCredentialsDescription>;
  /**
   * The same as `description(userName)`, the parameter given by name.
   *
   * @param options - the name of the user description being requested
   * @returns the future
   */
  description(options: {
    /** The name of the user description being requested. */
    userName: string | null;
  }): KafkaFuture<UserScramCredentialsDescription>;
  description(userName: unknown): KafkaFuture<UserScramCredentialsDescription> {
    DESCRIPTION_FORMS.match(userName, arguments.length);
    const name = DESCRIPTION_FORMS.value(userName, 'userName');
    if (name !== null && typeof name !== 'string') throw new TypeError(`userName must be a string or null, not ${kind(name)}`);
    return this.#description(name);
  }
}
