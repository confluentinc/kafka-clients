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

// The futures of an admin RPC's Result (private): each is a KafkaFuture over
// a native future the result handed out (native/admin.c), whose untimed wait
// starts here, when the Result is made, so the outcome is in JS when the core
// completes it (src/common/kafka_future.ts, the admin value source). The
// addon converts the value to plain values; `post` builds the classes.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { KafkaFuture } from '../common/kafka_future';

/** The native admin client handle (`internal/native.ts`). */
export type AdminHandle = ReturnType<typeof native.adminCreate>;

/** The box over an RPC's native result (`internal/native.ts`). */
export type ResultBox = ReturnType<typeof native.adminListTopics>;

/**
 * A future over the native future in `slot`, its untimed wait started.
 *
 * @param handle - the admin client's handle
 * @param slot - the native future's slot
 * @param post - builds the value from the addon's plain value
 * @returns the future
 */
function waiting<T>(handle: AdminHandle, slot: number, post: (raw: unknown) => T): KafkaFuture<T> {
  const future = KafkaFuture.internalCreate<T>(false);
  future.internalSetNative(handle, slot);
  native.adminFutureWait(handle, slot, (value, error) => {
    if (error !== null) {
      future.internalSetDelivery(null as T, error);
      return;
    }
    let built: T;
    try {
      built = post(value);
    } catch (thrown) {
      future.internalSetDelivery(null as T, thrown);
      return;
    }
    future.internalSetDelivery(built, null);
  });
  return future;
}

/**
 * A future over a native future the addon put in `slot` while it converted
 * another future's value (`ListTransactionsResult.byBrokerId`'s futures by
 * broker), its untimed wait started.
 *
 * @param handle - the admin client's handle
 * @param slot - the native future's slot
 * @param post - builds the value from the addon's plain value
 * @returns the future
 */
export function adminFutureOfSlot<T>(handle: AdminHandle, slot: number, post: (raw: unknown) => T): KafkaFuture<T> {
  return waiting(handle, slot, post);
}

/**
 * The future accessor `name` of a result hands out, for `key` (null for an
 * accessor of no key).
 *
 * @param handle - the admin client's handle
 * @param box - the result's box
 * @param name - the C accessor's name (`cluster_id`)
 * @param key - its key, or null
 * @param post - builds the value from the addon's plain value
 * @returns the future
 */
export function adminFuture<T>(handle: AdminHandle, box: ResultBox, name: string, key: unknown, post: (raw: unknown) => T): KafkaFuture<T> {
  return waiting(handle, native.adminResultFuture(handle, box, name, key), post);
}

/**
 * The futures of map accessor `name`, keyed by `keyOf` of each key the addon
 * read, in the map's order.
 *
 * @param handle - the admin client's handle
 * @param box - the result's box
 * @param name - the C accessor's name (`values`)
 * @param keyOf - builds a key from the addon's plain key
 * @param post - builds a value from the addon's plain value
 * @returns the futures, by key, or null when the accessor answers no map
 */
export function adminFutures<K, T>(
  handle: AdminHandle,
  box: ResultBox,
  name: string,
  keyOf: (raw: unknown) => K,
  post: (raw: unknown) => T,
): Array<[K, KafkaFuture<T>]> | null {
  return native.adminResultFutures(handle, box, name)?.map(([key, slot]) => [keyOf(key), waiting(handle, slot, post)]) ?? null;
}
