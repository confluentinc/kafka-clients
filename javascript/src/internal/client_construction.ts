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

// How KafkaProducer and KafkaConsumer are built (private), in Java's order:
// the configuration is parsed first, so its errors come first; the serde
// config functions get the configuration with the client.id the parsed
// configuration generated, as Java configures a serde with its originals()
// and that client.id; and the client is built from the same parsed
// configuration, which is freed on every path (native/config.c). create()
// parses before it awaits the functions, and hands the constructor what it
// made (`Prepared`).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('./native');
import { configEntries } from './config';
import {
  awaitedSerdes,
  checkSerdeKeys,
  closeSerde,
  configuredSerde,
  DESERIALIZER,
  isConstructionFailure,
  type Serde,
  type SerdePair,
  type SerdeRole,
} from './serde';

/** A parsed configuration (native/config.c). */
type ConfigHandle = ReturnType<typeof native.configCreate>;

/** What `create()` made before it calls the constructor: the parsed configuration and the serdes its functions made. */
export interface Prepared {
  /** The parsed configuration, which the constructor frees. */
  readonly config: ConfigHandle;
  /** The serdes the config functions made, the key's then the value's, null where none is. */
  readonly serdes: SerdePair;
}

/** A client's parts, once built: its native handle and the serdes it runs. */
export interface Built<H> {
  readonly handle: H;
  readonly key: Serde | null;
  readonly value: Serde | null;
}

/**
 * The configuration a serde config function gets: the client's, with the
 * `client.id` the parsed configuration generated, as Java's
 * `originals(Collections.singletonMap(CLIENT_ID_CONFIG, clientId))`.
 *
 * @param config - the parsed configuration
 * @param configs - the client's configuration
 * @returns the configuration, frozen
 */
function serdeConfigs(config: ConfigHandle, configs: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const out: Record<string, unknown> = {};
  // defineProperty, so that a `__proto__` key is a key, not the prototype.
  for (const [key, value] of Object.entries(configs)) {
    Object.defineProperty(out, key, { value, enumerable: true, writable: true, configurable: true });
  }
  Object.defineProperty(out, 'client.id', {
    value: native.configGetString(config, 'client.id'),
    enumerable: true,
    writable: true,
    configurable: true,
  });
  return Object.freeze(out);
}

/**
 * Builds a client, as Java's constructor does: the configuration is parsed
 * (unless `create()` did), the serde config functions whose serde is not given
 * are called with the configuration and its generated `client.id`, the key's
 * first, and the client is built from the parsed configuration. A serde given
 * wins, and its config key is ignored. On a failure Java's constructor closes
 * the serdes it has taken in the catch around its body, whose failure it
 * relabels "Failed to construct kafka producer" (or `consumer`): a
 * `ConfigError`, thrown before that catch, leaves the serdes given open, as
 * in Java, while those the functions made are the client's own, which nobody
 * else can close.
 *
 * @param role - the client's role
 * @param configs - the client's configuration, checked
 * @param given - the key's and the value's serdes given as arguments, or null
 * @param prepared - what `create()` made, or null
 * @param build - builds the native client over the parsed configuration
 * @returns the client's handle and its serdes
 *
 * @throws `ConfigError` when the configuration is invalid, or a serde config
 *   key holds no serde function
 * @throws `KafkaError` "Failed to construct kafka producer" (or `consumer`)
 *   when a serde config function throws, or the core cannot build the client
 */
export function construct<H>(
  role: SerdeRole,
  configs: Readonly<Record<string, unknown>>,
  given: readonly [Serde | null, Serde | null],
  prepared: Prepared | null,
  build: (config: ConfigHandle) => H,
): Built<H> {
  let config: ConfigHandle;
  if (prepared === null) {
    checkSerdeKeys(role, configs, given, false);
    config = native.configCreate(role === DESERIALIZER, configEntries(configs, role.keys));
  } else {
    config = prepared.config;
  }
  let keyMade: Serde | null = prepared?.serdes[0] ?? null;
  let valueMade: Serde | null = prepared?.serdes[1] ?? null;
  // How many serdes construction has taken, key first: Java's constructor
  // assigns the key's, then the value's, and its catch closes only those.
  let taken = prepared === null ? 0 : 2;
  try {
    if (prepared === null) {
      const forFunctions = serdeConfigs(config, configs);
      if (given[0] === null) keyMade = configuredSerde(role, forFunctions, 0);
      taken = 1;
      if (given[1] === null) valueMade = configuredSerde(role, forFunctions, 1);
      taken = 2;
    }
    const handle = build(config);
    return { handle, key: given[0] ?? keyMade, value: given[1] ?? valueMade };
  } catch (error) {
    const construction = isConstructionFailure(role, error);
    if (taken >= 1) closeSerde(role, construction ? (given[0] ?? keyMade) : keyMade, role.args[0]);
    if (taken >= 2) closeSerde(role, construction ? (given[1] ?? valueMade) : valueMade, role.args[1]);
    throw error;
  } finally {
    native.configFree(config);
  }
}

/**
 * What `create()` does before it calls the constructor: the configuration is
 * parsed, and each serde config function whose serde is not given is awaited
 * in turn, the key's first, with the configuration and its generated
 * `client.id`. The parsed configuration is held across the awaits and freed
 * on a failure; on success the constructor frees it.
 *
 * @param role - the client's role
 * @param configs - the client's configuration, checked
 * @param given - the key's and the value's serdes given as arguments, or null
 * @returns a Promise of the parsed configuration and the serdes made
 *
 * @throws `ConfigError` (a rejection) when the configuration is invalid, or a
 *   serde config key holds no serde function or its function gives no serde
 * @throws `KafkaError` "Failed to construct kafka producer" (or `consumer`, a
 *   rejection) when a serde config function throws or its Promise rejects
 */
export function prepare(
  role: SerdeRole,
  configs: Readonly<Record<string, unknown>>,
  given: readonly [Serde | null, Serde | null],
): Promise<Prepared> {
  checkSerdeKeys(role, configs, given, true);
  const config = native.configCreate(role === DESERIALIZER, configEntries(configs, role.keys));
  let forFunctions: Readonly<Record<string, unknown>>;
  try {
    forFunctions = serdeConfigs(config, configs);
  } catch (error) {
    native.configFree(config);
    throw error;
  }
  return awaitedSerdes(role, forFunctions, given).then(
    (serdes) => ({ config, serdes }),
    (error: unknown) => {
      native.configFree(config);
      throw error;
    },
  );
}
