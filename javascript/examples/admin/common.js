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

// The command line every admin example shares, as rust/examples/admin/common
// has it for the Rust examples, so a command carries over between clients:
//
//   node examples/admin/<rpc>.js [-X key=value]... [--flag] [--name=value]... [<argument>]...
//
// `-X key=value` sets a client configuration key (repeatable),
// KAFKA_BOOTSTRAP_SERVERS names the brokers (default localhost:9092) and
// `--help` prints the example's usage. The exit code is 0 when every call
// succeeded, 1 when a call failed and 2 for a wrong command line.

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { Admin, AdminClientConfig } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');
const {
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclOperation,
  AclPermissionType,
} = require('@confluentinc/kafka-javascript/common/acl');
const { ConfigResource } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

/** A wrong command line: printed with the usage text, exit code 2. */
class UsageError extends Error {}

/** `--help`: the usage text is printed and the example succeeds. */
class Help extends Error {}

/**
 * Ends the example with a usage error.
 *
 * @param {string} message what is wrong with the command line
 * @returns {never}
 */
function usage(message) {
  throw new UsageError(message);
}

let anyFailed = false;

/**
 * The error a future's `get()` rejected with: `ExecutionError` (Java's
 * `ExecutionException`) holds the call's or the key's own error as its cause.
 *
 * @param {unknown} error what `get()` rejected with
 * @returns {unknown} the error to report
 */
function causeOf(error) {
  return error instanceof ExecutionError && error.cause !== undefined ? error.cause : error;
}

/**
 * An error with its causes, as `Name: message (caused by: Name: message)`.
 *
 * @param {unknown} error the error
 * @returns {string} its description
 */
function describe(error) {
  let text = String(error);
  let cause = error instanceof Error ? error.cause : undefined;
  while (cause !== undefined && cause !== null) {
    // eslint-disable-next-line @typescript-eslint/no-base-to-string -- an error's cause may be any value, printed as String() prints it
    text += ` (caused by: ${String(cause)})`;
    cause = cause instanceof Error ? cause.cause : undefined;
  }
  return text;
}

/**
 * Reports one failed key and goes on; the example then exits with 1.
 *
 * @param {string} message what failed
 * @param {unknown} error the key's error (`causeOf` what `get()` rejected with)
 */
function fail(message, error) {
  anyFailed = true;
  console.error(`${message}: ${describe(causeOf(error))}`);
}

/**
 * The future a Result holds for `key`, one of the keys the call was given.
 * A Result keyed by the request has a future for each of its keys; one it
 * lacks is reported as that key's failure.
 *
 * @template T
 * @param {{ get(key: any): T | null | undefined }} futures the Result's futures by key
 * @param {unknown} key a key of the request
 * @returns {T} its future
 */
function futureOf(futures, key) {
  const future = futures.get(key);
  if (future === null || future === undefined) throw new Error(`the result has no future for ${String(key)}`);
  return future;
}

/** The parsed command line. */
class Args {
  /** @type {Record<string, string>} */
  config = {};
  /** @type {Array<[string, string | null]>} */
  #options = [];
  /** @type {string[]} */
  #positional = [];

  /** @param {string[]} raw the arguments after the script's name */
  constructor(raw) {
    for (let i = 0; i < raw.length; i++) {
      const arg = raw[i];
      if (arg === '--help') throw new Help();
      if (arg === '-X') {
        const property = raw[++i];
        // Not repeated in the error: it may be a secret such as sasl.jaas.config.
        const at = property === undefined ? -1 : property.indexOf('=');
        if (property === undefined || at < 0) usage('-X needs a key=value argument');
        this.config[property.slice(0, at)] = property.slice(at + 1);
      } else if (arg.startsWith('--')) {
        const option = arg.slice(2);
        const at = option.indexOf('=');
        this.#options.push(at < 0 ? [option, null] : [option.slice(0, at), option.slice(at + 1)]);
      } else {
        this.#positional.push(arg);
      }
    }
  }

  /**
   * Takes the boolean option `--name`.
   *
   * @param {string} name the option's name
   * @returns {boolean} whether it was given
   */
  flag(name) {
    let found = false;
    for (const value of this.#take(name)) {
      if (value !== null) usage(`--${name} takes no value`);
      found = true;
    }
    return found;
  }

  /**
   * Takes the option `--name=value`, if given; the last one wins.
   *
   * @param {string} name the option's name
   * @returns {string | null} its value, or null when it was not given
   */
  value(name) {
    return this.values(name).pop() ?? null;
  }

  /**
   * Takes every `--name=value` option, in command-line order.
   *
   * @param {string} name the option's name
   * @returns {string[]} their values
   */
  values(name) {
    return this.#take(name).map((value) => (value === null ? usage(`--${name} needs a value: --${name}=...`) : value));
  }

  /**
   * The positional arguments, failing on any option not taken before.
   *
   * @returns {string[]} the arguments
   */
  positional() {
    if (this.#options.length > 0) usage(`unknown option --${this.#options[0][0]}`);
    return this.#positional;
  }

  /**
   * @param {string} name
   * @returns {Array<string | null>}
   */
  #take(name) {
    const taken = this.#options.filter(([option]) => option === name).map(([, value]) => value);
    this.#options = this.#options.filter(([option]) => option !== name);
    return taken;
  }
}

/**
 * Runs an example: parses the command line, creates the admin client, calls
 * `run`, closes the client and sets the exit code.
 *
 * The client is closed in a `finally`: `await using admin = Admin.create(...)`
 * does the same where the runtime supports `await using` (Node 24 and later).
 *
 * @param {string} usageText the example's usage line
 * @param {(admin: import('@confluentinc/kafka-javascript/admin').Admin, args: Args) => Promise<void>} run the example
 */
function main(usageText, run) {
  // runMain reports every error itself and only sets the exit code, so its
  // Promise never rejects.
  void runMain(usageText, run);
}

/**
 * The body of `main`.
 *
 * @param {string} usageText the example's usage line
 * @param {(admin: import('@confluentinc/kafka-javascript/admin').Admin, args: Args) => Promise<void>} run the example
 * @returns {Promise<void>} settles once the exit code is set; never rejects
 */
async function runMain(usageText, run) {
  try {
    const args = new Args(process.argv.slice(2));
    const admin = Admin.create({
      configs: { [AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG]: process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092', ...args.config },
    });
    try {
      await run(admin, args);
    } finally {
      await admin.close();
    }
    process.exitCode = anyFailed ? 1 : 0;
  } catch (error) {
    if (error instanceof Help) {
      console.log(usageText);
      process.exitCode = 0;
    } else if (error instanceof UsageError) {
      console.error(`${error.message}\n\n${usageText}`);
      process.exitCode = 2;
    } else {
      console.error(`Error: ${describe(causeOf(error))}`);
      process.exitCode = 1;
    }
  }
}

/**
 * A Java `int` from the command line.
 *
 * @param {string} what what the number is, for the usage error
 * @param {string} text the argument
 * @returns {number} the number
 */
function parseInt32(what, text) {
  const value = /^[+-]?\d+$/.test(text) ? Number(text) : NaN;
  if (!Number.isInteger(value) || value < -(2 ** 31) || value >= 2 ** 31) usage(`invalid ${what}: ${JSON.stringify(text)}`);
  return value;
}

/**
 * A Java `short` from the command line.
 *
 * @param {string} what what the number is, for the usage error
 * @param {string} text the argument
 * @returns {number} the number
 */
function parseInt16(what, text) {
  const value = parseInt32(what, text);
  if (value < -(2 ** 15) || value >= 2 ** 15) usage(`invalid ${what}: ${JSON.stringify(text)}`);
  return value;
}

/**
 * A Java `long` from the command line.
 *
 * @param {string} what what the number is, for the usage error
 * @param {string} text the argument
 * @returns {bigint} the number
 */
function parseLong(what, text) {
  const value = /^[+-]?\d+$/.test(text) ? BigInt(text) : null;
  if (value === null || value < -(2n ** 63n) || value >= 2n ** 63n) usage(`invalid ${what}: ${JSON.stringify(text)}`);
  return value;
}

/**
 * A Java `double` from the command line.
 *
 * @param {string} what what the number is, for the usage error
 * @param {string} text the argument
 * @returns {number} the number
 */
function parseDouble(what, text) {
  // Rust's `f64` grammar: a sign, then `inf`, `infinity` or `nan` in any case,
  // or decimal digits with an optional point and exponent; no hexadecimal,
  // binary or octal form and no surrounding whitespace, which Number() takes.
  const match = /^([+-]?)(?:(inf|infinity)|(nan)|((?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?))$/i.exec(text);
  if (match === null) usage(`invalid ${what}: ${JSON.stringify(text)}`);
  if (match[2] !== undefined) return match[1] === '-' ? -Infinity : Infinity;
  if (match[3] !== undefined) return NaN;
  return Number(text);
}

/**
 * Orders two strings by their UTF-16 code units, as Rust's examples order
 * them by bytes, whatever the host's locale (`localeCompare` follows it, and
 * ignores case and punctuation at its first level).
 *
 * @param {string} a a string
 * @param {string} b another
 * @returns {number} negative, zero or positive, as `a` sorts before, with or after `b`
 */
function compareStrings(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * One of a set of names, read case-insensitively with `-` as `_`.
 *
 * @template T
 * @param {string} what what the name is, for the usage error
 * @param {string} text the argument
 * @param {Array<[string, T]>} choices the names and their values
 * @returns {T} the chosen value
 */
function choose(what, text, choices) {
  const wanted = text.toLowerCase().replaceAll('-', '_');
  const found = choices.find(([name]) => name === wanted);
  if (found === undefined) {
    usage(`invalid ${what}: ${JSON.stringify(text)}, expected one of ${choices.map(([name]) => name).join(', ')}`);
  }
  return found[1];
}

/**
 * The arguments in groups of `size`.
 *
 * @param {string[]} args the arguments
 * @param {number} size the group size
 * @param {boolean} required whether at least one group is needed
 * @returns {string[][]} the groups
 */
function groups(args, size, required) {
  if (args.length % size !== 0 || (required && args.length === 0)) usage(`expected arguments in groups of ${size}`);
  const out = [];
  for (let i = 0; i < args.length; i += size) out.push(args.slice(i, i + size));
  return out;
}

/**
 * A `<topic> <partition>` group.
 *
 * @param {string[]} group the group
 * @returns {TopicPartition} the partition
 */
function topicPartition(group) {
  return new TopicPartition({ topic: group[0], partition: parseInt32('partition', group[1]) });
}

/**
 * `<topic> <partition>` pairs.
 *
 * @param {string[]} args the arguments
 * @param {boolean} required whether at least one pair is needed
 * @returns {TopicPartition[]} the partitions
 */
function topicPartitions(args, required) {
  return groups(args, 2, required).map(topicPartition);
}

/**
 * A comma-separated list, empty items left out.
 *
 * @param {string} text the list
 * @returns {string[]} its items
 */
function commaList(text) {
  return text.split(',').filter((item) => item !== '');
}

/** The config resource types, by their command-line names. */
const RESOURCE_TYPES = 'types: topic, broker, broker_logger, group, client_metrics';

/**
 * A config resource type from the command line.
 *
 * @param {string} text the argument
 * @returns {ConfigResource.Type} the type
 */
function resourceType(text) {
  return choose('resource type', text, [
    ['topic', ConfigResource.Type.TOPIC],
    ['broker', ConfigResource.Type.BROKER],
    ['broker_logger', ConfigResource.Type.BROKER_LOGGER],
    ['group', ConfigResource.Type.GROUP],
    ['client_metrics', ConfigResource.Type.CLIENT_METRICS],
  ]);
}

/**
 * A config resource as `<type> <name>`.
 *
 * @param {ConfigResource} resource the resource
 * @returns {string} its label
 */
function resourceLabel(resource) {
  return `${resource.type().name().toLowerCase()} ${resource.name()}`;
}

/** The seven arguments of an ACL binding or filter, for the usage text. */
const ACL_ARGUMENTS = '<resource_type> <resource_name> <pattern_type> <principal> <host> <operation> <permission_type>';

/**
 * A parsed enum constant, refused when it is the enum's UNKNOWN.
 *
 * @template T
 * @param {string} what what the constant is, for the usage error
 * @param {string} text the argument
 * @param {T} parsed the constant parsed from it
 * @param {T} unknown the enum's UNKNOWN
 * @returns {T} the constant
 */
function known(what, text, parsed, unknown) {
  if (parsed === unknown) usage(`invalid ${what}: ${JSON.stringify(text)}`);
  return parsed;
}

/**
 * The enum constants of an ACL binding or filter's seven arguments.
 *
 * @param {string[]} group the arguments
 * @returns {{resourceType: ResourceType, patternType: PatternType, operation: AclOperation, permissionType: AclPermissionType}} the constants
 */
function aclEnums(group) {
  return {
    resourceType: known('resource type', group[0], ResourceType.fromString(group[0]), ResourceType.UNKNOWN),
    patternType: known('pattern type', group[2], PatternType.fromString(group[2].toUpperCase()), PatternType.UNKNOWN),
    operation: known('operation', group[5], AclOperation.fromString(group[5]), AclOperation.UNKNOWN),
    permissionType: known('permission type', group[6], AclPermissionType.fromString(group[6]), AclPermissionType.UNKNOWN),
  };
}

/**
 * An ACL binding from its seven arguments.
 *
 * @param {string[]} group the arguments
 * @returns {AclBinding} the binding
 */
function aclBinding(group) {
  const { resourceType, patternType, operation, permissionType } = aclEnums(group);
  try {
    return new AclBinding({
      pattern: new ResourcePattern({ resourceType, name: group[1], patternType }),
      entry: new AccessControlEntry({ principal: group[3], host: group[4], operation, permissionType }),
    });
  } catch (error) {
    return usage(`invalid ACL binding: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * An ACL binding filter from its seven arguments: `ANY` or `None` as the
 * name, the principal or the host matches any.
 *
 * @param {string[]} group the arguments
 * @returns {AclBindingFilter} the filter
 */
function aclBindingFilter(group) {
  const { resourceType, patternType, operation, permissionType } = aclEnums(group);
  /** @param {string} text */
  const anyOr = (text) => (text === 'ANY' || text === 'None' ? null : text);
  return new AclBindingFilter({
    patternFilter: new ResourcePatternFilter({ resourceType, name: anyOr(group[1]), patternType }),
    entryFilter: new AccessControlEntryFilter({ principal: anyOr(group[3]), host: anyOr(group[4]), operation, permissionType }),
  });
}

/**
 * An ACL binding as its seven arguments.
 *
 * @param {AclBinding} binding the binding
 * @returns {string} its text
 */
function bindingText(binding) {
  const pattern = binding.pattern();
  const entry = binding.entry();
  return [
    pattern.resourceType(),
    pattern.name(),
    pattern.patternType(),
    entry.principal(),
    entry.host(),
    entry.operation(),
    entry.permissionType(),
  ].join(' ');
}

/**
 * A principal from `<type>:<name>`.
 *
 * @param {string} text the argument
 * @returns {KafkaPrincipal} the principal
 */
function principal(text) {
  const at = text.indexOf(':');
  if (at <= 0 || at === text.length - 1) usage(`invalid principal ${JSON.stringify(text)}, expected <type>:<name>`);
  return new KafkaPrincipal({ principalType: text.slice(0, at), name: text.slice(at + 1) });
}

/**
 * A client quota entity as `type=name,...`, the default entity's name as `<default>`.
 *
 * @param {import('@confluentinc/kafka-javascript/common/quota').ClientQuotaEntity} entity the entity
 * @returns {string} its text
 */
function entityText(entity) {
  return [...entity.entries()]
    .map(([type, name]) => `${type}=${name ?? '<default>'}`)
    .sort()
    .join(',');
}

/**
 * Bytes as lower-case hex.
 *
 * @param {Uint8Array} bytes the bytes
 * @returns {string} the hex
 */
function toHex(bytes) {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('hex');
}

/**
 * Hex as bytes.
 *
 * @param {string} text the hex
 * @returns {Buffer} the bytes
 */
function fromHex(text) {
  if (text.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(text)) usage('invalid hex string');
  return Buffer.from(text, 'hex');
}

module.exports = {
  ACL_ARGUMENTS,
  Args,
  aclBinding,
  aclBindingFilter,
  bindingText,
  causeOf,
  compareStrings,
  choose,
  commaList,
  describe,
  fail,
  fromHex,
  futureOf,
  groups,
  main,
  parseDouble,
  parseInt16,
  parseInt32,
  parseLong,
  principal,
  entityText,
  RESOURCE_TYPES,
  resourceLabel,
  resourceType,
  toHex,
  topicPartition,
  topicPartitions,
  usage,
};
