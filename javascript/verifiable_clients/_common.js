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

// What the verifiable producer shares with the other tools of this directory,
// the JavaScript twin of python/verifiable_clients/_common.py.
//
// Java gives each tool its own copy of printJson and the shutdown hook, and the
// argument parser and loadProps come from argparse4j and
// org.apache.kafka.common.utils.Utils; here they are written once. The
// argument parser reports errors with the messages of Python's argparse (a port
// of which argparse4j is), so the tools of both bindings fail alike.

const fs = require('node:fs');
const path = require('node:path');

/** Java's System.currentTimeMillis(). */
function nowMillis() {
  return Date.now();
}

/**
 * An event as Jackson writes it: Java's `@JsonPropertyOrder({"timestamp",
 * "name"})` puts `timestamp` then `name` first, and the event's own properties
 * follow in declaration order (a JavaScript object keeps the order its string
 * keys were added in).
 */
function event(name, fields = {}) {
  return { timestamp: nowMillis(), name, ...fields };
}

// Set once a write to stdout failed: the later writes are dropped, as Java's
// PrintStream only sets its error flag and the Python tool points stdout at
// the null device.
let stdoutBroken = false;

/**
 * Write `text` to file descriptor 1 at once, synchronously: the system tests
 * read stdout through a pipe, and a buffered line could be seen late or, after
 * a SIGKILL, never. A pipe Node made non-blocking answers EAGAIN while it is
 * full; the write is retried until the reader drained it.
 */
function writeStdout(text) {
  if (stdoutBroken) return;
  let buffer = Buffer.from(text, 'utf8');
  while (buffer.length > 0) {
    try {
      const written = fs.writeSync(1, buffer);
      buffer = buffer.subarray(written);
    } catch (error) {
      if (error && error.code === 'EAGAIN') continue;
      stdoutBroken = true;
      return;
    }
  }
}

/**
 * Java's printJson: one compact JSON object per line on stdout, or a
 * diagnostic if the data cannot be written as JSON (a BigInt, a cycle). A
 * failed write is dropped, so a closed pipe does not end the tool or skip its
 * shutdown sequence.
 */
function printJson(data) {
  let line;
  try {
    line = JSON.stringify(data);
    if (line === undefined) throw new TypeError(`${typeof data} is not JSON`);
  } catch (error) {
    line = `Bad data can't be written as json: ${error && error.message}`;
  }
  writeStdout(`${line}\n`);
}

/** A line of plain text on stdout, as Java's System.out.println. */
function printLine(text) {
  writeStdout(`${text}\n`);
}

// The log levels of VERIFIABLE_CLIENT_LOG_LEVEL, as Python's logging names them.
const LEVELS = { DEBUG: 10, INFO: 20, WARNING: 30, WARN: 30, ERROR: 40, CRITICAL: 50 };

/**
 * The tool's log, on stderr, as the Java tools' log4j configuration keeps logs
 * off stdout. `VERIFIABLE_CLIENT_LOG_LEVEL` sets the level (`WARNING` by
 * default). The client's own logs go to stderr through the core (`RUST_LOG`).
 */
function logger(name) {
  const configured = LEVELS[String(process.env.VERIFIABLE_CLIENT_LOG_LEVEL || 'WARNING').toUpperCase()];
  const threshold = configured === undefined ? LEVELS.WARNING : configured;
  const write = (level, message) => {
    if (LEVELS[level] < threshold) return;
    try {
      fs.writeSync(2, `[${new Date().toISOString()}] ${level} ${message} (${name})\n`);
    } catch {
      // stderr is gone: nothing is left to tell.
    }
  };
  return {
    debug: (message) => write('DEBUG', message),
    info: (message) => write('INFO', message),
    warning: (message) => write('WARNING', message),
    error: (message) => write('ERROR', message),
  };
}

/**
 * argparse4j's ArgumentParserException: a command line the parser rejects, or
 * one createFromArgs rejects after parsing.
 */
class ArgumentParserError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ArgumentParserError';
  }
}

/** `-h` / `--help` was given: the help is printed and the tool exits 0. */
class HelpRequested extends Error {
  constructor() {
    super('help requested');
    this.name = 'HelpRequested';
  }
}

// Python's argparse reads a token as a negative number, not an option, when it
// looks like one and the parser has no option that does.
const NEGATIVE_NUMBER = /^-\d+$|^-\d*\.\d+$/;
// Python's int(): optional whitespace and sign, decimal digits with single
// underscores between them.
const INT = /^\s*[+-]?\d+(?:_\d+)*\s*$/;

/**
 * The argparse4j parser the Java tools use, with what the tools need of it:
 * options taking one value (a string, or an `int` read as Python's `int()`
 * reads one), `choices`, required options, one required mutually exclusive
 * group, defaults shown in the help, and `-h` / `--help`. `parseArgs` throws
 * `ArgumentParserError` with Python's argparse messages, where argparse4j
 * throws to `main`, which prints it and exits 1.
 */
class ArgumentParser {
  constructor(prog, description) {
    this.prog = prog;
    this.description = description;
    this.options = [];
    this.groups = [];
  }

  /**
   * Adds an option. `spec`: `dest`, `type` (`'int'` or `'string'`, the
   * default), `defaultValue`, `required`, `choices`, `metavar`, `help`.
   */
  addArgument(flag, spec = {}) {
    const option = {
      flag,
      dest: spec.dest || flag.replace(/^--/, '').replace(/[-.]/g, '_'),
      type: spec.type || 'string',
      defaultValue: spec.defaultValue === undefined ? null : spec.defaultValue,
      required: Boolean(spec.required),
      choices: spec.choices || null,
      metavar: spec.metavar || flag.replace(/^--/, '').toUpperCase(),
      help: spec.help || '',
      group: spec.group || null,
    };
    this.options.push(option);
    return option;
  }

  /**
   * A group of options of which at most one may be given, and exactly one when
   * `required`. Returns an object whose `addArgument` adds to it.
   */
  addMutuallyExclusiveGroup({ title, description, required = false } = {}) {
    const group = { title, description, required, options: [] };
    this.groups.push(group);
    return {
      addArgument: (flag, spec = {}) => {
        const option = this.addArgument(flag, { ...spec, group });
        group.options.push(option);
        return option;
      },
    };
  }

  /** The `usage:` line, as argparse formats it. */
  formatUsage() {
    const parts = ['[-h]'];
    const shown = new Set();
    for (const option of this.options) {
      if (shown.has(option)) continue;
      if (option.group !== null) {
        const members = option.group.options.map((o) => `${o.flag} ${o.metavar}`).join(' | ');
        parts.push(option.group.required && option.group.options.length > 1 ? `(${members})` : option.group.required ? members : `[${members}]`);
        for (const member of option.group.options) shown.add(member);
        continue;
      }
      shown.add(option);
      const text = `${option.flag} ${option.metavar}`;
      parts.push(option.required ? text : `[${text}]`);
    }
    return `usage: ${this.prog} ${parts.join(' ')}\n`;
  }

  /** The help: usage, description, then each option with its default. */
  formatHelp() {
    const lines = [this.formatUsage(), this.description, '', 'options:', '  -h, --help            show this help message and exit'];
    const describe = (option) => {
      const shownDefault = option.defaultValue === null ? 'None' : String(option.defaultValue);
      lines.push(`  ${option.flag} ${option.metavar}`);
      lines.push(`                        ${option.help} (default: ${shownDefault})`);
    };
    for (const option of this.options) if (option.group === null) describe(option);
    for (const group of this.groups) {
      lines.push('', `${group.title}:`);
      if (group.description) lines.push(`  ${group.description}`, '');
      for (const option of group.options) describe(option);
    }
    return `${lines.join('\n')}\n`;
  }

  /**
   * Parses `argv` into an object keyed by each option's `dest`, holding its
   * value or its default.
   *
   * @throws {ArgumentParserError} on a command line the parser rejects
   * @throws {HelpRequested} when `-h` or `--help` is given
   */
  parseArgs(argv) {
    const byFlag = new Map(this.options.map((o) => [o.flag, o]));
    const result = {};
    for (const option of this.options) result[option.dest] = option.defaultValue;
    const seen = new Set();
    const unrecognized = [];
    for (let i = 0; i < argv.length; i++) {
      const token = argv[i];
      if (token === '-h' || token === '--help') throw new HelpRequested();
      const eq = token.startsWith('--') ? token.indexOf('=') : -1;
      const flag = eq > 0 ? token.slice(0, eq) : token;
      const option = byFlag.get(flag);
      if (option === undefined) {
        unrecognized.push(token);
        continue;
      }
      let raw;
      if (eq > 0) {
        raw = token.slice(eq + 1);
      } else {
        const next = argv[i + 1];
        if (next === undefined || (next.startsWith('-') && !NEGATIVE_NUMBER.test(next))) {
          throw new ArgumentParserError(`argument ${option.flag}: expected one argument`);
        }
        raw = next;
        i += 1;
      }
      if (option.group !== null) {
        const other = option.group.options.find((o) => o !== option && seen.has(o));
        if (other !== undefined) {
          throw new ArgumentParserError(`argument ${option.flag}: not allowed with argument ${other.flag}`);
        }
      }
      result[option.dest] = this.#convert(option, raw);
      seen.add(option);
    }
    const missing = this.options.filter((o) => o.required && !seen.has(o)).map((o) => o.flag);
    if (missing.length > 0) {
      throw new ArgumentParserError(`the following arguments are required: ${missing.join(', ')}`);
    }
    for (const group of this.groups) {
      if (group.required && !group.options.some((o) => seen.has(o))) {
        throw new ArgumentParserError(`one of the arguments ${group.options.map((o) => o.flag).join(' ')} is required`);
      }
    }
    if (unrecognized.length > 0) {
      throw new ArgumentParserError(`unrecognized arguments: ${unrecognized.join(' ')}`);
    }
    return result;
  }

  #convert(option, raw) {
    let value = raw;
    if (option.type === 'int') {
      if (!INT.test(raw)) throw new ArgumentParserError(`argument ${option.flag}: invalid int value: '${raw}'`);
      value = Number(raw.trim().replace(/_/g, ''));
    }
    if (option.choices !== null && !option.choices.includes(value)) {
      throw new ArgumentParserError(
        `argument ${option.flag}: invalid choice: '${raw}' (choose from ${option.choices.map((c) => `'${c}'`).join(', ')})`,
      );
    }
    return value;
  }
}

/**
 * Java's Utils.loadProps: read a properties file. An unreadable file throws
 * ArgumentParserError with the I/O error's message, as createFromArgs rethrows
 * the IOException.
 */
function loadProps(filename) {
  let contents;
  try {
    // java.util.Properties.load(InputStream) reads ISO 8859-1.
    contents = fs.readFileSync(filename, 'latin1');
  } catch (error) {
    throw new ArgumentParserError(`${error.message}`);
  }
  return parseProperties(contents);
}

// The whitespace java.util.Properties skips: space, tab and form feed.
const WHITESPACE = ' \t\f';

function isWhitespace(c) {
  return c !== undefined && c !== '' && WHITESPACE.includes(c);
}

function stripWhitespace(text) {
  let start = 0;
  let end = text.length;
  while (start < end && isWhitespace(text[start])) start++;
  while (end > start && isWhitespace(text[end - 1])) end--;
  return text.slice(start, end);
}

/**
 * java.util.Properties.load: `key=value`, `key:value` and `key value` lines,
 * `#` and `!` comment lines, blank lines, a line ending in an odd number of
 * backslashes continued on the next one (whose leading whitespace is dropped),
 * and backslash escapes in keys and values. Kafka's system tests write
 * multi-line values this way: `sasl.jaas.config` for a SASL_PLAINTEXT client
 * spans the JAAS file's lines. A value is also trimmed, as Java's ConfigDef
 * trims what it reads. A later line for a key replaces an earlier one. A
 * malformed `\uXXXX` escape throws, as Java's IllegalArgumentException ends
 * its main. Returns a plain object of strings.
 */
function parseProperties(contents) {
  const props = {};
  for (const line of logicalLines(contents)) {
    const [key, value] = splitProperty(line);
    props[key] = stripWhitespace(value);
  }
  return props;
}

/**
 * Properties.LineReader: the logical lines, comments and blank lines left out,
 * each continued line joined to the next without its leading whitespace. A
 * comment character starts a comment only at the start of a natural line,
 * never on a continuation.
 */
function logicalLines(contents) {
  const lines = [];
  let current = null;
  for (const natural of contents.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n')) {
    let start = 0;
    while (start < natural.length && isWhitespace(natural[start])) start++;
    const line = natural.slice(start);
    if (current === null && (line === '' || line[0] === '#' || line[0] === '!')) continue;
    let backslashes = 0;
    while (backslashes < line.length && line[line.length - 1 - backslashes] === '\\') backslashes++;
    const continued = backslashes % 2 === 1;
    const part = continued ? line.slice(0, -1) : line;
    current = current === null ? part : current + part;
    if (!continued) {
      lines.push(current);
      current = null;
    }
  }
  if (current !== null) lines.push(current);
  return lines;
}

/**
 * Properties.load0: the key runs up to the first unescaped `=`, `:` or
 * whitespace; whitespace and one `=` or `:` separator are then skipped, and the
 * rest is the value.
 */
function splitProperty(line) {
  let keyEnd = line.length;
  let valueStart = line.length;
  let hasSeparator = false;
  let precedingBackslash = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (!precedingBackslash && (c === '=' || c === ':')) {
      keyEnd = i;
      valueStart = i + 1;
      hasSeparator = true;
      break;
    }
    if (!precedingBackslash && isWhitespace(c)) {
      keyEnd = i;
      valueStart = i + 1;
      break;
    }
    precedingBackslash = c === '\\' && !precedingBackslash;
  }
  while (valueStart < line.length) {
    const c = line[valueStart];
    if (!isWhitespace(c)) {
      if (hasSeparator || (c !== '=' && c !== ':')) break;
      hasSeparator = true;
    }
    valueStart++;
  }
  return [unescape(line.slice(0, keyEnd)), unescape(line.slice(valueStart))];
}

const ESCAPES = { t: '\t', r: '\r', n: '\n', f: '\f' };

/**
 * Properties.loadConvert: `\t`, `\n`, `\r`, `\f` and `\uXXXX` escapes, and any
 * other escaped character as itself. A `\uXXXX` escape is one UTF-16 code
 * unit, as in Java, so a surrogate pair written as two escapes is one
 * character of the resulting string.
 */
function unescape(text) {
  if (!text.includes('\\')) return text;
  let out = '';
  let i = 0;
  while (i < text.length) {
    let c = text[i++];
    if (c !== '\\' || i === text.length) {
      out += c;
      continue;
    }
    c = text[i++];
    if (c === 'u') {
      const digits = text.slice(i, i + 4);
      if (!/^[0-9a-fA-F]{4}$/.test(digits)) throw new RangeError('Malformed \\uxxxx encoding.');
      out += String.fromCharCode(parseInt(digits, 16));
      i += 4;
    } else {
      out += ESCAPES[c] === undefined ? c : ESCAPES[c];
    }
  }
  return out;
}

/**
 * Java's Runtime.addShutdownHook: the JVM runs a shutdown hook on SIGINT and
 * SIGTERM, the signal the system tests send for a clean shutdown.
 *
 * `hook` (which may return a Promise) runs once, at the first signal, from the
 * event loop. Later signals are ignored, as the JVM ignores them once shutdown
 * has begun; the listeners stay installed, so a later signal cannot kill the
 * process with Node's default action, and Ctrl+C only runs the hook, as in
 * Java.
 */
class ShutdownHook {
  #hook;
  #running = null;
  #signalled = false;
  #listener;

  constructor(hook) {
    this.#hook = hook;
    this.#listener = () => this.#onSignal();
    process.on('SIGINT', this.#listener);
    process.on('SIGTERM', this.#listener);
  }

  /** Whether a signal started the hook. */
  get started() {
    return this.#signalled;
  }

  #onSignal() {
    if (this.#running !== null) return;
    this.#signalled = true;
    this.#running = Promise.resolve().then(() => this.#hook());
  }

  /**
   * Called as the tool ends: no hook starts from here on, and one already
   * started is awaited, as the JVM waits for its shutdown hooks before it
   * exits. A failed hook rejects here.
   */
  async finish() {
    if (this.#running === null) this.#running = Promise.resolve();
    await this.#running;
  }
}

/**
 * The client's public classes by constructor, each with the import path that
 * exports it, read from the package's `exports` map: Java's
 * `getClass().toString()` names a class by its package, which a JavaScript
 * error does not carry.
 */
let publicClasses = null;

function publicClassPaths() {
  if (publicClasses !== null) return publicClasses;
  publicClasses = new Map();
  let exportsMap = {};
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
    exportsMap = pkg.exports || {};
    for (const subpath of Object.keys(exportsMap)) {
      const importPath = subpath === '.' ? pkg.name : `${pkg.name}/${subpath.replace(/^\.\//, '')}`;
      let module;
      try {
        module = require(importPath);
      } catch {
        continue;
      }
      for (const value of Object.values(module)) {
        if (typeof value === 'function' && !publicClasses.has(value)) publicClasses.set(value, importPath);
      }
    }
  } catch {
    // No package.json beside this directory: classes are named without a path.
  }
  return publicClasses;
}

/**
 * Java's `Exception.getClass().toString()`: `"class "` and the class's full
 * name, here the import path that exports the client's class and its name
 * (`class @confluentinc/kafka-javascript/common.KafkaError`), or the bare name
 * of a class the client does not export (`class TypeError`).
 */
function className(error) {
  const ctor = error !== null && error !== undefined ? error.constructor : undefined;
  if (typeof ctor !== 'function') return `class ${typeof error}`;
  const importPath = publicClassPaths().get(ctor);
  return importPath === undefined ? `class ${ctor.name}` : `class ${importPath}.${ctor.name}`;
}

/**
 * Java's `Exception.getMessage()`: the error's message, null for none (a
 * client error's message is `''` when Java's is null).
 */
function errorMessage(error) {
  if (error === null || error === undefined) return null;
  const message = typeof error === 'object' || typeof error === 'function' ? error.message : String(error);
  return typeof message === 'string' && message !== '' ? message : null;
}

module.exports = {
  ArgumentParser,
  ArgumentParserError,
  HelpRequested,
  ShutdownHook,
  className,
  errorMessage,
  event,
  loadProps,
  logger,
  nowMillis,
  parseProperties,
  printJson,
  printLine,
};
