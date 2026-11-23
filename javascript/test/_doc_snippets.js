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

// The code blocks of the user docs (README.md, MIGRATION.md), for the tests
// that check them: test/static/doc_snippets.test.js type-checks every block,
// test/unit/index.test.js and test/integration/doc_examples.test.js run the
// ones a user can run as written, or with the names the text around them
// defines. A block that is a fragment (it uses names an earlier block or the
// text defines) is marked `<!-- snippet: fragment -->` on the line before its
// fence.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const util = require('node:util');

const ROOT = path.join(__dirname, '..');
const DOCS = ['README.md', 'MIGRATION.md'];
const FRAGMENT = '<!-- snippet: fragment -->';

/**
 * A code block of a doc: where it is, the heading it is under, its language
 * and its lines, the fence's indentation removed.
 *
 * @typedef {{ doc: string, line: number, heading: string, lang: string, code: string[], fragment: boolean }} Block
 */

/**
 * The fenced code blocks of a Markdown text.
 *
 * @param {string} doc the doc's name
 * @param {string} text its text
 * @returns {Block[]} its blocks
 */
function blocksOf(doc, text) {
  const lines = text.split('\n');
  /** @type {Block[]} */
  const blocks = [];
  let heading = '';
  for (let i = 0; i < lines.length; i++) {
    const title = /^#+\s+(.*)$/.exec(lines[i]);
    if (title !== null) heading = title[1].trim();
    const open = /^(\s*)```(\S*)\s*$/.exec(lines[i]);
    if (open === null) continue;
    const [, indent, lang] = open;
    let end = i + 1;
    while (end < lines.length && !/^\s*```\s*$/.test(lines[end])) end++;
    assert.ok(end < lines.length, `${doc}:${i + 1}: a code block is not closed`);
    let before = i - 1;
    while (before >= 0 && lines[before].trim() === '') before--;
    blocks.push({
      doc,
      line: i + 1,
      heading,
      lang,
      code: lines.slice(i + 1, end).map((l) => (l.startsWith(indent) ? l.slice(indent.length) : l)),
      fragment: before >= 0 && lines[before].trim() === FRAGMENT,
    });
    i = end;
  }
  return blocks;
}

/**
 * The blocks of every doc.
 *
 * @returns {Block[]} the blocks, in the docs' order
 */
function docBlocks() {
  return DOCS.flatMap((doc) => blocksOf(doc, fs.readFileSync(path.join(ROOT, doc), 'utf8')));
}

/**
 * The one `js` block under a heading of a doc that contains a text.
 *
 * @param {string} doc the doc
 * @param {string} heading the heading it is under
 * @param {string} contains a text of the block, to tell blocks under one heading apart
 * @returns {Block} the block
 */
function jsBlock(doc, heading, contains = '') {
  const found = docBlocks().filter(
    (b) => b.doc === doc && b.heading === heading && b.lang === 'js' && b.code.join('\n').includes(contains),
  );
  assert.equal(found.length, 1, `${doc} has ${found.length} js blocks under "${heading}" containing ${JSON.stringify(contains)}`);
  return found[0];
}

/** @type {new (...args: string[]) => (...args: unknown[]) => Promise<unknown>} */
const AsyncFunction = /** @type {never} */ (Object.getPrototypeOf(async () => {}).constructor);

/**
 * What a block printed with `console.log`, and what the producers it created were.
 *
 * @typedef {{ printed: string[], created: import('@confluentinc/kafka-javascript/producer').KafkaProducer<unknown, unknown>[] }} Run
 */

/**
 * Runs a block as a user's async code: its `require` is the package's, except
 * that the KafkaProducer it gets records each producer it creates, and its
 * `console.log` records what it prints. Each `[from, to]` of `replace` is
 * replaced first, once, and must be found. `scope` gives the names the text
 * around a fragment defines (with `producerClasses`, the producer package's
 * classes, its KafkaProducer the recording one), and `result`, when given, is
 * an expression of the block's names the run returns. The producers the
 * block created are closed after it, at once (a second close() resolves).
 *
 * @param {Block} block the block
 * @param {{ replace?: [string, string][], producerClasses?: boolean, scope?: Record<string, unknown>, result?: string }} [options] how
 * @returns {Promise<{ run: Run, value: unknown, error: unknown }>} what it printed and created, and what it returned or threw
 */
async function runBlock(block, options = {}) {
  let code = block.code.join('\n');
  for (const [from, to] of options.replace ?? []) {
    assert.ok(code.includes(from), `${block.doc}:${block.line}: the block has no ${JSON.stringify(from)}`);
    code = code.replace(from, to);
  }
  if (options.result !== undefined) code += `\nreturn ${options.result};`;
  const producers = require('@confluentinc/kafka-javascript/producer');
  /** @type {Run} */
  const run = { printed: [], created: [] };
  const Base =
    /** @type {new (...args: unknown[]) => import('@confluentinc/kafka-javascript/producer').KafkaProducer<unknown, unknown>} */ (
      /** @type {unknown} */ (producers.KafkaProducer)
    );
  /** The producer a block constructs, recorded so the run can close it. */
  class KafkaProducer extends Base {
    /** @param {unknown[]} args the constructor's */
    constructor(...args) {
      super(...args);
      run.created.push(this);
    }
  }
  const tracked = { ...producers, KafkaProducer };
  const docRequire = (/** @type {string} */ id) => (id === '@confluentinc/kafka-javascript/producer' ? tracked : require(id));
  const docConsole = { ...console, log: (/** @type {unknown[]} */ ...args) => void run.printed.push(util.format(...args)) };
  const scope = { require: docRequire, console: docConsole, ...(options.producerClasses ? tracked : {}), ...(options.scope ?? {}) };
  const fn = new AsyncFunction(...Object.keys(scope), code);
  try {
    return { run, value: await fn(...Object.values(scope)), error: undefined };
  } catch (error) {
    return { run, value: undefined, error };
  } finally {
    // What the block left open; a second close() resolves.
    for (const producer of run.created) await producer.close({ timeout: 0 });
  }
}

module.exports = { ROOT, DOCS, FRAGMENT, blocksOf, docBlocks, jsBlock, runBlock };
