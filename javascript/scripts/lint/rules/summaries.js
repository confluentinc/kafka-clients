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

// check-summaries (javascript/CLAUDE.md, Comments, point 2): every class and
// interface of the files the Comments rule covers (scripts/lint/comment_sources.js),
// exported or not, has a TSDoc summary, the text of its doc comment before the
// first tag, and every file of native/ opens, after its licence header, with a
// comment saying what it does. ESLint's half (`ts`) reads the TypeScript and
// JavaScript classes, class expressions and interfaces; the runner's
// (`other`) the files of native/ and the Java tool's classes and interfaces
// (tools/java-vectors/), read with the C tokenizer.

const { scan } = require('../c_source');
const { scopeFiles, HASH_LIKE } = require('../comment_sources');

/** The last line of every licence header. */
const LICENSE_END = 'limitations under the License.';

/** The summary of doc comment text `text` (`/** ... *\/`): its text before the first tag, without the markers. */
function docSummary(text) {
  const lines = text
    .replace(/^\/\*\*/, '')
    .replace(/\*\/$/, '')
    .split('\n')
    .map((l) => l.replace(/^\s*\*?\s?/, '').trim());
  const out = [];
  for (const l of lines) {
    if (l.startsWith('@')) break;
    out.push(l);
  }
  return out.join(' ').trim();
}

/** The name of class or interface `node`: its own, its variable's for a class expression, or `(anonymous)`. */
function nameOf(ts, node) {
  if (node.name) return node.name.text;
  if (node.parent && ts.isVariableDeclaration(node.parent) && ts.isIdentifier(node.parent.name)) return node.parent.name.text;
  return '(anonymous)';
}

/**
 * The summary of class or interface `node` of source `sf`: the text before
 * the first tag of the doc comment TypeScript attaches to it (or to its
 * variable), else of a doc comment right before it or before the parentheses
 * around it, as on a class expression passed as an argument
 * (`new (class {})()`); `null` when it has no doc comment.
 */
function summaryOf(ts, sf, node) {
  const docs = ts.getJSDocCommentsAndTags(node).filter(ts.isJSDoc);
  if (docs.length > 0) return docSummary(docs[docs.length - 1].getText(sf));
  const text = sf.getFullText();
  for (let host = node; host !== undefined; host = ts.isParenthesizedExpression(host.parent) ? host.parent : undefined) {
    // A comment on the line of the token before is that token's trailing one.
    const ranges = [...(ts.getTrailingCommentRanges(text, host.pos) || []), ...(ts.getLeadingCommentRanges(text, host.pos) || [])].filter(
      (r) => text.startsWith('/**', r.pos),
    );
    if (ranges.length > 0) return docSummary(text.slice(ranges[ranges.length - 1].pos, ranges[ranges.length - 1].end));
  }
  return null;
}

/** The findings on the classes and interfaces of TypeScript or JavaScript file `rel`; adds to `count`. */
function tsFindings(ctx, rel, count) {
  const ts = ctx.typescript();
  const sf = ctx.sourceFile(rel);
  const out = [];
  const visit = (node) => {
    const isClass = ts.isClassDeclaration(node) || ts.isClassExpression(node);
    if (isClass || ts.isInterfaceDeclaration(node)) {
      count.n += 1;
      const summary = summaryOf(ts, sf, node);
      if (!summary) {
        const what = isClass ? 'class' : 'interface';
        const name = nameOf(ts, node);
        const at = sf.getLineAndCharacterOfPosition(node.getStart(sf));
        out.push({
          kind: 'no-summary',
          subject: `${rel}:${name}`,
          at: { file: rel, line: at.line + 1, column: at.character },
          message:
            `the ${what} \`${name}\` (line ${at.line + 1}) has no TSDoc summary` +
            `${summary === null ? '' : ', only tags'}: give it one concise sentence of what it does`,
        });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return out;
}

/** The modifiers and annotations that may stand between a Java declaration's doc comment and its keyword. */
const JAVA_MODIFIERS = new Set(['public', 'protected', 'private', 'static', 'final', 'abstract', 'sealed', 'strictfp']);

/** The findings on the classes and interfaces of Java file `rel`; adds to `count`. */
function javaFindings(ctx, rel, count) {
  const scanned = scan(ctx.read(rel));
  const { tokens, comments, text } = scanned;
  const out = [];
  // The offset of each comment's start, to find the one right before a declaration.
  const commentAt = new Map();
  let offset = 0;
  const lineStarts = [0];
  for (let k = 0; k < text.length; k++) if (text[k] === '\n') lineStarts.push(k + 1);
  for (const c of comments) {
    offset = text.indexOf(c.text, lineStarts[c.line - 1]);
    commentAt.set(offset + c.text.length, c);
  }
  for (let i = 0; i + 1 < tokens.length; i++) {
    const t = tokens[i];
    if (t.type !== 'ident' || (t.value !== 'class' && t.value !== 'interface') || tokens[i + 1].type !== 'ident') continue;
    // `x.class` is a class literal, not a declaration.
    if (i > 0 && tokens[i - 1].value === '.') continue;
    count.n += 1;
    let first = i;
    while (first > 0 && tokens[first - 1].type === 'ident' && JAVA_MODIFIERS.has(tokens[first - 1].value)) first--;
    const before = text.slice(0, tokens[first].start).trimEnd();
    const doc = commentAt.get(before.length);
    const summary = doc && doc.text.startsWith('/**') ? docSummary(doc.text) : '';
    if (!summary) {
      const name = tokens[i + 1].value;
      out.push({
        kind: 'no-summary',
        subject: `${rel}:${name}`,
        at: { file: rel, line: t.line, column: 0 },
        message: `the ${t.value} \`${name}\` (line ${t.line}) has no Javadoc summary: give it one concise sentence of what it does`,
      });
    }
  }
  return out;
}

/** The finding on native/ file `rel` when it does not open with a summary after its licence header, or null. */
function fileFinding(ctx, rel) {
  const lines = ctx.read(rel).split('\n');
  const hash = HASH_LIKE.some((e) => rel.endsWith(e));
  const isComment = (l) => (hash ? l.startsWith('#') : /^(\/\/|\/\*|\*)/.test(l));
  const blankComment = (l) => /^(\/\/+|\/\*+|\*+\/?|#+)$/.test(l);
  const end = lines.findIndex((l) => l.includes(LICENSE_END));
  let message = null;
  if (end < 0) {
    message = 'has no licence header to open with a summary after: give it its header, then a comment saying what the file does';
  } else {
    const next = lines
      .slice(end + 1)
      .map((l) => l.trim())
      .find((l) => l !== '' && !blankComment(l));
    if (next === undefined || !isComment(next)) {
      message = 'does not open, after its licence header, with a comment saying what the file does: add one';
    }
  }
  return message === null ? null : { kind: 'no-file-summary', subject: rel, at: { file: rel, line: 1, column: 0 }, message };
}

module.exports = {
  name: 'check-summaries',
  kinds: ['no-summary', 'no-file-summary'],
  /**
   * The files this instance reads: `all`, `ts` (the TypeScript and
   * JavaScript files, ESLint's `kafka/summaries`) or `other` (native/ and the
   * Java tool, the runner's).
   */
  PART: 'all',
  get needs() {
    return this.PART === 'other' ? [] : ['typescript'];
  },
  hint:
    'Give every class and interface one concise TSDoc summary of what it does, and every file of native/ one ' +
    'after its licence header (javascript/CLAUDE.md, Comments, point 2).',
  check(ctx) {
    const findings = [];
    const count = { n: 0 };
    const readTs = this.PART !== 'other';
    const readOther = this.PART !== 'ts';
    if (readTs) {
      for (const rel of scopeFiles(ctx, 'ts')) findings.push(...tsFindings(ctx, rel, count));
    }
    if (readOther) {
      for (const rel of scopeFiles(ctx, 'other')) {
        if (rel.endsWith('.java')) findings.push(...javaFindings(ctx, rel, count));
      }
      for (const rel of ctx.files(['native'])) {
        count.n += 1;
        const f = fileFinding(ctx, rel);
        if (f !== null) findings.push(f);
      }
    }
    return { findings, checked: count.n };
  },
};
