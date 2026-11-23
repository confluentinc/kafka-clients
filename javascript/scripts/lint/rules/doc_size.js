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

// check-doc-size (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Documentation: "a comment has at most 150% of the Javadoc's words ... as
// .claude/rules/documentation.md says for Rust"): §5 of that file, in TSDoc,
// so the two clients count alike. The Java words are the index's (`javadoc`:
// each Javadoc translated and counted by `check-api-docs`' code, one that only
// points elsewhere replaced by the one it points to). The TypeScript words are
// counted as the Rust lint counts (`wordCount`), and only what has a Java
// counterpart: the description, `@remarks` (Rust's notes), `@see` and
// `@example`; a `@param` Java has (the options object's when Java has any);
// `@returns` when Java has `@return`; a `@throws` naming the error of a Java
// `@throws`. Checked: the TSDoc of every export, public member and overload
// signature mapping to a Java member with a Javadoc (scripts/lint/java_ts.js,
// `javaTargets` included), a signature against the Java form the overload-shape
// mapping finds for it, else against every form of the set, passing when it
// fits one; a function type translating a one-method interface (`Callback`)
// against the interface's Javadoc and its method's together. Not checked: a
// member with no Java counterpart (an invented one, an `...Args` key, the keys
// of an options object), and what has no Javadoc.

const { atNode } = require('../location');
const {
  javaIndexOf,
  mappedItems,
  exportedItems,
  itemMembers,
  publicMembers,
  javaMember,
  memberTags,
  shortName,
  signaturesOf,
  formParameters,
  functionType,
} = require('../java_ts');
const { taggedMember } = require('./java_members');
const { LEFT_OUT, MERGED_FORMS, liveForms, formOptionNames, formKey } = require('./overload_shape');

/** The block tags whose text counts whatever Java has: Rust's notes and the sections of no tag. */
const COUNTED_TAGS = new Set(['remarks', 'see', 'example']);

/**
 * Doc comment `text` (`/** ... *\/`) as `{description, blocks}`, each block
 * `{tag, text}`: the lines without their `*`, a block tag starting a line
 * outside a code fence.
 */
function parseTsdoc(text) {
  const lines = text
    .replace(/^\/\*\*/, '')
    .replace(/\*\/$/, '')
    .split('\n')
    .map((l) => l.replace(/^\s*\*? ?/, ''));
  const description = [];
  const blocks = [];
  let fence = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith('```')) fence = !fence;
    const tag = fence ? null : /^@([A-Za-z]+)(.*)$/.exec(trimmed);
    if (tag) blocks.push({ tag: tag[1], text: tag[2] });
    else if (blocks.length > 0) blocks[blocks.length - 1].text += `\n${line}`;
    else description.push(line);
  }
  for (const b of blocks) b.text = b.text.trimEnd();
  return { description: description.join('\n').trim(), blocks };
}

/** `text` with each Markdown link `[label](target)`, and each `[label]`, replaced by its label (the Rust lint's `strip_links`). */
function stripLinks(text) {
  let out = '';
  let rest = text;
  for (let open = rest.indexOf('['); open >= 0; open = rest.indexOf('[')) {
    out += rest.slice(0, open);
    const after = rest.slice(open + 1);
    const close = after.indexOf(']');
    if (close < 0) return out + rest.slice(open);
    out += after.slice(0, close);
    const tail = after.slice(close + 1);
    const target = tail.startsWith('(') ? tail.indexOf(')') : -1;
    rest = target >= 0 ? tail.slice(target + 1) : tail;
  }
  return out + rest;
}

/**
 * `text` with TSDoc's inline tags as the Rust translation renders Javadoc's:
 * `{@link X | label}` its label, `{@link X}` the code span `` `X` `` (one
 * word, as `` `C::foo` `` is), any other inline tag nothing.
 */
function inlineTags(text) {
  return text.replace(/\{@(\w+)([^}]*)\}/g, (all, tag, content) => {
    if (!/^link/.test(tag)) return '';
    const body = content.trim();
    const bar = body.indexOf('|');
    const space = body.search(/\s/);
    if (bar >= 0) return body.slice(bar + 1).trim();
    if (space >= 0) return body.slice(space + 1).trim();
    return body === '' ? '' : `\`${body}\``;
  });
}

/**
 * The words of `text` (documentation.md §5, the Rust lint's `word_count`):
 * runs of letters, digits and `_`, after links are reduced to their text,
 * each code span one word; heading and code-fence lines are not counted.
 */
function wordCount(text) {
  let fence = false;
  const kept = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trimStart();
    if (trimmed.startsWith('```')) {
      fence = !fence;
      continue;
    }
    const hashes = trimmed.length - trimmed.replace(/^#+/, '').length;
    if (!fence && hashes > 0 && trimmed.slice(hashes).startsWith(' ')) continue;
    kept.push(line);
  }
  const parts = stripLinks(inlineTags(kept.join('\n'))).split('`');
  // With an odd number of backticks the last one opens no span.
  const spans = parts.length % 2 === 0 ? parts.length - 2 : parts.length;
  let n = 0;
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 1 && i < spans) n += parts[i].trim() === '' ? 0 : 1;
    else n += parts[i].split(/[^\p{L}\p{N}_]+/u).filter((w) => w !== '').length;
  }
  return n;
}

/** The JavaScript error names a Java `@throws` of class `cls` is documented as (Names: `Exception` becomes `Error`; Types: a `NullPointerException` is a `TypeError`). */
function errorNames(cls) {
  const names = [cls];
  if (cls.endsWith('Exception')) names.push(`${cls.slice(0, -'Exception'.length)}Error`);
  if (cls === 'NullPointerException') names.push('TypeError');
  return names;
}

/** The error a `@throws` text names first: its leading code span, link or word. */
function thrownName(text) {
  const m = /^\s*(?:\{@link\w*\s+([\w.$]+)[^}]*\}|`([\w.$]+)`|([\w.$]+))/.exec(text);
  if (m === null) return null;
  return (m[1] || m[2] || m[3]).split('.').pop();
}

/**
 * The words of doc `doc` that count against Java size `java` (`{params,
 * return, throws}`), for a signature taking the options objects `options`.
 */
function countedWords(doc, java, options) {
  const javaParams = new Set(java.params.map(([n]) => n));
  const javaThrows = new Set(java.throws.flatMap(([c]) => errorNames(c)));
  let n = wordCount(doc.description);
  for (const b of doc.blocks) {
    if (COUNTED_TAGS.has(b.tag)) n += wordCount(b.text);
    else if (b.tag === 'param') {
      const m = /^\s*([\w$.]+)\s*(?:-\s*)?([^]*)$/.exec(b.text);
      if (m === null) continue;
      const name = m[1].split('.')[0];
      // The entry as the Rust translation renders it, `` `name` - text ``.
      if (javaParams.has(name) || (options.has(name) && javaParams.size > 0)) n += 1 + wordCount(m[2]);
    } else if (b.tag === 'returns' || b.tag === 'return') {
      if (java.return !== null) n += wordCount(b.text);
    } else if (b.tag === 'throws') {
      if (javaThrows.has(thrownName(b.text))) n += wordCount(b.text);
    }
  }
  return n;
}

/** The last doc comment TypeScript attaches to declaration `node`, as its text, or null. */
function docText(ts, node) {
  const docs = ts.getJSDocCommentsAndTags(node).filter((d) => ts.isJSDoc(d));
  return docs.length === 0 ? null : docs[docs.length - 1].getText(node.getSourceFile());
}

/** The sizes of `sizes` with each of `more` added to it, part by part (a one-method interface and its method). */
function joined(sizes, more) {
  if (sizes.length === 0) return more;
  if (more.length === 0) return sizes;
  return sizes.flatMap((a) =>
    more.map((b) => ({
      body: a.body + b.body,
      params: [...a.params, ...b.params],
      return: a.return === null ? b.return : b.return === null ? a.return : a.return + b.return,
      throws: [...a.throws, ...b.throws],
      words: a.words + b.words,
    })),
  );
}

/** The Java forms (overloads) signature `sig` of member `jm` translates, by the overload-shape mapping, else all. */
function signatureForms(ts, checker, sig, jm) {
  let forms = liveForms(jm.overloads).filter((f) => !LEFT_OUT.has(`${jm.owner.fqn}#${formKey(f)}`));
  if (MERGED_FORMS.has(`${jm.owner.fqn}#${jm.name}`) && forms.length > 0) {
    forms = [forms.reduce((a, f) => (f.params.length > a.params.length ? f : a))];
  }
  if (forms.length < 2) return forms;
  const names = formOptionNames(forms);
  const ps = formParameters(ts, sig);
  const pick = (pred) => {
    const found = forms.filter((f, k) => pred(f, k));
    return found.length > 0 ? found : forms;
  };
  if (ps.length === 0) return pick((f) => f.params.length === 0);
  if (ps.length !== 1 || !ts.isIdentifier(ps[0].name)) return forms;
  const bare = ps[0].name.text;
  const bareForms = forms.filter((f, k) => f.params.length === 1 && names[k][0] === bare);
  if (bareForms.length > 0) return bareForms;
  const type = checker.getNonNullableType(checker.getTypeAtLocation(ps[0]));
  const required = checker
    .getPropertiesOfType(type)
    .filter((p) => (p.flags & ts.SymbolFlags.Optional) === 0)
    .filter((p) => (checker.getNonNullableType(checker.getTypeOfSymbolAtLocation(p, ps[0])).flags & ts.TypeFlags.Never) === 0)
    .map((p) => p.name);
  return pick((f, k) => names[k].length === required.length && names[k].every((n) => required.includes(n)));
}

/** The parameters of signature `sig` that are an options object: one parameter of an object type that is no class instance, array or function. */
function optionsParams(ts, checker, sig) {
  const ps = sig ? formParameters(ts, sig) : [];
  if (ps.length !== 1 || !ts.isIdentifier(ps[0].name)) return new Set();
  const t = checker.getNonNullableType(checker.getTypeAtLocation(ps[0]));
  const isOptions =
    (t.flags & ts.TypeFlags.Object) !== 0 &&
    !checker.isArrayType(t) &&
    t.getCallSignatures().length === 0 &&
    !(t.symbol && t.symbol.flags & ts.SymbolFlags.Class);
  return isOptions ? new Set([ps[0].name.text]) : new Set();
}

/** The Javadoc sizes of the Java members `jm` names: a field's, or each overload's. */
function sizesOfMember(jm, overloads) {
  if (jm.kind === 'field') return jm.field.javadoc || [];
  return (overloads || jm.overloads).flatMap((o) => o.javadoc || []);
}

module.exports = {
  name: 'check-doc-size',
  kinds: ['doc-size'],
  needs: ['program', 'java'],
  hint:
    "Cut the TSDoc to at most 150% of the Javadoc's words, keeping Java's contract first and saying the " +
    'JavaScript notes in `@remarks` briefly (javascript/CLAUDE.md, JavaScript Binding Conventions, Documentation; ' +
    '.claude/rules/documentation.md §5); a generated file is fixed in its generator.',
  wordCount,
  parseTsdoc,
  check(ctx) {
    const { ts, checker } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    /** Checks doc comment `text` of `subject` (declared at `node`) against Java sizes `sizes`. */
    const measure = (subject, node, text, sizes, javaRef, options = new Set()) => {
      if (text === null || sizes.length === 0) return;
      checked += 1;
      const doc = parseTsdoc(text);
      let best = null;
      for (const java of sizes) {
        const words = countedWords(doc, java, options);
        if (best === null || 2 * words - 3 * java.words < 2 * best.words - 3 * best.java) best = { words, java: java.words };
      }
      if (2 * best.words <= 3 * best.java) return;
      findings.push({
        kind: 'doc-size',
        subject,
        at: atNode(ctx, node),
        message: `${best.words} words, more than 150% of the Javadoc's ${best.java} (at most ${Math.floor((best.java * 3) / 2)}) of ${javaRef}`,
      });
    };
    for (const { item, java, isError } of mappedItems(ctx)) {
      if (item.tags.jsOnly || java === null || !java.cls) continue;
      const cls = java.cls;
      let classSizes = cls.javadoc;
      if (functionType(cls)) {
        // The function is the interface's one method too.
        const methods = cls.overloads.filter((o) => !o.static && !o.default && o.name !== cls.path[cls.path.length - 1]);
        if (methods.length === 1) classSizes = joined(classSizes, methods[0].javadoc);
      }
      measure(`${item.rel}:${item.name}`, item.node, docText(ts, item.node), classSizes, shortName(cls));
      const members = isError ? publicMembers(ts, item.node) : itemMembers(ts, item);
      for (const m of members) {
        const tags = memberTags(ctx, item, m);
        if (tags.jsOnly) continue;
        const jm = tags.java ? taggedMember(index, tags.java.text) : javaMember(index, cls, m);
        if (jm === null) continue;
        const ref = `${shortName(jm.owner)}#${jm.name}`;
        if (m.kind !== 'method' && m.kind !== 'constructor') {
          measure(`${item.name}.${m.name}`, m.node, docText(ts, m.node), sizesOfMember(jm), ref);
          continue;
        }
        const sigs = signaturesOf(ts, m.node);
        sigs.forEach((sig, n) => {
          const subject = sigs.length > 1 ? `${item.name}.${m.name}/${n + 1}` : `${item.name}.${m.name}`;
          // A method translating a field (a public mutable field's setter)
          // against the field's Javadoc; an error's constructors, of one shape
          // for every class, against any form's.
          const forms = jm.kind === 'field' ? null : isError ? jm.overloads : signatureForms(ts, checker, sig, jm);
          const at = forms !== null && forms.length === 1 ? `${shortName(jm.owner)}#${formKey(forms[0])}` : ref;
          measure(subject, sig, docText(ts, sig), sizesOfMember(jm, forms), at, optionsParams(ts, checker, sig));
        });
      }
    }
    // An exported function javaTargets maps to a Java member: each signature
    // against that member's Javadocs.
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'function' || item.tags.jsOnly || !item.tags.java) continue;
      const jm = taggedMember(index, item.tags.java.text);
      if (jm === null) continue;
      const symbol = item.node.name ? checker.getSymbolAtLocation(item.node.name) : undefined;
      const decls = ((symbol && symbol.declarations) || [item.node]).filter((d) => ts.isFunctionDeclaration(d));
      const overloads = decls.filter((d) => d.body === undefined);
      const sigs = overloads.length > 0 ? overloads : decls;
      sigs.forEach((sig, n) => {
        const subject = sigs.length > 1 ? `${item.rel}:${item.name}/${n + 1}` : `${item.rel}:${item.name}`;
        const ref = `${shortName(jm.owner)}#${jm.name}`;
        measure(subject, sig, docText(ts, sig), sizesOfMember(jm), ref, optionsParams(ts, checker, sig));
      });
    }
    return { findings, checked };
  },
};
