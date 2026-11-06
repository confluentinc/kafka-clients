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

// J6 check-overload-shape (javascript/CLAUDE.md "Java shape, JS idiom": one
// Java overload set is one function, overloaded constructors take one options
// object, `close()` / `close(Duration)` is `close({timeoutMs}?)`):
//   - no two TypeScript members translate one Java member;
//   - a Java overload set with more than one public, live form is one function
//     taking one options object, whose properties are the union of the forms'
//     parameter names (a `Duration x` as `xMs`, PLAN.md §8 decision 8;
//     parameters at one position whose Java types map to one host type are
//     one, named after the earliest-declared form), each required exactly
//     when every form takes it, unless its Java parameter may be null
//     (decision 10: omission means null), and the object itself required
//     exactly when every form takes a parameter;
//   - or, when the forms differ only by trailing parameters, one function
//     taking the longest form's parameters, those the shorter forms lack
//     optional (decision 9, `send(record, callback?)`).
// The checker gives the options type's properties and their optionality. A
// config class (its constants only) is not asked.

const { mappedMembers } = require('./java_members');
const { shortName, constantsOnly } = require('../java_ts');

/** The public, live forms of an overload set, one per parameter list. */
function liveForms(overloads) {
  const seen = new Set();
  return overloads.filter((o) => {
    const key = o.params.join(',');
    if (o.visibility !== 'public' || o.deprecated || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** The name a Java parameter takes in an options object: a `Duration x` is `xMs`. */
function optionName(type, name) {
  return type === 'Duration' ? `${name}Ms` : name;
}

/** A form as the findings print it: `close(Duration timeout)`. */
function formText(o) {
  return `${o.name}(${o.params.map((t, i) => `${t} ${o.param_names[i] || ''}`.trim()).join(', ')})`;
}

/**
 * The host (TypeScript) kind of a Java type, for merging parameters: two
 * types of one kind are one parameter, a type variable being a kind of its own.
 */
function hostKind(type) {
  const base = type.replace(/<.*$/, '');
  if (/^(byte|short|int|long|float|double|Byte|Short|Integer|Long|Float|Double|Number|Duration)$/.test(base)) return 'number';
  if (base === 'boolean' || base === 'Boolean') return 'boolean';
  if (base === 'String') return 'string';
  if (base === 'Map' || base === 'Properties') return 'configs';
  // A type variable keeps its own kind, as in the Python conventions J6
  // follows (`ProducerRecord[K, V]` keeps `key` and `value` apart), although
  // `K` and `V` are both `Data` in JavaScript.
  if (base === 'K' || base === 'V') return base;
  if (type === 'byte[]') return 'Data';
  if (/\[\]$/.test(type) || /^(List|Set|Collection)$/.test(base)) return 'array';
  return base;
}

/**
 * The option names of each form of an overload set, in declaration order: a
 * parameter's Java name (a `Duration x` as `xMs`, PLAN.md decision 8), except
 * that a parameter at the position of an earlier form's whose Java type maps
 * to the same host type takes that earlier name (the Python conventions'
 * "same position, Java types mapping to the same host type -> one parameter
 * named after the earliest-declared overload": `KafkaProducer(Map configs)` /
 * `(Properties properties)` -> `configs`).
 */
function formOptionNames(forms) {
  const out = [];
  for (const f of forms) {
    out.push(
      f.params.map((t, i) => {
        const earlier = out.findIndex((names, k) => names[i] !== undefined && hostKind(forms[k].params[i]) === hostKind(t));
        return earlier >= 0 ? out[earlier][i] : optionName(t, f.param_names[i]);
      }),
    );
  }
  return out;
}

/** Boxed Java types, which hold null. */
const BOXED = new Set(['Byte', 'Short', 'Integer', 'Long', 'Float', 'Double', 'Boolean', 'Character']);

/** A positive statement that a parameter may be null (PLAN.md §8 decision 10). */
const NULLABLE = /\b(may be null|can be null|if null|null means|null for|or null|null indicates|nullable)\b/i;
/** A statement that it may not. */
const NOT_NULL = /\b(not null|non-null|may not be null|must not be null|cannot be null|can not be null|not be null)\b/i;

/**
 * Whether Java parameter `i` of form `f` may be null, as PLAN.md §8 decision
 * 10 (the user's, 2026-11-05) reads it: its type is a boxed primitive or a
 * key / value type variable (`K`, `V`), or the form's Javadoc `@param` for it
 * says so positively ("may be null", "can be null", "if null", "null means",
 * "null for", "or null", "null indicates", "nullable", read after `{@code x}`
 * is rendered as `x`) and denies it nowhere ("not null",
 * "non-null", "may not be null", "must not be null", "cannot be null").
 */
function mayBeNull(f, i) {
  const type = f.params[i];
  if (BOXED.has(type) || type === 'K' || type === 'V') return true;
  const tag = f.javadoc && f.javadoc.params.find((p) => p.name === f.param_names[i]);
  if (!tag) return false;
  // Read as the Javadoc renders: `{@code null}` and `{@link x}` as their text.
  const text = tag.text.replace(/\{@(?:code|literal|link|linkplain)\s+([^}]*)\}/g, '$1').replace(/\s+/g, ' ');
  return NULLABLE.test(text) && !NOT_NULL.test(text);
}

/**
 * Whether forms `forms` differ only by trailing parameters: each one's
 * option names are a prefix of the longest's.
 */
function trailingOnly(forms, names) {
  const longest = names.reduce((a, b) => (b.length > a.length ? b : a), []);
  return names.every((n) => n.every((x, i) => longest[i] === x));
}

module.exports = {
  name: 'check-overload-shape',
  kinds: [
    'duplicate',
    'not-options',
    'option-missing',
    'option-extra',
    'option-optional',
    'option-required',
    'options-optional',
    'options-required',
  ],
  needs: ['program', 'java'],
  hint:
    'Translate a Java overload set as one function taking one options object named after the overloads\' ' +
    'parameters (a Duration x as xMs), required as every overload requires it (javascript/CLAUDE.md "Java ' +
    'shape, JS idiom").',
  liveForms,
  optionName,
  formOptionNames,
  mayBeNull,
  check(ctx) {
    const { ts, checker } = ctx.program();
    const findings = [];
    let checked = 0;
    const translations = new Map();
    for (const { item, java, m, jm } of mappedMembers(ctx)) {
      if (jm === null || constantsOnly(java.cls)) continue;
      checked += 1;
      const key = `${jm.owner.fqn}#${jm.name}#${m.isStatic}`;
      const subject = `${item.name}.${m.name}`;
      if (translations.has(key)) {
        findings.push({
          kind: 'duplicate',
          subject,
          message: `and ${translations.get(key)} both translate ${shortName(jm.owner)}#${jm.name}`,
        });
      } else {
        translations.set(key, subject);
      }
      if (jm.kind === 'field' || (m.kind !== 'method' && m.kind !== 'constructor')) continue;
      const forms = liveForms(jm.overloads);
      if (forms.length < 2) continue;
      const params = m.node.parameters ? [...m.node.parameters] : [];
      const javaName = `${shortName(jm.owner)}#${jm.name}`;
      const optionsType = params.length === 1 ? checker.getNonNullableType(checker.getTypeAtLocation(params[0])) : null;
      const isOptions =
        optionsType !== null &&
        !params[0].dotDotDotToken &&
        optionsType.flags & ts.TypeFlags.Object &&
        !(optionsType.isUnion && optionsType.isUnion()) &&
        !checker.isArrayType(optionsType) &&
        optionsType.getCallSignatures().length === 0 &&
        !(optionsType.symbol && optionsType.symbol.flags & ts.SymbolFlags.Class);
      const names = formOptionNames(forms);
      if (!isOptions && trailingOnly(forms, names)) {
        // Decision 9 (the user's, 2026-11-05): forms differing only by
        // trailing parameters may be one function with those parameters
        // optional (`send(record, callback?)`).
        const min = Math.min(...names.map((n) => n.length));
        const max = Math.max(...names.map((n) => n.length));
        const optional = (p) => !!(p.questionToken || p.initializer);
        if (params.length === max && params.every((p, i) => optional(p) === i >= min) && !params.some((p) => p.dotDotDotToken)) {
          continue;
        }
      }
      if (!isOptions) {
        const written = params.map((p) => p.getText().replace(/\s+/g, ' ')).join(', ');
        findings.push({
          kind: 'not-options',
          subject,
          message: `translates ${forms.length} Java forms of ${javaName} (${forms.map(formText).join('; ')}) but takes (${written}), not one options object`,
        });
        continue;
      }
      const typeName = checker.typeToString(optionsType);
      // Each option: whether every form takes it, and whether a form's Java
      // parameter of that name may be null (then it may be left out).
      const expected = new Map();
      forms.forEach((f, k) => {
        names[k].forEach((name, i) => {
          if (!expected.has(name)) expected.set(name, { inAll: names.every((n) => n.includes(name)), nullable: false });
          if (mayBeNull(f, i)) expected.get(name).nullable = true;
        });
      });
      const union = [...expected.keys()].join(', ');
      const props = new Map(checker.getPropertiesOfType(optionsType).map((p) => [p.name, p]));
      for (const [name, { inAll, nullable }] of expected) {
        const prop = props.get(name);
        if (!prop) {
          findings.push({ kind: 'option-missing', subject: `${typeName}.${name}`, message: `is missing, though ${javaName} names it (${union})` });
          continue;
        }
        const optional = (prop.flags & ts.SymbolFlags.Optional) !== 0;
        if (inAll && optional && !nullable) {
          findings.push({ kind: 'option-optional', subject: `${typeName}.${name}`, message: `is optional but every Java form of ${javaName} takes ${name}` });
        } else if (!inAll && !optional) {
          const without = forms.find((g, k) => !names[k].includes(name));
          findings.push({ kind: 'option-required', subject: `${typeName}.${name}`, message: `is required but ${formText(without)} does not take it` });
        }
      }
      for (const name of props.keys()) {
        if (!expected.has(name)) {
          findings.push({ kind: 'option-extra', subject: `${typeName}.${name}`, message: `no Java form of ${javaName} names it (${union})` });
        }
      }
      const optionalParam = !!(params[0].questionToken || params[0].initializer);
      const someEmpty = forms.some((f) => f.params.length === 0);
      if (someEmpty && !optionalParam) {
        findings.push({ kind: 'options-required', subject, message: `requires its options, but ${javaName}() takes none` });
      } else if (!someEmpty && optionalParam) {
        findings.push({ kind: 'options-optional', subject, message: `takes its options as optional, but every Java form of ${javaName} takes a parameter` });
      }
    }
    return { findings, checked };
  },
};
