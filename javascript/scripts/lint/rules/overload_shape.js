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
// Java overload set is one function taking one object of named parameters,
// a one-parameter form also takes the value on its own, and src/internal/args.ts
// checks a call against the Java forms). Read over the function's TypeScript
// signatures (its overload signatures, else the implementation):
//   - no two TypeScript members translate one Java member;
//   - a Java overload set with more than one public, live form takes one
//     options object, whose properties are the union of the forms' parameter
//     names, Java's own (a `Duration timeout` is `timeout`; parameters at one
//     position whose Java types map to one host type are one, named after the
//     earliest-declared form), each required exactly when every form takes it
//     (a form is given whole: a parameter Java may take as null is given as
//     `null`), and the object itself required exactly when every form takes a
//     parameter, unless a signature takes no parameter;
//     and, as each Java form has one TypeScript overload signature (root
//     CLAUDE.md, JavaScript Binding Conventions, Signatures), one options
//     signature translates one form (forms whose parameters have the same
//     names, `KafkaProducer(Map)` / `(Properties)`, being one), a form of no
//     parameter having a signature of none (`signature-per-form` otherwise);
//   - or one signature per Java form (`ProducerRecord`): each names
//     exactly its form's parameters, required (`signature-form` otherwise),
//     and closes every name of the other forms with a property typed `never`
//     (`option-unclosed` otherwise);
//   - a signature taking one parameter named as a one-parameter form's is that
//     form's bare value (`send(record)`, `close(timeout)`); a set with a
//     one-parameter form and no such signature is `bare-missing`;
//   - a form no signature translates is its own `overload-missing` finding,
//     keyed by that form (`KafkaProducer.constructor/KafkaProducer(Map,Serializer,Serializer)`),
//     so a form not translated yet can be baselined without hiding a wrong
//     shape of the forms that are, and the options are checked against the
//     translated forms (against every form when none is translated);
//   - a function taking no parameter translates the forms that take none,
//     each other form being `overload-missing` the same way
//     (`KafkaFuture.get/get(long,TimeUnit)`); positional parameters other
//     than a bare value, or a bare value with no options object, are
//     `not-options`.
// The checker gives the options type's properties and their optionality. A
// config class (its constants only) is not asked.

const { atNode } = require('../location');
const { mappedMembers } = require('./java_members');

/**
 * The Java overload sets the root conventions translate as one method taking
 * the longest form's parameters, with one signature, by member, with the
 * reason: such a set is checked as that one form.
 */
const MERGED_FORMS = new Map([
  [
    'org.apache.kafka.common.serialization.Serializer#serialize',
    "root CLAUDE.md, JavaScript Binding Conventions, Serialization: a serde has one method for Java's two serialize methods (a deviation), which the user implements",
  ],
]);
const { shortName, constantsOnly, signaturesOf, formParameters } = require('../java_ts');

/**
 * A form with Java's `long x, TimeUnit unit` pair as the one `Duration x` it
 * translates to (javascript/CLAUDE.md "Java shape, JS idiom": a `TimeUnit` is
 * merged with the `long` it qualifies, `get(long timeout, TimeUnit unit)` ->
 * `get({timeout})`); `java` keeps the Java form for the findings' text.
 */
function mergeTimeUnit(o) {
  // `types`: the parameter types as written, type arguments kept (the index's
  // `param_types`), for the rules that read types; `params` stays erased.
  const types = Array.isArray(o.param_types) && o.param_types.length === o.params.length ? o.param_types : o.params;
  const i = o.params.findIndex((t, k) => t === 'long' && o.params[k + 1] === 'TimeUnit');
  if (i < 0) return { ...o, types };
  const names = o.param_names || [];
  return {
    ...o,
    java: o,
    params: [...o.params.slice(0, i), 'Duration', ...o.params.slice(i + 2)],
    types: [...types.slice(0, i), 'Duration', ...types.slice(i + 2)],
    param_names: [...names.slice(0, i), names[i], ...names.slice(i + 2)],
  };
}

/** The public, live forms of an overload set, one per parameter list, a `TimeUnit` merged (`mergeTimeUnit`). */
function liveForms(overloads) {
  const seen = new Set();
  return overloads
    .filter((o) => {
      const key = o.params.join(',');
      if (o.visibility !== 'public' || o.deprecated || seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .map(mergeTimeUnit);
}

/** The name a Java parameter takes in an options object: its Java name, a `Duration timeout` included. */
function optionName(type, name) {
  void type;
  return name;
}

/** A form as a finding's subject names it: `get(long,TimeUnit)`, the types erased. */
function formKey(form) {
  const o = form.java || form;
  return `${o.name}(${o.params.map((t) => t.replace(/<.*>$/, '')).join(',')})`;
}

/** A form as the findings print it: `close(Duration timeout)`. */
function formText(form) {
  const o = form.java || form;
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
 * parameter's Java name, except
 * that a parameter at the position of an earlier form's whose Java type maps
 * to the same host type takes that earlier name, unless the earlier form has
 * a parameter of its own name (the Python conventions'
 * "same position, Java types mapping to the same host type -> one parameter
 * named after the earliest-declared overload": `KafkaProducer(Map configs)` /
 * `(Properties properties)` -> `configs`).
 */
function formOptionNames(forms) {
  const out = [];
  for (const f of forms) {
    out.push(
      f.params.map((t, i) => {
        // Not when the earlier form names a parameter so itself: the forms
        // then share the name, at another position (`MockProducer(Cluster
        // cluster, boolean autoComplete, ...)` / `(boolean autoComplete,
        // ...)`).
        const own = f.param_names[i];
        const earlier = out.findIndex(
          (names, k) => names[i] !== undefined && hostKind(forms[k].params[i]) === hostKind(t) && !(forms[k].param_names || []).includes(own),
        );
        return earlier >= 0 ? out[earlier][i] : optionName(t, f.param_names[i]);
      }),
    );
  }
  return out;
}

module.exports = {
  name: 'check-overload-shape',
  kinds: [
    'duplicate',
    'not-options',
    'overload-missing',
    'option-missing',
    'option-extra',
    'option-optional',
    'option-required',
    'options-optional',
    'options-required',
    'bare-missing',
    'signature-form',
    'option-unclosed',
    'signature-per-form',
  ],
  needs: ['program', 'java'],
  hint:
    'Translate a Java overload set as one function taking one options object named after the overloads\' ' +
    'parameters, with one TypeScript overload signature per Java form (root CLAUDE.md, JavaScript Binding ' +
    'Conventions, Signatures).',
  liveForms,
  optionName,
  formOptionNames,
  signaturesOf,
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
      const at = atNode(ctx, m.node);
      if (translations.has(key)) {
        findings.push({
          at,
          kind: 'duplicate',
          subject,
          message: `and ${translations.get(key)} both translate ${shortName(jm.owner)}#${jm.name}`,
        });
      } else {
        translations.set(key, subject);
      }
      if (jm.kind === 'field' || (m.kind !== 'method' && m.kind !== 'constructor')) continue;
      let forms = liveForms(jm.overloads);
      if (MERGED_FORMS.has(`${jm.owner.fqn}#${jm.name}`)) forms = [forms.reduce((a, f) => (f.params.length > a.params.length ? f : a))];
      if (forms.length < 2) continue;
      const javaName = `${shortName(jm.owner)}#${jm.name}`;
      const allNames = formOptionNames(forms);
      const written = (sig) => formParameters(ts, sig).map((p) => p.getText().replace(/\s+/g, ' ')).join(', ');
      const missingForm = (f) => ({
        at,
        kind: 'overload-missing',
        subject: `${subject}/${formKey(f)}`,
        message: `${javaName}'s form ${formText(f)} has no translation: ${subject} takes (${written(m.node)})`,
      });
      // The one-parameter forms' names: a signature taking that one parameter,
      // by that name, is the bare value (`send(record)`, `close(timeout)`).
      const bareNames = new Set(forms.map((f, k) => (f.params.length === 1 ? allNames[k][0] : null)).filter((n) => n !== null));
      // One-parameter forms of several names would be told apart by type,
      // which src/internal/args.ts does not do: none takes a bare value.
      if (bareNames.size > 1) bareNames.clear();
      const isOptionsParam = (p) => {
        const t = checker.getNonNullableType(checker.getTypeAtLocation(p));
        return (
          !p.dotDotDotToken &&
          (t.flags & ts.TypeFlags.Object) !== 0 &&
          !(t.isUnion && t.isUnion()) &&
          !checker.isArrayType(t) &&
          t.getCallSignatures().length === 0 &&
          !(t.symbol && t.symbol.flags & ts.SymbolFlags.Class)
        );
      };
      const optionSigs = [];
      const bareSigs = [];
      let zeroSig = false;
      let positional = null;
      for (const sig of signaturesOf(ts, m.node)) {
        const ps = formParameters(ts, sig);
        if (ps.length === 0) zeroSig = true;
        else if (ps.length === 1 && !ps[0].dotDotDotToken && ts.isIdentifier(ps[0].name) && bareNames.has(ps[0].name.text)) bareSigs.push(sig);
        else if (ps.length === 1 && isOptionsParam(ps[0])) optionSigs.push(sig);
        else if (positional === null) positional = sig;
      }
      if (positional !== null || (optionSigs.length === 0 && bareSigs.length > 0)) {
        const sig = positional ?? bareSigs[0];
        findings.push({
          at,
          kind: 'not-options',
          subject,
          message: `translates ${forms.length} Java forms of ${javaName} (${forms.map(formText).join('; ')}) but takes (${written(sig)}), not one options object`,
        });
        continue;
      }
      // The forms a signature with no parameter or a bare value translates.
      const coveredBy = (k) => (zeroSig && forms[k].params.length === 0) || (bareSigs.length > 0 && forms[k].params.length === 1);
      if (optionSigs.length === 0) {
        // A function taking no parameter translates the forms that take none.
        if (zeroSig && forms.some((f) => f.params.length === 0)) {
          for (const [k, f] of forms.entries()) if (!coveredBy(k)) findings.push(missingForm(f));
          continue;
        }
        findings.push({
          at,
          kind: 'not-options',
          subject,
          message: `translates ${forms.length} Java forms of ${javaName} (${forms.map(formText).join('; ')}) but takes (${written(m.node)}), not one options object`,
        });
        continue;
      }
      if (bareNames.size > 0 && bareSigs.length === 0) {
        const f = forms.find((g) => g.params.length === 1);
        findings.push({
          at,
          kind: 'bare-missing',
          subject,
          message: `takes no bare value, though ${javaName}'s form ${formText(f)} has one parameter: add a signature taking ${[...bareNames][0]} on its own`,
        });
      }
      const neverProp = (prop, sig) =>
        (checker.getNonNullableType(checker.getTypeOfSymbolAtLocation(prop, sig.parameters[0])).flags & ts.TypeFlags.Never) !== 0;
      if (optionSigs.length > 1) {
        // One signature per Java form, so that the declarations accept a
        // form's parameters only: each names exactly its form's parameters,
        // required, and closes the others with a property typed `never`.
        const union = [...new Set(allNames.flat())];
        const covered = new Set();
        for (const sig of optionSigs) {
          const p0 = sig.parameters[0];
          const type = checker.getNonNullableType(checker.getTypeAtLocation(p0));
          const typeName = checker.typeToString(type);
          const sigAt = atNode(ctx, sig);
          const required = [];
          const closed = [];
          for (const prop of checker.getPropertiesOfType(type)) {
            const decl = prop.declarations && prop.declarations.length > 0 ? atNode(ctx, prop.declarations[0]) : sigAt;
            if (!union.includes(prop.name)) {
              findings.push({ at: decl, kind: 'option-extra', subject: `${typeName}.${prop.name}`, message: `no Java form of ${javaName} names it (${union.join(', ')})` });
            } else if (neverProp(prop, sig)) {
              closed.push(prop.name);
            } else if ((prop.flags & ts.SymbolFlags.Optional) !== 0) {
              // Reported as optional; not again as unclosed below.
              closed.push(prop.name);
              findings.push({ at: decl, kind: 'option-optional', subject: `${typeName}.${prop.name}`, message: `is optional, but a signature of one Java form of ${javaName} takes its form's parameters` });
            } else {
              required.push(prop.name);
            }
          }
          const k = allNames.findIndex((n) => n.length === required.length && n.every((x) => required.includes(x)));
          // The other forms' names closed with `never`, so that a value
          // holding one (a variable, which no excess-property check reads)
          // matches no signature it does not belong to.
          for (const name of union) {
            if (required.includes(name) || closed.includes(name)) continue;
            findings.push({
              at: sigAt,
              kind: 'option-unclosed',
              subject: `${typeName}.${name}`,
              message: `neither takes nor closes ${name} (\`${name}?: never\`), which another Java form of ${javaName} takes`,
            });
          }
          if (k < 0) {
            findings.push({ at: sigAt, kind: 'signature-form', subject: `${subject}/(${required.join(',')})`, message: `takes (${required.join(', ')}), which is no Java form of ${javaName}` });
          } else {
            covered.add(k);
          }
        }
        for (const [k, f] of forms.entries()) if (!covered.has(k) && !coveredBy(k)) findings.push(missingForm(f));
        continue;
      }
      const sig = optionSigs[0];
      const optionsType = checker.getNonNullableType(checker.getTypeAtLocation(sig.parameters[0]));
      const typeName = checker.typeToString(optionsType);
      const props = new Map(checker.getPropertiesOfType(optionsType).map((p) => [p.name, p]));
      // The forms the options translate: those whose every parameter is an
      // option. The others are each a missing form, unless none is translated,
      // when the options are checked against every form.
      // A form of no parameter that a signature of none translates is not the
      // options signature's.
      let kept = forms.map((f, k) => k).filter((k) => allNames[k].every((n) => props.has(n)) && !(zeroSig && forms[k].params.length === 0));
      if (kept.length === 0) kept = forms.map((f, k) => k);
      else for (const [k, f] of forms.entries()) if (!kept.includes(k) && !coveredBy(k)) findings.push(missingForm(f));
      const translated = kept.map((k) => forms[k]);
      const names = kept.map((k) => allNames[k]);
      // One signature per Java form: the one options signature stands for one
      // form, the forms of no parameter having a signature of their own.
      const standsFor = new Set(kept.map((k) => [...allNames[k]].sort().join(',')));
      if (standsFor.size > 1) {
        findings.push({
          at,
          kind: 'signature-per-form',
          subject,
          message: `translates ${standsFor.size} Java forms of ${javaName} (${kept.map((k) => formText(forms[k])).join('; ')}) with one options signature: give each form its own overload signature`,
        });
      }
      // Each option, and whether every translated form takes it.
      const expected = new Map();
      for (const n of names) for (const name of n) if (!expected.has(name)) expected.set(name, names.every((x) => x.includes(name)));
      const union = [...expected.keys()].join(', ');
      for (const [name, inAll] of expected) {
        const prop = props.get(name);
        if (!prop) {
          findings.push({ at, kind: 'option-missing', subject: `${typeName}.${name}`, message: `is missing, though ${javaName} names it (${union})` });
          continue;
        }
        const optional = (prop.flags & ts.SymbolFlags.Optional) !== 0;
        const propAt = prop.declarations && prop.declarations.length > 0 ? atNode(ctx, prop.declarations[0]) : at;
        if (inAll && optional) {
          findings.push({ at: propAt, kind: 'option-optional', subject: `${typeName}.${name}`, message: `is optional but every Java form of ${javaName} takes ${name}` });
        } else if (!inAll && !optional) {
          const without = translated.find((g, k) => !names[k].includes(name));
          findings.push({ at: propAt, kind: 'option-required', subject: `${typeName}.${name}`, message: `is required but ${formText(without)} does not take it` });
        }
      }
      for (const name of props.keys()) {
        if (!expected.has(name)) {
          const decl = props.get(name).declarations;
          findings.push({ at: decl && decl.length > 0 ? atNode(ctx, decl[0]) : at, kind: 'option-extra', subject: `${typeName}.${name}`, message: `no Java form of ${javaName} names it (${union})` });
        }
      }
      const optionalParam = !!(sig.parameters[0].questionToken || sig.parameters[0].initializer);
      const someEmpty = translated.some((f) => f.params.length === 0);
      if (someEmpty && !optionalParam && !zeroSig) {
        findings.push({ at, kind: 'options-required', subject, message: `requires its options, but ${javaName}() takes none` });
      } else if (!someEmpty && optionalParam) {
        findings.push({ at, kind: 'options-optional', subject, message: `takes its options as optional, but every Java form of ${javaName} takes a parameter` });
      }
    }
    return { findings, checked };
  },
};
