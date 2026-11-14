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

// check-no-repeated-precondition (root CLAUDE.md, JavaScript Binding
// Conventions, Errors, Q35: "The binding repeats none of Java's
// preconditions. The FFI checks the mandatory parameters and the core checks
// the rest"; Implementation over the FFI, Q58, Q60: the binding's own checks
// are a listed few): no string literal of src/ or native/ holds the message
// of a precondition the FFI or the core checks (`repeated-precondition`), so
// the binding cannot raise it itself. The binding's own listed checks (the
// argument forms, run-time types, the 2^53 and integer range checks, the
// serde config keys, `new.target`, the `group.id` check of groupMetadata())
// write their own messages, none of them below. The closed-client messages
// are check-no-closed-flag's. Not checked: a precondition repeated under
// another message, or with none.

const { tsLiterals, cLiterals } = require('../literals');

/** The precondition messages of Java and the core, each with the side that checks it. */
const PRECONDITIONS = [
  ['The timeout cannot be negative.', "Java's KafkaProducer.close and AsyncKafkaConsumer.close; the producer FFI's close checks it"],
  ['Invalid negative timeout', "Java's Timer; the consumer FFI checks a negative timeout"],
  ['Timeout must not be negative', "the core's _with_timeout check, which the consumer FFI reports"],
  ['is not safe for multi-threaded access', "the consumer's busy check, which the FFI reports (Q48)"],
  ['invocation inside a callback is not permitted', "flush() inside a send callback, which the core refuses (Q46)"],
  ['Consumer is not subscribed to any topics or assigned any partitions', "poll() on a consumer with nothing to fetch, which the core checks"],
];

module.exports = {
  name: 'check-no-repeated-precondition',
  kinds: ['repeated-precondition'],
  needs: ['program'],
  PRECONDITIONS,
  hint:
    'Pass the argument on to the FFI and let its error reach the caller: the binding repeats none of the ' +
    "FFI's or the core's preconditions (root CLAUDE.md, JavaScript Binding Conventions, Errors, Q35).",
  check(ctx) {
    const findings = [];
    let checked = 0;
    for (const lit of [...tsLiterals(ctx), ...cLiterals(ctx)]) {
      checked++;
      const hit = PRECONDITIONS.find(([text]) => lit.text.includes(text));
      if (hit) {
        findings.push({
          kind: 'repeated-precondition',
          subject: `${lit.rel}:${lit.line}`,
          message: `writes "${hit[0]}", the message of a precondition the binding does not repeat (${hit[1]})`,
        });
      }
    }
    return { findings, checked };
  },
};
