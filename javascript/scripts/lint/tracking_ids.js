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

// The phase, review and question IDs javascript/CLAUDE.md (Comments, point 5)
// bans from comments, which the design notes and the rules hold:
//   - a question ID of the kickstart questions: Q and a number;
//   - a phase, group or plan label: one capital letter and a number, an
//     optional lowercase letter after it (the linter plan's rules, a review's
//     finding, a plan's open gap), but the real names of NAMES;
//   - a review ID: the word Critic or Actor and a number, or the name of a
//     review's comments file (COMMENTS, a dot and a number, or DONE, FP or FN);
//   - a test plan's row label: two to five capitals or digits, a dash and two
//     digits, but the real names of ROW_NAMES.
// An ID counts as a whole word only: inside an identifier (`ARG_A1`), a hex
// number (`0xA1`), a colour (`#A1`), a code point (`U+D800`) or a word it is
// none.

/** Names of one capital and a number that are no IDs: the JavaScript engine and the C standards. */
const NAMES = new Set(['V8', 'C89', 'C90', 'C99', 'C11', 'C17', 'C18', 'C23']);

/** Names of the row-label form that are no IDs: character encodings and a checksum. */
const ROW_NAMES = new Set(['UTF-16', 'UTF-32', 'CRC-32']);

/** What may not come right before an ID: a letter, a digit, `_`, or a mark that makes it part of a token. */
const BEFORE = '(?<![\\p{L}\\p{N}_#+\\\\$.])';

/** What may not come right after an ID: a letter, a digit or `_`. */
const AFTER = '(?![\\p{L}\\p{N}_])';

/** Each form: its regular expression and what it names. */
const FORMS = [
  { re: new RegExp(`${BEFORE}Q[0-9]{1,3}${AFTER}`, 'gu'), what: 'a question ID' },
  { re: new RegExp(`${BEFORE}[A-PR-Z][0-9]{1,3}[a-z]?${AFTER}`, 'gu'), what: 'a phase or plan label', names: NAMES },
  { re: new RegExp(`${BEFORE}(?:Critic|Actor)s?\\s+[0-9]+${AFTER}`, 'giu'), what: 'a review ID' },
  { re: new RegExp(`${BEFORE}COMMENTS\\.(?:DONE\\.)?(?:[0-9]+|FP|FN)${AFTER}`, 'gu'), what: 'a review ID' },
  { re: new RegExp(`${BEFORE}[A-Z][A-Z0-9]{1,4}-[0-9]{2}${AFTER}`, 'gu'), what: "a test plan's row label", names: ROW_NAMES },
];

/** The IDs in `text`: `{at, id, what}`, `at` the offset, in order, one per offset. */
function trackingIds(text) {
  const out = [];
  for (const { re, what, names } of FORMS) {
    re.lastIndex = 0;
    for (const m of text.matchAll(re)) {
      const id = m[0].replace(/\s+/gu, ' ');
      if (names !== undefined && names.has(id)) continue;
      out.push({ at: m.index, id, what });
    }
  }
  out.sort((a, b) => a.at - b.at);
  return out.filter((hit, i) => i === 0 || out[i - 1].at !== hit.at);
}

module.exports = { NAMES, ROW_NAMES, trackingIds };
