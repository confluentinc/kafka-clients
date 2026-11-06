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

// A burn-down baseline, as rust/xtask/src/lint_custom/baseline.rs: a file
// listing the finding keys the rule linter still waives
// (javascript/lint-baseline.txt).
//
// Each line is a finding key (`<kind> <subject>`), or `prefix:<p>` waiving
// every finding whose subject starts with `p`; `#` starts a comment. A line
// that waives nothing is itself a finding, so the file only shrinks.
//
// One file serves every rule, so the runner applies it rule by rule: a key
// line belongs to the rule whose finding kinds include its kind, and is stale
// when that rule ran and nothing matched it; a `prefix:` line, and a key line
// whose kind no rule has, can only be judged once every rule ran
// (`Baseline.outsideKinds`).

const fs = require('node:fs');

/** The key that waives `finding`: `<kind> <subject>`. */
function key(finding) {
  return `${finding.kind} ${finding.subject}`;
}

/** The finding reporting that line `line` of baseline `file` is wrong. */
function baselineFinding(file, line, detail) {
  return { kind: 'baseline', subject: `${file}: ${line}`, message: detail };
}

const STALE_KEY = 'stale entry: no finding matches it any more; delete the line';
const STALE_PREFIX = 'stale entry: no finding starts with it any more; delete the line';
const DUPLICATE = 'listed twice; delete one';

class Baseline {
  /** The baseline of `text`, read from `file` (the name findings report). */
  static parse(file, text) {
    const keys = new Set();
    const prefixes = [];
    const duplicates = [];
    for (const raw of text.split('\n')) {
      const line = raw.trim();
      if (line === '' || line.startsWith('#')) continue;
      if (line.startsWith('prefix:')) {
        const prefix = line.slice('prefix:'.length);
        if (prefixes.includes(prefix)) duplicates.push(line);
        else prefixes.push(prefix);
      } else if (keys.has(line)) {
        duplicates.push(line);
      } else {
        keys.add(line);
      }
    }
    return new Baseline(file, keys, prefixes, duplicates);
  }

  /** Reads `path`, reported as `name`; a missing file waives nothing. */
  static load(path, name = path) {
    let text = '';
    try {
      text = fs.readFileSync(path, 'utf8');
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
    }
    return Baseline.parse(name, text);
  }

  constructor(file, keys, prefixes, duplicates) {
    this.file = file;
    this.keys = keys;
    this.prefixes = prefixes;
    this.duplicates = duplicates;
    this.usedKeys = new Set();
    this.usedPrefixes = new Set();
  }

  /** `findings` without the ones this baseline waives, recording which lines waived them. */
  waive(findings) {
    const kept = [];
    for (const f of findings) {
      const k = key(f);
      if (this.keys.has(k)) {
        this.usedKeys.add(k);
        continue;
      }
      const prefix = this.prefixes.find((p) => f.subject.startsWith(p));
      if (prefix !== undefined) {
        this.usedPrefixes.add(prefix);
        continue;
      }
      kept.push(f);
    }
    return kept;
  }

  /**
   * One finding per wrong line among those `owns(line)` accepts: a key line
   * that waived nothing, a `prefix:` line that waived nothing, a line listed
   * twice. `forKinds` and `outsideKinds` build the two `owns` the runner uses.
   */
  leftovers(owns = () => true) {
    const out = [];
    for (const k of this.keys) {
      if (owns(k) && !this.usedKeys.has(k)) out.push(baselineFinding(this.file, k, STALE_KEY));
    }
    for (const p of this.prefixes) {
      const line = `prefix:${p}`;
      if (owns(line) && !this.usedPrefixes.has(p)) out.push(baselineFinding(this.file, line, STALE_PREFIX));
    }
    for (const line of this.duplicates) {
      if (owns(line)) out.push(baselineFinding(this.file, line, DUPLICATE));
    }
    return out;
  }

  /** The `owns` of a rule with finding kinds `kinds`: its key lines. */
  static forKinds(kinds) {
    return (line) => !line.startsWith('prefix:') && kinds.includes(line.split(' ')[0]);
  }

  /** The `owns` left once every rule, with finding kinds `kinds`, ran: `prefix:` lines and unknown kinds. */
  static outsideKinds(kinds) {
    return (line) => line.startsWith('prefix:') || !kinds.includes(line.split(' ')[0]);
  }

  /** `findings` waived, plus one finding per wrong line: baseline.rs's `apply`, for one rule's file. */
  apply(findings) {
    return [...this.waive(findings), ...this.leftovers()];
  }
}

module.exports = { Baseline, key };
