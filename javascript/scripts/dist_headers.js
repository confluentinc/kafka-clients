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

// Puts each compiled file's licence header first (root CLAUDE.md, Build,
// packaging and release, Q76: every `.js` and `.d.ts` file in each tarball
// starts with it). `make build-ts` runs it after tsc. tsc keeps a source's
// header in its `.js`, but after the `"use strict";` it emits first, and
// drops it from the `.d.ts`; so this moves the header above `"use strict";`
// (a directive may follow comments) and writes the source's header at the top
// of the `.d.ts`. Each source map is fixed to match, line for line: the
// `.js.map`'s first lines are reordered as the lines are, and the
// `.d.ts.map`'s mappings start as many lines later as the header has. The
// header is Confluent's, or one of the two others the root allows
// (`HEADERS`); a source without one fails the build.
//
// Usage: node scripts/dist_headers.js <src> <dist>

const fs = require('node:fs');
const path = require('node:path');

/**
 * The licence headers a source may start with (root CLAUDE.md, Translation
 * Rules 1.3 and Build, packaging and release, Q76), as `//` comment lines:
 *   - Confluent's copyright line with the Apache 2.0 text, every translated
 *     file's: exactly 13 lines;
 *   - the Apache Software Foundation's, for a file copied unchanged from
 *     Apache Kafka;
 *   - OpenJDK's GPL version 2 with the Classpath Exception, for code
 *     translated from OpenJDK.
 * Each is recognised by its first and last line (and, for OpenJDK's, its
 * Classpath Exception sentence); the header is the file's leading run of `//`
 * lines.
 */
const HEADERS = [
  {
    name: 'Confluent',
    first: /^\/\/ Copyright 20[0-9]{2} Confluent Inc\.$/,
    last: '// limitations under the License.',
    lines: 13,
  },
  {
    name: 'Apache Software Foundation',
    first: /^\/\/ Licensed to the Apache Software Foundation \(ASF\) under one or more$/,
    last: '// limitations under the License.',
  },
  {
    name: 'OpenJDK',
    first: /^\/\/ Copyright \(c\) [0-9]{4}(, [0-9]{4})?, Oracle and\/or its affiliates\. All rights reserved\.$/,
    last: '// questions.',
    contains: '// particular file as subject to the "Classpath" exception as provided',
  },
];

/** The licence header `lines` start with, its lines, or null when it has none. */
function headerOf(lines) {
  let n = 0;
  while (n < lines.length && (lines[n] === '//' || lines[n].startsWith('// '))) n++;
  if (n === 0) return null;
  for (const h of HEADERS) {
    if (!h.first.test(lines[0])) continue;
    // The header ends at the first line that is its last line.
    const end = lines.slice(0, n).indexOf(h.last);
    if (end < 0) continue;
    if (h.lines !== undefined && end + 1 !== h.lines) continue;
    const header = lines.slice(0, end + 1);
    if (h.contains !== undefined && !header.includes(h.contains)) continue;
    return header;
  }
  return null;
}

/** Rewrites the `mappings` of the source map at `file` with `fix(lineGroups)`. */
function fixMap(file, fix) {
  if (!fs.existsSync(file)) return;
  const map = JSON.parse(fs.readFileSync(file, 'utf8'));
  map.mappings = fix(map.mappings.split(';')).join(';');
  fs.writeFileSync(file, JSON.stringify(map));
}

/** Moves the header of a compiled `.js` above tsc's `"use strict";`. */
function fixJs(file, source) {
  const lines = fs.readFileSync(file, 'utf8').split('\n');
  if (headerOf(lines)) return;
  const header = lines[0] === '"use strict";' ? headerOf(lines.slice(1)) : null;
  if (!header) throw new Error(`${file}: does not start with "use strict"; and the licence header of ${source}`);
  const n = header.length;
  const moved = [...lines.slice(1, n + 1), lines[0], ...lines.slice(n + 1)];
  fs.writeFileSync(file, moved.join('\n'));
  fixMap(`${file}.map`, (groups) => {
    while (groups.length < n + 1) groups.push('');
    return [...groups.slice(1, n + 1), groups[0], ...groups.slice(n + 1)];
  });
}

/** Writes `header` at the top of a compiled `.d.ts`. */
function fixDts(file, header) {
  const text = fs.readFileSync(file, 'utf8');
  if (headerOf(text.split('\n'))) return;
  fs.writeFileSync(file, `${header.join('\n')}\n${text}`);
  fixMap(`${file}.map`, (groups) => [...new Array(header.length).fill(''), ...groups]);
}

/** Every `.ts` file under `dir`, relative to it. */
function sources(dir, base = dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...sources(full, base));
    else if (entry.name.endsWith('.ts')) out.push(path.relative(base, full));
  }
  return out.sort();
}

function main(src, dist) {
  for (const rel of sources(src)) {
    const source = path.join(src, rel);
    const header = headerOf(fs.readFileSync(source, 'utf8').split('\n'));
    if (!header) throw new Error(`${source}: does not start with the licence header`);
    const stem = path.join(dist, rel.slice(0, -'.ts'.length));
    fixJs(`${stem}.js`, source);
    if (fs.existsSync(`${stem}.d.ts`)) fixDts(`${stem}.d.ts`, header);
  }
}

if (require.main === module) {
  const [src, dist] = process.argv.slice(2);
  if (!src || !dist) {
    console.error('usage: node scripts/dist_headers.js <src> <dist>');
    process.exit(2);
  }
  try {
    main(src, dist);
  } catch (e) {
    console.error(`dist_headers: ${e.message}`);
    process.exit(1);
  }
}

module.exports = { headerOf };
