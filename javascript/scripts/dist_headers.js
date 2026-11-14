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
// `.d.ts.map`'s mappings start as many lines later as the header has. A
// source without the header fails the build.
//
// Usage: node scripts/dist_headers.js <src> <dist>

const fs = require('node:fs');
const path = require('node:path');

/** The header every source starts with: its 13 comment lines. */
const HEADER_LINES = 13;
const FIRST_LINE = /^\/\/ Copyright 20[0-9]{2} Confluent Inc\.$/;
const LAST_LINE = '// limitations under the License.';

/** The header of `text`, its first HEADER_LINES lines, or null when it has none. */
function headerOf(lines) {
  if (lines.length < HEADER_LINES || !FIRST_LINE.test(lines[0]) || lines[HEADER_LINES - 1] !== LAST_LINE) return null;
  if (!lines.slice(0, HEADER_LINES).every((l) => l === '//' || l.startsWith('// '))) return null;
  return lines.slice(0, HEADER_LINES);
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
  if (lines[0] !== '"use strict";' || !headerOf(lines.slice(1))) {
    throw new Error(`${file}: does not start with "use strict"; and the licence header of ${source}`);
  }
  const moved = [...lines.slice(1, HEADER_LINES + 1), lines[0], ...lines.slice(HEADER_LINES + 1)];
  fs.writeFileSync(file, moved.join('\n'));
  fixMap(`${file}.map`, (groups) => {
    while (groups.length < HEADER_LINES + 1) groups.push('');
    return [...groups.slice(1, HEADER_LINES + 1), groups[0], ...groups.slice(HEADER_LINES + 1)];
  });
}

/** Writes `header` at the top of a compiled `.d.ts`. */
function fixDts(file, header) {
  const text = fs.readFileSync(file, 'utf8');
  if (headerOf(text.split('\n'))) return;
  fs.writeFileSync(file, `${header.join('\n')}\n${text}`);
  fixMap(`${file}.map`, (groups) => [...new Array(HEADER_LINES).fill(''), ...groups]);
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

module.exports = { headerOf, HEADER_LINES };
