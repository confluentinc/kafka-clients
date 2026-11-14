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

// check-commonjs-pin (root CLAUDE.md, JavaScript Binding Conventions, Layout
// and modules, Q13: "The package is CommonJS only, as 1.x is. `tsc` emits
// CommonJS with `module: nodenext`, `package.json` has no `"type":
// "module"`, and each `exports` entry has the `types` and `default`
// conditions"):
//   - package.json's `"type"` is not `"module"` (`package-type`): it is
//     absent, CommonJS by Node's default, or `"commonjs"`, which publint
//     suggests;
//   - tsconfig.json's `module` and `moduleResolution` are `nodenext`
//     (`tsconfig-module`);
//   - each `exports` entry is an object of exactly the conditions `types`
//     then `default` (`exports-conditions`): `types` first, as TypeScript
//     reads the first condition that matches.
// Not checked: that the emitted files are CommonJS.

const PACKAGE = 'package.json';
const TSCONFIG = 'tsconfig.json';
const CONDITIONS = ['types', 'default'];

module.exports = {
  name: 'check-commonjs-pin',
  kinds: ['package-type', 'tsconfig-module', 'exports-conditions', 'commonjs-unreadable'],
  needs: ['typescript'],
  hint:
    'Keep the package CommonJS only: no "type": "module" in package.json, module and moduleResolution nodenext, and each ' +
    'exports entry { types, default } (root CLAUDE.md, JavaScript Binding Conventions, Layout and modules, Q13).',
  check(ctx) {
    const ts = ctx.typescript();
    const findings = [];
    let checked = 0;
    let pkg = null;
    try {
      pkg = JSON.parse(ctx.read(PACKAGE));
    } catch (e) {
      findings.push({ kind: 'commonjs-unreadable', subject: PACKAGE, message: `cannot be read as JSON: ${e.message}` });
    }
    if (pkg !== null) {
      checked++;
      if (Object.hasOwn(pkg, 'type') && pkg.type !== 'commonjs') {
        findings.push({
          kind: 'package-type',
          subject: `${PACKAGE}:type`,
          message: `is ${JSON.stringify(pkg.type)}: the package is CommonJS, with no "type" or "type": "commonjs"`,
        });
      }
      for (const [subpath, entry] of Object.entries(pkg.exports || {})) {
        checked++;
        const keys = entry !== null && typeof entry === 'object' && !Array.isArray(entry) ? Object.keys(entry) : null;
        if (keys === null || keys.length !== CONDITIONS.length || keys.some((k, i) => k !== CONDITIONS[i])) {
          findings.push({
            kind: 'exports-conditions',
            subject: `${PACKAGE}:exports["${subpath}"]`,
            message: `has ${keys === null ? JSON.stringify(entry) : `the conditions ${keys.join(', ') || 'none'}`}, not exactly types then default`,
          });
        }
      }
    }
    let text = null;
    try {
      text = ctx.read(TSCONFIG);
    } catch (e) {
      findings.push({ kind: 'commonjs-unreadable', subject: TSCONFIG, message: `cannot be read: ${e.message}` });
    }
    if (text !== null) {
      const parsed = ts.parseConfigFileTextToJson(TSCONFIG, text);
      if (parsed.error) {
        findings.push({ kind: 'commonjs-unreadable', subject: TSCONFIG, message: 'cannot be parsed' });
      } else {
        const options = (parsed.config && parsed.config.compilerOptions) || {};
        for (const key of ['module', 'moduleResolution']) {
          checked++;
          if (typeof options[key] !== 'string' || options[key].toLowerCase() !== 'nodenext') {
            findings.push({
              kind: 'tsconfig-module',
              subject: `${TSCONFIG}:${key}`,
              message: `is ${JSON.stringify(options[key])}, not "nodenext": tsc emits the package's CommonJS with nodenext`,
            });
          }
        }
      }
    }
    return { findings, checked };
  },
};
