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

// scripts/dist_headers.js, which `make build-ts` runs after tsc: each compiled
// .js and .d.ts starts with its source's licence header, the source maps
// shifted to match, and a source without the header fails the build.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'dist_headers.js');
const HEADER = fs.readFileSync(SCRIPT, 'utf8').split('\n').slice(0, 13);
// The Foundation's header, which a translation that copied its Java file's
// header would carry: refused, as Kafka ships no TypeScript.
const ASF = [
  '// Licensed to the Apache Software Foundation (ASF) under one or more',
  '// contributor license agreements. See the NOTICE file distributed with',
  '// this work for additional information regarding copyright ownership.',
  '// The ASF licenses this file to You under the Apache License, Version 2.0',
  '// (the "License"); you may not use this file except in compliance with',
  '// the License. You may obtain a copy of the License at',
  '//',
  '//    http://www.apache.org/licenses/LICENSE-2.0',
  '//',
  '// Unless required by applicable law or agreed to in writing, software',
  '// distributed under the License is distributed on an "AS IS" BASIS,',
  '// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.',
  '// See the License for the specific language governing permissions and',
  '// limitations under the License.',
];
// Code translated from OpenJDK keeps GPL with the Classpath Exception.
const OPENJDK = [
  '// Copyright (c) 1997, 2021, Oracle and/or its affiliates. All rights reserved.',
  '// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.',
  '//',
  '// This code is free software; you can redistribute it and/or modify it',
  '// under the terms of the GNU General Public License version 2 only, as',
  '// published by the Free Software Foundation.  Oracle designates this',
  '// particular file as subject to the "Classpath" exception as provided',
  '// by Oracle in the LICENSE file that accompanied this code.',
  '//',
  '// Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA',
  '// or visit www.oracle.com if you need additional information or have any',
  '// questions.',
];

function fixture(sourceHeader) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dist-headers-'));
  fs.mkdirSync(path.join(dir, 'src'));
  fs.mkdirSync(path.join(dir, 'dist'));
  fs.writeFileSync(path.join(dir, 'src', 'a.ts'), `${sourceHeader.join('\n')}\n\nexport const a = 1;\n`);
  fs.writeFileSync(path.join(dir, 'dist', 'a.js'), ['"use strict";', ...sourceHeader, 'exports.a = 1;', ''].join('\n'));
  fs.writeFileSync(path.join(dir, 'dist', 'a.js.map'), JSON.stringify({ version: 3, mappings: `A;${'B;'.repeat(sourceHeader.length)}C` }));
  fs.writeFileSync(path.join(dir, 'dist', 'a.d.ts'), 'export declare const a = 1;\n');
  fs.writeFileSync(path.join(dir, 'dist', 'a.d.ts.map'), JSON.stringify({ version: 3, mappings: 'D' }));
  return dir;
}

const run = (dir) => spawnSync(process.execPath, [SCRIPT, path.join(dir, 'src'), path.join(dir, 'dist')], { encoding: 'utf8' });

test('the header goes first in the .js and the .d.ts, and the maps follow it', () => {
  const dir = fixture(HEADER);
  try {
    const r = run(dir);
    assert.equal(r.status, 0, r.stderr);
    const read = (f) => fs.readFileSync(path.join(dir, 'dist', f), 'utf8');
    assert.equal(read('a.js'), [...HEADER, '"use strict";', 'exports.a = 1;', ''].join('\n'));
    assert.equal(read('a.d.ts'), [...HEADER, 'export declare const a = 1;', ''].join('\n'));
    assert.equal(JSON.parse(read('a.js.map')).mappings, `${'B;'.repeat(13)}A;C`);
    assert.equal(JSON.parse(read('a.d.ts.map')).mappings, `${';'.repeat(13)}D`);
    // A second run changes nothing.
    assert.equal(run(dir).status, 0);
    assert.equal(read('a.d.ts'), [...HEADER, 'export declare const a = 1;', ''].join('\n'));
    assert.equal(JSON.parse(read('a.js.map')).mappings, `${'B;'.repeat(13)}A;C`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("OpenJDK's header, which the root allows for a translation from OpenJDK, goes first too", () => {
  for (const header of [OPENJDK]) {
    const dir = fixture(header);
    try {
      const r = run(dir);
      assert.equal(r.status, 0, r.stderr);
      const read = (f) => fs.readFileSync(path.join(dir, 'dist', f), 'utf8');
      assert.equal(read('a.js'), [...header, '"use strict";', 'exports.a = 1;', ''].join('\n'));
      assert.equal(read('a.d.ts'), [...header, 'export declare const a = 1;', ''].join('\n'));
      assert.equal(JSON.parse(read('a.js.map')).mappings, `${'B;'.repeat(header.length)}A;C`);
      assert.equal(JSON.parse(read('a.d.ts.map')).mappings, `${';'.repeat(header.length)}D`);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }
});

test('a source without the licence header fails the build', () => {
  // Another comment, a Confluent header cut short, OpenJDK's without its Classpath Exception, and
  // the Foundation's, a Java file's header copied into a translation.
  for (const header of [['// Some other comment'], HEADER.slice(0, 12), OPENJDK.filter((l) => !l.includes('Classpath')), ASF]) {
    const dir = fixture(header);
    try {
      const r = run(dir);
      assert.equal(r.status, 1, header[0]);
      assert.match(r.stderr, /^dist_headers: .*a\.ts: does not start with the licence header\n$/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }
});

// .semaphore/check-npm.py package: a .js or .d.ts in a tarball that does not
// start with a licence header is a finding, whatever else is wrong with the
// fake tarballs here, the Foundation's header included; one with Confluent's
// or OpenJDK's is not.
test('check-npm.py package reports each tarball file without the licence header', () => {
  const repo = path.join(__dirname, '..', '..', '..');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'check-npm-headers-'));
  try {
    const version = '4.4.0-beta.1';
    const files = {
      'dist/good.js': [...HEADER, '"use strict";', ''].join('\n'),
      'dist/good.d.ts': [...HEADER, 'export {};', ''].join('\n'),
      'dist/asf.js': [...ASF, '"use strict";', ''].join('\n'),
      'dist/openjdk.d.ts': [...OPENJDK, 'export {};', ''].join('\n'),
      'dist/strict_first.js': ['"use strict";', ...HEADER, ''].join('\n'),
      'dist/bare.d.ts': 'export {};\n',
    };
    const out = path.join(dir, 'out');
    fs.mkdirSync(out);
    for (const platform of [null, 'linux-x64-gnu', 'darwin-arm64']) {
      const stage = path.join(dir, platform || 'main', 'package');
      fs.mkdirSync(path.join(stage, 'dist'), { recursive: true });
      fs.writeFileSync(path.join(stage, 'package.json'), '{}');
      if (platform === null) for (const [name, text] of Object.entries(files)) fs.writeFileSync(path.join(stage, name), text);
      const tarball = path.join(out, `confluentinc-kafka-javascript${platform ? `-${platform}` : ''}-${version}.tgz`);
      // COPYFILE_DISABLE: no macOS AppleDouble ._ entries.
      const tar = spawnSync('tar', ['-czf', tarball, '-C', path.dirname(stage), 'package'], {
        encoding: 'utf8',
        env: { ...process.env, COPYFILE_DISABLE: '1' },
      });
      assert.equal(tar.status, 0, tar.stderr);
    }
    const r = spawnSync(process.env.PYTHON || 'python3', ['.semaphore/check-npm.py', 'package', out, '--version', version, '--dev'], {
      cwd: repo,
      encoding: 'utf8',
    });
    assert.equal(r.status, 1, r.stdout);
    const reported = [...r.stderr.matchAll(/: (dist\/\S+) does not start with the licence header/g)].map((m) => m[1]).sort();
    assert.deepEqual(reported, ['dist/asf.js', 'dist/bare.d.ts', 'dist/strict_first.js']);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
