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

// The HTML reference (`make docs`, javascript/CLAUDE.md, JavaScript Binding
// Conventions, Documentation, Q64): TypeDoc over src/, one module per import
// path of package.json's `exports`, so the reference has the package's own
// layout. A member marked `@internal` is left out, as tsc's `stripInternal`
// leaves it out of the declarations, and each package's
// `@packageDocumentation`, with its list of what is left out, is its module's
// page. The output, build/docs/, is git-ignored.

const path = require('node:path');

const pkg = require('./package.json');

/** The source index of each import path: `./dist/x/index.d.ts` -> `src/x/index.ts`. */
const entryPoints = Object.values(pkg.exports).map(({ types }) =>
  path.join('src', path.relative('dist', types)).replace(/\.d\.ts$/, '.ts'),
);

module.exports = {
  entryPoints,
  entryPointStrategy: 'resolve',
  tsconfig: 'tsconfig.json',
  out: 'build/docs',
  name: pkg.name,
  readme: 'README.md',
  excludeInternal: true,
  excludePrivate: true,
  // The Javadoc the docs quote carries Java code blocks.
  highlightLanguages: ['bash', 'java', 'javascript', 'json', 'typescript'],
  // The build fails on a broken link or a comment TypeDoc cannot read, so a
  // reference that builds has no such gap. A type a signature uses but no
  // import path exports (`SerdeConfigs`, `BareValue`, ...) is the package's
  // design, not a gap: the declarations carry it, and so does the page of the
  // member that uses it.
  validation: { notExported: false, invalidLink: true, rewrittenLink: true },
  treatWarningsAsErrors: true,
};
