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

// What every rule of the rule linter reads, loaded once and only when a rule
// asks for it: the TypeScript program over lib/ with its type checker, the
// Java index (`cargo xtask java-index --json`), the addon's C sources and the
// bindings header. A rule names the inputs it needs (`needs`); one that is
// missing makes the rule fail with the reason, never skip.

const fs = require('node:fs');
const path = require('node:path');

const { headerPath } = require('../header');

/** The directories `files` and `allFiles` never enter: dependencies, outputs, git's and agents' notes. */
const SKIPPED_DIRS = new Set(['node_modules', 'build', 'dist', '.git', '.claude']);

/** Every file under `dir`, relative to `base` with `/` separators, sorted; `[]` if `dir` does not exist. */
function walk(dir, base) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (e) {
    if (e.code === 'ENOENT' || e.code === 'ENOTDIR') return [];
    throw e;
  }
  const out = [];
  for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!SKIPPED_DIRS.has(entry.name)) out.push(...walk(full, base));
    } else if (entry.isFile()) {
      out.push(path.relative(base, full).split(path.sep).join('/'));
    }
  }
  return out;
}

/** The inputs a rule may need, each with what checks it is there. */
const INPUTS = {
  /** lib/ with at least one `.ts` source. */
  lib: (ctx) => (ctx.files(['lib'], ['.ts']).length > 0 ? null : `no TypeScript sources under ${ctx.rel(ctx.libDir)}/`),
  /** src/ with at least one `.c` source. */
  src: (ctx) => (ctx.files(['src'], ['.c']).length > 0 ? null : `no C sources under ${ctx.rel(ctx.srcDir)}/`),
  /** The `typescript` package (a devDependency). */
  typescript: (ctx) => {
    try {
      ctx.typescript();
      return null;
    } catch (e) {
      return `cannot load the typescript package (${e.message.split('\n')[0]}): run \`npm ci\` in javascript/`;
    }
  },
  /** The TypeScript program over lib/ (tsconfig.json). */
  program: (ctx) => {
    const ts = INPUTS.typescript(ctx);
    if (ts !== null) return ts;
    if (!fs.existsSync(ctx.tsconfigPath)) return `no ${ctx.rel(ctx.tsconfigPath)}`;
    try {
      ctx.program();
      return null;
    } catch (e) {
      return e.message;
    }
  },
  /** The Java index. */
  java: (ctx) => {
    if (!fs.existsSync(ctx.javaIndexPath)) {
      return `no Java index at ${ctx.rel(ctx.javaIndexPath)} (run \`cargo xtask java-index --json\` from rust/)`;
    }
    let index;
    try {
      index = ctx.javaIndex();
    } catch (e) {
      return `cannot read the Java index ${ctx.rel(ctx.javaIndexPath)}: ${e.message}`;
    }
    if (index === null || typeof index !== 'object' || index.schema !== 1) {
      return `the Java index ${ctx.rel(ctx.javaIndexPath)} is not schema 1 (rerun \`cargo xtask java-index --json\`)`;
    }
    if (!Array.isArray(index.classes) || index.classes.length === 0) {
      return `the Java index ${ctx.rel(ctx.javaIndexPath)} lists no class`;
    }
    return null;
  },
  /** The bindings header. */
  header: (ctx) =>
    fs.existsSync(ctx.headerPath)
      ? null
      : `no bindings header at ${ctx.headerPath} ` +
        '(build the Rust library first: `make build-javascript` from the repository root)',
};

class Context {
  /**
   * @param options.root - the client's directory (javascript/)
   * @param options.javaIndexPath - the Java index (default `<root>/build/java-index.json`)
   * @param options.headerPath - the bindings header (default the one the addon is built against)
   * @param options.loadTypescript - loads the `typescript` module (default `require`; tests replace it)
   */
  constructor({ root, javaIndexPath, headerPath: header, loadTypescript } = {}) {
    this.root = path.resolve(root || path.join(__dirname, '..', '..'));
    this.libDir = path.join(this.root, 'lib');
    this.srcDir = path.join(this.root, 'src');
    this.tsconfigPath = path.join(this.root, 'tsconfig.json');
    this.javaIndexPath = javaIndexPath || path.join(this.root, 'build', 'java-index.json');
    this.headerPath = header || headerPath();
    this.loadTypescript = loadTypescript || (() => require('typescript'));
    this.cache = new Map();
  }

  /** `compute()`, once per `name`. */
  once(name, compute) {
    if (!this.cache.has(name)) this.cache.set(name, compute());
    return this.cache.get(name);
  }

  /** `p` relative to the root, with `/` separators. */
  rel(p) {
    return path.relative(this.root, p).split(path.sep).join('/');
  }

  /** Why input `name` is missing, or null. */
  missing(name) {
    const check = INPUTS[name];
    if (check === undefined) throw new Error(`unknown lint input \`${name}\``);
    return check(this);
  }

  /** The files under the root's `dirs` (a plain file name is taken as is) with one of `exts` (all when omitted). */
  files(dirs, exts) {
    const out = [];
    for (const d of dirs) {
      const full = path.join(this.root, d);
      const found = fs.existsSync(full) && fs.statSync(full).isFile() ? [d] : walk(full, this.root);
      out.push(...found.filter((f) => exts === undefined || exts.some((e) => f.endsWith(e))));
    }
    return out;
  }

  /**
   * Every file of the client, relative to the root, sorted: the whole tree,
   * read from the file system (never from git, whose index and environment
   * vary with the caller), outside `node_modules/`, `build/`, `dist/`,
   * `.git/` and `.claude/` (agents' notes, never the client's).
   */
  allFiles() {
    return this.once('allFiles', () => walk(this.root, this.root));
  }

  /** The text of root-relative file `rel`, read once. */
  read(rel) {
    return this.once(`file:${rel}`, () => fs.readFileSync(path.join(this.root, rel), 'utf8'));
  }

  /** The `typescript` module. */
  typescript() {
    return this.once('typescript', () => this.loadTypescript());
  }

  /** `{program, checker, sourceFiles}`: the program of tsconfig.json, its type checker, and its lib/ sources. */
  program() {
    return this.once('program', () => {
      const ts = this.typescript();
      const read = ts.readConfigFile(this.tsconfigPath, ts.sys.readFile);
      if (read.error) throw new Error(ts.flattenDiagnosticMessageText(read.error.messageText, '\n'));
      const parsed = ts.parseJsonConfigFileContent(read.config, ts.sys, this.root);
      if (parsed.errors.length > 0) {
        throw new Error(parsed.errors.map((d) => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('; '));
      }
      const program = ts.createProgram({ rootNames: parsed.fileNames, options: parsed.options });
      const lib = this.libDir + path.sep;
      const sourceFiles = program
        .getSourceFiles()
        .filter((sf) => path.resolve(sf.fileName).startsWith(lib))
        .sort((a, b) => (a.fileName < b.fileName ? -1 : 1));
      return { ts, program, checker: program.getTypeChecker(), sourceFiles };
    });
  }

  /** The parsed (not type-checked) TypeScript or JavaScript source of root-relative file `rel`. */
  sourceFile(rel) {
    return this.once(`sf:${rel}`, () => {
      const ts = this.typescript();
      const kind = rel.endsWith('.ts') ? ts.ScriptKind.TS : ts.ScriptKind.JS;
      return ts.createSourceFile(rel, this.read(rel), ts.ScriptTarget.Latest, true, kind);
    });
  }

  /** The Java index (`build/java-index.json`). */
  javaIndex() {
    return this.once('java', () => JSON.parse(fs.readFileSync(this.javaIndexPath, 'utf8')));
  }

  /** The bindings header's text. */
  header() {
    return this.once('header', () => fs.readFileSync(this.headerPath, 'utf8'));
  }
}

module.exports = { Context, walk };
