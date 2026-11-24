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

// check-implementation-docs (scripts/lint/rules/implementation_docs.js): the
// TSDoc of a class member that implements an interface member.

const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-implementation-docs';

const SHAPE = `/** A thing with a name. */
export interface Named {
  /** Its name. */
  readonly name: string;
}
/** A shape. */
export interface Shape extends Named {
  /**
   * The area.
   *
   * @returns the area
   */
  area(): number;
  /**
   * Scales it.
   *
   * @param by - the factor
   */
  scale(by: number): void;
  /**
   * Its values.
   *
   * @returns the iterator
   */
  [Symbol.iterator](): Iterator<number>;
}
`;

test('a member implementing an interface member of src/ keeps at most one @remarks alone', () => {
  const root = project({
    'tsconfig.json': TSCONFIG,
    'src/shape.ts': SHAPE,
    'src/square.ts': `import type { Shape } from './shape';
/** A square. */
export class Square implements Shape, Iterable<number> {
  /** The name. */
  readonly name = 'square';
  /** @remarks A square's area is its side squared. */
  area(): number {
    return 1;
  }
  /**
   * Scales it.
   *
   * @param by - the factor
   */
  scale(by: number): void {
    void by;
  }
  [Symbol.iterator](): Iterator<number> {
    return [1][Symbol.iterator]();
  }
  /**
   * The side.
   *
   * @returns the side
   */
  side(): number {
    return 1;
  }
  /** @remarks Not an implementation. */
  perimeter(): number {
    return 4;
  }
  /** A static member implements nothing. */
  static area(): number {
    return 0;
  }
}
`,
    'src/circle.ts': `import type { Shape } from './shape';
/** A circle. */
export class Circle implements Shape {
  readonly name = 'circle';
  /** @remarks One. */
  area(): number;
  /** @remarks Two. */
  area(x?: number): number;
  area(): number {
    return 3;
  }
  /**
   * @remarks One.
   * @remarks Two.
   */
  scale(by: number): void {
    void by;
  }
  [Symbol.iterator](): Iterator<number> {
    return [1][Symbol.iterator]();
  }
}
/** A list: the member of TypeScript's own Iterable keeps its doc. */
export class List implements Iterable<number> {
  /**
   * Iterates.
   *
   * @returns the iterator
   */
  [Symbol.iterator](): Iterator<number> {
    return [1][Symbol.iterator]();
  }
}
`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  const rest =
    ': keep at most one, a `@remarks` alone, for what the class does differently, and move what holds for every implementation to the interface';
  assert.deepEqual(findings(r), [
    'src/circle.ts:Circle.area: `Circle.area` implements `Shape.area`, whose TSDoc TypeDoc shows, but carries 2 doc comment(s)' + rest,
    'src/circle.ts:Circle.scale: `Circle.scale` implements `Shape.scale`, whose TSDoc TypeDoc shows, but carries 1 doc comment(s)' + rest,
    'src/square.ts:Square.name: `Square.name` implements `Named.name`, whose TSDoc TypeDoc shows, but carries 1 doc comment(s)' + rest,
    'src/square.ts:Square.scale: `Square.scale` implements `Shape.scale`, whose TSDoc TypeDoc shows, but carries 1 doc comment(s)' + rest,
    'src/square.ts:Square.perimeter: a doc comment of one `@remarks` alone (line 29) on `Square.perimeter`, which implements no interface member: give it a summary and the tags the docs lint asks for',
  ]);
  assert.ok(r.err.includes('❌ check-implementation-docs: 5 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      'Put the TSDoc on the interface that declares the member; a class member implementing it keeps at most one `@remarks`, for what the class does differently, and a doc comment of one `@remarks` alone sits only there (javascript/CLAUDE.md, Comments, point 1).',
    ),
  );
});

test('a clean project passes, each implementing member and doc comment counted; it needs the program', () => {
  let r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'src/shape.ts': SHAPE,
      'src/square.ts': `import type { Shape } from './shape';
/** A square. */
export class Square implements Shape {
  readonly name = 'square';
  area(): number {
    return 1;
  }
  scale(by: number): void {
    void by;
  }
  [Symbol.iterator](): Iterator<number> {
    return [1][Symbol.iterator]();
  }
}
`,
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
  // Square's four members, and the seven doc comments of the interfaces and the class.
  assert.ok(r.out.includes('✅ check-implementation-docs: 11 item(s) checked'), r.text);
  r = lint(project({ 'src/a.ts': 'export {};\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(
    r.err.some((l) => l.startsWith('❌ check-implementation-docs: cannot run, no tsconfig.json')),
    r.text,
  );
});
