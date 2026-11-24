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

// check-public-audience (scripts/lint/rules/public_audience.js).

const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex, mapping, method, INVENTED } = require('./_java_fixture');

const RULE = 'check-public-audience';

const fixture = (files = {}, classes = CLASSES) =>
  project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(classes), ...LIB, ...files });

test('Public classes and their public members pass; an error class is exempt', () => {
  // A non-Public error class is still exported: one class per FFI-enum exception.
  const classes = CLASSES.map((c) => (c.fqn.endsWith('FooException') ? { ...c, public_audience: false } : c));
  const r = lint(fixture({}, classes), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, Mode, ProducerConfig, Callback, Header, Future; and the 25 members check-java-members maps (setMode included).
  assert.ok(r.out.includes('✅ check-public-audience: 33 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/sealed.ts': 'export class Sealed {}\n',
      'src/producer/internals/acc.ts': 'export class Acc {}\n',
      'src/common/x/unsupp.ts': 'export class Unsupp {}\n',
      'src/producer/pkg.ts': 'export class Pkg {\n  constructor() {}\n}\n',
      'src/producer/rec.ts': LIB['src/producer/rec.ts'].replace('extra(): void {}', 'extra(): void {}\n  hidden(): void {}'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/common/x/unsupp.ts:Unsupp: translates common.x.Unsupp, whose package common.x is not a supported Kafka API',
    'src/producer/internals/acc.ts:Acc: translates clients.producer.internals.Acc, whose package clients.producer.internals is internal',
    'src/producer/sealed.ts:Sealed: translates clients.producer.Sealed, which is not @InterfaceAudience.Public in Kafka 4.4.0-rc3',
    'Pkg.constructor: translates the package-private constructor clients.producer.Pkg#Pkg',
    'Rec.hidden: translates the private method clients.producer.Rec#hidden',
  ]);
});

test("a class the JS conventions make public is exempt: Java's internal RecordHeader, the class an app builds a header with", () => {
  const recordHeader = javaClass('common.header.internals.RecordHeader', {
    public_audience: false,
    overloads: [method('RecordHeader', ['String', 'byte[]'])],
  });
  const files = (/** @type {string} */ name) => ({
    [`src/common/header/${name === 'RecordHeader' ? 'record_header' : 'other_header'}.ts`]: `/** A header. */\nexport class ${name} {}\n`,
    'lint-mapping.json': mapping({
      invented: INVENTED,
      javaTargets: {
        [`src/common/header/${name === 'RecordHeader' ? 'record_header' : 'other_header'}.ts:${name}`]:
          name === 'RecordHeader'
            ? 'org.apache.kafka.common.header.internals.RecordHeader'
            : 'org.apache.kafka.clients.producer.internals.Acc',
      },
    }),
  });
  let r = lint(fixture(files('RecordHeader'), [...CLASSES, recordHeader]), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Another internal class is not.
  r = lint(fixture(files('OtherHeader'), [...CLASSES, recordHeader]), [RULE]);
  assert.deepEqual(findings(r), [
    'src/common/header/other_header.ts:OtherHeader: translates clients.producer.internals.Acc, whose package clients.producer.internals is internal',
  ]);
});

test('a class nested in a test-fixture class is exempt, as its outer class is; one nested in another class is not', () => {
  const classes = [
    ...CLASSES,
    javaClass('clients.admin.MockAdminClient', { public_audience: false }),
    javaClass('clients.admin.MockAdminClient$Builder', { public_audience: false }),
    javaClass('clients.producer.Sealed', { public_audience: false }),
    javaClass('clients.producer.Sealed$Inner', { public_audience: false }),
  ];
  const r = lint(
    fixture(
      {
        'src/admin/mock_admin_client.ts':
          'export class MockAdminClient {}\nexport declare namespace MockAdminClient {\n  export class Builder {}\n}\n',
        'src/producer/sealed.ts': 'export class Sealed {}\nexport declare namespace Sealed {\n  export class Inner {}\n}\n',
      },
      classes,
    ),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'src/producer/sealed.ts:Sealed: translates clients.producer.Sealed, which is not @InterfaceAudience.Public in Kafka 4.4.0-rc3',
    'src/producer/sealed.ts:Sealed.Inner: translates clients.producer.Sealed$Inner, which is not @InterfaceAudience.Public in Kafka 4.4.0-rc3',
  ]);
});

test("a private class nested in a test-fixture class is refused: the exemption covers only the fixture's public nested classes", () => {
  const classes = [
    ...CLASSES,
    javaClass('clients.admin.MockAdminClient', { public_audience: false }),
    javaClass('clients.admin.MockAdminClient$Builder', { public_audience: false }),
    javaClass('clients.admin.MockAdminClient$TopicMetadata', { public_audience: false, visibility: 'private' }),
  ];
  const r = lint(
    fixture(
      {
        'src/admin/mock_admin_client.ts':
          'export class MockAdminClient {}\nexport declare namespace MockAdminClient {\n  export class Builder {}\n  export class TopicMetadata {}\n}\n',
      },
      classes,
    ),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'src/admin/mock_admin_client.ts:MockAdminClient.TopicMetadata: translates clients.admin.MockAdminClient$TopicMetadata, which is not @InterfaceAudience.Public in Kafka 4.4.0-rc3',
    'src/admin/mock_admin_client.ts:MockAdminClient.TopicMetadata: translates clients.admin.MockAdminClient$TopicMetadata, a private nested class in Java',
  ]);
});

test("a class nested in a Public class is refused unless Java declares it public, though the index gives it its outer class's audience", () => {
  const classes = [
    ...CLASSES,
    javaClass('clients.producer.Rec$Secret', { visibility: 'private' }),
    javaClass('clients.producer.Rec$Kept', { visibility: 'package-private' }),
    javaClass('clients.producer.Rec$Open', {}),
  ];
  const r = lint(
    fixture(
      {
        'src/producer/rec.ts':
          LIB['src/producer/rec.ts'] +
          'export declare namespace Rec {\n  export class Secret {}\n  export class Kept {}\n  export class Open {}\n}\n',
      },
      classes,
    ),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'src/producer/rec.ts:Rec.Secret: translates clients.producer.Rec$Secret, a private nested class in Java',
    'src/producer/rec.ts:Rec.Kept: translates clients.producer.Rec$Kept, a package-private nested class in Java',
  ]);
});

test('an invented export translates nothing, so it is not checked', () => {
  const invented = { ...INVENTED, 'src/producer/sealed.ts:Sealed': 'A JavaScript helper.' };
  const r = lint(
    fixture({
      'src/producer/sealed.ts': '/** A JavaScript helper. */\nexport class Sealed {}\n',
      'lint-mapping.json': mapping({ invented }),
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-public-audience: cannot run, no Java index/);
});

test('nothing translated fails: 0 items checked', () => {
  const r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([javaClass('clients.producer.Rec')]),
      'src/a.ts': 'export const a = 1;\n',
    }),
    [RULE],
  );
  assert.ok(r.err.includes('❌ check-public-audience: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});
