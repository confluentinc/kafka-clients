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

// Java's ResourcePatternFilterTest (Java keeps it in its common.acl test package), then
// the class's other members.

const assert = require('node:assert/strict');

const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

/**
 * @param {import('@confluentinc/kafka-javascript/common/resource').ResourceType} resourceType
 * @param {string | null} name
 * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} patternType
 */
const filter = (resourceType, name, patternType) => new ResourcePatternFilter({ resourceType, name, patternType });
/**
 * @param {import('@confluentinc/kafka-javascript/common/resource').ResourceType} resourceType
 * @param {string} name
 * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} patternType
 */
const pattern = (resourceType, name, patternType) => new ResourcePattern({ resourceType, name, patternType });

test('shouldBeUnknownIfResourceTypeUnknown', () => {
  assert.equal(filter(ResourceType.UNKNOWN, null, PatternType.LITERAL).isUnknown(), true);
});

test('shouldBeUnknownIfPatternTypeUnknown', () => {
  assert.equal(filter(ResourceType.GROUP, null, PatternType.UNKNOWN).isUnknown(), true);
});

test('shouldNotMatchIfDifferentResourceType', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.LITERAL).matches(pattern(ResourceType.GROUP, 'Name', PatternType.LITERAL)),
    false,
  );
});

test('shouldNotMatchIfDifferentName', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Different', PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    false,
  );
});

test('shouldNotMatchIfDifferentNameCase', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'NAME', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)),
    false,
  );
});

test('shouldNotMatchIfDifferentPatternType', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    false,
  );
});

test('shouldMatchWhereResourceTypeIsAny', () => {
  assert.equal(
    filter(ResourceType.ANY, 'Name', PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    true,
  );
});

test('shouldMatchWhereResourceNameIsAny', () => {
  assert.equal(
    filter(ResourceType.TOPIC, null, PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    true,
  );
});

test('shouldMatchWherePatternTypeIsAny', () => {
  assert.equal(filter(ResourceType.TOPIC, null, PatternType.ANY).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)), true);
});

test('shouldMatchWherePatternTypeIsMatch', () => {
  assert.equal(
    filter(ResourceType.TOPIC, null, PatternType.MATCH).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    true,
  );
});

test('shouldMatchLiteralIfExactMatch', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)),
    true,
  );
});

test('shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeAny', () => {
  assert.equal(filter(ResourceType.TOPIC, 'Name', PatternType.ANY).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)), true);
});

test('shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeMatch', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.MATCH).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)),
    true,
  );
});

test('shouldNotMatchLiteralIfNamePrefixed', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name-something', PatternType.MATCH).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)),
    false,
  );
});

test('shouldMatchLiteralWildcardIfExactMatch', () => {
  assert.equal(filter(ResourceType.TOPIC, '*', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, '*', PatternType.LITERAL)), true);
});

test('shouldNotMatchLiteralWildcardAgainstOtherName', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, '*', PatternType.LITERAL)),
    false,
  );
});

test('shouldNotMatchLiteralWildcardTheWayAround', () => {
  assert.equal(
    filter(ResourceType.TOPIC, '*', PatternType.LITERAL).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.LITERAL)),
    false,
  );
});

test('shouldNotMatchLiteralWildcardIfFilterHasPatternTypeOfAny', () => {
  assert.equal(filter(ResourceType.TOPIC, 'Name', PatternType.ANY).matches(pattern(ResourceType.TOPIC, '*', PatternType.LITERAL)), false);
});

test('shouldMatchLiteralWildcardIfFilterHasPatternTypeOfMatch', () => {
  assert.equal(filter(ResourceType.TOPIC, 'Name', PatternType.MATCH).matches(pattern(ResourceType.TOPIC, '*', PatternType.LITERAL)), true);
});

test('shouldMatchPrefixedIfExactMatch', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    true,
  );
});

test('shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name', PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name-something', PatternType.PREFIXED)),
    false,
  );
});

test('shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name-something', PatternType.PREFIXED).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    false,
  );
});

test('shouldNotMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name-something', PatternType.ANY).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    false,
  );
});

test('shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsMatch', () => {
  assert.equal(
    filter(ResourceType.TOPIC, 'Name-something', PatternType.MATCH).matches(pattern(ResourceType.TOPIC, 'Name', PatternType.PREFIXED)),
    true,
  );
});
test("findIndefiniteField, matchesAtMostOne, equals, hashCode, toString and the JSON form are Java's", () => {
  assert.equal(ResourcePatternFilter.ANY.findIndefiniteField(), 'Resource type is ANY.');
  assert.equal(filter(ResourceType.UNKNOWN, 'n', PatternType.LITERAL).findIndefiniteField(), 'Resource type is UNKNOWN.');
  assert.equal(filter(ResourceType.TOPIC, null, PatternType.LITERAL).findIndefiniteField(), 'Resource name is NULL.');
  assert.equal(filter(ResourceType.TOPIC, 'n', PatternType.MATCH).findIndefiniteField(), 'Resource pattern type is MATCH.');
  assert.equal(filter(ResourceType.TOPIC, 'n', PatternType.UNKNOWN).findIndefiniteField(), 'Resource pattern type is UNKNOWN.');
  assert.equal(filter(ResourceType.TOPIC, 'n', PatternType.ANY).findIndefiniteField(), null);
  assert.ok(filter(ResourceType.TOPIC, 'n', PatternType.LITERAL).matchesAtMostOne());
  assert.ok(!ResourcePatternFilter.ANY.matchesAtMostOne());
  assert.equal(ResourcePatternFilter.ANY.toString(), 'ResourcePattern(resourceType=ANY, name=<any>, patternType=ANY)');
  assert.equal(JSON.stringify(ResourcePatternFilter.ANY), '{"resourceType":"ANY","name":null,"patternType":"ANY"}');
  assert.ok(filter(ResourceType.ANY, null, PatternType.ANY).equals(ResourcePatternFilter.ANY));
  assert.equal(filter(ResourceType.ANY, null, PatternType.ANY).hashCode(), ResourcePatternFilter.ANY.hashCode());
  assert.ok(!ResourcePatternFilter.ANY.equals(filter(ResourceType.ANY, 'x', PatternType.ANY)));
});

test("matches reads the pattern only where Java dereferences it, and throws Java's error for an unsupported pattern type", () => {
  // Java's filter of ANY, null, ANY never reads the pattern.
  // @ts-expect-error a null pattern
  assert.equal(ResourcePatternFilter.ANY.matches(null), true);
  // @ts-expect-error a null pattern, which this filter reads
  assert.throws(() => filter(ResourceType.TOPIC, null, PatternType.ANY).matches(null), {
    name: 'TypeError',
    message: 'pattern must not be null',
  });
  assert.throws(() => filter(ResourceType.TOPIC, 'n', PatternType.MATCH).matches(pattern(ResourceType.TOPIC, 'n', PatternType.UNKNOWN)), {
    name: 'IllegalArgumentError',
    message: 'Unsupported PatternType: UNKNOWN',
  });
  // @ts-expect-error a null resource type
  assert.throws(() => filter(null, 'n', PatternType.ANY), { name: 'TypeError', message: 'resourceType must not be null' });
  // @ts-expect-error a null pattern type
  assert.throws(() => filter(ResourceType.ANY, 'n', null), { name: 'TypeError', message: 'patternType must not be null' });
});
