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

// Java's org.apache.kafka.clients.admin.FeatureMetadata, a value class that
// keeps Java's methods. Its Java constructor is package-private: a private
// constructor here, the client building it through `internalCreate`
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q16). Java's
// `Optional<Long>` epoch is `bigint | null` (Types). The getters return a new
// `Map` on each call, as Java's return a new `HashMap`. equals, hashCode and
// toString are Java's; Java's HashMap order is unspecified, so the maps keep
// the order the broker gave.

import { JavaForms } from '../internal/args';
import { listHashCode, longHashCode, mapEquals, mapHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import type { FinalizedVersionRange } from './finalized_version_range';
import type { SupportedVersionRange } from './supported_version_range';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const FINALIZED_FEATURES_FORMS = READER('finalizedFeatures');
const FINALIZED_FEATURES_EPOCH_FORMS = READER('finalizedFeaturesEpoch');
const SUPPORTED_FEATURES_FORMS = READER('supportedFeatures');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['other']]);

/**
 * `FeatureMetadata.mapToString`: `{(a -> x), (b -> y)}`.
 *
 * @param map - the feature versions, by name
 * @returns the text
 */
function mapToString(map: ReadonlyMap<string, unknown>): string {
  return `{${[...map].map(([k, v]) => `(${k} -> ${javaString(v)})`).join(', ')}}`;
}

/**
 * Encapsulates details about finalized as well as supported features. This
 * is useful when trying to retrieve the finalized and supported features.
 *
 * Instances come from the admin client's `describeFeatures()`.
 */
export class FeatureMetadata {
  readonly #finalizedFeatures: ReadonlyMap<string, FinalizedVersionRange>;
  readonly #finalizedFeaturesEpoch: bigint | null;
  readonly #supportedFeatures: ReadonlyMap<string, SupportedVersionRange>;

  private constructor(
    finalizedFeatures: ReadonlyMap<string, FinalizedVersionRange>,
    finalizedFeaturesEpoch: bigint | null,
    supportedFeatures: ReadonlyMap<string, SupportedVersionRange>,
  ) {
    this.#finalizedFeatures = new Map(finalizedFeatures);
    this.#finalizedFeaturesEpoch = finalizedFeaturesEpoch;
    this.#supportedFeatures = new Map(supportedFeatures);
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param finalizedFeatures - the finalized feature versions, by name
   * @param finalizedFeaturesEpoch - the epoch, or null when absent
   * @param supportedFeatures - the supported feature versions, by name
   * @returns the metadata
   * @internal
   */
  static internalCreate(
    finalizedFeatures: ReadonlyMap<string, FinalizedVersionRange>,
    finalizedFeaturesEpoch: bigint | null,
    supportedFeatures: ReadonlyMap<string, SupportedVersionRange>,
  ): FeatureMetadata {
    return new FeatureMetadata(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
  }

  /**
   * Returns a map of finalized feature versions. Each entry in the map
   * contains a key being a feature name and the value being a range of
   * version levels supported by every broker in the cluster.
   *
   * @returns a new map, which the caller may change
   * @throws `TypeError` when it is given an argument: it only reads
   */
  finalizedFeatures(): Map<string, FinalizedVersionRange> {
    if (arguments.length !== 0) FINALIZED_FEATURES_FORMS.match(undefined, arguments.length);
    return new Map(this.#finalizedFeatures);
  }

  /**
   * The epoch for the finalized features. If the returned value is empty, it
   * means the finalized features are absent/unavailable.
   *
   * @returns the epoch, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  finalizedFeaturesEpoch(): bigint | null {
    if (arguments.length !== 0) FINALIZED_FEATURES_EPOCH_FORMS.match(undefined, arguments.length);
    return this.#finalizedFeaturesEpoch;
  }

  /**
   * Returns a map of supported feature versions. Each entry in the map
   * contains a key being a feature name and the value being a range of
   * versions supported by a particular broker in the cluster.
   *
   * @returns a new map, which the caller may change
   * @throws `TypeError` when it is given an argument: it only reads
   */
  supportedFeatures(): Map<string, SupportedVersionRange> {
    if (arguments.length !== 0) SUPPORTED_FEATURES_FORMS.match(undefined, arguments.length);
    return new Map(this.#supportedFeatures);
  }

  /**
   * Whether `other` is a FeatureMetadata with the same features and epoch.
   *
   * @param other - the value to compare with, on its own or as `{ other }`
   * @returns whether they are equal
   */
  equals(other: unknown): boolean {
    EQUALS_FORMS.match(other, arguments.length);
    const o = EQUALS_FORMS.value(other, 'other');
    if (o === this) return true;
    if (!(o instanceof FeatureMetadata)) return false;
    return (
      mapEquals(this.#finalizedFeatures, o.#finalizedFeatures) &&
      this.#finalizedFeaturesEpoch === o.#finalizedFeaturesEpoch &&
      mapEquals(this.#supportedFeatures, o.#supportedFeatures)
    );
  }

  /**
   * Java's `Objects.hash(finalizedFeatures, finalizedFeaturesEpoch,
   * supportedFeatures)`, an empty epoch hashing as 0 (`Optional.hashCode`).
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    const epoch = this.#finalizedFeaturesEpoch === null ? 0 : longHashCode(this.#finalizedFeaturesEpoch);
    return listHashCode([mapHashCode(this.#finalizedFeatures), epoch, mapHashCode(this.#supportedFeatures)]);
  }

  /**
   * Java's `toString()`: `FeatureMetadata{finalizedFeatures:{(a -> x)},
   * finalizedFeaturesEpoch:1, supportedFeatures:{(a -> y)}}`, `<none>` for an
   * empty epoch.
   *
   * @returns the text
   */
  toString(): string {
    const epoch = this.#finalizedFeaturesEpoch === null ? '<none>' : String(this.#finalizedFeaturesEpoch);
    return `FeatureMetadata{finalizedFeatures:${mapToString(this.#finalizedFeatures)}, finalizedFeaturesEpoch:${epoch}, supportedFeatures:${mapToString(this.#supportedFeatures)}}`;
  }

  /**
   * The metadata as `JSON.stringify` writes it: `{ finalizedFeatures,
   * finalizedFeaturesEpoch, supportedFeatures }`, the maps as objects keyed
   * by feature name and the epoch as a decimal string.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): {
    finalizedFeatures: Record<string, FinalizedVersionRange>;
    finalizedFeaturesEpoch: string | null;
    supportedFeatures: Record<string, SupportedVersionRange>;
  } {
    return {
      finalizedFeatures: Object.fromEntries(this.#finalizedFeatures),
      finalizedFeaturesEpoch: this.#finalizedFeaturesEpoch === null ? null : String(this.#finalizedFeaturesEpoch),
      supportedFeatures: Object.fromEntries(this.#supportedFeatures),
    };
  }
}
