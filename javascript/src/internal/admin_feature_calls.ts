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

// The admin client's feature and controller RPCs (private), as
// src/internal/admin_calls.ts has the others. Each checks the call's form and
// its arguments in Java's order, runs the RPC and builds its Result from the
// native result's futures, as Python's confluent_kafka/admin/_conversions.py
// composes them. Where Java stores a null and reads it only later, on its own
// thread, the call reports it with a `TypeError`, a deviation the admin
// package's documentation lists.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { isPlainObject, kind } from './args';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { optionSetters, optionsFirst, rpcForms, voidOf } from './admin_calls';
import { checkInt } from './java_number';
import { DescribeFeaturesOptions } from '../admin/describe_features_options';
import { DescribeFeaturesResult } from '../admin/describe_features_result';
import { FeatureMetadata } from '../admin/feature_metadata';
import { FeatureUpdate } from '../admin/feature_update';
import { FinalizedVersionRange } from '../admin/finalized_version_range';
import { SupportedVersionRange } from '../admin/supported_version_range';
import { UnregisterControllerOptions } from '../admin/unregister_controller_options';
import { UnregisterControllerResult } from '../admin/unregister_controller_result';
import { UpdateFeaturesOptions } from '../admin/update_features_options';
import { UpdateFeaturesResult } from '../admin/update_features_result';

const DESCRIBE_FEATURES_FORMS = rpcForms('describeFeatures', [], DescribeFeaturesOptions);
const UPDATE_FEATURES_FORMS = rpcForms('updateFeatures', ['featureUpdates'], UpdateFeaturesOptions);
const UNREGISTER_CONTROLLER_FORMS = rpcForms('unregisterController', ['controllerId'], UnregisterControllerOptions);

/** Feature metadata as the addon reads it: [finalized, epoch, supported]. */
type FeatureMetadataFields = [Array<[string, [number, number]]>, bigint | null, Array<[string, [number, number]]>];

/**
 * The feature metadata the addon read.
 *
 * @param raw - its fields
 * @returns the metadata
 */
function featureMetadataOf(raw: unknown): FeatureMetadata {
  const [finalized, epoch, supported] = raw as FeatureMetadataFields;
  return FeatureMetadata.internalCreate(
    new Map(
      finalized.map(([name, [minVersionLevel, maxVersionLevel]]) => [
        name,
        new FinalizedVersionRange({ minVersionLevel, maxVersionLevel }),
      ]),
    ),
    epoch,
    new Map(supported.map(([name, [minVersion, maxVersion]]) => [name, new SupportedVersionRange({ minVersion, maxVersion })])),
  );
}

/**
 * `describeFeatures()` and `describeFeatures(options)`.
 *
 * @param handle - the client's handle
 * @param options - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeFeatures(handle: AdminHandle, options: unknown, count: number): DescribeFeaturesResult {
  DESCRIBE_FEATURES_FORMS.match(options, count);
  // Java's first dereference is the options' (nodeId()).
  optionsFirst(DESCRIBE_FEATURES_FORMS, options);
  const box = native.adminDescribeFeatures(handle, optionSetters(DESCRIBE_FEATURES_FORMS, options, DescribeFeaturesOptions));
  return DescribeFeaturesResult.internalCreate(adminFuture(handle, box, 'feature_metadata', null, featureMetadataOf));
}

/**
 * `updateFeatures(featureUpdates)` and `updateFeatures(featureUpdates,
 * options)`, the map a `Map` or a plain object keyed by feature name.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function updateFeatures(handle: AdminHandle, args: unknown, count: number): UpdateFeaturesResult {
  UPDATE_FEATURES_FORMS.match(args, count);
  // Java's first dereference is the map's (isEmpty()); the core then refuses
  // an empty map and a blank or null feature name, and reads each update when
  // it builds the request.
  const updates = UPDATE_FEATURES_FORMS.value(args, 'featureUpdates');
  if (updates === null) throw new TypeError('featureUpdates must not be null');
  let entries: Array<[unknown, unknown]>;
  if (updates instanceof Map) entries = [...(updates as Map<unknown, unknown>)];
  else if (isPlainObject(updates)) entries = Object.entries(updates);
  else throw new TypeError(`featureUpdates must be a Map or a plain object, not ${kind(updates)}`);
  const pairs = entries.map(([feature, update]): [string, [number, number]] => {
    if (feature !== null && typeof feature !== 'string')
      throw new TypeError(`featureUpdates must be keyed by strings, not ${kind(feature)}`);
    if (update === null) throw new TypeError('featureUpdates must not contain null');
    if (!(update instanceof FeatureUpdate)) throw new TypeError(`featureUpdates must hold only FeatureUpdates, not ${kind(update)}`);
    // Plain JavaScript can store a null upgrade type, as Java's constructor
    // does; Java reads it when it builds the request.
    const type = update.upgradeType() as FeatureUpdate.UpgradeType | null;
    if (type === null) throw new TypeError('featureUpdates must not contain a FeatureUpdate with a null upgradeType');
    // On KafkaAdminClient a null name is Java's blank one (Utils.isBlank(null)),
    // which the core refuses with Java's message. The C map takes no NULL key,
    // so MockAdminClient keys it by "" where Java's mock uses null (a listed
    // deviation).
    return [feature ?? '', [update.maxVersionLevel(), type.code()]];
  });
  const box = native.adminUpdateFeatures(handle, pairs, optionSetters(UPDATE_FEATURES_FORMS, args, UpdateFeaturesOptions));
  return UpdateFeaturesResult.internalCreate(new Map(adminFutures(handle, box, 'values', (raw) => raw as string, voidOf) ?? []));
}

/**
 * `unregisterController(controllerId)` and `unregisterController(controllerId,
 * options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function unregisterController(handle: AdminHandle, args: unknown, count: number): UnregisterControllerResult {
  UNREGISTER_CONTROLLER_FORMS.match(args, count);
  // Java's `int` cannot be null; its first dereference is the options'
  // (timeoutMs()).
  const controllerId = checkInt(UNREGISTER_CONTROLLER_FORMS.value(args, 'controllerId'), 'controllerId');
  optionsFirst(UNREGISTER_CONTROLLER_FORMS, args);
  const box = native.adminUnregisterController(
    handle,
    controllerId,
    optionSetters(UNREGISTER_CONTROLLER_FORMS, args, UnregisterControllerOptions),
  );
  return UnregisterControllerResult.internalCreate(adminFuture(handle, box, 'all', null, voidOf));
}
