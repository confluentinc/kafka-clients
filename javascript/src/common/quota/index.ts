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

/**
 * The public classes of Java's `org.apache.kafka.common.quota` this client
 * has: the client quota entities, filters and alterations the admin client's
 * quota calls take and return.
 *
 * Invented *(js-only)*: `ClientQuotaEntityMap`, Java's
 * `Map<ClientQuotaEntity, V>`: JavaScript has no value equality for map keys.
 *
 * @packageDocumentation
 */

export { ClientQuotaAlteration } from './client_quota_alteration';
export { ClientQuotaEntity } from './client_quota_entity';
export { ClientQuotaEntityMap } from './client_quota_entity_map';
export { ClientQuotaFilter } from './client_quota_filter';
export { ClientQuotaFilterComponent } from './client_quota_filter_component';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
