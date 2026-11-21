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

// The public classes of Java's org.apache.kafka.common.acl this client has:
// the ACL bindings and filters the admin client's ACL calls take and return,
// and AclOperation, which its descriptions hold. AclBindingMap and
// AclBindingFilterMap are js-only: Java's maps keyed by them.

export { AccessControlEntry } from './access_control_entry';
export { AccessControlEntryFilter } from './access_control_entry_filter';
export { AclBinding } from './acl_binding';
export { AclBindingFilter } from './acl_binding_filter';
export { AclBindingFilterMap } from './acl_binding_filter_map';
export { AclBindingMap } from './acl_binding_map';
export { AclOperation } from './acl_operation';
export { AclPermissionType } from './acl_permission_type';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;
