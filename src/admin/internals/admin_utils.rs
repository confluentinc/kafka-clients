// Copyright 2025 Confluent Inc.
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

//! Admin-internal utility helpers.
//!
//! Corresponds to `org.apache.kafka.clients.admin.internals.AdminUtils`.

use std::collections::BTreeSet;

use crate::common::acl::AclOperation;
use crate::common::requests::metadata_response::AUTHORIZED_OPERATIONS_OMITTED;
use crate::common::utils::from_32_bit_field;

/// Decodes a 32-bit authorized-operations field into the set of valid
/// [`AclOperation`]s, filtering out `UNKNOWN`, `ALL` and `ANY`.
///
/// Corresponds to `AdminUtils.validAclOperations`, including its nullability:
/// Java returns `null` when the field is [`AUTHORIZED_OPERATIONS_OMITTED`] —
/// meaning "the broker did not report them" — which is distinct from a broker
/// reporting an empty set. `None` is that `null`.
pub(crate) fn valid_acl_operations(authorized_operations: i32) -> Option<BTreeSet<AclOperation>> {
    if authorized_operations == AUTHORIZED_OPERATIONS_OMITTED {
        return None;
    }
    Some(
        from_32_bit_field(authorized_operations)
            .into_iter()
            .map(AclOperation::from_code)
            .filter(|op| *op != AclOperation::Unknown && *op != AclOperation::All && *op != AclOperation::Any)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_returns_none_not_an_empty_set() {
        // Java's `validAclOperations` returns null here. `None` and
        // `Some(empty)` are different answers: "not reported" vs "reported as
        // none permitted".
        assert_eq!(valid_acl_operations(AUTHORIZED_OPERATIONS_OMITTED), None);
    }

    #[test]
    fn decodes_and_filters_bits() {
        // AclOperation codes: Read=3, Write=4, Create=5 → bits 3, 4, 5.
        let field = (1 << 3) | (1 << 4) | (1 << 5);
        let ops = valid_acl_operations(field).expect("reported, so Some");
        assert!(ops.contains(&AclOperation::Read));
        assert!(ops.contains(&AclOperation::Write));
        assert!(ops.contains(&AclOperation::Create));
    }

    #[test]
    fn filters_all_and_any() {
        // bit 1 = ANY (code 1), bit 2 = ALL (code 2). Both filtered out, leaving
        // a reported-but-empty set — `Some(empty)`, not `None`.
        let field = (1 << 1) | (1 << 2);
        assert_eq!(valid_acl_operations(field), Some(BTreeSet::new()));
    }
}
