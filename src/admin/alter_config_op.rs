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

//! An alter configuration operation.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterConfigOp`.

use super::ConfigEntry;

/// The operation type of an [`AlterConfigOp`].
///
/// Corresponds to `AlterConfigOp.OpType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OpType {
    /// Set the value of the configuration entry.
    Set,
    /// Revert the configuration entry to the default value (possibly null).
    Delete,
    /// (For list-type configuration entries only.) Add the specified values to
    /// the current value of the configuration entry. If the configuration value
    /// has not been set, adds to the default value.
    Append,
    /// (For list-type configuration entries only.) Removes the specified values
    /// from the current value of the configuration entry. It is legal to remove
    /// values that are not currently in the configuration entry. Removing all
    /// entries from the current configuration value leaves an empty list and
    /// does NOT revert to the default value of the entry.
    Subtract,
}

impl OpType {
    /// Returns the wire id for this operation type.
    ///
    /// Corresponds to `AlterConfigOp.OpType.id()`.
    pub fn id(&self) -> i8 {
        match self {
            OpType::Set => 0,
            OpType::Delete => 1,
            OpType::Append => 2,
            OpType::Subtract => 3,
        }
    }

    /// Returns the operation type for the given wire id, or `None` if the id is
    /// unrecognized.
    ///
    /// Corresponds to `AlterConfigOp.OpType.forId(byte)` (which returns `null`
    /// for an unknown id).
    pub fn for_id(id: i8) -> Option<OpType> {
        match id {
            0 => Some(OpType::Set),
            1 => Some(OpType::Delete),
            2 => Some(OpType::Append),
            3 => Some(OpType::Subtract),
            _ => None,
        }
    }
}

/// A class representing an alter configuration entry containing name, value and
/// operation type.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterConfigOp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterConfigOp {
    config_entry: ConfigEntry,
    op_type: OpType,
}

impl AlterConfigOp {
    /// Creates a new alter config operation.
    pub fn new(config_entry: ConfigEntry, operation_type: OpType) -> Self {
        Self { config_entry, op_type: operation_type }
    }

    /// Returns the config entry.
    pub fn config_entry(&self) -> &ConfigEntry {
        &self.config_entry
    }

    /// Returns the operation type.
    pub fn op_type(&self) -> OpType {
        self.op_type
    }
}

impl std::fmt::Display for AlterConfigOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AlterConfigOp{{opType={:?}, configEntry={:?}}}",
            self.op_type, self.config_entry
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_type_id_round_trip() {
        for op in [OpType::Set, OpType::Delete, OpType::Append, OpType::Subtract] {
            assert_eq!(OpType::for_id(op.id()), Some(op));
        }
    }

    #[test]
    fn op_type_ids_match_java() {
        assert_eq!(OpType::Set.id(), 0);
        assert_eq!(OpType::Delete.id(), 1);
        assert_eq!(OpType::Append.id(), 2);
        assert_eq!(OpType::Subtract.id(), 3);
    }

    #[test]
    fn op_type_for_unknown_id_is_none() {
        assert_eq!(OpType::for_id(7), None);
    }

    #[test]
    fn accessors_return_constructor_values() {
        let entry = ConfigEntry::new("retention.ms".to_string(), Some("1000".to_string()));
        let op = AlterConfigOp::new(entry.clone(), OpType::Set);
        assert_eq!(op.config_entry(), &entry);
        assert_eq!(op.op_type(), OpType::Set);
    }

    #[test]
    fn equality_compares_entry_and_op() {
        let entry = ConfigEntry::new("a".to_string(), Some("b".to_string()));
        let a = AlterConfigOp::new(entry.clone(), OpType::Set);
        let b = AlterConfigOp::new(entry.clone(), OpType::Set);
        let c = AlterConfigOp::new(entry, OpType::Delete);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
