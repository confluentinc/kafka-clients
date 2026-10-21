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

//! Client quota alteration type.
//!
//! Corresponds to `org.apache.kafka.common.quota.ClientQuotaAlteration`.

use crate::common::quota::ClientQuotaEntity;

/// A single quota alteration operation.
///
/// Corresponds to `org.apache.kafka.common.quota.ClientQuotaAlteration.Op`.
///
/// Equality and hashing follow Java's `Objects.equals(value, that.value)` and
/// `Objects.hash(key, value)`, which compare and hash a `Double` through
/// `Double.doubleToLongBits`: every `NaN` is equal to every other, and `0.0`
/// differs from `-0.0`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration$Op")]
pub struct Op {
    key: String,
    value: Option<f64>,
}

/// Java's `Double.doubleToLongBits`: the bits of `value`, with every `NaN`
/// folded into the canonical one.
fn double_to_long_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

impl PartialEq for Op {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value.map(double_to_long_bits) == other.value.map(double_to_long_bits)
    }
}

impl Eq for Op {}

impl std::hash::Hash for Op {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.value.map(double_to_long_bits).hash(state);
    }
}

impl Op {
    /// Constructs an alteration op.
    ///
    /// If `value` is `Some`, the existing value is updated; if `value` is
    /// `None`, the existing value is cleared (quota removal). This mirrors
    /// Java's nullable `Double value`, where `null` signals removal.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration$Op#Op")]
    pub fn new(key: impl Into<String>, value: Option<f64>) -> Self {
        Self { key: key.into(), value }
    }

    /// Returns the quota type to alter.
    ///
    /// Mirrors `Op.key()`.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration$Op#key")]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the value to set, or `None` to clear the existing value.
    ///
    /// Mirrors `Op.value()`, where `null` means the existing value is cleared.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration$Op#value")]
    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value {
            Some(v) => write!(f, "ClientQuotaAlteration.Op(key={}, value={v})", self.key),
            None => write!(f, "ClientQuotaAlteration.Op(key={}, value=null)", self.key),
        }
    }
}

/// Describes a configuration alteration to be made to a client quota entity.
///
/// Corresponds to `org.apache.kafka.common.quota.ClientQuotaAlteration`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration")]
pub struct ClientQuotaAlteration {
    entity: ClientQuotaEntity,
    ops: Vec<Op>,
}

impl ClientQuotaAlteration {
    /// Constructs an alteration for the given entity.
    ///
    /// Mirrors `ClientQuotaAlteration(ClientQuotaEntity, Collection<Op>)`.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration#ClientQuotaAlteration")]
    pub fn new(entity: ClientQuotaEntity, ops: Vec<Op>) -> Self {
        Self { entity, ops }
    }

    /// Returns the entity whose config will be modified.
    ///
    /// Mirrors `ClientQuotaAlteration.entity()`.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration#entity")]
    pub fn entity(&self) -> &ClientQuotaEntity {
        &self.entity
    }

    /// Returns the alterations to perform.
    ///
    /// Mirrors `ClientQuotaAlteration.ops()`.
    #[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration#ops")]
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }
}

impl std::fmt::Display for ClientQuotaAlteration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientQuotaAlteration(entity={}, ops={:?})", self.entity, self.ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn entity() -> ClientQuotaEntity {
        let mut m = HashMap::new();
        m.insert(ClientQuotaEntity::USER.to_string(), Some("u1".to_string()));
        ClientQuotaEntity::new(m)
    }

    // New test, no Java original: the four `common/quota` classes have no
    // dedicated Java test files (upstream gap, see DoD #3 note in PLAN.md).
    #[test]
    fn op_set_vs_remove() {
        let set = Op::new("consumer_byte_rate", Some(10000.0));
        assert_eq!(set.key(), "consumer_byte_rate");
        assert_eq!(set.value(), Some(10000.0));

        // A `None` value signals removal (Java `null`).
        let remove = Op::new("producer_byte_rate", None);
        assert_eq!(remove.value(), None);
    }

    // New test, no Java original.
    #[test]
    fn op_equality() {
        assert_eq!(Op::new("k", Some(1.0)), Op::new("k", Some(1.0)));
        assert_ne!(Op::new("k", Some(1.0)), Op::new("k", None));
    }

    // New test, no Java original: pins `Double.equals` on the value, under
    // which every NaN is equal and the two zeros differ, and hashing agrees.
    #[test]
    fn op_compares_its_value_as_java_double_equals() {
        use std::hash::BuildHasher;
        let other_nan = f64::from_bits(f64::NAN.to_bits() ^ 1);
        assert!(other_nan.is_nan());
        let a = Op::new("k", Some(f64::NAN));
        let b = Op::new("k", Some(other_nan));
        assert_eq!(a, b);
        let state = std::collections::hash_map::RandomState::new();
        assert_eq!(state.hash_one(&a), state.hash_one(&b));
        assert_ne!(Op::new("k", Some(0.0)), Op::new("k", Some(-0.0)));
    }

    // New test, no Java original.
    #[test]
    fn alteration_accessors() {
        let alt = ClientQuotaAlteration::new(entity(), vec![Op::new("consumer_byte_rate", Some(1.0))]);
        assert_eq!(alt.entity(), &entity());
        assert_eq!(alt.ops().len(), 1);
    }
}
