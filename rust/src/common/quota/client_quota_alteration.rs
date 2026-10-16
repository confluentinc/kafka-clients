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
/// Equality is Java's `equals`, which compares the boxed `Double` values with
/// `Double.equals`: a NaN value equals a NaN value, and `0.0` differs from
/// `-0.0`, unlike `f64`'s `==`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.quota.ClientQuotaAlteration$Op")]
pub struct Op {
    key: String,
    value: Option<f64>,
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

/// Java's `Op.equals` (`ClientQuotaAlteration.java:60-66`) compares the values
/// with `Objects.equals`, that is `Double.equals`, which compares
/// `Double.doubleToLongBits`: every NaN is one value, and the two zeros are
/// two.
impl PartialEq for Op {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value.map(double_to_long_bits) == other.value.map(double_to_long_bits)
    }
}

/// Reflexive, since NaN equals NaN under [`PartialEq`] above.
impl Eq for Op {}

/// Java's `Double.doubleToLongBits`: the IEEE 754 bit pattern of `value`, with
/// every NaN collapsed to the one canonical NaN (`0x7ff8000000000000`).
fn double_to_long_bits(value: f64) -> u64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
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
#[derive(Clone, Debug, PartialEq)]
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

    /// New test, no Java original: `Op.equals` follows `Double.equals`
    /// (`ClientQuotaAlteration.java:60-66`), so a NaN value equals a NaN value,
    /// whatever its bit pattern, and `0.0` differs from `-0.0`, both unlike
    /// `f64`'s `==`.
    #[test]
    fn op_equality_follows_java_double_equals() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        assert!(other_nan.is_nan() && other_nan.to_bits() != f64::NAN.to_bits());
        assert_eq!(Op::new("k", Some(f64::NAN)), Op::new("k", Some(f64::NAN)));
        assert_eq!(Op::new("k", Some(f64::NAN)), Op::new("k", Some(other_nan)));
        assert_ne!(Op::new("k", Some(0.0)), Op::new("k", Some(-0.0)));
        assert_ne!(Op::new("k", Some(f64::NAN)), Op::new("k", None));
        assert_ne!(Op::new("k", Some(1.0)), Op::new("other", Some(1.0)));
    }

    // New test, no Java original.
    #[test]
    fn alteration_accessors() {
        let alt = ClientQuotaAlteration::new(entity(), vec![Op::new("consumer_byte_rate", Some(1.0))]);
        assert_eq!(alt.entity(), &entity());
        assert_eq!(alt.ops().len(), 1);
    }
}
