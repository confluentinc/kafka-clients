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

//! Translation of `org.apache.kafka.common.serialization.ListSerializer`.
//!
//! See `phase3b_list_serde_gap.md` (in actor-memory) for the cross-process
//! Java-FQN limitation: Java's `ListSerializer` embeds the inner serializer's
//! class name in the wire form. Rust doesn't have class-FQN reflection; the
//! caller passes a [`InnerKind`] tag at construction so the Rust round-trip
//! stays correct. Bytes produced by Rust will not parse in Java without the
//! Java-side FQN prefix.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::serialization::Serializer;

/// Sentinel value used by Java to mark a `null` list entry in
/// `VARIABLE_SIZE` strategy. Mirrors `Serdes.ListSerde.NULL_ENTRY_VALUE`.
pub const NULL_ENTRY_VALUE: i32 = -1;

/// Whether each entry in the list has a fixed wire size or a per-entry
/// length prefix. Mirrors Java's `Serdes.ListSerde.SerializationStrategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationStrategy {
    /// Each entry occupies the same number of bytes; null indices are
    /// recorded out-of-band in a `null_index_list`.
    ConstantSize = 0,
    /// Each entry is preceded by a 4-byte length prefix; `NULL_ENTRY_VALUE`
    /// marks a `null` entry.
    VariableSize = 1,
}

impl SerializationStrategy {
    /// Returns the wire byte for this strategy. Matches Java's
    /// `enum.ordinal()`.
    pub fn ordinal(self) -> u8 {
        self as u8
    }

    /// Look up a strategy by ordinal. Returns
    /// `KafkaError::Serialization` for an unknown ordinal.
    pub fn from_ordinal(o: u8) -> Result<Self, KafkaError> {
        match o {
            0 => Ok(SerializationStrategy::ConstantSize),
            1 => Ok(SerializationStrategy::VariableSize),
            _ => Err(KafkaError::Serialization(
                "Invalid serialization strategy flag value".to_string(),
            )),
        }
    }
}

/// Tag identifying the inner serializer kind so we can pick the
/// `ConstantSize` vs `VariableSize` strategy without runtime reflection.
///
/// Java looks up `inner.getClass()` against a static list of fixed-length
/// serializer classes (`Short/Integer/Float/Long/Double/UUID`). Rust does
/// not have class-FQN reflection, so the caller passes the tag at
/// construction time. See `phase3b_list_serde_gap.md` for context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InnerKind {
    /// Serializer for a fixed-size primitive — entries are exactly
    /// `primitive_size` bytes each.
    FixedSize(usize),
    /// Serializer for a variable-length type — each entry is preceded by
    /// a 4-byte length prefix on the wire.
    VariableSize,
}

impl InnerKind {
    fn strategy(self) -> SerializationStrategy {
        match self {
            InnerKind::FixedSize(_) => SerializationStrategy::ConstantSize,
            InnerKind::VariableSize => SerializationStrategy::VariableSize,
        }
    }
}

/// Serializer for `Vec<Option<T>>` matching Java's
/// `ListSerializer<Inner> implements Serializer<List<Inner>>`.
///
/// Each element is serialized via the inner [`Serializer`]; `null` (Rust
/// `None`) entries are encoded per the active [`SerializationStrategy`].
///
/// Wire layout:
///
/// ```text
/// strategy_byte (1)
/// if ConstantSize:
///   null_index_count (i32 BE)
///   null_index_count * (i32 BE) // indices
/// list_size (i32 BE)
/// list_size * { entry_payload }
///   where entry_payload =
///     if VariableSize: i32 BE length, then payload bytes (length == NULL_ENTRY_VALUE for null)
///     if ConstantSize: payload bytes (omitted for null indices)
/// ```
pub struct ListSerializer<T, S>
where
    S: Serializer<T> + 'static,
{
    inner: S,
    inner_kind: InnerKind,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T, S> ListSerializer<T, S>
where
    S: Serializer<T> + 'static,
{
    /// Construct from an inner serializer and its kind tag.
    pub fn new(inner: S, inner_kind: InnerKind) -> Self {
        ListSerializer { inner, inner_kind, _marker: std::marker::PhantomData }
    }

    /// Get the inner serializer. Mirrors Java's `getInnerSerializer()`.
    pub fn inner_serializer(&self) -> &S {
        &self.inner
    }

    /// Strategy used for this serializer.
    pub fn strategy(&self) -> SerializationStrategy {
        self.inner_kind.strategy()
    }
}

impl<T, S> Serializer<Vec<Option<T>>> for ListSerializer<T, S>
where
    T: Send + Sync,
    S: Serializer<T> + 'static,
{
    fn configure(&mut self, _configs: &HashMap<String, String>, _is_key: bool) -> Result<(), KafkaError> {
        // Java's `configure` requires a runtime-class-loaded inner serde.
        // Rust always constructs `ListSerializer` with a concrete inner
        // serializer via `new`, so the Java "no-arg ctor + configure" path
        // does not apply. See `phase3b_list_serde_gap.md`.
        Ok(())
    }

    fn serialize(&self, topic: &str, data: Option<&Vec<Option<T>>>) -> Result<Option<Vec<u8>>, KafkaError> {
        let Some(list) = data else { return Ok(None) };
        let mut out = Vec::new();
        self.serialize_into(topic, list, &mut out)?;
        Ok(Some(out))
    }

    fn serialize_to(&self, topic: &str, data: Option<&Vec<Option<T>>>, out: &mut Vec<u8>) -> Result<bool, KafkaError> {
        let Some(list) = data else { return Ok(false) };
        self.serialize_into(topic, list, out)?;
        Ok(true)
    }
}

impl<T, S> ListSerializer<T, S>
where
    T: Send + Sync,
    S: Serializer<T> + 'static,
{
    fn serialize_into(&self, topic: &str, list: &[Option<T>], out: &mut Vec<u8>) -> Result<(), KafkaError> {
        let strategy = self.inner_kind.strategy();
        out.push(strategy.ordinal());
        if strategy == SerializationStrategy::ConstantSize {
            // Build the null-index list first so we can write it out before
            // the entries.
            let null_indices: Vec<i32> = list
                .iter()
                .enumerate()
                .filter_map(|(i, e)| if e.is_none() { Some(i as i32) } else { None })
                .collect();
            out.extend_from_slice(&(null_indices.len() as i32).to_be_bytes());
            for idx in null_indices {
                out.extend_from_slice(&idx.to_be_bytes());
            }
        }
        let size = list.len() as i32;
        out.extend_from_slice(&size.to_be_bytes());
        for entry in list {
            match entry {
                None => {
                    if strategy == SerializationStrategy::VariableSize {
                        out.extend_from_slice(&NULL_ENTRY_VALUE.to_be_bytes());
                    }
                    // ConstantSize: nulls are skipped (already recorded in null_index_list)
                },
                Some(v) => {
                    if strategy == SerializationStrategy::VariableSize {
                        // Serialize into a temp buffer to learn the length, then write the length-prefixed payload.
                        // (We cannot easily avoid this allocation without a multi-pass design; the cost is one
                        // allocation per non-null entry on the variable path. Fixed-size lists — the producer hot
                        // path candidate — avoid this.)
                        let bytes = self.inner.serialize(topic, Some(v))?.unwrap_or_default();
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(&bytes);
                    } else {
                        // ConstantSize: write directly into out.
                        let written = self.inner.serialize_to(topic, Some(v), out)?;
                        debug_assert!(written, "non-null entry must produce bytes");
                    }
                },
            }
        }
        Ok(())
    }
}
