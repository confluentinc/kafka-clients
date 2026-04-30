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

//! Translation of `org.apache.kafka.common.serialization.ListDeserializer`.

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::common::serialization::Deserializer;
use crate::common::serialization::list_serializer::{InnerKind, NULL_ENTRY_VALUE, SerializationStrategy};

/// Deserializer for `Vec<Option<T>>` matching Java's
/// `ListDeserializer<Inner> implements Deserializer<List<Inner>>`.
///
/// See [`super::ListSerializer`] for the wire format.
pub struct ListDeserializer<T, D>
where
    D: Deserializer<T> + 'static,
{
    inner: D,
    inner_kind: InnerKind,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T, D> ListDeserializer<T, D>
where
    D: Deserializer<T> + 'static,
{
    /// Construct from an inner deserializer and its kind tag.
    pub fn new(inner: D, inner_kind: InnerKind) -> Self {
        ListDeserializer { inner, inner_kind, _marker: std::marker::PhantomData }
    }

    /// Get the inner deserializer. Mirrors Java's `innerDeserializer()`.
    pub fn inner_deserializer(&self) -> &D {
        &self.inner
    }
}

impl<T, D> Deserializer<Vec<Option<T>>> for ListDeserializer<T, D>
where
    T: Send + Sync,
    D: Deserializer<T> + 'static,
{
    fn configure(&mut self, _configs: &HashMap<String, String>, _is_key: bool) -> Result<(), KafkaError> {
        // See `ListSerializer::configure` for why this is a no-op.
        Ok(())
    }

    fn deserialize(&self, topic: &str, data: Option<&[u8]>) -> Result<Option<Vec<Option<T>>>, KafkaError> {
        let Some(bytes) = data else { return Ok(None) };
        let mut cursor = Cursor::new(bytes);
        let strategy_byte = cursor.read_u8()?;
        let strategy = SerializationStrategy::from_ordinal(strategy_byte)?;

        let null_indices = if strategy == SerializationStrategy::ConstantSize {
            let count = cursor.read_i32()? as usize;
            let mut indices = Vec::with_capacity(count);
            for _ in 0..count {
                indices.push(cursor.read_i32()?);
            }
            Some(indices)
        } else {
            None
        };

        let size = cursor.read_i32()? as usize;
        let mut result: Vec<Option<T>> = Vec::with_capacity(size);

        let primitive_size = match self.inner_kind {
            InnerKind::FixedSize(s) => Some(s),
            InnerKind::VariableSize => None,
        };

        for i in 0..size {
            let entry_size = match strategy {
                SerializationStrategy::ConstantSize => primitive_size.unwrap_or(0),
                SerializationStrategy::VariableSize => cursor.read_i32()? as usize,
            };
            // Variable-size null marker.
            let is_null_var =
                strategy == SerializationStrategy::VariableSize && (entry_size as i32) == NULL_ENTRY_VALUE;
            // Constant-size null marker via index list.
            let is_null_const = null_indices.as_ref().is_some_and(|idxs| idxs.contains(&(i as i32)));
            if is_null_var || is_null_const {
                result.push(None);
                continue;
            }
            let payload = cursor.read_slice(entry_size)?;
            let value = self.inner.deserialize(topic, Some(payload))?;
            result.push(value);
        }

        Ok(Some(result))
    }
}

/// Tiny zero-allocation cursor over a byte slice. Replaces Java's
/// `DataInputStream(ByteArrayInputStream(...))` plumbing.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn ensure(&self, n: usize) -> Result<(), KafkaError> {
        if self.pos + n > self.buf.len() {
            return Err(KafkaError::Serialization(
                "End of the stream was reached prematurely".to_string(),
            ));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, KafkaError> {
        self.ensure(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_i32(&mut self) -> Result<i32, KafkaError> {
        self.ensure(4)?;
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_slice(&mut self, n: usize) -> Result<&'a [u8], KafkaError> {
        self.ensure(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}
