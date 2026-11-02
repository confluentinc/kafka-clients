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

//! Self-checking records: what an integrity writer sends, and how a reader
//! proves it got back exactly that.
//!
//! Every record carries an identity header ([`IDENTITY_HEADER`], always the
//! last header) naming the run, the writer, the writer's sequence number, the
//! producer incarnation, the transaction outcome the writer intends, the
//! timestamp it set, and a SHA-256 digest over all of that plus the record's
//! key, value and other headers. SHA-256 is deliberately not the CRC32C the
//! client computes for every batch, so a bug in the client's own checksum code
//! cannot hide a corruption.
//!
//! The key, value and other headers are generated from `(seed, writer, seq)` by
//! [`RecordGenerator`] (the contract of Apache Kafka's trogdor
//! `PayloadGenerator`: one position, always the same bytes), so nothing needs
//! to be stored. When a digest does not match, the reader regenerates the
//! expected record and reports which field and which byte differ.

use std::collections::HashMap;
use std::fmt::Write as _;

use aws_lc_rs::digest::{Context, SHA256};
use confluent_kafka::common::header::{Header, Headers, RecordHeader, RecordHeaders};
use confluent_kafka::consumer::ConsumerRecord;
use confluent_kafka::producer::{ProducerRecord, ProducerRecordOptionsBuilder};

/// The identity header's key. It is the last header of every record.
pub const IDENTITY_HEADER: &str = "x-integrity";

const MAGIC: u8 = b'I';
const VERSION: u8 = 1;
const DIGEST_LEN: usize = 32;
/// magic, version, run id, writer, incarnation, seq, intent, timestamp.
const FIELDS_LEN: usize = 1 + 1 + 8 + 4 + 4 + 8 + 1 + 8;
const IDENTITY_LEN: usize = FIELDS_LEN + DIGEST_LEN;

/// Header keys the generator draws from. Few keys, so a record with several
/// headers often repeats one (Kafka headers are an ordered list, duplicates
/// allowed); one is non-ASCII UTF-8.
const HEADER_KEYS: [&str; 4] = ["h0", "h1", "trace-id", "hé"];

/// SplitMix64: a tiny, fast, seedable generator. Deterministic across
/// platforms and independent of any crate's versioned algorithm, so a seed
/// printed by one run regenerates the same records in another.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`; `bound` must be positive. The modulo bias is
    /// irrelevant for test data.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    /// `true` with probability `pct` percent.
    pub fn percent(&mut self, pct: u8) -> bool {
        self.below(100) < u64::from(pct)
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below((hi - lo) as u64 + 1) as usize
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// The seed of one record's generator: `seed`, `writer` and `seq` mixed so
/// that neighbouring records share nothing.
fn record_seed(seed: u64, writer: u32, seq: u64) -> u64 {
    let mixed = seed
        .wrapping_add(u64::from(writer).wrapping_mul(0xD1B5_4A32_D192_ED03))
        .wrapping_add(seq.wrapping_mul(0x8CB9_2BA7_2F3D_8DD7));
    SplitMix64::new(mixed).next_u64()
}

/// The transaction outcome a writer intends for a record. A reader with
/// `isolation.level=read_committed` must never see a record marked
/// [`Abort`](TxnIntent::Abort) (the header form of the expected-status header
/// Apache Kafka's `TransactionsBounceTest` puts on its records).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnIntent {
    /// Not sent in a transaction.
    None,
    Commit,
    Abort,
}

impl TxnIntent {
    fn code(self) -> u8 {
        match self {
            TxnIntent::None => 0,
            TxnIntent::Commit => 1,
            TxnIntent::Abort => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(TxnIntent::None),
            1 => Some(TxnIntent::Commit),
            2 => Some(TxnIntent::Abort),
            _ => None,
        }
    }
}

/// One record of a run: the writer that produced it and its sequence number.
/// A copied record (consume-transform-produce) keeps the id of its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordId {
    pub writer: u32,
    pub seq: u64,
}

/// Everything the identity header says about a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub run_id: u64,
    pub id: RecordId,
    /// Which producer instance of the writer sent it (a writer restarts its
    /// producer to exercise producer-id initialisation).
    pub incarnation: u32,
    pub intent: TxnIntent,
    /// The `CreateTime` timestamp the writer set on the record.
    pub timestamp: i64,
}

impl Identity {
    fn fields(&self) -> [u8; FIELDS_LEN] {
        let mut out = [0u8; FIELDS_LEN];
        out[0] = MAGIC;
        out[1] = VERSION;
        out[2..10].copy_from_slice(&self.run_id.to_be_bytes());
        out[10..14].copy_from_slice(&self.id.writer.to_be_bytes());
        out[14..18].copy_from_slice(&self.incarnation.to_be_bytes());
        out[18..26].copy_from_slice(&self.id.seq.to_be_bytes());
        out[26] = self.intent.code();
        out[27..35].copy_from_slice(&self.timestamp.to_be_bytes());
        out
    }

    fn parse(bytes: &[u8]) -> Result<(Identity, &[u8]), String> {
        if bytes.len() != IDENTITY_LEN {
            return Err(format!("identity header is {} byte(s), expected {IDENTITY_LEN}", bytes.len()));
        }
        if bytes[0] != MAGIC || bytes[1] != VERSION {
            return Err(format!("identity header starts {:#04x} {:#04x}", bytes[0], bytes[1]));
        }
        let u64_at = |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        let u32_at = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
        let intent = TxnIntent::from_code(bytes[26]).ok_or_else(|| format!("unknown intent code {}", bytes[26]))?;
        let identity = Identity {
            run_id: u64_at(2),
            id: RecordId { writer: u32_at(10), seq: u64_at(18) },
            incarnation: u32_at(14),
            intent,
            timestamp: u64_at(27) as i64,
        };
        Ok((identity, &bytes[FIELDS_LEN..]))
    }
}

/// What a generated record holds besides its identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}

/// The shapes and sizes a writer's records are drawn from.
#[derive(Clone, Debug)]
pub struct PayloadProfile {
    /// Value sizes are uniform in `min_value..=max_value`, unless
    /// `value_sizes` is non-empty: then each record picks one of those.
    pub min_value: usize,
    pub max_value: usize,
    pub value_sizes: Vec<usize>,
    pub max_key: usize,
    pub max_headers: usize,
    pub max_header_value: usize,
    pub null_key_pct: u8,
    pub empty_key_pct: u8,
    pub null_value_pct: u8,
    pub empty_value_pct: u8,
    pub null_header_value_pct: u8,
    /// The share of each value that is a short repeated pattern rather than
    /// random bytes, so compression codecs have something to compress. 0 makes
    /// values incompressible.
    pub compressible_pct: u8,
}

impl PayloadProfile {
    /// The everyday mix: values up to 4 KiB, some null keys, a few tombstones
    /// and empty values, up to four headers.
    pub fn mixed() -> Self {
        Self {
            min_value: 0,
            max_value: 4096,
            value_sizes: Vec::new(),
            max_key: 32,
            max_headers: 4,
            max_header_value: 64,
            null_key_pct: 10,
            empty_key_pct: 2,
            null_value_pct: 2,
            empty_value_pct: 3,
            null_header_value_pct: 10,
            compressible_pct: 50,
        }
    }

    /// Mostly the unusual shapes: null and empty keys and values, many
    /// headers with repeated keys and null values.
    pub fn edge_shapes() -> Self {
        Self {
            min_value: 0,
            max_value: 64,
            value_sizes: Vec::new(),
            max_key: 8,
            max_headers: 6,
            max_header_value: 16,
            null_key_pct: 30,
            empty_key_pct: 20,
            null_value_pct: 25,
            empty_value_pct: 25,
            null_header_value_pct: 40,
            compressible_pct: 0,
        }
    }

    /// Values of exactly these sizes, never null, one optional header.
    pub fn sizes(value_sizes: Vec<usize>) -> Self {
        Self {
            min_value: 0,
            max_value: 0,
            value_sizes,
            max_key: 16,
            max_headers: 1,
            max_header_value: 16,
            null_key_pct: 0,
            empty_key_pct: 0,
            null_value_pct: 0,
            empty_value_pct: 0,
            null_header_value_pct: 0,
            compressible_pct: 0,
        }
    }

    /// Values uniform in `min..=max`, never null, incompressible.
    pub fn sized(min: usize, max: usize) -> Self {
        Self { min_value: min, max_value: max, ..Self::sizes(Vec::new()) }
    }
}

/// Builds the payload of record `(writer, seq)`; the same inputs always give
/// the same bytes.
#[derive(Clone, Debug)]
pub struct RecordGenerator {
    seed: u64,
    profile: PayloadProfile,
}

impl RecordGenerator {
    pub fn new(seed: u64, profile: PayloadProfile) -> Self {
        Self { seed, profile }
    }

    pub fn generate(&self, writer: u32, seq: u64) -> Payload {
        let p = &self.profile;
        let mut rng = SplitMix64::new(record_seed(self.seed, writer, seq));
        let key = if rng.percent(p.null_key_pct) {
            None
        } else if rng.percent(p.empty_key_pct) {
            Some(Vec::new())
        } else {
            let len = rng.range(1, p.max_key.max(1));
            Some(random_bytes(&mut rng, len, 0))
        };
        let value = if rng.percent(p.null_value_pct) {
            None
        } else if rng.percent(p.empty_value_pct) {
            Some(Vec::new())
        } else {
            let len = if p.value_sizes.is_empty() {
                rng.range(p.min_value, p.max_value)
            } else {
                p.value_sizes[rng.below(p.value_sizes.len() as u64) as usize]
            };
            Some(random_bytes(&mut rng, len, p.compressible_pct))
        };
        let header_count = rng.range(0, p.max_headers);
        let headers = (0..header_count)
            .map(|_| {
                let key = HEADER_KEYS[rng.below(HEADER_KEYS.len() as u64) as usize].to_string();
                let value = if rng.percent(p.null_header_value_pct) {
                    None
                } else {
                    let len = rng.range(0, p.max_header_value);
                    Some(random_bytes(&mut rng, len, 0))
                };
                RecordHeader::new(key, value)
            })
            .collect();
        Payload { key, value, headers }
    }
}

/// `len` bytes: the first `100 - compressible_pct` percent random, the rest a
/// 16-byte random pattern repeated.
fn random_bytes(rng: &mut SplitMix64, len: usize, compressible_pct: u8) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let random_len = len * usize::from(100 - compressible_pct.min(100)) / 100;
    let (random, pattern) = out.split_at_mut(random_len);
    rng.fill(random);
    let mut unit = [0u8; 16];
    rng.fill(&mut unit);
    for (i, b) in pattern.iter_mut().enumerate() {
        *b = unit[i % unit.len()];
    }
    out
}

fn update_bytes(ctx: &mut Context, bytes: Option<&[u8]>) {
    match bytes {
        None => ctx.update(&(-1i32).to_be_bytes()),
        Some(bytes) => {
            ctx.update(&(bytes.len() as u32).to_be_bytes());
            ctx.update(bytes);
        },
    }
}

/// SHA-256 over the identity fields, the key, the value and every header but
/// the identity header, in order; each byte string is length-prefixed and a
/// null is `-1`, so `None` and empty differ.
fn digest<'a>(
    fields: &[u8],
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> [u8; DIGEST_LEN] {
    let mut ctx = Context::new(&SHA256);
    ctx.update(fields);
    update_bytes(&mut ctx, key);
    update_bytes(&mut ctx, value);
    for (header_key, header_value) in headers {
        update_bytes(&mut ctx, Some(header_key.as_bytes()));
        update_bytes(&mut ctx, header_value);
    }
    let mut out = [0u8; DIGEST_LEN];
    out.copy_from_slice(ctx.finish().as_ref());
    out
}

/// The record `identity` names, with `payload` and the identity header last.
/// `partition` is `None` to let the partitioner choose.
pub fn build_record(
    topic: &str,
    partition: Option<i32>,
    identity: &Identity,
    payload: Payload,
) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    let fields = identity.fields();
    let sum = digest(
        &fields,
        payload.key.as_deref(),
        payload.value.as_deref(),
        payload.headers.iter().map(|h| (h.key(), h.value())),
    );
    let mut identity_value = Vec::with_capacity(IDENTITY_LEN);
    identity_value.extend_from_slice(&fields);
    identity_value.extend_from_slice(&sum);
    let mut headers = payload.headers;
    headers.push(RecordHeader::new(IDENTITY_HEADER.to_string(), Some(identity_value)));
    let options = ProducerRecordOptionsBuilder::new()
        .set_topic(topic.to_string())
        .set_partition(partition)
        .set_timestamp(Some(identity.timestamp))
        .set_key(payload.key)
        .set_value(payload.value)
        .set_headers(Some(RecordHeaders::with_header_iter(headers)))
        .build()
        .expect("every mandatory parameter is set");
    ProducerRecord::with_options(options).expect("valid record")
}

/// `record` re-sent unchanged to the same partition of `topic`: the copy a
/// consume-transform-produce loop writes. It keeps the source's identity.
pub fn copy_record(record: &ConsumerRecord<Vec<u8>, Vec<u8>>, topic: &str) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    let options = ProducerRecordOptionsBuilder::new()
        .set_topic(topic.to_string())
        .set_partition(Some(record.partition()))
        .set_timestamp(Some(record.timestamp()))
        .set_key(record.key().cloned())
        .set_value(record.value().cloned())
        .set_headers(Some(RecordHeaders::with_header_slice(record.headers().to_array())))
        .build()
        .expect("every mandatory parameter is set");
    ProducerRecord::with_options(options).expect("valid record")
}

/// Why a consumed record failed verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Defect {
    /// No identity header, or it is not the last header.
    MissingIdentity,
    /// The identity header is there but cannot be parsed, or appears twice.
    MalformedIdentity,
    /// A record of another run (a topic name collision).
    ForeignRecord,
    /// The key, value, headers or identity fields are not what was sent.
    DigestMismatch,
    /// The record's timestamp is not the one the writer set.
    TimestampMismatch,
}

#[derive(Clone, Debug)]
pub struct VerifyFailure {
    pub defect: Defect,
    pub detail: String,
}

/// Checks that `record` is exactly what its writer sent and returns its
/// identity. `generators` maps a writer id to its generator; it is only used
/// to describe a mismatch.
pub fn verify_record(
    record: &ConsumerRecord<Vec<u8>, Vec<u8>>,
    run_id: u64,
    generators: &HashMap<u32, RecordGenerator>,
) -> Result<Identity, VerifyFailure> {
    let fail = |defect, detail: String| VerifyFailure { defect, detail };
    let headers = record.headers().to_array();
    let Some((identity_header, other_headers)) = headers.split_last() else {
        return Err(fail(Defect::MissingIdentity, "the record has no headers".to_string()));
    };
    if identity_header.key() != IDENTITY_HEADER {
        return Err(fail(
            Defect::MissingIdentity,
            format!("the last header is {:?}, not {IDENTITY_HEADER:?}", identity_header.key()),
        ));
    }
    if other_headers.iter().any(|h| h.key() == IDENTITY_HEADER) {
        return Err(fail(Defect::MalformedIdentity, "the identity header appears twice".to_string()));
    }
    let Some(identity_bytes) = identity_header.value() else {
        return Err(fail(Defect::MalformedIdentity, "the identity header is null".to_string()));
    };
    let (identity, stored_sum) = Identity::parse(identity_bytes).map_err(|e| fail(Defect::MalformedIdentity, e))?;
    if identity.run_id != run_id {
        return Err(fail(
            Defect::ForeignRecord,
            format!("run id {:#x}, expected {run_id:#x}", identity.run_id),
        ));
    }
    let key = record.key().map(Vec::as_slice);
    let value = record.value().map(Vec::as_slice);
    let sum = digest(
        &identity.fields(),
        key,
        value,
        other_headers.iter().map(|h| (h.key(), h.value())),
    );
    if sum[..] != *stored_sum {
        let detail = match generators.get(&identity.id.writer) {
            Some(generator) => {
                let expected = generator.generate(identity.id.writer, identity.id.seq);
                describe_difference(&expected, key, value, other_headers)
            },
            None => format!("no generator registered for writer {}", identity.id.writer),
        };
        return Err(fail(Defect::DigestMismatch, format!("{:?}: {detail}", identity.id)));
    }
    if record.timestamp() != identity.timestamp {
        return Err(fail(
            Defect::TimestampMismatch,
            format!(
                "{:?}: timestamp {}, the writer set {}",
                identity.id,
                record.timestamp(),
                identity.timestamp
            ),
        ));
    }
    Ok(identity)
}

/// Which field of a record differs from `expected`, and where. Only called
/// once the digest has failed, to say why.
fn describe_difference(
    expected: &Payload,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[RecordHeader],
) -> String {
    let mut out = String::new();
    describe_bytes(&mut out, "key", expected.key.as_deref(), key);
    describe_bytes(&mut out, "value", expected.value.as_deref(), value);
    if expected.headers.len() != headers.len() {
        let _ = write!(out, "{} header(s), expected {}; ", headers.len(), expected.headers.len());
    } else {
        for (i, (want, got)) in expected.headers.iter().zip(headers).enumerate() {
            if want.key() != got.key() {
                let _ = write!(out, "header {i} key {:?}, expected {:?}; ", got.key(), want.key());
            }
            describe_bytes(&mut out, &format!("header {i} value"), want.value(), got.value());
        }
    }
    if out.is_empty() {
        "key, value and headers match the generator, so the identity fields were changed".to_string()
    } else {
        out
    }
}

fn describe_bytes(out: &mut String, what: &str, expected: Option<&[u8]>, actual: Option<&[u8]>) {
    match (expected, actual) {
        (None, None) => {},
        (None, Some(a)) => {
            let _ = write!(out, "{what} is {} byte(s), expected null; ", a.len());
        },
        (Some(_), None) => {
            let _ = write!(out, "{what} is null; ");
        },
        (Some(e), Some(a)) if e.len() != a.len() => {
            let _ = write!(out, "{what} is {} byte(s), expected {}; ", a.len(), e.len());
        },
        (Some(e), Some(a)) => {
            if let Some(i) = e.iter().zip(a).position(|(x, y)| x != y) {
                let _ = write!(out, "{what} byte {i} is {:#04x}, expected {:#04x}; ", a[i], e[i]);
            }
        },
    }
}
