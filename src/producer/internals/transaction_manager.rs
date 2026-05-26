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

//! Translation of `org.apache.kafka.clients.producer.internals.TransactionManager`.
//!
//! **Placeholder for a future transactions/idempotent-producer milestone.**
//! Per Phase 6 NOTES.md "Plug-in contract for future transactions",
//! this milestone (Milestone 1) does NOT translate the transactional /
//! idempotent producer paths. `enable.idempotence=true` and
//! `transactional.id` are rejected at config-validation time (Phase 7).
//!
//! Phase 6b–6e carry a `transaction_manager: Option<TransactionManager>`
//! field on [`ProducerBatch`](super::producer_batch::ProducerBatch),
//! `RecordAccumulator`, and `Sender` — always wired to `None` this
//! milestone. Each Java `if (transactionManager != null) { … }` branch
//! translates to `if let Some(tm) = &self.transaction_manager { … }`
//! with an empty body today (or `unreachable!()` for paths the compiler
//! cannot prove dead). Wiring the future translation = filling those
//! bodies; no constructor or call-site signature churn.
//!
//! This is **not** a `TODO` against CLAUDE.md rule 5 — the runtime
//! contract is unambiguous: `None` → non-tx path; reaching the
//! `Some(_)` arms is impossible because config validation rejects the
//! inputs that would set them.

#![allow(dead_code)] // Phase 6d / 6e wire `Option<TransactionManager>` constructor params.

/// Placeholder for the full `TransactionManager`. See module docs.
///
/// Constructed only by a future transactions milestone. No methods
/// today; [`ProducerBatch`](super::producer_batch::ProducerBatch),
/// `RecordAccumulator`, and `Sender` only check whether the
/// `Option<TransactionManager>` is `Some` / `None`.
pub(crate) struct TransactionManager;
