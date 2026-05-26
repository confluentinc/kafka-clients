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

//! Translation of `org.apache.kafka.common.config.TopicConfig` (constants only).
//!
//! These keys describe per-topic broker configuration. The producer doesn't
//! consume these directly, but `ProducerConfig` references some of them
//! (e.g. `compression.type`) and the AdminClient (out of scope for this
//! milestone) will consume the full set later.

pub const SEGMENT_BYTES_CONFIG: &str = "segment.bytes";
pub const SEGMENT_MS_CONFIG: &str = "segment.ms";
pub const SEGMENT_JITTER_MS_CONFIG: &str = "segment.jitter.ms";
pub const SEGMENT_INDEX_BYTES_CONFIG: &str = "segment.index.bytes";
pub const FLUSH_MESSAGES_INTERVAL_CONFIG: &str = "flush.messages";
pub const FLUSH_MS_CONFIG: &str = "flush.ms";
pub const RETENTION_BYTES_CONFIG: &str = "retention.bytes";
pub const RETENTION_MS_CONFIG: &str = "retention.ms";

pub const REMOTE_LOG_STORAGE_ENABLE_CONFIG: &str = "remote.storage.enable";
pub const LOCAL_LOG_RETENTION_MS_CONFIG: &str = "local.retention.ms";
pub const LOCAL_LOG_RETENTION_BYTES_CONFIG: &str = "local.retention.bytes";
pub const REMOTE_LOG_COPY_DISABLE_CONFIG: &str = "remote.log.copy.disable";
pub const REMOTE_LOG_DELETE_ON_DISABLE_CONFIG: &str = "remote.log.delete.on.disable";

pub const MAX_MESSAGE_BYTES_CONFIG: &str = "max.message.bytes";
pub const INDEX_INTERVAL_BYTES_CONFIG: &str = "index.interval.bytes";
pub const FILE_DELETE_DELAY_MS_CONFIG: &str = "file.delete.delay.ms";
pub const DELETE_RETENTION_MS_CONFIG: &str = "delete.retention.ms";
pub const MIN_COMPACTION_LAG_MS_CONFIG: &str = "min.compaction.lag.ms";
pub const MAX_COMPACTION_LAG_MS_CONFIG: &str = "max.compaction.lag.ms";
pub const MIN_CLEANABLE_DIRTY_RATIO_CONFIG: &str = "min.cleanable.dirty.ratio";

pub const CLEANUP_POLICY_CONFIG: &str = "cleanup.policy";
pub const CLEANUP_POLICY_COMPACT: &str = "compact";
pub const CLEANUP_POLICY_DELETE: &str = "delete";

pub const UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG: &str = "unclean.leader.election.enable";
pub const MIN_IN_SYNC_REPLICAS_CONFIG: &str = "min.insync.replicas";

pub const COMPRESSION_TYPE_CONFIG: &str = "compression.type";
pub const COMPRESSION_GZIP_LEVEL_CONFIG: &str = "compression.gzip.level";
pub const COMPRESSION_LZ4_LEVEL_CONFIG: &str = "compression.lz4.level";
pub const COMPRESSION_ZSTD_LEVEL_CONFIG: &str = "compression.zstd.level";

pub const PREALLOCATE_CONFIG: &str = "preallocate";
pub const MESSAGE_TIMESTAMP_TYPE_CONFIG: &str = "message.timestamp.type";
pub const MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG: &str = "message.timestamp.before.max.ms";
pub const MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG: &str = "message.timestamp.after.max.ms";
pub const MESSAGE_DOWNCONVERSION_ENABLE_CONFIG: &str = "message.downconversion.enable";
