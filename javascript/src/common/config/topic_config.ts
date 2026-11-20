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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// Java's `org.apache.kafka.common.config.TopicConfig`;
// `cargo xtask check-generated` checks it is up to date.

/**
 * The constants of Java's `org.apache.kafka.common.config.TopicConfig`: every
 * `public static final` field, evaluated from the Java source, is a static
 * property of the same name; a deprecated one is left out. A `List` constant is
 * a frozen array and a `Set` constant a read-only set whose `add`, `delete` and
 * `clear` throw `UnsupportedOperationError`, both in Java's order.
 *
 * Java: `org.apache.kafka.common.config.TopicConfig`.
 */
export class TopicConfig {
  /**
   * This configuration controls the segment file size for the log. Retention
   * and cleaning is always done a file at a time so a larger segment size means
   * fewer files but less granular control over retention. The active segment is
   * rolled once it reaches this size.
   */
  static readonly SEGMENT_BYTES_CONFIG: string = 'segment.bytes';
  /**
   * The documentation of `TopicConfig.SEGMENT_BYTES_CONFIG`.
   */
  static readonly SEGMENT_BYTES_DOC: string = 'This configuration controls the segment file size for the log. Retention and cleaning is always done a file at a time so a larger segment size means fewer files but less granular control over retention. The active segment is rolled once it reaches this size.';
  /**
   * This configuration controls the period of time after which Kafka will force
   * the log to roll even if the segment file isn't full to ensure that
   * retention can delete or compact old data. This forces active segment
   * rolling by time, even if the active segment has not reached
   * `segment.bytes`. For compacted topics, `max.compaction.lag.ms` can trigger
   * active segment rolling sooner: the effective time-based roll threshold is
   * the smaller of `segment.ms` and `max.compaction.lag.ms`.
   */
  static readonly SEGMENT_MS_CONFIG: string = 'segment.ms';
  /**
   * The documentation of `TopicConfig.SEGMENT_MS_CONFIG`.
   */
  static readonly SEGMENT_MS_DOC: string = 'This configuration controls the period of time after which Kafka will force the log to roll even if the segment file isn\'t full to ensure that retention can delete or compact old data. This forces active segment rolling by time, even if the active segment has not reached <code>segment.bytes</code>. For compacted topics, <code>max.compaction.lag.ms</code> can trigger active segment rolling sooner: the effective time-based roll threshold is the smaller of <code>segment.ms</code> and <code>max.compaction.lag.ms</code>.';
  /**
   * The maximum random jitter subtracted from the scheduled segment roll time
   * to avoid thundering herds of segment rolling
   */
  static readonly SEGMENT_JITTER_MS_CONFIG: string = 'segment.jitter.ms';
  /**
   * The documentation of `TopicConfig.SEGMENT_JITTER_MS_CONFIG`.
   */
  static readonly SEGMENT_JITTER_MS_DOC: string = 'The maximum random jitter subtracted from the scheduled segment roll time to avoid thundering herds of segment rolling';
  /**
   * This configuration controls the size of the index that maps offsets to file
   * positions. We preallocate this index file and shrink it only after log
   * rolls. You generally should not need to change this setting.
   */
  static readonly SEGMENT_INDEX_BYTES_CONFIG: string = 'segment.index.bytes';
  /**
   * The documentation of `TopicConfig.SEGMENT_INDEX_BYTES_CONFIG`.
   */
  static readonly SEGMENT_INDEX_BYTES_DOC: string = 'This configuration controls the size of the index that maps offsets to file positions. We preallocate this index file and shrink it only after log rolls. You generally should not need to change this setting.';
  /**
   * This setting allows specifying an interval at which we will force an fsync
   * of data written to the log. For example if this was set to 1 we would fsync
   * after every message; if it were 5 we would fsync after every five messages.
   * In general we recommend you not set this and use replication for durability
   * and allow the operating system's background flush capabilities as it is
   * more efficient. This setting can be overridden on a per-topic basis (see
   * [the per-topic configuration
   * section](https://kafka.apache.org/documentation/#topicconfigs)).
   */
  static readonly FLUSH_MESSAGES_INTERVAL_CONFIG: string = 'flush.messages';
  /**
   * The documentation of `TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG`.
   */
  static readonly FLUSH_MESSAGES_INTERVAL_DOC: string = 'This setting allows specifying an interval at which we will force an fsync of data written to the log. For example if this was set to 1 we would fsync after every message; if it were 5 we would fsync after every five messages. In general we recommend you not set this and use replication for durability and allow the operating system\'s background flush capabilities as it is more efficient. This setting can be overridden on a per-topic basis (see <a href="https://kafka.apache.org/documentation/#topicconfigs">the per-topic configuration section</a>).';
  /**
   * This setting allows specifying a time interval at which we will force an
   * fsync of data written to the log. For example if this was set to 1000 we
   * would fsync after 1000 ms had passed. Note that this setting depends on the
   * broker-level configuration `log.flush.scheduler.interval.ms`, which
   * controls how frequently the flush check occurs. In general we recommend you
   * not set this and use replication for durability and allow the operating
   * system's background flush capabilities as it is more efficient.
   */
  static readonly FLUSH_MS_CONFIG: string = 'flush.ms';
  /**
   * The documentation of `TopicConfig.FLUSH_MS_CONFIG`.
   */
  static readonly FLUSH_MS_DOC: string = 'This setting allows specifying a time interval at which we will force an fsync of data written to the log. For example if this was set to 1000 we would fsync after 1000 ms had passed. Note that this setting depends on the broker-level configuration <code>log.flush.scheduler.interval.ms</code>, which controls how frequently the flush check occurs. In general we recommend you not set this and use replication for durability and allow the operating system\'s background flush capabilities as it is more efficient.';
  /**
   * This configuration controls the maximum size a partition (which consists of
   * log segments) can grow to before we will discard old log segments to free
   * up space if we are using the "delete" retention policy. By default there is
   * no size limit only a time limit. Since this limit is enforced at the
   * partition level, multiply it by the number of partitions to compute the
   * topic retention in bytes. Additionally, retention.bytes configuration
   * operates independently of "segment.ms" and "segment.bytes" configurations.
   * Moreover, it triggers the rolling of new segment if the retention.bytes is
   * configured to zero.
   */
  static readonly RETENTION_BYTES_CONFIG: string = 'retention.bytes';
  /**
   * The documentation of `TopicConfig.RETENTION_BYTES_CONFIG`.
   */
  static readonly RETENTION_BYTES_DOC: string = 'This configuration controls the maximum size a partition (which consists of log segments) can grow to before we will discard old log segments to free up space if we are using the "delete" retention policy. By default there is no size limit only a time limit. Since this limit is enforced at the partition level, multiply it by the number of partitions to compute the topic retention in bytes. Additionally, retention.bytes configuration operates independently of "segment.ms" and "segment.bytes" configurations. Moreover, it triggers the rolling of new segment if the retention.bytes is configured to zero.';
  /**
   * This configuration controls the maximum time we will retain a log before we
   * will discard old log segments to free up space if we are using the "delete"
   * retention policy. This represents an SLA on how soon consumers must read
   * their data. If set to -1, no time limit is applied. Additionally,
   * retention.ms configuration operates independently of "segment.ms" and
   * "segment.bytes" configurations. Moreover, it triggers the rolling of new
   * segment if the retention.ms condition is satisfied.
   */
  static readonly RETENTION_MS_CONFIG: string = 'retention.ms';
  /**
   * The documentation of `TopicConfig.RETENTION_MS_CONFIG`.
   */
  static readonly RETENTION_MS_DOC: string = 'This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the "delete" retention policy. This represents an SLA on how soon consumers must read their data. If set to -1, no time limit is applied. Additionally, retention.ms configuration operates independently of "segment.ms" and "segment.bytes" configurations. Moreover, it triggers the rolling of new segment if the retention.ms condition is satisfied.';
  /**
   * To enable tiered storage for a topic, set this configuration to true. To
   * disable tiered storage for a topic that has it enabled, set this
   * configuration to false. When disabling, you must also set
   * `remote.log.delete.on.disable` to true.
   */
  static readonly REMOTE_LOG_STORAGE_ENABLE_CONFIG: string = 'remote.storage.enable';
  /**
   * The documentation of `TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG`.
   */
  static readonly REMOTE_LOG_STORAGE_ENABLE_DOC: string = 'To enable tiered storage for a topic, set this configuration to true. To disable tiered storage for a topic that has it enabled, set this configuration to false. When disabling, you must also set <code>remote.log.delete.on.disable</code> to true.';
  /**
   * The number of milliseconds to keep the local log segment before it gets
   * deleted. Default value is -2, it represents `retention.ms` value is to be
   * used. The effective value should always be less than or equal to
   * `retention.ms` value.
   */
  static readonly LOCAL_LOG_RETENTION_MS_CONFIG: string = 'local.retention.ms';
  /**
   * The documentation of `TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG`.
   */
  static readonly LOCAL_LOG_RETENTION_MS_DOC: string = 'The number of milliseconds to keep the local log segment before it gets deleted. Default value is -2, it represents <code>retention.ms</code> value is to be used. The effective value should always be less than or equal to <code>retention.ms</code> value.';
  /**
   * The maximum size of local log segments that can grow for a partition before
   * it deletes the old segments. Default value is -2, it represents
   * `retention.bytes` value to be used. The effective value should always be
   * less than or equal to `retention.bytes` value.
   */
  static readonly LOCAL_LOG_RETENTION_BYTES_CONFIG: string = 'local.retention.bytes';
  /**
   * The documentation of `TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG`.
   */
  static readonly LOCAL_LOG_RETENTION_BYTES_DOC: string = 'The maximum size of local log segments that can grow for a partition before it deletes the old segments. Default value is -2, it represents <code>retention.bytes</code> value to be used. The effective value should always be less than or equal to <code>retention.bytes</code> value.';
  /**
   * Determines whether tiered data for a topic should become read only, and no
   * more data uploading on a topic. Once this config is set to true, the local
   * retention configuration (i.e. local.retention.ms/bytes) becomes irrelevant,
   * and all data expiration follows the topic-wide retention configuration(i.e.
   * retention.ms/bytes).
   */
  static readonly REMOTE_LOG_COPY_DISABLE_CONFIG: string = 'remote.log.copy.disable';
  /**
   * The documentation of `TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG`.
   */
  static readonly REMOTE_LOG_COPY_DISABLE_DOC: string = 'Determines whether tiered data for a topic should become read only, and no more data uploading on a topic. Once this config is set to true, the local retention configuration (i.e. local.retention.ms/bytes) becomes irrelevant, and all data expiration follows the topic-wide retention configuration(i.e. retention.ms/bytes).';
  /**
   * Controls one of the two upload eligibility checks (time and size) for
   * copying segments to remote storage. A non-active segment is upload-eligible
   * when either this time-based check or `remote.copy.lag.bytes` is satisfied.
   * When set to 0, uploads are immediately eligible regardless of lag checks.
   * When set to a positive value (ms), the segment is time-eligible once
   * elapsed time since its latest record reaches this value. When set to -1,
   * this value is derived from effective local retention time
   * (`local.retention.ms`). If that effective local retention time is unlimited
   * (-1), this time-based check is not applied. A positive value should not
   * exceed effective local retention time unless local retention is unlimited
   * (-1).
   */
  static readonly REMOTE_COPY_LAG_MS_CONFIG: string = 'remote.copy.lag.ms';
  /**
   * The documentation of `TopicConfig.REMOTE_COPY_LAG_MS_CONFIG`.
   */
  static readonly REMOTE_COPY_LAG_MS_DOC: string = 'Controls one of the two upload eligibility checks (time and size) for copying segments to remote storage. A non-active segment is upload-eligible when either this time-based check or <code>remote.copy.lag.bytes</code> is satisfied. When set to 0, uploads are immediately eligible regardless of lag checks. When set to a positive value (ms), the segment is time-eligible once elapsed time since its latest record reaches this value. When set to -1, this value is derived from effective local retention time (<code>local.retention.ms</code>). If that effective local retention time is unlimited (-1), this time-based check is not applied. A positive value should not exceed effective local retention time unless local retention is unlimited (-1).';
  /**
   * Controls one of the two upload eligibility checks (time and size) for
   * copying segments to remote storage. A non-active segment is upload-eligible
   * when either this size-based check or `remote.copy.lag.ms` is satisfied.
   * When set to 0, uploads are immediately eligible regardless of lag checks.
   * When set to a positive value (bytes), the segment is size-eligible once
   * bytes of newer local log data after that segment reaches this value. When
   * set to -1, this value is derived from effective local retention size
   * (`local.retention.bytes`). If that effective local retention size is
   * unlimited (-1), this size-based check is not applied. A positive value
   * should not exceed effective local retention size unless local retention is
   * unlimited (-1).
   */
  static readonly REMOTE_COPY_LAG_BYTES_CONFIG: string = 'remote.copy.lag.bytes';
  /**
   * The documentation of `TopicConfig.REMOTE_COPY_LAG_BYTES_CONFIG`.
   */
  static readonly REMOTE_COPY_LAG_BYTES_DOC: string = 'Controls one of the two upload eligibility checks (time and size) for copying segments to remote storage. A non-active segment is upload-eligible when either this size-based check or <code>remote.copy.lag.ms</code> is satisfied. When set to 0, uploads are immediately eligible regardless of lag checks. When set to a positive value (bytes), the segment is size-eligible once bytes of newer local log data after that segment reaches this value. When set to -1, this value is derived from effective local retention size (<code>local.retention.bytes</code>). If that effective local retention size is unlimited (-1), this size-based check is not applied. A positive value should not exceed effective local retention size unless local retention is unlimited (-1).';
  /**
   * Determines whether tiered data for a topic should be deleted after tiered
   * storage is disabled on a topic. This configuration should be enabled when
   * trying to set `remote.storage.enable` from true to false
   */
  static readonly REMOTE_LOG_DELETE_ON_DISABLE_CONFIG: string = 'remote.log.delete.on.disable';
  /**
   * The documentation of `TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG`.
   */
  static readonly REMOTE_LOG_DELETE_ON_DISABLE_DOC: string = 'Determines whether tiered data for a topic should be deleted after tiered storage is disabled on a topic. This configuration should be enabled when trying to set <code>remote.storage.enable</code> from true to false';
  /**
   * The largest record batch size allowed by Kafka (after compression if
   * compression is enabled).
   */
  static readonly MAX_MESSAGE_BYTES_CONFIG: string = 'max.message.bytes';
  /**
   * The documentation of `TopicConfig.MAX_MESSAGE_BYTES_CONFIG`.
   */
  static readonly MAX_MESSAGE_BYTES_DOC: string = 'The largest record batch size allowed by Kafka (after compression if compression is enabled).';
  /**
   * The maximum decompressed size, in bytes, of a single record the broker will
   * accept; larger records are rejected with an `InvalidRecordError`. It is
   * enforced where the broker decompresses records: produce validation of
   * compressed batches, log compaction, and offset lookups by timestamp or max
   * timestamp against the local log or remote storage. Unlike
   * `max.message.bytes`, which bounds the compressed batch on the wire, this
   * bounds the decompressed size of an individual record. Lowering it below the
   * size of already-stored records makes compaction and timestamp offset
   * lookups of affected partitions fail until it is raised again. The default
   * imposes no limit beyond the largest record the broker could allocate
   * anyway; set a smaller value to enforce a per-record cap.
   */
  static readonly MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG: string = 'max.decompressed.message.bytes';
  /**
   * The documentation of `TopicConfig.MAX_DECOMPRESSED_MESSAGE_BYTES_CONFIG`.
   */
  static readonly MAX_DECOMPRESSED_MESSAGE_BYTES_DOC: string = 'The maximum decompressed size, in bytes, of a single record the broker will accept; larger records are rejected with an <code>InvalidRecordException</code>. It is enforced where the broker decompresses records: produce validation of compressed batches, log compaction, and offset lookups by timestamp or max timestamp against the local log or remote storage. Unlike <code>max.message.bytes</code>, which bounds the compressed batch on the wire, this bounds the decompressed size of an individual record. Lowering it below the size of already-stored records makes compaction and timestamp offset lookups of affected partitions fail until it is raised again. The default imposes no limit beyond the largest record the broker could allocate anyway; set a smaller value to enforce a per-record cap.';
  /**
   * This setting controls how frequently Kafka adds entries to its offset index
   * and, conditionally, to its time index. The default setting ensures that we
   * index a message roughly every 4096 bytes. More frequent indexing allows
   * reads to jump closer to the exact position in the log but results in larger
   * index files. You probably don't need to change this.
   *
   * Note: the time index will be inserted only when the timestamp is greater
   * than the last indexed timestamp.
   */
  static readonly INDEX_INTERVAL_BYTES_CONFIG: string = 'index.interval.bytes';
  /**
   * The documentation of `TopicConfig.INDEX_INTERVAL_BYTES_CONFIG`.
   */
  static readonly INDEX_INTERVAL_BYTES_DOC: string = 'This setting controls how frequently Kafka adds entries to its offset index and, conditionally, to its time index. The default setting ensures that we index a message roughly every 4096 bytes. More frequent indexing allows reads to jump closer to the exact position in the log but results in larger index files. You probably don\'t need to change this.<p> Note: the time index will be inserted only when the timestamp is greater than the last indexed timestamp.</p>';
  /**
   * The time to wait before deleting a file from the filesystem
   */
  static readonly FILE_DELETE_DELAY_MS_CONFIG: string = 'file.delete.delay.ms';
  /**
   * The documentation of `TopicConfig.FILE_DELETE_DELAY_MS_CONFIG`.
   */
  static readonly FILE_DELETE_DELAY_MS_DOC: string = 'The time to wait before deleting a file from the filesystem';
  /**
   * The amount of time to retain delete tombstone markers for [log
   * compacted](https://kafka.apache.org/documentation/#compaction) topics. This
   * setting also gives a bound on the time in which a consumer must complete a
   * read if they begin from offset 0 to ensure that they get a valid snapshot
   * of the final stage (otherwise delete tombstones may be collected before
   * they complete their scan).
   */
  static readonly DELETE_RETENTION_MS_CONFIG: string = 'delete.retention.ms';
  /**
   * The documentation of `TopicConfig.DELETE_RETENTION_MS_CONFIG`.
   */
  static readonly DELETE_RETENTION_MS_DOC: string = 'The amount of time to retain delete tombstone markers for <a href="https://kafka.apache.org/documentation/#compaction">log compacted</a> topics. This setting also gives a bound on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete tombstones may be collected before they complete their scan).';
  /**
   * The minimum time a message will remain uncompacted in the log. Only
   * applicable for logs that are being compacted.
   */
  static readonly MIN_COMPACTION_LAG_MS_CONFIG: string = 'min.compaction.lag.ms';
  /**
   * The documentation of `TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG`.
   */
  static readonly MIN_COMPACTION_LAG_MS_DOC: string = 'The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted.';
  /**
   * The maximum time a message will remain ineligible for compaction in the
   * log. Only applicable for logs that are being compacted. Because the active
   * segment is never compacted, for compacted topics this value also drives
   * active segment rolling: the effective time-based roll threshold is the
   * smaller of `segment.ms` and `max.compaction.lag.ms`. Active segment rolling
   * moves records out of the active segment, after which
   * `max.compaction.lag.ms` makes them eligible for compaction even if
   * `min.cleanable.dirty.ratio` is not met. See [log
   * compaction](https://kafka.apache.org/documentation/#compaction).
   */
  static readonly MAX_COMPACTION_LAG_MS_CONFIG: string = 'max.compaction.lag.ms';
  /**
   * The documentation of `TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG`.
   */
  static readonly MAX_COMPACTION_LAG_MS_DOC: string = 'The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted. Because the active segment is never compacted, for compacted topics this value also drives active segment rolling: the effective time-based roll threshold is the smaller of <code>segment.ms</code> and <code>max.compaction.lag.ms</code>. Active segment rolling moves records out of the active segment, after which <code>max.compaction.lag.ms</code> makes them eligible for compaction even if <code>min.cleanable.dirty.ratio</code> is not met. See <a href="https://kafka.apache.org/documentation/#compaction">log compaction</a>.';
  /**
   * This configuration controls how frequently the log compactor will attempt
   * to clean the log (assuming [log
   * compaction](https://kafka.apache.org/documentation/#compaction) is
   * enabled). By default we will avoid cleaning a log where more than 50% of
   * the log has been compacted. This ratio bounds the maximum space wasted in
   * the log by duplicates (at 50% at most 50% of the log could be duplicates).
   * A higher ratio will mean fewer, more efficient cleanings but will mean more
   * wasted space in the log. If the max.compaction.lag.ms or the
   * min.compaction.lag.ms configurations are also specified, then the log
   * compactor considers the log to be eligible for compaction as soon as
   * either: (i) the dirty ratio threshold has been met and the log has had
   * dirty (uncompacted) records for at least the min.compaction.lag.ms
   * duration, or (ii) if the log has had dirty (uncompacted) records for at
   * most the max.compaction.lag.ms period.
   */
  static readonly MIN_CLEANABLE_DIRTY_RATIO_CONFIG: string = 'min.cleanable.dirty.ratio';
  /**
   * The documentation of `TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG`.
   */
  static readonly MIN_CLEANABLE_DIRTY_RATIO_DOC: string = 'This configuration controls how frequently the log compactor will attempt to clean the log (assuming <a href="https://kafka.apache.org/documentation/#compaction">log compaction</a> is enabled). By default we will avoid cleaning a log where more than 50% of the log has been compacted. This ratio bounds the maximum space wasted in the log by duplicates (at 50% at most 50% of the log could be duplicates). A higher ratio will mean fewer, more efficient cleanings but will mean more wasted space in the log. If the max.compaction.lag.ms or the min.compaction.lag.ms configurations are also specified, then the log compactor considers the log to be eligible for compaction as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) records for at least the min.compaction.lag.ms duration, or (ii) if the log has had dirty (uncompacted) records for at most the max.compaction.lag.ms period.';
  /**
   * This config designates the retention policy to use on log segments. The
   * "delete" policy (which is the default) will discard old segments when their
   * retention time or size limit has been reached. The "compact" policy will
   * enable [log
   * compaction](https://kafka.apache.org/documentation/#compaction), which
   * retains the latest value for each key. It is also possible to specify both
   * policies in a comma-separated list (e.g. "delete,compact"). In this case,
   * old segments will be discarded per the retention time and size
   * configuration, while retained segments will be compacted. An empty list
   * means infinite retention - no cleanup policies will be applied and log
   * segments will be retained indefinitely. Note that with remote storage
   * enabled, local retention limits (log.local.retention.ms and
   * log.local.retention.bytes) are still applied to local segments.
   */
  static readonly CLEANUP_POLICY_CONFIG: string = 'cleanup.policy';
  /**
   * Java's `TopicConfig.CLEANUP_POLICY_COMPACT`.
   */
  static readonly CLEANUP_POLICY_COMPACT: string = 'compact';
  /**
   * Java's `TopicConfig.CLEANUP_POLICY_DELETE`.
   */
  static readonly CLEANUP_POLICY_DELETE: string = 'delete';
  /**
   * The documentation of `TopicConfig.CLEANUP_POLICY_CONFIG`.
   */
  static readonly CLEANUP_POLICY_DOC: string = 'This config designates the retention policy to use on log segments. The "delete" policy (which is the default) will discard old segments when their retention time or size limit has been reached. The "compact" policy will enable <a href="https://kafka.apache.org/documentation/#compaction">log compaction</a>, which retains the latest value for each key. It is also possible to specify both policies in a comma-separated list (e.g. "delete,compact"). In this case, old segments will be discarded per the retention time and size configuration, while retained segments will be compacted. An empty list means infinite retention - no cleanup policies will be applied and log segments will be retained indefinitely. Note that with remote storage enabled, local retention limits (log.local.retention.ms and log.local.retention.bytes) are still applied to local segments.';
  /**
   * Indicates whether to enable replicas not in the ISR set to be elected as
   * leader as a last resort, even though doing so may result in data loss.
   *
   * Note: In KRaft mode, when enabling this config dynamically, it needs to
   * wait for the unclean leader electionthread to trigger election periodically
   * (default is 5 minutes). Please run `kafka-leader-election.sh` with
   * `unclean` option to trigger the unclean leader election immediately if
   * needed.
   */
  static readonly UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG: string = 'unclean.leader.election.enable';
  /**
   * The documentation of `TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG`.
   */
  static readonly UNCLEAN_LEADER_ELECTION_ENABLE_DOC: string = 'Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss.<p>Note: In KRaft mode, when enabling this config dynamically, it needs to wait for the unclean leader electionthread to trigger election periodically (default is 5 minutes). Please run <code>kafka-leader-election.sh</code> with <code>unclean</code> option to trigger the unclean leader election immediately if needed.</p>';
  /**
   * Specifies the *minimum* number of in-sync replicas (including the leader)
   * required for a write to succeed when a producer sets `acks` to "all" (or
   * "-1"). In the `acks=all` case, every in-sync replica must acknowledge a
   * write for it to be considered successful. E.g., if a topic has
   * `replication.factor` of 3 and the ISR set includes all three replicas, then
   * all three replicas must acknowledge an `acks=all` write for it to succeed,
   * even if `min.insync.replicas` happens to be less than 3. If `acks=all` and
   * the current ISR set contains fewer than `min.insync.replicas` members, then
   * the producer will raise an exception (either `NotEnoughReplicas` or
   * `NotEnoughReplicasAfterAppend`).
   *
   * Regardless of the `acks` setting, the messages will not be visible to the
   * consumers until they are replicated to all in-sync replicas and the
   * `min.insync.replicas` condition is met.
   *
   * When used together, `min.insync.replicas` and `acks` allow you to enforce
   * greater durability guarantees. A typical scenario would be to create a
   * topic with a replication factor of 3, set `min.insync.replicas` to 2, and
   * produce with `acks` of "all". This ensures that a majority of replicas must
   * persist a write before it's considered successful by the producer and it's
   * visible to consumers.
   *
   * Note that when the Eligible Leader Replicas feature is enabled, the
   * semantics of this config changes. Please refer to [the ELR
   * section](https://kafka.apache.org/documentation/#eligible_leader_replicas)
   * for more info.
   */
  static readonly MIN_IN_SYNC_REPLICAS_CONFIG: string = 'min.insync.replicas';
  /**
   * The documentation of `TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG`.
   */
  static readonly MIN_IN_SYNC_REPLICAS_DOC: string = 'Specifies the <i>minimum</i> number of in-sync replicas (including the leader) required for a write to succeed when a producer sets <code>acks</code> to "all" (or "-1"). In the <code>acks=all</code> case, every in-sync replica must acknowledge a write for it to be considered successful. E.g., if a topic has <code>replication.factor</code> of 3 and the ISR set includes all three replicas, then all three replicas must acknowledge an <code>acks=all</code> write for it to succeed, even if <code>min.insync.replicas</code> happens to be less than 3. If <code>acks=all</code> and the current ISR set contains fewer than <code>min.insync.replicas</code> members, then the producer will raise an exception (either <code>NotEnoughReplicas</code> or <code>NotEnoughReplicasAfterAppend</code>).<br> Regardless of the <code>acks</code> setting, the messages will not be visible to the consumers until they are replicated to all in-sync replicas and the <code>min.insync.replicas</code> condition is met.<br> When used together, <code>min.insync.replicas</code> and <code>acks</code> allow you to enforce greater durability guarantees. A typical scenario would be to create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and produce with <code>acks</code> of "all". This ensures that a majority of replicas must persist a write before it\'s considered successful by the producer and it\'s visible to consumers.<p>Note that when the Eligible Leader Replicas feature is enabled, the semantics of this config changes. Please refer to <a href="https://kafka.apache.org/documentation/#eligible_leader_replicas">the ELR section</a> for more info.</p>';
  /**
   * The compression type for all data generated by the producer. The default is
   * none (i.e. no compression). Valid values are `none`, `gzip`, `snappy`,
   * `lz4`, or `zstd`. Compression is of full batches of data, so the efficacy
   * of batching will also impact the compression ratio (more batching means
   * better compression).
   */
  static readonly COMPRESSION_TYPE_CONFIG: string = 'compression.type';
  /**
   * The documentation of `TopicConfig.COMPRESSION_TYPE_CONFIG`.
   */
  static readonly COMPRESSION_TYPE_DOC: string = 'Specify the final compression type for a given topic. This configuration accepts the standard compression codecs (\'gzip\', \'snappy\', \'lz4\', \'zstd\'). It additionally accepts \'uncompressed\' which is equivalent to no compression; and \'producer\' which means retain the original compression codec set by the producer.';
  /**
   * The compression level to use if compression.type is set to `gzip`.
   */
  static readonly COMPRESSION_GZIP_LEVEL_CONFIG: string = 'compression.gzip.level';
  /**
   * The documentation of `TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG`.
   */
  static readonly COMPRESSION_GZIP_LEVEL_DOC: string = 'The compression level to use if compression.type is set to <code>gzip</code>.';
  /**
   * The compression level to use if compression.type is set to `lz4`.
   */
  static readonly COMPRESSION_LZ4_LEVEL_CONFIG: string = 'compression.lz4.level';
  /**
   * The documentation of `TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG`.
   */
  static readonly COMPRESSION_LZ4_LEVEL_DOC: string = 'The compression level to use if compression.type is set to <code>lz4</code>.';
  /**
   * The compression level to use if compression.type is set to `zstd`.
   */
  static readonly COMPRESSION_ZSTD_LEVEL_CONFIG: string = 'compression.zstd.level';
  /**
   * The documentation of `TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG`.
   */
  static readonly COMPRESSION_ZSTD_LEVEL_DOC: string = 'The compression level to use if compression.type is set to <code>zstd</code>.';
  /**
   * True if we should preallocate the file on disk when creating a new log
   * segment.
   */
  static readonly PREALLOCATE_CONFIG: string = 'preallocate';
  /**
   * The documentation of `TopicConfig.PREALLOCATE_CONFIG`.
   */
  static readonly PREALLOCATE_DOC: string = 'True if we should preallocate the file on disk when creating a new log segment.';
  /**
   * Define whether the timestamp in the message is message create time or log
   * append time.
   */
  static readonly MESSAGE_TIMESTAMP_TYPE_CONFIG: string = 'message.timestamp.type';
  /**
   * The documentation of `TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG`.
   */
  static readonly MESSAGE_TIMESTAMP_TYPE_DOC: string = 'Define whether the timestamp in the message is message create time or log append time.';
  /**
   * This configuration sets the allowable timestamp difference between the
   * broker's timestamp and the message timestamp. The message timestamp can be
   * earlier than or equal to the broker's timestamp, with the maximum allowable
   * difference determined by the value set in this configuration. If
   * message.timestamp.type=CreateTime, the message will be rejected if the
   * difference in timestamps exceeds this specified threshold. This
   * configuration is ignored if message.timestamp.type=LogAppendTime.
   */
  static readonly MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG: string = 'message.timestamp.before.max.ms';
  /**
   * The documentation of `TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG`.
   */
  static readonly MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC: string = 'This configuration sets the allowable timestamp difference between the broker\'s timestamp and the message timestamp. The message timestamp can be earlier than or equal to the broker\'s timestamp, with the maximum allowable difference determined by the value set in this configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.';
  /**
   * This configuration sets the allowable timestamp difference between the
   * message timestamp and the broker's timestamp. The message timestamp can be
   * later than or equal to the broker's timestamp, with the maximum allowable
   * difference determined by the value set in this configuration. If
   * message.timestamp.type=CreateTime, the message will be rejected if the
   * difference in timestamps exceeds this specified threshold. This
   * configuration is ignored if message.timestamp.type=LogAppendTime.
   */
  static readonly MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG: string = 'message.timestamp.after.max.ms';
  /**
   * The documentation of `TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG`.
   */
  static readonly MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC: string = 'This configuration sets the allowable timestamp difference between the message timestamp and the broker\'s timestamp. The message timestamp can be later than or equal to the broker\'s timestamp, with the maximum allowable difference determined by the value set in this configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.';
  /**
   * Enable this topic to be used as a dead-letter queue for share groups. When
   * set to `true`, share groups can write undeliverable records to this topic.
   * When set to `false` (the default), attempts to use this topic as a DLQ will
   * be rejected.
   */
  static readonly ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG: string = 'errors.deadletterqueue.group.enable';
  /**
   * The documentation of
   * `TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG`.
   */
  static readonly ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_DOC: string = 'Enable this topic to be used as a dead-letter queue for share groups. When set to <code>true</code>, share groups can write undeliverable records to this topic. When set to <code>false</code> (the default), attempts to use this topic as a DLQ will be rejected.';

  /**
   * Java's default constructor. The class holds only its constants, which are static.
   */
  constructor() {}
}
