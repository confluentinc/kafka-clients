# AK 4.3.1 → 4.4.0-rc4: commits touching clients/src (190)

Source: `git -C kafka log --no-merges --right-only --cherry-pick 4.3.1...4.4.0-rc4 -- clients/src`. Classified from subject + touched files (planning scan, 2026-10-07); Phase 13 confirms each row.

rel: P = port, T = test-only port, D = doc/log only, N = no-op for Rust (Java refactor/idiom/already applied), O = out of scope, R = reverted within range, A = already in 4.3.1 (ported in M13)

| sha | component | rel | tag | date | subject | main / test lines |
|---|---|---|---|---|---|---|
| 06bedcf369 | Admin | D |  | 2026-04-08 | MINOR: Fix log statement formatting in KafkaAdminClient (#21990) | 1+/1- / +/- |
| 112686cfa8 | Admin | D |  | 2026-04-08 | MINOR: fix typo in KafkaAdminClient to align the meaning (#21988) | 3+/3- / +/- |
| 74ef4dd97a | Admin | D |  | 2026-07-29 | MINOR: Fix duplicated closing brace in `{@link}` Javadoc tags in admin Result classes (#22986) | 4+/4- / +/- |
| f4afec0cb2 | Admin | D |  | 2026-04-08 | MINOR: Fix double negative typos in Javadoc and exception messages (#21977) | 2+/2- / +/- |
| 1a443b2d23 | Admin | N |  | 2026-08-06 | MINOR: Split KafkaAdminClientTest into per-domain test classes (#22339) | +/- / 11455+/10966- |
| 9a8fd60fa2 | Admin | N |  | 2026-04-01 | KAFKA-19932: adding handling of OOM and avoiding wrapped as timeout (#21117) | 6+/0- / 26+/0- |
| b56323874b | Admin | O | raft | 2026-05-04 | MINOR: Add tests for RaftVoterEndpoint (#22195) | 1+/1- / 128+/0- |
| 3b849ff2bd | Admin | P |  | 2026-07-16 | KAFKA-19663 Unify kafka-broker-api-versions tool to cluster-tool and support bootstrap controllers (#20598) | 70+/3- / +/- |
| 58f63f448e | Admin | P |  | 2026-07-29 | MINOR: Complete nodeApiVersions future when describeFeatures fails (#22978) | 2+/1- / 12+/10- |
| c274a7348f | Admin | P |  | 2026-08-03 | KAFKA-20395: Support unregistering controllers (#22191) | 388+/2- / 151+/72- |
| cb2f143b0d | Admin | P |  | 2026-07-22 | KAFKA-20673 Skip stale-leader lookup retry while the AdminClient is closing (follow-up) (#22529) | 9+/0- / +/- |
| 01ba1b2d4d | Broker | O |  | 2026-07-21 | MINOR: Fix RandomAccessFile leak in FileRecords.openChannel (#22878) | 7+/2- / +/- |
| 3bfcd4cfbf | Broker | O |  | 2026-06-30 | KAFKA-20633: Update the default value of remote copy lag bytes (#22394) | 14+/12- / +/- |
| 6be48c6e54 | Broker | O |  | 2026-05-04 | KAFKA-20441: Fix handling of cordoned log dirs (#22070) | 6+/9- / +/- |
| 70dfc4236c | Broker | O |  | 2026-03-24 | KAFKA-19562 Replace hand-written AbortedTxn with generated protocol (#21577) | 36+/0- / +/- |
| 72b21c706e | Broker | O |  | 2026-09-15 | KAFKA-20979: Fix another gap in index metadata sync logic (#23316) | 2+/15- / 2+/0- |
| 86328c25ee | Broker | O |  | 2026-04-06 | KAFKA-19566 Deprecate ClientQuotaCallback#updateClusterMetadata (#21958) | 6+/2- / +/- |
| 9c7fad3397 | Broker | O |  | 2026-06-17 | Require expected issuer and audience for the SASL/OAUTHBEARER broker validator | 89+/12- / 306+/8- |
| 9c943129eb | Broker | O |  | 2026-08-07 | KAFKA-20816: Add support for CIDR into Authorizer.authorizeByResourceType (#22883) | 66+/1- / 112+/0- |
| ae03a1e456 | Broker | O |  | 2026-05-10 | MINOR: Fix outdated Javadoc link in AuthorizableRequestContext (#22230) | 6+/3- / +/- |
| b69c07c816 | Broker | O |  | 2026-09-13 | Bound decompressed record size (#23447) | 157+/34- / 310+/22- |
| cd5ce52240 | Broker | O |  | 2026-06-19 | KAFKA-20664: Clarify docs on max compaction lag, segment.ms, and segment.bytes for active segment rolling (#22489) | 14+/3- / +/- |
| ed9898d7d3 | Broker | O |  | 2026-07-30 | KAFKA-20831: Cache SASL principal per authentication (#22880) | 10+/2- / 47+/0- |
| edcada2a48 | Broker | O |  | 2026-05-22 | KAFKA-19893: Reduce tiered storage redundancy with delayed upload (KIP-1241)  (#20913) | 16+/0- / +/- |
| 0071092b36 | Classic | O |  | 2026-07-09 | KAFKA-16630: Fix flaky classic consumer poll test (#22787) | +/- / 21+/6- |
| 01f50af8b8 | Classic | O |  | 2026-07-27 | KAFKA-20253: Trigger rejoin on heartbeat thread AuthenticationException (#22073) | 1+/0- / 61+/0- |
| 673107f49d | Classic | O |  | 2026-07-08 | KAFKA-20778: Avoid unneeded rebalance on classic consumer when replica unavailable (#22768) | 66+/20- / 141+/13- |
| 93ee413aca | Classic | O |  | 2026-08-10 | KAFKA-20232: Fix WakeupException in awaitMetadataUpdate() (#21592) | 13+/2- / 15+/0- |
| 9ea8c3931c | Classic | O |  | 2026-08-11 | MINOR: update the javadocs in ConsumerNetworkClient (#23122) | 8+/0- / +/- |
| 05185c1ef8 | Common | D |  | 2026-03-24 | KAFKA-20341 Clarify return semantics of assignment(), subscription(), and metrics() in consumer Javadoc (#21849) | 38+/4- / +/- |
| 150f09f784 | Common | D |  | 2026-04-23 | KAFKA-20519 Fix broken links in configuration option docs (#22127) | 7+/7- / +/- |
| 26e8df61e3 | Common | D |  | 2026-08-03 | MINOR: Fix formatBytes to return "0 B" for zero bytes (#22921) | 3+/0- / 1+/0- |
| 7c7fc5fac2 | Common | D |  | 2026-06-30 | MINOR: Clarify SerializationException Javadoc (#22700) | 1+/1- / +/- |
| 8c15611221 | Common | D |  | 2026-07-07 | MINOR: Fix various typos in javadoc, logs and messages (#22565) | 5+/5- / +/- |
| da3b5e78ed | Common | D |  | 2026-07-14 | MINOR: Document connections.max.idle.ms dependency on max.poll.interval.ms for Classic consumer (#22752) | 29+/2- / +/- |
| 0a367aa1ec | Common | N | move | 2026-04-21 | KAFKA-20297 Move OperatingSystem, Java, Exit... into internal (#22093) | 11+/12- / 8+/10- |
| 10805c9782 | Common | N | move | 2026-05-02 | KAFKA-20297 Move SecurityUtils, ConfigUtils, LogContext, AppInfoParser into internal (#22110) | 113+/114- / 94+/93- |
| 162b3a1bcf | Common | N |  | 2026-05-02 | MINOR: Fix various typos and formatting issues across multiple modules (#22177) | +/- / 6+/6- |
| 2299a06e31 | Common | N | move | 2026-04-15 | KAFKA-20297: Move AbstractIterator, CircularIterator, CloseableIterator... into internal (#22052) | 25+/25- / 7+/7- |
| 282eef9d05 | Common | N |  | 2026-04-20 | MINOR: Add DCL to improve performance (#22098) | 16+/11- / +/- |
| 2ad4c7a2ef | Common | N |  | 2026-06-20 | MINOR: avoid redundant HashMap lookups and simplify Uuid conversions (#22122) | 2+/8- / +/- |
| 3b6c8385ca | Common | N |  | 2026-05-05 | MINOR: Move client test utilities to test fixtures and resolve shadow jar conflicts (#22201) | +/- / 7+/7185- |
| 4048aa19fa | Common | N |  | 2026-06-06 | MINOR: Replace Collections factory methods with Java 11+ equivalents in clients (#22060) | 72+/130- / +/- |
| 496ed66512 | Common | N |  | 2026-06-08 | MINOR: Remove unused maybeEmitMetric from MetricsEmitter (#22497) | 0+/9- / +/- |
| 4ae78a36ac | Common | N | move | 2026-04-18 | KAFKA-20297 Move ByteBufferUnmapper, BufferSupplier, ChunkedBytesStream into internal (#22081) | 26+/26- / 24+/20- |
| 5eff4ceb5f | Common | N |  | 2026-03-30 | MINOR: Replace Collections factory methods with Java 11+ equivalents in group-coordinator and part of clients (#21876) | 88+/113- / +/- |
| 6c3b63b825 | Common | N |  | 2026-06-26 | MINOR: Reduce allocations constructing RecordHeaders (#22641) | 7+/2- / +/- |
| 6cd450e677 | Common | N | move | 2026-07-13 | MINOR: Move ByteBufferInputStream and ByteBufferOutputStream to internal package (#22814) | 19+/19- / 16+/19- |
| a1c155ee28 | Common | N |  | 2026-03-30 | KAFKA-20297 Cleanup `org.apache.kafka.common.utils.CollectionUtils` (#21818) | 34+/119- / 19+/96- |
| b347f4bd2e | Common | N | move | 2026-03-31 | KAFKA-20297 move ImplicitLinkedHashCollection, ImplicitLinkedHashMultiCollection, and ByteUtils from utils to internals (#21856) | 87+/116- / 25+/21- |
| b4c977544f | Common | N | move | 2026-04-15 | KAFKA-20297 move KafkaThread, ThreadUtils, ExponentialBackoff and ExponentialBackoffManager to internal (#22049) | 24+/20- / 3+/3- |
| cab3cea505 | Common | N |  | 2026-06-11 | MINOR: fix suppress warnings in ConsumerRecordsTest and KafkaProducerTest (#22522) | +/- / 3+/1- |
| cac4d1f5c5 | Common | N |  | 2026-07-03 | MINOR: Remove clients test-fixtures shadow JAR rewire hack (#22699) | 107+/11- / 293+/484- |
| d85d257c99 | Common | N | move | 2026-07-09 | KAFKA-20297 Move ChildFirstClassLoader, ProducerIdAndEpoch into internal (#22765) | 11+/11- / 5+/5- |
| e7a568863e | Common | N | move | 2026-04-16 | KAFKA-20297 move Crc32C Checksums and Sanitizer from utils to utils.internals (#22072) | 7+/6- / 3+/3- |
| fac6838045 | Common | N | kip1265 | 2026-07-02 | KAFKA-20032: KIP-1265 Automatic detection of internal API usage (#21337) | 1841+/16- / +/- |
| c0579dbfb4 | Common | O | untranslated | 2026-07-24 | KAFKA-20769: ListDeserializer can silently deserialize a corrupted entry when the input is truncated mid-entry (#22755) | 7+/1- / 34+/0- |
| d995cd14cc | Common | O | untranslated | 2026-06-24 | KAFKA-20700: Resolve symlinks in AllowedPaths before validation (#22631) | 14+/5- / 22+/4- |
| 46ad599a6e | Common | P | metrics | 2026-06-01 | MINOR: Add toString() to KafkaMetric for readable logging (#22129) | 20+/0- / 67+/7- |
| 474798afbe | Common | P |  | 2026-04-30 | KAFKA-20072: Don't generate IDs with hyphens (#21313) | 3+/3- / 1+/1- |
| 8ba75d04cb | Common | P | errors | 2026-07-30 | MINOR: Add generic fenced state error message (#22994) | 1+/1- / +/- |
| 8b6d31f00c | Common | T |  | 2026-04-08 | KAFKA-20297 move testIncrement and testIncrementUpperBoundary to ByteUtilsTest (#21913) | +/- / 166+/78- |
| 44bafc60e7 | Consumer | A | in431 | 2026-04-28 | KAFKA-20426 Using both group.id and assign() causes a busy loop in AsyncKafkaConsumer (#22018) | 7+/2- / 82+/0- |
| 4b9eddc132 | Consumer | A | in431 | 2026-04-11 | KAFKA-20428: Fix unsubscribe failure with assignment updates (#22011) | 50+/4- / 115+/6- |
| 45c4bdc48a | Consumer | D |  | 2026-04-14 | MINOR: Clarify docs for consumer leave options & rebalances (#22058) | 14+/7- / +/- |
| 67850b0b68 | Consumer | D |  | 2026-04-02 | KAFKA-20136: Add javadoc to public APIs in org.apache.kafka.clients.consumer module (#21429) | 427+/18- / +/- |
| 6ce2682d7b | Consumer | D |  | 2026-07-28 | KAFKA-20539 Prevent Hot Data Loss on Partition Expansion for Latest Policy (#22784) | 16+/2- / +/- |
| 6f4b47d61c | Consumer | D |  | 2026-04-09 | KAFKA-20089 clients - update javadocs --  while topic being created in background (#21959) | 10+/2- / +/- |
| a39e714c6f | Consumer | D |  | 2026-04-14 | KAFKA-20449: Downgrade "not updating high watermark" log from WARN to DEBUG (#22055) | 2+/2- / +/- |
| cacc16797b | Consumer | D |  | 2026-08-03 | MINOR: Fix malformed Javadoc link tags in clients and streams (#23049) | 5+/5- / +/- |
| 096cc96fee | Consumer | N | interrupt | 2026-06-17 | KAFKA-20522: Refine test coverage for consumer close-on-interrupt best-effort behavior. (#22138) | +/- / 114+/0- |
| 455cbdfea2 | Consumer | N |  | 2026-08-02 | MINOR: Remove unused SubscriptionState.hasPartitionsNeedingValidation (#23019) | 0+/10- / +/- |
| 6291384496 | Consumer | N | deprecated | 2026-06-08 | KAFKA-20660: Mark deprecated ConsumerRecords constructor with forRemoval=true (#22469) | 1+/1- / +/- |
| 7c2b51eea0 | Consumer | N |  | 2026-06-24 | MINOR: Fix raw type warnings in AbstractHeartbeatRequestManagerTest (#22637) | +/- / 21+/7- |
| 8cfc1cc97f | Consumer | N | deprecated | 2026-06-09 | KAFKA-20660: Add detection of use of legacy ConsumerRecords(Map) constructor (#22468) | 34+/1- / 65+/0- |
| c09c9cd0ff | Consumer | N |  | 2026-06-09 | MINOR: Fix minor typos in CompletableEventReaperTest and LeaderEpochFileCache (#22427) | +/- / 3+/3- |
| c39e2af92c | Consumer | N |  | 2026-04-07 | KAFKA-20408 Remove ConsumerMetrics wrapper (#21974) | 13+/55- / +/- |
| 02c0ce9707 | Consumer | P | metrics | 2026-07-02 | KAFKA-20750: Add divide-by-zero error check in KafkaConsumerMetrics/KafkaShareConsumerMetrics. (#22718) | 4+/2- / 39+/0- |
| 20e952c783 | Consumer | P |  | 2026-07-28 | KAFKA-20780: Fix to clear completed inflight poll on empty fetch response and time left  (#22979) | 12+/7- / 168+/6- |
| 2768948823 | Consumer | P |  | 2026-05-15 | KAFKA-20575: Add onPartitionsLost support for MockConsume (#22273) | 28+/0- / 105+/0- |
| 28de22de34 | Consumer | P |  | 2026-07-16 | KAFKA-20253 Heartbeat CPU spin fix for Consumer protocol (#22836) | 28+/1- / 60+/0- |
| 56410c311b | Consumer | P |  | 2026-07-13 | KAFKA-20761: Add client logs for consumer group configs defined on the broker (#22749) | 8+/1- / 65+/3- |
| 5d03ccff57 | Consumer | P |  | 2026-04-04 | KAFKA-15529 Fix race condition between isConsumed and position updates in AsyncKafkaConsumer fetch path (#21476) | 13+/2- / 32+/0- |
| 642e0a5db0 | Consumer | P | kip909 | 2026-08-20 | KAFKA-20854 A more obvious busy loop due to KIP-909 (#23014) | 125+/57- / 164+/8- |
| 65ffe10e3b | Consumer | P |  | 2026-08-13 | KAFKA-20187: Fix for AsyncKafkaConsumer not clearing endOffsetRequested flag (#23123) | 87+/43- / 8+/12- |
| 6a6b536fbc | Consumer | P |  | 2026-06-10 | KAFKA-20681: Consolidate heartbeat success handling for consumer and share (#22524) | 95+/107- / +/- |
| 8c0ca4ae35 | Consumer | P |  | 2026-07-07 | KAFKA-18812: Improve consumer API errors upon background thread failure (#22035) | 26+/1- / 46+/0- |
| 9a28bd23ad | Consumer | P | metrics | 2026-03-19 | KAFKA-19542: Consumer.close() does not remove all added sensors from Metrics (#21038) | 305+/123- / 188+/7- |
| b8429b93a7 | Consumer | P |  | 2026-06-11 | MINOR: remove no-op call when HB fails and member already unsubscribed (#22530) | 0+/1- / 1+/1- |
| cd44c5de0f | Consumer | P | kip909 | 2026-09-03 | KAFKA-20970 Another busy loop happens if auto.commit.interval.ms is less than bootstrap.resolve.timeout.ms (#23227) | 30+/4- / 108+/0- |
| d0e0ec478c | Consumer | P |  | 2026-04-27 | KAFKA-20312: Handle null leader during OffsetFetcher regroup safely (#21760) | 35+/8- / 119+/0- |
| d12e95da90 | Consumer | P | kip909 | 2026-09-10 | KAFKA-21010 Another busy loop happens if the member is in JOINING state and bootstrap DNS resolution is not finished (#23348) | 29+/18- / 169+/10- |
| e320142b7c | Consumer | P |  | 2026-06-04 | KAFKA-20145: Avoid redundant partial HB acks from network-thread reconcile (#22278) | 9+/3- / 44+/1- |
| f7dbf0bf3b | Consumer | P |  | 2026-05-21 | KAFKA-20570: Catch RuntimeException in ConsumerProtocol deserialization in group coordinator (#22264) | 8+/8- / 85+/0- |
| fe88647935 | Consumer | P |  | 2026-08-06 | KAFKA-20765: Fix OffsetFetch stale-epoch retry spinning in dedup loop (#22747) | 22+/9- / 60+/0- |
| 05033d348c | Consumer | R |  | 2026-06-10 | KAFKA-20385 [1/N]: Add RebalanceConsumer interface and callback impl (#22270) | 867+/32- / 925+/0- |
| 79a29b0675 | Consumer | R |  | 2026-08-12 | MINOR: Revert KAFKA-20385 and KAFKA-20684 on 4.4 (#23133) | 219+/1159- / 223+/1235- |
| a98049e989 | Consumer | R |  | 2026-08-05 | KAFKA-20385 [2/N]: Add setRebalanceListener into *Consumer (#22271) | 383+/253- / 518+/408- |
| bc756ed514 | Consumer | R |  | 2026-08-10 | KAFKA-20684 [1/N]: Remove test SubscriptionState subscribe overload (#23102) | 0+/31- / 53+/76- |
| dacff9c85a | Consumer | R |  | 2026-08-10 | KAFKA-20385 [3/N]: Null-check for listener regn on AsyncKafkaConsumer (#23101) | 2+/1- / +/- |
| 0fd8327920 | Consumer | T |  | 2026-03-31 | KAFKA-20315: Add tests for single in-flight poll events in Async and Share consumers (#21800) | +/- / 80+/0- |
| 159d696005 | Consumer | T |  | 2026-05-20 | KAFKA-20565 Fix flaky test testAutoCommitSentBeforePositionUpdate timeout (#22256) | +/- / 2+/1- |
| 16e976ac8e | Consumer | T |  | 2026-06-02 | KAFKA-20423: Fix flakiness of testWakeupWithFetchDataAvailable (#22364) | +/- / 24+/3- |
| 1a46339e90 | Consumer | T |  | 2026-06-21 | MINOR: Fix assertion and grammar issues in KafkaConsumerTest (#22613) | +/- / 3+/3- |
| 36aab4fddd | Consumer | T |  | 2026-06-15 | MINOR: Clarify consumer behaviour on partition pause & re-assignments  (#22538) | 14+/1- / 116+/0- |
| 40e9fcd742 | Consumer | T |  | 2026-04-08 | KAFKA-20119 Clarify that `Consumer#unsubscribe` does not trigger auto-commit (#21424) | 4+/0- / 94+/0- |
| 624ca392ef | Consumer | T |  | 2026-07-02 | MINOR: extend fetch session test to avoid false positive (#22734) | +/- / 9+/1- |
| 7c010c7583 | Consumer | T |  | 2026-06-30 | KAFKA-20733: Fix fetchResponseWithUnexpectedPartitionIsIgnored passing for wrong reason with CONSUMER protocol (#22651) | +/- / 35+/2- |
| a5137f7c38 | Consumer | T |  | 2026-07-04 | KAFKA-20759 testUnsubscribeDoesNotCommitOffsetsEvenWithAutoCommitEnabled hangs in closing (#22733) | +/- / 4+/0- |
| c75e10d229 | Consumer | T |  | 2026-04-28 | KAFKA-20424 : clients: Update KafkaConsumerTest comments,tests with relevant protocol (#22144) | +/- / 20+/39- |
| e7b0cb7908 | Consumer | T |  | 2026-06-10 | KAFKA-18862: Consolidate shared heartbeat request manager tests (#22425) | +/- / 366+/564- |
| 3e32d9aa3c | Network | D |  | 2026-04-22 | MINOR: add missing javadoc to Metadata.java (#22106) | 4+/4- / +/- |
| 525b278288 | Network | D |  | 2026-07-06 | KAFKA-19117 Client Throttling Log messages should be of log level - WARN (Java client) (#19456) | 1+/1- / +/- |
| 85b0e80272 | Network | D |  | 2026-06-19 | KAFKA-20713: Lower level for client log on self-healing session not found (#22617) | 10+/2- / +/- |
| 7f1ec08519 | Network | N |  | 2026-06-19 | MINOR: add missing log enabled check  (#22629) | 10+/6- / +/- |
| 0720ba1141 | Network | P | kip909 | 2026-08-06 | MINOR: KIP-909 follow-ups (port validation, dead code, doc, test mock) (#23070) | 7+/9- / 2+/1- |
| 0df48ff5c5 | Network | P | kip909 | 2026-08-18 | KAFKA-20939: Client applications broken by DNS resolution failure behaviour change (#23188) | 69+/18- / 69+/0- |
| 0ef4a4c80e | Network | P | kip1242 | 2026-06-08 | KAFKA-20246: Add clusterId and nodeId to ApiVersionsRequest (2/N) (#22187) | 85+/13- / 70+/16- |
| 123ee9e45d | Network | P |  | 2026-04-10 | KAFKA-20393: Fix stickyNode using stale IP when broker address changes (#21983) | 14+/0- / 97+/0- |
| 507d01da42 | Network | P | kip909 | 2026-07-29 | KAFKA-14648 Do not fail clients if bootstrap servers is not immediately resolvable (#21080) | 668+/158- / 351+/113- |
| 7be741d08b | Network | P | kip1242 | 2026-07-06 | KAFKA-20246: Add clusterId and nodeId to ApiVersionsRequest (3/N) (#22512) | 55+/25- / 91+/30- |
| 87943b2ff8 | Network | P | kip909 | 2026-08-21 | KAFKA-20939 Mark "bootstrap.resolve.timeout.ms" as an experimental feature (#23214) | 12+/1- / +/- |
| ede01b871e | Network | P | kip1242 | 2026-04-02 | KAFKA-20246: Detection and handling of misrouted connections [1/N] (#21766) | 36+/3- / 2+/2- |
| fc18c47efd | Producer | D | rack | 2026-06-24 | KAFKA-19193: add Javadoc for rack-aware params in BuiltInPartitioner (#22432) | 3+/0- / +/- |
| 36a69b49c5 | Producer | N |  | 2026-07-27 | KAFKA-16937 Inline Time#waitObject to ProducerMetadata#awaitUpdate (#22083) | 31+/36- / 77+/128- |
| 9f15f3c540 | Producer | N |  | 2026-07-24 | KAFKA-20804: Reduce lock contention in ProducerMetadata#add (#22810) | 26+/11- / 28+/0- |
| 165d7ec933 | Producer | P | rack | 2026-06-23 | KAFKA-19193: throw ConfigException if rack is empty in rack-aware mode (#22433) | 2+/1- / +/- |
| 1aed299b3e | Producer | P | chunked | 2026-07-30 | KAFKA-20578: Initial producer incremental allocation for uncompressed data (#22654) | 1407+/120- / 1468+/3- |
| 6208dfc014 | Producer | P | kip1319 | 2026-06-09 | KAFKA-20444: [11/N] Refresh metadata before TxnOffsetCommit (KIP-1319) (#22460) | 53+/3- / 159+/0- |
| 7f5861817d | Producer | P | kip1319 | 2026-05-10 | KAFKA-20444: [6/N] Handle GROUP_ID_NOT_FOUND and STALE_MEMBER_EPOCH in TransactionManager (KIP-1319) (#22239) | 7+/1- / 28+/0- |
| 83976543fe | Producer | P | kip1319 | 2026-06-02 | KAFKA-20444: [10/N] Wire topic IDs through TransactionManager (KIP-1319) (#22443) | 84+/27- / 147+/22- |
| 88b48794ea | Producer | P | rack | 2026-06-23 | KAFKA-19193: trace-log rack-specific load stats in rack-aware mode (#22434) | 13+/2- / +/- |
| 89f3888c87 | Producer | P | kip1319 | 2026-05-27 | KAFKA-20444: [9/N] Preserve topic-level structure in TxnOffsetCommit response handling (KIP-1319) (#22265) | 53+/60- / 0+/7- |
| a3f17327de | Producer | P | rack | 2026-04-22 | KAFKA-19193 Support rack-aware partitioning for Kafka producer (#19850) | 141+/28- / 236+/27- |
| b9945c8e84 | Producer | P | kip1319 | 2026-06-10 | KAFKA-20444: [12/12] Mark TxnOffsetCommit v6 as stable (KIP-1319) (#22523) | 3+/5- / 11+/23- |
| 31e698de7f | Producer | R |  | 2026-09-04 | KAFKA-19414: Revert 2PC public API changes for 4.4 (#23356) | 14+/185- / 0+/429- |
| 930ebc5608 | Producer | T | kip1319 | 2026-08-06 | MINOR: Deflake KafkaProducerTest.testSendOffsetsToTransactionTriggersMetadataRefreshThenNegotiatesV6() (#23079) | +/- / 2+/2- |
| afed4b8183 | Security | N |  | 2026-05-14 | KAFKA-20440: Use default Keystore type instead of hardcoding PKCS12 keystore type (#22041) | 2+/2- / 6+/0- |
| ab71359239 | Security | O |  | 2026-08-06 | KAFKA-20874: Close SslFactory in JaasOptionsUtils.createSSLSocketFactory() (#23005) | 10+/4- / 10+/0- |
| 05b3d2adcd | Share | O |  | 2026-07-03 | KAFKA-20736: Improve removal of unassigned partitions from share sessions (#22720) | 21+/0- / 53+/8- |
| 1277346c9b | Share | O |  | 2026-09-21 | KAFKA-21106: Resolve merging of batches in ShareFetch (#23499) | 79+/62- / 230+/7- |
| 12e135bd26 | Share | O |  | 2026-07-28 | MINOR: Fix Javadoc code sample in KafkaShareConsumer (#22974) | 2+/2- / +/- |
| 2cc6ef65e5 | Share | O |  | 2026-07-30 | KAFKA-20736: Close empty share sessions safely (#22888) | 232+/115- / 638+/80- |
| 33ea66bf7e | Share | O |  | 2026-07-20 | KAFKA-20736: Improve share session handling on leader change (#22766) | 267+/133- / 438+/27- |
| b4da5b66e1 | Share | O |  | 2026-06-23 | KAFKA-20524: Improve reset offset usability (#22607) | 8+/2- / +/- |
| b6d2503710 | Share | O |  | 2026-04-21 | KAFKA-20410: Add DLQ configuration parameters for Share Groups (KIP-1191) (#21979) | 6+/0- / +/- |
| c3a830f8d2 | Share | O |  | 2026-07-14 | KAFKA-20585 : Share Fetch latency metric incorrectly updated during ShareAcknowledgeResponse (#22296) | 4+/7- / 51+/3- |
| cc5b632871 | Share | O |  | 2026-09-24 | KAFKA-21106 Omit partitions with no in-flight records from ShareFetch#records (#23532) | 9+/6- / 207+/2- |
| d1c0bd82c0 | Share | O |  | 2026-07-29 | KAFKA-14648: Do not fail clients if bootstrap servers is not immediately resolvable (2/2) (#22897) | 11+/4- / 48+/0- |
| eb8f9c916c | Share | O |  | 2026-06-30 | KAFKA-20720: Lower level for client log on self-healing share session… (#22691) | 16+/4- / +/- |
| 1ba3ca579c | Streams | O |  | 2026-07-01 | MINOR: Task-offset timer should not be reset if nothing was sent (#22715) | 7+/6- / 47+/0- |
| 238eb6e6a7 | Streams | O |  | 2026-06-23 | KAFKA-20116: Make task-end-offset-sum available to client background thread (2/N) (#22608) | 9+/1- / 26+/12- |
| 359763b286 | Streams | O |  | 2026-07-29 | KAFKA-20790:  Add group.streams.assignors broker config + loader (#22920) | 30+/7- / 4+/2- |
| 402087dc42 | Streams | O |  | 2026-06-23 | KAFKA-20626: Add topology description to Admin client, DescribeStreamsGroups handler, and CLI tools (#22636) | 694+/5- / 443+/0- |
| 5a91e404fa | Streams | O |  | 2026-06-25 | KAFKA-20169: Support static membership for Kafka Streams with the streams rebalance protocol at Client Side. (#22559) | 8+/0- / 169+/38- |
| 678c0e07e4 | Streams | O |  | 2026-07-17 | MINOR: Kafka Streams should log broker provided KIP-1071 config values (#22730) | 25+/3- / 308+/1- |
| 844de98644 | Streams | O |  | 2026-06-25 | KAFKA-20116: Send task-(end)-offset to broker (3/N) (#21803) | 75+/5- / 410+/11- |
| 89bc8808f9 | Streams | O |  | 2026-06-22 | KAFKA-20116: Make task-offset-sum available to client background thread (1/N) (#22595) | 10+/1- / 40+/27- |
| 89ccd6a126 | Streams | O |  | 2026-08-04 | KAFKA-20868: Add warmup tasks to IQ metadata (#23006) | 1+/1- / +/- |
| 8d76fddb5c | Streams | O |  | 2026-08-03 | KAFKA-20782: Fix lost IQ metadata in "streams" protocol (#22778) | 12+/2- / 61+/0- |
| 91958dfa28 | Streams | O |  | 2026-06-29 | KAFKA-20169: Add ducktape test code for static membership in KIP-1071 (#22561) | +/- / 0+/30- |
| 9391bdd4f4 | Streams | O |  | 2026-06-23 | KAFKA-20625: Add StreamsGroupTopologyDescriptionRequestManager and Streams client topology push [1/N] (#22639) | 20+/0- / 27+/0- |
| 953f0a0827 | Streams | O |  | 2026-08-06 | KAFKA-20861: Kafka Streams should handle corrupted assignments pro-actively (#22999) | 39+/0- / 101+/0- |
| 9aa8295b7c | Streams | O |  | 2026-08-13 | KAFKA-20894: Make StreamsGroupDescription.toString null-safe (#23083) | 3+/1- / 3+/0- |
| 9ec73ca7b0 | Streams | O |  | 2026-09-05 | KAFKA-21025: StreamsGroupHeartbeatRequest must drop warmup tasks for v0 (#23341) | 12+/0- / 49+/0- |
| a3362c491e | Streams | O |  | 2026-06-24 | KAFKA-20625: Add StreamsGroupTopologyDescriptionRequestManager and Streams client topology push [2/N] (#22640) | 204+/0- / 428+/2- |
| b91561ac8a | Streams | O |  | 2026-07-25 | KAFKA-20744: Add back `rack.aware.assignment.tags` config (#22213) | 40+/12- / 180+/0- |
| bbe1227697 | Streams | O |  | 2026-08-13 | KAFKA-20870: Add test for the owned-task report for a single-role assignment change (#23002) | +/- / 88+/0- |
| be4d31c869 | Streams | O |  | 2026-07-01 | KAFKA-20655: Mark KIP-1331 RPCs stable before release (#22711) | 2+/12- / +/- |
| ca0c89d83a | Streams | O |  | 2026-06-25 | MINOR: Add logging for topology description push in streams group protocol (#22670) | 4+/0- / +/- |
| d933e42b59 | Streams | O |  | 2026-08-13 | MINOR: few code cleanups for KIP-1071 (#23080) | 28+/9- / 73+/0- |
| d9c3b92588 | Streams | O |  | 2026-05-28 | KAFKA-20167 Introduce CloseOptions.DEFAULT for Kafka Streams (#21579) | 114+/35- / 164+/5- |
| e78cd1fb7c | Streams | O |  | 2026-06-25 | KAFKA-20116: Send task-(end)-offset only if changed (4/N) (#22645) | 23+/6- / 211+/19- |
| eb722e43f0 | Streams | O |  | 2026-06-05 | KAFKA-18652: Add `acceptable.recovery.lag` config (#21799) | 31+/2- / 133+/0- |
| 04c3c00f25 | Wire | D |  | 2026-06-02 | MINOR: Add missing GROUP_ID_NOT_FOUND in {Consumer,Share,Streams}GroupHeartbeatResponse schema (#22448) | 4+/1- / +/- |
| 38227ab768 | Wire | N |  | 2026-05-06 | KAFKA-20551: Remove unnecessary generics from TxnOffsetCommitResponse, OffsetCommitResponse, and OffsetDeleteResponse (#22210) | 12+/9- / 2+/1- |
| 78fd214d5f | Wire | N |  | 2026-04-16 | MINOR: formatting fix on UpdateRaftVoterRequest class declaration (#22010) | 1+/1- / +/- |
| c7d574e75f | Wire | N |  | 2026-04-03 | KAFKA-20371: Add Iterable constructor to generated message collections (#21894) | +/- / 70+/70- |
| 1a770734fe | Wire | P | hardening | 2026-09-16 | Bound array and tagged-field allocation in generated message readers | 6+/0- / 257+/2- |
| 20c2450e5b | Wire | P | kip1319 | 2026-04-28 | MINOR: Reshape TxnOffsetCommitResponse.Builder (#22146) | 55+/23- / 145+/10- |
| 2342c80dca | Wire | P | kip1319 | 2026-05-06 | KAFKA-20444: [3/N] Allow building TxnOffsetCommit v6 requests with topic IDs (KIP-1319) (#22215) | 41+/8- / 139+/35- |
| 239a3e4990 | Wire | P |  | 2026-08-04 | KAFKA-18157: Consider UnsupportedVersionException child class to represent the case of unsupported fields (#22405) | 97+/57- / 66+/9- |
| 319dd61cb3 | Wire | P | kip1319 | 2026-05-10 | KAFKA-20444: [5/N] Resolve TxnOffsetCommit topic IDs in KafkaApis (KIP-1319) (#22238) | 4+/0- / +/- |
| 63f445aaa9 | Wire | P |  | 2026-07-27 | KAFKA-20828: Derive client throttling from response schema (#22908) | 1+/44- / 55+/0- |
| 723847904b | Wire | P | kip1319 | 2026-05-06 | KAFKA-20444: [2/N] Update OffsetMetadataManager to use new TxnOffsetCommit errors (KIP-1319) (#22214) | 18+/0- / +/- |
| 7340eefc48 | Wire | P | kip1319 | 2026-04-28 | MINOR: Reshape TxnOffsetCommitRequest.Builder (#22147) | 22+/50- / 51+/69- |
| 7562044781 | Wire | P | kip1319 | 2026-05-05 | KAFKA-20444: [1/N] Add TxnOffsetCommit v6 schema (KIP-1319) (#22205) | 49+/11- / 43+/89- |
| 7997c9ebe0 | Wire | P | specsync | 2026-06-03 | KAFKA-20620: Add StreamsGroupTopologyDescriptionUpdate RPC schema and extend StreamsGroupDescribe/Heartbeat (#22397) | 420+/13- / 141+/11- |
| baa064e422 | Wire | P | kip1319 | 2026-05-07 | KAFKA-20444: [4/N] Prepare TxnOffsetCommitResponse for topic IDs (KIP-1319) (#22224) | 58+/34- / 112+/42- |
| f66a67fcef | Wire | P | hardening | 2026-09-25 | MINOR: Apply MessageUtil array limits in ArrayOf and CompactArrayOf | 18+/6- / 25+/0- |
