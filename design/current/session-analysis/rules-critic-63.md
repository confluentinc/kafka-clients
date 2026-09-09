# Critic 63 — coverage table for `.claude/rules/python-binding-interface.md`

Maps each decided spec/decision/audit item to the rule (§) in the new file that covers it, or
notes the omission. "Rule N" = section N of `python-binding-interface.md`.
Findings keyed to `COMMENTS.63.md`.

## Design principles (spec §3 P1–P12)

| Spec principle | Rule | Status |
|---|---|---|
| P1 Java contract, Python idiom | 4.1, 2 (R1) | covered |
| P2 Overload collapse | 3.3, 4.2 | covered |
| P3 Keyword-only | 3.2, 4.3 | covered (2 exemptions stated) |
| P4 Typed error hierarchy | 4.4, 5 | covered |
| P5 Sync/async peers | 4.5 | covered |
| P6 Non-instantiable bases | 4.6 | covered |
| P7 Inferred generics | 4.7, 12 | covered |
| P8 Serde is any callable | 4.8, 8 | covered |
| P9 Async engine underneath | 4.9 | covered |
| P10 Context manager | 4.10 | covered |
| P11 Regex broker-side (`SubscriptionPattern`) | 4.11, 3.4 | covered |
| P12 `xAsync` names not reused (`commit`/`commit_nowait`) | 4.12 | covered |

## Base rules / transforms (audit §0 R1–R6, R2.1–R2.15)

| Item | Rule | Status |
|---|---|---|
| R1 Java name & type by default | 2 | covered |
| R2.1 snake_case / PascalCase / module (drop `clients`) | 3.1 | covered |
| R2.2 keyword-only + 2 exemptions | 3.2 | covered (admin examples — finding #4) |
| R2.3 overload collapse to union | 3.3 | covered |
| R2.4 same-name/diff-type union + stubs; overlap fallback | 3.4 | covered; overlap = note #5 |
| R2.5/2.7 `Duration`, `Optional<T>` | 3.7, 3.8 | covered |
| R2.6 collections | 3.8 | covered |
| R2.8 callback interfaces → aliases | 3.9 | covered |
| R2.9 `Exception→Error` class names only | 3.10 | covered |
| R2.10 `Future`/`byte[]`/`Headers` | 3.11 | covered |
| R2.11 public mutable fields → `set_<field>()` | 3.12 | covered |
| R2.12 deprecated methods KEPT | 3.13 | covered |
| R2.13 builder-positional | 3.2(b), 3.14 | covered |
| R2.14 getter/setter pair → one method, 2 stubs | 3.6 | covered |
| R2.15 `timeoutMs` stays `int` ms | 3.7 (last sentence) | covered (admin-scoped) |
| R3 combination validation, mechanics deferred | 3.5 | covered; mechanics = finding #6 |
| R4 FFI mapping per combination | 10 | **contradiction — blocker #1/#12** |
| R5 Java comparison pass | 2, 14 | covered |
| R6 parked improvements list | 13 | covered |

## Decisions D1–D28

| Decision | Rule | Status |
|---|---|---|
| D1 error model (typed hierarchy, no code/predicates, generated) | 5 | covered; abstract-set open = finding #11 |
| D2 `metrics()` pull-based; `MetricName`/`Metric`/`KafkaMetric` | 6 (module), 10 (core gap) | partial — `metrics()` present in spec §6; core gap noted |
| D3 `commit`/`commit_nowait` | 4.12 | covered |
| D4 `AsyncProducer.send` double await | — | deferred in spec; not a rule item (acceptable) |
| D5 non-instantiable bases | 4.6 | covered |
| D6 serde design | 4.8, 8 | covered |
| D7 timeout kept, unwired = silently ignored | 3.7 | **partial — note #14 (tension w/ rule 10)** |
| D7 addendum negative Duration → IllegalArgumentError | 3.7 | covered |
| D8 partitioner (Preview default, declared surface) | 6 (module lists Partitioner) | partial — surface named, not detailed (acceptable, deferred) |
| D9 mocks object-taking; `closed()` method | 3.9, 11 | covered |
| D10 bytes default, memoryview opt-in + retention | 8 | **partial — retention rule omitted, finding #9** |
| D11 inferred generics | 4.7, 12 | covered |
| D12 naming/module/async placement | 6 | covered |
| D13 record mutability | — | SKIPPED in spec; correctly absent |
| D14 callback thread map | 7 | covered (lifecycle vs infra split) |
| D15 producer flush-on-`__exit__` | 4.10, 7 | covered |
| D16 accessor methods | 3.15 | covered |
| D17 `subscribe` collapse (broker-side pattern only) | 3.4, 4.11 | covered |
| D18 `client_id()` dropped | — | omission of a *removal*; acceptable (absent = correct) but not stated |
| D19 `pending_count()` dropped | — | same as D18 |
| D20 `acknowledge` collapse | 3.3 (share) | covered by general collapse rule |
| D21 `seek` async + distinct-name params | 3.4 | covered (distinct-name branch) |
| D22 `enforce_rebalance` removed | — | absent = correct; not stated |
| D23 `transaction()` cm removed | — | absent = correct; not stated |
| D24 `closed` — mock method only | 4.10, 11 | covered |
| D25-A negative Duration | 3.7 | covered |
| D25-C `committed()` → `OffsetAndMetadata \| None` | 3.8 (Optional) | covered by transform, not named |
| D25-D′ `error_cb`/`logger` removed | 9 ("no `error_cb`, no `logger`") | covered |
| D25-D `on_delivery` bg thread | 7 | covered |
| D25-E `group.id` optional → `InvalidGroupIdError` | 9 | covered |
| D25-F async listener may be `async def` | 7 | **partial — reentrancy/ConsumerHandle omitted, finding #3** |
| D25-G thread safety (producer safe, consumer exclusive) | 7 | covered |
| D25 gap 1/8 async-listener reentrancy deadlock | 7 (implementation-gap clause) | **partial — deadlock not named, finding #3** |
| D26 fix 1 `close(CloseOptions)` collapse | 3.6, 3.3 | covered |
| D26 fix 2 `seek` distinct names | 3.4 | covered |
| D26 fix 3 `records` (not `records_for_topic`) | 3.3 | covered by no-split rule |
| D26 fix 4 `RecordMetadata` has_offset etc. | 3.8 | **partial — site not named, finding #8** |
| D26 fix 5 `set_poll_exception` (Exception→Error class-only) | 3.10 | covered |
| D26 fix 6 MockProducer set_<field>() | 3.12 | covered |
| D26 R2 fix 2 `Node.has_rack()` | 3.8 | partial — site not named (finding #8) |
| D26 R2 fix `ConsumerGroupMetadata` deprecated ctor + defaults | 3.13 | covered by "deprecated kept" (behavioral defaults not named) |
| D27 `@overload` stubs, site list | 3.6, 3.4 | covered |
| D27 `MockConsumer` union ctor + `string` owner-flag | 3.4 | partial — no-default/no-no-arg-ctor + owner-flag not stated (finding #9) |
| D28 admin rulings | 1 ("admin paused; do not implement") | out of scope — correctly excluded |

## Cross-cutting decided items with no rule (finding #9)

| Item | Decided where | In file? |
|---|---|---|
| `ConsumerRecords` deprecated records-only ctor (can't answer `next_offsets()`) | spec §5.2, D25 master re-check | no |
| `TopicIdPartition` share-only scoping + 2-stub ctor | spec §5.1 NOTE, D27 | no |
| `memoryview` retention ("one view pins its batch") | spec §5.4/§5.2, D10 | no (finding #9) |
| `WakeupError` / rotating-token `wakeup()` semantics | spec §3 P9, §5.5; consumer-threading §11 | no (finding #7) |
| `ConsumerHandle` (§41) reentrancy for async listener | consumer-threading §31/§41, D25-F | no (finding #3) |

## Existing-rule contradictions

| New rule | Conflicts with | Finding |
|---|---|---|
| 10 "each combination → distinct FFI entry point" | CLAUDE.md §2 (presence-only → one method w/ `Option`; >3 params → `_options` builder) | **blocker #1, #12** |
| 1 "mocks under `src/test/java`" | Java fact — mocks are in `src/main/java` | **blocker #2** |
| 10 core-gap "never a silent no-op" | D7 "unwired timeout silently ignored" | note #14 |

## bindings-rule-proposals.md items

| Proposal | Status vs new file |
|---|---|
| 1 admin §11 stale / mirror consumer FFI + shared dispatcher | untouched (admin paused); dispatcher referenced but not designated — minor (finding #13) |
| 2 `_async` rustdoc thread contract, verify in header | absorbed (rule 7 final bullet) |
| 3 timed join → `tokio::time::timeout` | untouched — out of scope for the binding rule (core rule) |

## Summary counts

- Blockers: 2
- Findings: 8
- Notes: 4
