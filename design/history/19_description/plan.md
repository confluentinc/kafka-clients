# PR #19: Add docs for KIP-1035 (StateStore managed changelog offsets)

## AK Commit

- **Commit**: `b047948ad33d182ff1220e61d00cafe79313f605`
- **Author**: Bill Bejeck
- **Message**: `MINOR: Add docs for KIP-1035 (#22161)`
- **Branch**: trunk
- **Changed files**: `docs/streams/upgrade-guide.md` (+4 lines, 1 file)

## Summary of AK Change

This commit adds documentation to the Kafka Streams upgrade guide for
[KIP-1035: StateStore managed changelog offsets](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1035%3A+StateStore+managed+changelog+offsets).

Two sections are added:

1. **Notable compatibility / downgrade note** (downgrading 4.3.x → 4.2.x): Since Kafka Streams
   4.3.0, state store changelog offsets are persisted inside each state store rather than in a
   per-task `.checkpoint` file. For RocksDB stores the offsets live in a dedicated `offsets`
   column family. Older versions of Kafka Streams do not declare this column family, so they
   will crash on startup if they encounter a state directory written by 4.3+. Users must delete
   the local state directory before downgrading.

2. **4.3 release notes** explaining the KIP-1035 change as an internal infrastructure change:
   - Existing `.checkpoint` files are migrated automatically on first startup.
   - No operator action is required for normal upgrades.
   - EOS crash behaviour (wipe + restore from changelog) is unchanged in 4.3.
   - KIP-1035 is a prerequisite for
     [KIP-892: Transactional Semantics for StateStores](https://cwiki.apache.org/confluence/display/KAFKA/KIP-892%3A+Transactional+Semantics+for+StateStores).
   - Authors of custom `StateStore` implementations may opt-in by implementing `managesOffsets()`,
     `commit(Map<TopicPartition, Long>)`, and `committedOffset(TopicPartition)`.

## Impact on Rust Translation

**This commit requires no changes to the Rust client.**

Reasons:

1. **Documentation-only change** — no Java source code was modified; only a Markdown file in
   `docs/streams/` was updated. There is nothing to translate.

2. **Kafka Streams is out of scope** — The Rust project translates the Apache Kafka *client*
   library (`clients/` subtree). Kafka Streams is a separate library (`streams/` subtree) and is
   explicitly out of scope for this project (see `CLAUDE.md` → "Java source in `kafka/` directory
   (Apache Kafka 4.2)" and the Milestone definitions).

3. **No client API or protocol changes** — KIP-1035 changes how Kafka Streams internally manages
   state store checkpoint offsets. It introduces no new Kafka protocol RPCs, no changes to
   producer/consumer APIs, and no changes to the wire format that the client library handles.

## Decision

No implementation work is required. This PR is a no-op for the Rust translation.

## Phases

| Phase | Description | Files | Status |
|-------|-------------|-------|--------|
| — | No changes needed | — | N/A |
