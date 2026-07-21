# Phase 1: Share wire protocol layer

## Goal

Hand-written request/response wrappers over the generated share `*Data` structs,
plus the `ShareSessionHandler` client-side session state machine. This is the
wire foundation the Phase 3 fetch path and Phase 5 request manager build on. No
consumer orchestration, no managers — just the request/response types, the
session handler, and their tests.

## Branch

`milestone9-share-consumer`. All commits land here.

## Java sources

All paths relative to `kafka/clients/src/main/java/`, submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

- `org/apache/kafka/common/requests/ShareRequestMetadata.java`
- `org/apache/kafka/common/requests/ShareFetchRequest.java` / `ShareFetchResponse.java`
- `org/apache/kafka/common/requests/ShareAcknowledgeRequest.java` / `ShareAcknowledgeResponse.java`
- `org/apache/kafka/common/requests/ShareGroupHeartbeatRequest.java` / `ShareGroupHeartbeatResponse.java`
- `org/apache/kafka/clients/consumer/internals/ShareSessionHandler.java`

Tests:

- `ShareSessionHandlerTest.java`
- Byte-level encoding tests for each wrapper (DoD §3 — wire types need
  known-vector encoding tests, not just round-trips).

## Rust output

Mirrors `src/common/requests/consumer_group_heartbeat_request.rs` +
`abstract_request.rs` / `abstract_response.rs`:

- `src/common/requests/share_request_metadata.rs` (`ShareRequestMetadata` —
  epoch / session constants)
- `src/common/requests/share_fetch_request.rs` / `share_fetch_response.rs`
- `src/common/requests/share_acknowledge_request.rs` / `share_acknowledge_response.rs`
- `src/common/requests/share_group_heartbeat_request.rs` / `share_group_heartbeat_response.rs`
- `src/consumer/internals/share_session_handler.rs` (`ShareSessionHandler`)
- Registration in `abstract_request.rs` / `abstract_response.rs` / `mod.rs`.

The message specs under `generator/messages/Share*.json` were synced to the
Kafka 4.2 wire versions (commit `82d74d2`) so the generated `*Data` structs match
the broker the integration tests run against.

## Commits

- `82d74d2` — sync share-message specs to Kafka 4.2 wire versions
- `d8ed7e9` — share wire protocol wrappers + `ShareSessionHandler`

Note: the early acknowledgement support types pulled in alongside this phase
(`AcknowledgeType`, `Acknowledgements`, `AcknowledgementBatch`, `ShareAcquireMode`,
`ShareFetchConfig`) landed in `631f2bf` and its fixup `38a3ecf`; the Phase 1
Critic review covered those types, so their resolved findings are recorded in
this phase's `COMMENTS.DONE.1.md`. The remaining acknowledgement core is Phase 2.

## Verification

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` — clean.
- Byte-level encoding tests against known vectors for each wrapper.
- All `ShareSessionHandlerTest` methods translated.
