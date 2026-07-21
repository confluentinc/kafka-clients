---
name: milestone11-phase1-share-wire
description: Milestone-11 Phase 1 (KIP-932 share wire+session layer) landing notes — stale generator specs, blocker classes pulled in, dead_code precedent
metadata:
  type: project
---

Milestone 11 = KIP-932 client-side share consumer (agent N=1), 7 phases. Phase 1 = share wire protocol + `ShareSessionHandler`.

**Stale generator specs gotcha (important for later share phases):** `generator/messages/Share*.json` were an OLDER revision than the vendored Kafka 4.2 source in `kafka/.../resources/common/message/Share*.json`. The generator copies lacked v2 fields (`ShareAcquireMode`, `IsRenewAck` on ShareFetch/Ack requests; `AcquisitionLockTimeoutMs` on ShareAcknowledgeResponse) and were `validVersions:"1"` vs `"1-2"`. Fix: copied the 6 vendored kafka Share*.json over the generator copies and rebuilt. If a later phase hits a "missing field" on a generated `Share*Data`, re-check the generator spec is synced to `kafka/`.
**Why:** the `kafka/` tree is the 4.2 source of truth; the generator specs had drifted. **How to apply:** before wrapping any generated `*Data`, diff `generator/messages/X.json` against `kafka/clients/src/main/resources/common/message/X.json`.

**Blockers pulled into Phase 1** (ShareSessionHandler + its test need them; DoD §4): `AcknowledgeType` (public `consumer::acknowledge_type`), `ShareAcquireMode`, `AcknowledgementBatch`, `Acknowledgements`, `ShareFetchConfig` (all `consumer::internals`, pub(crate)). A later phase that "owns" these should REUSE not re-translate (they are complete/faithful) — DoD §6.

**dead_code precedent:** Phase-1 types are only exercised by tests until the share request-manager phase wires them. Added `#![allow(dead_code)]` per-file with a comment citing the `fetch_session_handler.rs` precedent. The wire wrappers in `common::requests` did NOT need it (used by ConcreteRequest/Response enums).

**Generated wire encoding facts (verified by capture):** KIP-848/932 messages are flexible (compact strings/arrays, unsigned-varint length = len+1, null = 0). A null nullable-STRUCT field writes a `-1` (0xFF) presence byte (generated writer line "null struct presence byte"), NOT varint-0. Byte-level known-vector tests live in `share_group_heartbeat_request.rs` / `share_group_heartbeat_response.rs`.

**Collections:** generated `Share*Data` use plain `Vec<...>` (no Java `*Collection` with `.find()`); the Java find-or-add pattern is translated to linear `position()` lookups in the builders.

**Java overload split:** `ShareSessionHandler.handleResponse` (overloaded for fetch vs ack) → `handle_fetch_response` / `handle_acknowledge_response`. ShareSessionHandler builds `ShareFetchRequest.Builder` (real RequestBuilder), so `newShareFetchBuilder(...).build().data()` → test helper `build_data(Option<builder>)` that builds at latest version and extracts the `ConcreteRequest::ShareFetch` data.
