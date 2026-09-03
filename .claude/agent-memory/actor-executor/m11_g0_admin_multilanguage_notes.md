---
name: m11-g0-admin-multilanguage
description: Milestone 11 G0 (admin multilanguage gRPC harness) — third-proto symbol collisions, the per-key result envelope choice, the per-file image build lists that must be extended, and why a 2-second integration run is not evidence of skipped containers
metadata:
  type: project
---

Slice G0 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: an
`AdminService` proto + `AdminBackend` harness trait + handlers in all three
servers, proving create/close on all four backends. Commits `1d20e401`,
`d868f237`, `7e536fb7` on `dev/admin-multilanguage`.

## Adding a *third* proto to the shared package is not just "copy the second"

All three protos share `package confluent.kafka.test` and are compiled into one
descriptor set, so **any message redeclared across files is a duplicate symbol**.
Consequences found by inspection, not by a compile error:

  - `TopicPartition` lives in `consumer_service.proto`, so the admin proto must
    `import "consumer_service.proto"` to use it — importing only
    `producer_service.proto` (KafkaError / Node / StatusResponse) is not enough.
  - `ListTopicsResponse` and `TopicListing` are already taken by the consumer
    service. Admin messages that would collide need an `Admin` prefix. This bites
    in G1, where `listTopics` is the first admin RPC with a colliding name.

## The per-key result envelope (the decision 46 RPCs depend on)

Chosen shape, per RPC:

    message <Rpc>Response { repeated <Rpc>Entry entries = 1; optional KafkaError error = 2; }
    message <Rpc>Entry { ResultKey key = 1; oneof outcome { KafkaError error = 2; <V> value = 3; } }

with a **shared** `ResultKey` (`oneof { name, topic_id, partition, broker_id }`)
and a shared `VoidResultEntry` / `VoidKeyedResponse` for the ~20 RPCs that
resolve to void per key.

Rejected: one mega `KeyedResult` unioning all 46 value types. It would be a
single envelope, but it deletes the compile-time guarantee that each RPC returns
its own value shape and hands **each of the three independently written servers**
a wrong-variant path that only fails at runtime — in a harness whose entire
purpose is catching cross-language disagreement.

`oneof outcome` rather than two independent `optional` fields because a
`KafkaFuture<V>` resolves to exactly one of value/error; the exception is the
void entry, where an absent error *is* the success signal. The top-level `error`
is not redundant: it carries failures preceding any per-key future (a
synchronous throw, an unknown handle id), and `entries` is then empty.

Uuids cross **as canonical strings** (`Uuid::to_string()`), because that is what
both the C FFI and `admin.py` already expose — not 16 raw bytes.

## The image build lists are per-file and silently incomplete

Adding a proto or a Python module means editing **four** files that each carry an
explicit list, none of which globs:

  - `bindings/python/Dockerfile.grpc` and `Dockerfile.grpc.async` — the proto
    COPY, the `grpc_tools.protoc` argument list, and the generated
    `*_pb2{,_grpc}.py` COPYs in the **runtime** stage as well as the builder.
    `admin.py` itself was absent from both (only `producer.py`/`consumer.py`
    were staged).
  - `bindings/c/Dockerfile.grpc` — the proto COPY.
  - `bindings/c/grpc_server/CMakeLists.txt` — `set(PROTO_NAMES ...)`.

Miss any and the server imports or links against stubs that were never
generated.

## A fast integration run is not a skipped one

The four-arm admin run finished in **1.9 s wall (3.3 s total)** and I treated
that as evidence the broker and gRPC containers had never started. It was not.
The consumer arm `test_ml_assign_and_consume` — which produces real records with
a native producer and reads them back **through** the Python/C containers, so it
cannot pass without both — finishes in 3.8 s. Cached image layers plus a warm
Docker VM start KRaft (`-Xmx512m`) and the two trivial servers in well under a
second each.

The right way to settle this doubt is to time a test that *cannot* pass without
the containers, not to reason about plausible container startup cost. Also note
the gRPC containers are gone from `docker ps` after the run — `backend_pool`'s
atexit hook `docker rm -f`s them — so their absence afterwards proves nothing
either.

**Caveat on G0's own strength:** create+close does not prove broker
connectivity. `new_admin_client` does not connect eagerly and `close` succeeds
regardless, so all four arms would pass against an unreachable bootstrap. G0
proves the plumbing (proto → client → factory → macro → three server handlers);
G1's first real RPC is what proves connectivity.

## Divergence found, not fixed (PLAN §0: report, don't fix)

`MockAdminClient::create(0)` fabricated a controller via
`brokers.first().cloned().unwrap_or_else(...)`. Java throws (`Builder.build()`
reads `brokers.get(0)`) and the FFI (`kafka_admin_MockAdminClient_new`) returned
null for `num_brokers < 1`, so the Rust core was more permissive than both Java
and its own C boundary. **Fixed later in the same PR** — see
[[m11-known-defect-fixes]] for the `Result`-not-`panic!` reasoning. Left here
because the "report, don't fix" rule for G0..G6 is still the rule; the fix was a
separate, explicitly requested slice.
