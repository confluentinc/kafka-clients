---
name: review-m11-g0-multilanguage-admin
description: M11 G0 admin gRPC harness review — value-carrying-an-embedded-error breaks oneof envelopes; harness-internal server divergence class; toolchain-has-no-clippy/rustfmt caveat
metadata:
  type: project
---

Round 12, `f871cda0..80ea6971` (4 commits). Found 2 substantive + 4 LOW; no code
defects in the harness code itself, no build/test failures.

## The reusable finding: "value carrying an embedded error" is a third outcome

A per-key Admin result is NOT "exactly one of value-or-error". Three RPCs have a
value type that itself holds an `Option<KafkaError>`, distinct from the per-key
future's error:

  - `TopicMetadataAndConfig` (`create_topics_result.rs`) — `values()` uses
    `then_apply(|_| ())` so the key SUCCEEDS while `topic_id()`/`num_partitions()`
    return `Err`. Java `KafkaAdminClient.java:1839-1846` `future.complete(tmac)`
    where tmac was built from `new TopicMetadataAndConfig(exception)`.
  - `LogDirDescription.error` (describeLogDirs)
  - `DeleteAclsResult::FilterResult{binding, exception}` (deleteAcls)

**Detection heuristic:** if the C FFI has a dedicated `*_error()` accessor on a
*value* handle (e.g. `kafka_admin_TopicMetadataAndConfig_error`), or the Python
class has an `error` slot alongside its data fields, the value carries an
embedded error. Both bindings model this; wire schemas / envelopes that assert
"oneof makes exactly-one structural" are wrong for those RPCs. Grep
`src/ffi/admin.rs` for `_error(` on value handles and `bindings/python/admin.py`
for `"error"` in `__slots__`.

**Why it matters:** dropping the field produces *false three-way agreement* —
all backends report `-1`/empty with no error, so the harness confirms a state it
silently discarded. That is the harness's own failure mode.

## Harness-internal divergence is its own defect class

The three servers must agree. Two G0 divergences, both from copying precedent:
  - Mock selection: `server.cc` uses `req->config().empty()`; both Python
    servers use `not config or all(not v for v in config.values())`. Pre-existing
    for producer/consumer too — but G0's proto comment newly documents the
    *broad* rule as the contract and attributes it to CreateProducer/CreateConsumer.
  - Constructor-failure variant: C++ synthesises `ILLEGAL_ARGUMENT(5)`; Python's
    `grpc_translate.py` fallback for a non-`kp.KafkaError` exception is
    `ILLEGAL_STATE(6)`. `_confluentkafka.c` raises plain `RuntimeError` on a
    null handle, so it always takes that fallback.

**Rule of thumb:** never accept "matches the producer/consumer precedent" for a
behavioural claim without checking *all three* servers — they already differ.

## Suspected-bug pattern worth re-checking each round (currently CLEAN)

`grpc_translate.py:_kafka_error_to_proto` gates on
`isinstance(err, kp.KafkaError)` (producer module's class) while the admin/consumer
handlers catch `ka.KafkaError`/`kc.KafkaError`. If any binding module ever defines
its OWN `KafkaError` instead of `from producer import KafkaError`, every error
from that module arrives as `code=-1` with fabricated `is_retriable`/`is_fatal`,
breaking error-code assertions on the Python backends only. Verified clean:
`admin.py:96` and `consumer.py:36` both re-import producer's class.

## Toolchain caveat is broader than "lint"

`rust-toolchain.toml` pins 1.95.0 with clippy+rustfmt components, but the active
1.95.0 (nix source tarball) has NEITHER: `cargo fmt` and `cargo clippy` both
report "no such command", and `rustfmt`/`clippy-driver` are not on PATH.
`xtask` calls them as `cargo fmt` / `cargo clippy` (`xtask/src/main.rs:59,184`),
so **format-check and lint carry the same caveat** — an Actor disclosing only
the lint caveat is understating. Round 11 left the same item open.

## Verification recipes that worked

  - D1 / "no production visibility widened": `git diff <base>..HEAD -- src/`
    returning empty is the whole proof. Cheap and decisive.
  - `__grpc` infix: `cargo test --features multilanguage-tests --test integration
    -- --list | grep <name>` enumerates the real expanded names. Don't reason
    about `paste!` output — list it.
  - Envelope key-type viability: check the key struct derives `Hash + Eq` AND is
    `pub`. Also: proto3 forbids a `map` field directly inside a `oneof`, so
    `ClientQuotaEntity` (a map) needs a wrapper arm.

## Adjudicated NON-defects (don't re-report)

  - `__rust` arm of the macro starts a Kafka container and is not excluded by
    `--skip __grpc` — identical to the producer/consumer macros; `test-rust-all-features`
    already requires Docker.
  - `async fn` in `AdminBackend` making it non-dyn-compatible — documented and
    correct; the cost (14 `&dyn Admin` helper sites become generic) is a G1 scope
    note, not a defect.
  - `AdminBackend::close` returning `Result` while `Admin::close` is `()` —
    the `Result` carries transport/binding failures only. Documented.
  - `fill_proto_error` (`server.cc:142`) destroys the handle it is given; no leak
    on the `CreateAdmin` failure path.
