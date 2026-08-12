---
name: m11-known-defect-fixes
description: Milestone 11 slice that fixed three disclosed defects in production code (mock controller fabrication, NewPartitions absent-vs-empty, SCRAM salt absent-vs-empty) — the Result-not-panic rule at an FFI-reachable site, and how to decide whether an absent-vs-empty fix is broker-observable
metadata:
  type: project
---

The slice after G6 on `dev/admin-multilanguage`: the first one in Milestone 11
that changed production code under `src/` rather than only the harness. Commits
`d12ad6be` (mock) and `c8af2933` (both absent-vs-empty collapses).

## `Result`, not `panic!`, at any site an `extern "C"` entry point can reach

`MockAdminClient::create` fabricated a controller for `num_brokers == 0` where
Java's `Builder.build()` throws `IndexOutOfBoundsException` from
`brokers.get(0)`. CLAUDE.md §10.1 would arguably permit a panic (an
out-of-bounds index on a caller-supplied count has the `ArithmeticException`
shape it names), but §10.2 wins here and the tiebreaker is the FFI:

**A panic in a core function that any `extern "C"` entry point calls is a
process abort, not an error.** Three such sites were already fixed earlier in
this milestone for exactly that reason. So when choosing between `panic!` and
`Result` for a Java-throws translation, check whether `src/ffi/` reaches the
function; if it does, `Result` is the only safe answer regardless of how
"unrecoverable" the condition looks.

Corollary that made this cheap: the FFI entry point then **drops its own
duplicate bound check** and maps `Err` to null. One source of truth for the
bound beats two that can drift.

Cost: 23 `MockAdminClient::create(N)` call sites, all in `#[cfg(test)]` modules
plus two harness constructors — a `perl -pi -e` appending
`.expect("num_brokers is at least 1")` handled all of them.

## Deciding whether an absent-vs-empty fix is *broker*-observable

Both collapses were the same defect (an `is_empty()` decision where Java has two
constructors) and both were fixed the same way (an explicit `bool` column,
following `all_partitions` / `cancel[i]` / `remove_all` /
`op_has_values[i][j]`). But their **observability differs**, and claiming
otherwise is the over-claim this milestone kept having to correct:

  - `NewPartitions.newAssignments`: **fully observable.** The controller compares
    the assignment-list length against the number of partitions being added
    (`ReplicationControlManager.java:1854-1860`), so `increaseTo(n, emptyList())`
    earns `INVALID_REPLICA_ASSIGNMENT` while `increaseTo(n)` succeeds. A harness
    scenario can assert both directions against the same broker.
  - SCRAM salt: **not observable in either direction.** The salt is write-only
    (`describeUserScramCredentials` has no salt field) and
    `ScramControlManager.validateUpsertion` checks only username, mechanism and
    iteration bounds (`ScramControlManager.java:284-296`) — a zero-length salt is
    stored as readily as a generated one. So the harness scenario proves the
    fixed path is *live* on all four backends and nothing more; the regression
    teeth have to sit at the boundary that collapsed (a `read_scram_alterations`
    unit test) plus the row-builder test in `admin.py`.

**The procedure**: before writing "a scenario would have caught this", find the
broker code that branches on the field. If no branch exists and no response
carries it, say so and name where the teeth actually are.

## Where a fix like this has to be wired (six layers, none optional)

`src/ffi/admin.rs` entry point + `read_*` marshaler → regenerated header →
`bindings/c/grpc_server/server.cc` → `bindings/python/_confluentkafka.c` →
`bindings/python/admin.py` + `grpc_translate.py` docstrings → C unit tests
(`bindings/c/tests/test_mock_admin.c`, `test_kafka_admin.c`) + Python unit tests.
Adding a parameter to a C entry point means every call site in the C tests needs
its arity bumped — grep the function name, do not trust the compiler, because
`make test-c` cannot run on this host (no `cmake`).

`admin.py` and `grpc_translate.py` already preserved both distinctions
(`None if x is None else ...`, `HasField`); only the C boundary collapsed them.
That is the recurring shape: **Python carries nullability naturally, C does not,
so the C column is where to look first.**

## Building and testing the Python bindings on this host

The repo `venv` has no `setuptools`, and the CodeArtifact index 401s, so
`pip install -e bindings/python` cannot work here and `make test-python` is out
of reach. The working route is the gRPC image, which already contains the built
extension and `admin.py`:

    docker run --rm -v "$REPO/bindings/python/test:/test:ro" -w /app \
      confluent-kafka-rust/python-grpc-server:dev sh -lc \
      'pip install --quiet pytest "pytest-asyncio<1.0"; \
       python -m pytest /test/unit -q -p no:cacheprovider -o asyncio_mode=auto'

`-o asyncio_mode=auto` is required: `pyproject.toml` supplies it normally, and it
is not in the mounted test tree, so without it every `async def` test fails with
"async def functions are not natively supported".

## Proving a *cross-backend* regression needs a rebuild round trip, and it is worth it

The strongest evidence for a defect like `NewPartitions.newAssignments` is the
3-against-1 split itself, and the only way to observe it is to revert the fix,
rebuild the Linux staticlib **and all three images**, and run the one scenario.
Under the reverted `is_empty()` decision the run was exactly:

    ..._is_rejected__rust        ... ok
    ..._is_rejected__grpc_c      ... FAILED
    ..._is_rejected__grpc_python ... FAILED
    ..._is_rejected__grpc_python_async ... FAILED
    c backend: increase_to_with_assignments(3, []) must be rejected, not treated as increase_to(3): ()

Two things make this cheap enough to be routine:

  - keep the reverted edit *inside* the function body (add `let _ = self.flag;`)
    so no C signature changes and only the lib layer rebuilds;
  - restoring produced **byte-identical image digests** to the pre-revert ones,
    which is itself the proof that the restore was exact — check the
    `rebuilt: sha256:X -> sha256:Y` lines and confirm Y matches the digest the
    full suite ran against.

## `cargo test <filter>` takes a substring, not a regex

`cargo test --test integration "a\|b"` matched nothing and still exited 0:
`test result: ok. 0 passed; ...; 534 filtered out`. **A zero-count pass is a
vacuous pass** — always read the passed count, not the exit code, when using a
filter. Loop over filters instead of trying to alternate them.

## `cargo xtask check-generated` fails whenever the build ran without rustfmt

`build.rs` formats the generated message code by invoking `rustfmt`, and the
pinned 1.95.0 toolchain does not ship it — so any `cargo build` done on the
default PATH leaves `target/*/build/*/out/generated/*.rs` unformatted and
`check-generated` then reports diffs that have nothing to do with the change
under review. The fix is to redo `cargo build` with the nix 1.97.1 rustfmt on
PATH; it is not a defect to investigate.

## `cargo xtask format` after committing is a rework tax

The `format-check`/`lint`/`check-bindings` trio only runs under the nix 1.97.1
triple (the pinned 1.95.0 ships neither `rustfmt` nor `clippy`):

    PATH=/nix/store/*-rustc-1.97.1/bin:/nix/store/*-rustfmt-1.97.1/bin:/nix/store/*-clippy-1.97.1/bin:$PATH

Run `format-check` **before** each commit, not after the last one — otherwise the
formatter rewrites an already-committed file and the fix has to be amended in.
`cargo xtask lint` does pass `--all-features --all-targets`, so it *does* reach
`tests/integration/` (an older memory note says otherwise; that is stale).
