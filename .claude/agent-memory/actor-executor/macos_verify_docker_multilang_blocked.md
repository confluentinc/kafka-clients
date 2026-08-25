---
name: macos-verify-docker-multilang-blocked
description: On a macOS host, `make verify`'s Docker multilanguage arms (test-integration-c / test-integration-python) cannot run — they copy host Mach-O artifacts into Linux containers
metadata:
  type: project
---

On a **macOS host**, `make verify` passes every native leg but the two Docker
multilanguage integration arms are fundamentally un-runnable, and this is NOT a
mechanical-shim fixable issue.

**Fact:** `bindings/c/Dockerfile.grpc` and `bindings/python/Dockerfile.grpc[.async]`
`COPY target/release/libconfluent_kafka.{a,so}` (host-built artifacts) into a
`debian:trixie-slim` container and link/compile against them there.

- macOS builds produce Mach-O **arm64** artifacts: `libconfluent_kafka.a` is a
  BSD-`ar` archive (`__.SYMDEF`) of Mach-O objects, and there is no
  `libconfluent_kafka.so` at all — only `libconfluent_kafka.dylib`.
- The C gRPC image fails at link: GNU ld reports
  `libconfluent_kafka.a: error adding symbols: archive has no index; run ranlib`.
- The Python gRPC image fails even earlier: `COPY .../libconfluent_kafka.so`
  references a file that macOS never emits.

**Why:** The Dockerfiles assume the host `target/` holds **Linux ELF** artifacts
(true in CI, which runs on Linux). Making these arms work on macOS needs a
harness/architecture change — cross-compile the Rust lib to a linux target, or
build it inside the container — which requires Manager sign-off, not a portability
shim.

**How to apply:** When asked to run `make verify` on macOS, treat build /
format-check / lint / test-rust-all-features / native test-c (ctest) / native
test-python (pytest unit) as the runnable set. Report the two Docker
multilanguage arms as environment-blocked with the exact archive/format reason;
do not attempt an unreviewed cross-compile or Dockerfile rewrite. The C11
`<threads.h>` shim ([[macos build]] `bindings/python/c11threads_compat.h`) is a
separate, already-landed macOS fix that unblocked native `build-python`.
