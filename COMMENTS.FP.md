# False positives in Critic reviews

Findings that were reported but did not hold on verification. Recorded so future
Critics can calibrate, per `.claude/rules/agent-roles.md`.

---

## Critic 41 (Milestone 11 Phase 1), finding 2 — PARTIALLY REJECTED

**Reported as:** "`MILESTONE-11 GUARD` is absent from `with_client`, leaving a
reachable public bypass" — Severity: Missing Requirement. The finding stated that
"an external caller can therefore construct a `KafkaProducer` from a config with
`transactional_id = Some(..)` or explicit `enable.idempotence=true` and get silent
at-least-once delivery."

**Rejected part: there is no external path.** `with_client` is `pub`, but its
signature makes it uncallable from outside the crate:

    pub fn with_client<C: KafkaClient + Send + 'static>(
        ...
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        ...
    ) -> Self

`ProducerMetadata` and `RecordAccumulator` both live under
`producer::internals`, which is declared `pub(crate) mod internals`
(`src/producer/mod.rs:18`) and re-exported `pub(crate)`
(`src/producer/internals/mod.rs:36-37`). An external caller can neither name nor
construct those arguments, so the function is effectively crate-private despite
the `pub` keyword. The claimed consequence — external users silently getting
at-least-once delivery — cannot occur.

The finding cited the rustdoc ("corresponds to the primary public constructor in
Java's KafkaProducer") as evidence against the "test-injection plumbing"
characterisation. That was fair as far as it went: the rustdoc *was* misleading.
But documentation is not reachability, and the signature is what governs.

**Accepted part: the plan narrowing and the misleading rustdoc were real.** The
approved plan named both constructors; only one was implemented, and the stated
rationale was partly wrong — it argued from the `-> Self` return type as though
that were a constraint, when it is a choice. The genuine defect was that the
deviation was not recorded where a reader would find it.

**Resolution:** the rustdoc on `with_client` now states plainly that it is the
injection seam rather than the user-facing constructor, that it is unreachable
externally and why, and carries a `MILESTONE-11 GUARD:` note explaining that the
guard is deliberately not duplicated there — with the condition under which that
should be revisited. No guard was added, and the return type was not changed.

**Calibration note for future Critics:** `pub` on a function whose parameters are
`pub(crate)` types does not make it externally reachable. Check argument
constructibility before characterising something as a public bypass — the
severity claim rested entirely on that.
