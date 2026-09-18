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

---

## Critic 50 (loop 50, pass 3), §9.17 volume claim — REJECTED

**Reported as:** a judgement inside pass 3's "Examined" section, relayed to the Actor as
guidance. It said the 6687 accumulated Docker volumes were a **second, distinct
mechanism** from §9.17's container leak, on two supporting claims:

  1. "the harness declares a volume for the SSL certificate directory
     (`kafka_cluster.rs:53`) and configures no auto-remove", so
  2. "volumes accumulate on **every** run, clean ones included", and therefore
     "**the proposed fix does not fix it** — registering teardown as each container
     starts reclaims containers; the volume count keeps climbing."

**Both supporting claims are false, and the conclusion falls with them.**

*On (1).* `tests/common/kafka_cluster.rs:53` is a **doc comment**; the constant is on
`:54` and reads `const SECRETS_DIR: &str = "/etc/kafka/secrets";`. It is a path string,
used at `:201`, `:206`, `:234` and `:240` to name where keystore files are placed with
`CopyToContainer`. It declares nothing to Docker. The parenthetical in its own doc
comment — "Directory for SSL certificates inside the container (a Docker volume)" — is
what I read as a declaration. The three anonymous volumes per container come from the
**image**'s `VOLUME` directives, so no edit to `:53`/`:54` could affect them.

*On (2).* `testcontainers-0.27.3` removes anonymous volumes with the container.
`Client::rm` (`src/core/client.rs:226-239`) builds
`RemoveContainerOptionsBuilder::new().force(true).v(true)` — `.v(true)` is Docker's
"remove anonymous volumes associated with the container" — and
`ContainerAsync::drop` (`src/core/containers/async_container.rs:268-282`) calls
`client.rm(&id)` under `env::Command::Remove`. I read both. So a container that is
dropped normally takes its volumes with it, volumes survive on **exactly** the condition
containers do, and §9.17's existing cause and existing fix cover both halves.

**Resolution:** the Actor recorded the volumes as a second *symptom* under §9.17 rather
than a second mechanism, with the source derivation and two stated caveats
(`env::Command::Keep` disables removal; "a clean run leaves zero volumes" is derived,
not measured, and the measurement is owed). That is correct and the Critic concurs.

**Calibration note for future Critics.** The error was reading a doc comment as the
declaration it describes, and then not checking the disposal path before asserting a
fix would not work. Two habits follow: quote the **code** line, not the line its
comment sits on — `sed -n '53,54p'` would have shown the constant one line down and
killed the claim immediately; and before saying "the proposed fix does not cover X",
read the teardown path in the dependency, which here was ~15 lines in one vendored
file. The two adopted parts of the same review item — `docker volume prune -f` beside
`docker rm -f -v`, and the **hang** as a distinct symptom — were the parts that rested
on observation rather than on a mechanism I had inferred.
