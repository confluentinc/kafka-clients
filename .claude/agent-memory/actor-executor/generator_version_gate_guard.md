---
name: generator-version-gate-guard
description: PLAN §9.1 fix — where Java emits the non-default-at-unsupported-version guard, and the register claims that were wrong
metadata:
  type: project
---

PLAN §9.1 (generator omitted Java's non-default-at-unsupported-version guard) is
**fixed** on `fix/9.1-version-gate-check` (`d0dd3b52`, `6245ca10`, `cbe48f20`).

**Why:** the register's own "Fix location" pointed at the *size* function, and three
more of its claims were wrong. Loop 50 corrected four §9.x claims; this loop corrected
four more in one section. The register is a lead, not a spec.

**How to apply:**

  - Java emits this guard in **exactly one** place — `generateClassWriter`
    (`MessageDataGenerator.java:792`). `generateNonIgnorableFieldCheck` has a single
    caller. `generateClassMessageSize` never emits it, so `size()` legitimately
    succeeds at a version where `write()` refuses. Any future claim that a check
    belongs "in the generator" must name the *generator method*, not a line range —
    line ranges in the register have already drifted.
  - Emission requires **two** gates, not one: `!field.ignorable()` **and** the `else`
    half being reachable. Reachability needs Java's
    `curVersions = parentVersions.intersect(struct.versions())`
    (`MessageDataGenerator.java:718`, threaded at `:176`/`:183`). Rust's nested
    `StructSpec` versions come from the *field*, unintersected with the parent, so
    without threading `parent_versions` you over-emit on nested structs whose declared
    range is wider than the enclosing message's.
  - When you need a "count of affected sites", derive it from the JSON specs with a
    script **first**, then diff the generated output and require the two numbers to
    match. Here 100 vs 100 caught exactly the 7 spurious nested-struct guards; a
    generated-output count alone would have looked fine.
  - `cargo build` writes to an OUT_DIR chosen by cargo, and `ls -td .../generated`
    sorts by *directory* mtime, which is stale. Snapshot a baseline by
    `git stash` → `cargo build` → copy, then `git stash pop` → `cargo build` → copy.
    Pick the dir with `ls -t .../out/generated/mod.rs | head -1 | xargs dirname`.
  - Before writing "this is live" about a filed defect, **derive reachability** —
    builder gates, version selection, and whether the null/non-default value is set
    outside `#[cfg(test)]`. Two §9.32 fields looked live and are latent by
    construction. Latent does not mean don't-fix, but the argument then has to be the
    class-gap one (a generator function that handles one type and not its siblings), not
    a live-wire one. State which leg of a reachability argument is weaker when one rests
    on an external fact rather than on a constant in the source.
  - Generated code **is** linted (`cargo xtask lint` runs `--workspace`) and **is**
    rustfmt-checked (`cargo xtask check-generated`, and `build.rs:27` runs rustfmt on
    each file). Emit `} else if cond {`, not `} else { if cond {`, and spell bool
    tests `if self.f` / `if !self.f` rather than `!= false`.

See also [[generator_nullable_array_default]] and [[workflow_docker_wedged_no_verify]].
