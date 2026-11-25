---
name: review-js-value-class-patterns
description: JS binding (javascript/) value-class review heuristics - option values forwarded raw into another JavaForms method, probing dist/ from the scratchpad, copy-verification recipe
metadata:
  type: feedback
---

Heuristics from reviewing the JS consumer's P1 value classes (Critic 150, 2026-11-25).
One real bug came out of it, and the self-declared departures were all sound.

- **A value read with `FORMS.value()` and passed on raw to another JavaForms method is a leak.**
  `ConsumerRecords.records({ partition })` handed the value to `TopicPartitionMap.get()`, which
  read a plain object as its own options object (`{ key: tp }` was accepted) and threw its own
  form error (`get() takes one of (key)`) for a wrong type. Sibling forms often check their type
  (`topic must be a string`), so compare each form's checks. `src/internal/payload.ts` has the
  shared checks (`topicPartition.take`, `topicPartitionOffsetMap`, `headers`).
  **How to apply:** in every new class, list each option value that reaches `get`/`has`/`set`/
  another class's method, and probe it with a string and a plain object.
- **Probe the built package directly:** write a script in the scratchpad that `require`s
  `javascript/dist/<pkg>/index.js` by its absolute path. Self-reference only works inside
  `javascript/`. Run `make -C javascript build-ts` first, since dist/ can predate an amended HEAD.
- **Copy-verification recipe:** compare `git show <src-commit> -- <file>` hunks against
  `git show <p1-commit> -- <file>`, using `grep '^[+-]'`, then `diff`, and check the hunk headers
  match. For a copied file, use `git show <sha>:<path> | diff - <file>`.
- **Not defects in this binding (avoid false positives):** bare `equals(x)` in tests (repo-wide
  practice); `toString()` of bytes as `Arrays.toString` (the producer's `show`, shared on purpose);
  `ConsumerRecords.EMPTY` typed `<never, never>`; `isEmpty()` meaning "the map is empty" (Java's
  `records.isEmpty()`); a bare `null` for `records()` fitting no form (Java's `records(null)` is
  ambiguous); engine `TypeError` text for a null element deep in a user-built map. Also, a Java
  method's full `@throws` list sits on its first TS signature (often the bare form) and covers the
  named form too: `SecurityProtocol.valueOf` set that precedent, so an error only the named form
  raises, listed on the bare signature, is not a misstatement. Point 3 of the Comments rule
  (which comments to keep) is judged against the density of the already-reviewed producer files.
- **What IS worth reporting in TSDoc:** wording that contradicts tested behaviour. P1 round 2 found
  "RangeError when the timeout is not a whole count of milliseconds", while fractions are rounded
  down. Compare each `@throws` with a sibling that documents the same helper (the constructor's,
  `producer.ts`'s `close()`).
- **Drift in the binding's notes after a phase:** "the one place `exception` is an identifier", the
  "today" list of `DATA_CLASSES`, and "no helper exists yet" in `check-default-helpers`. The
  Manager updates the notes at hand-off, so report these as notes, not issues.

**Why:** the user wants demonstrated findings only, with no false positives, and the conventions
require run-time type checks for plain JavaScript callers.

Related: [[review-expectations]]
