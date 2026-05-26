---
name: Phase 7d Round 1 fixup patterns
description: Java exception-class-hierarchy translation for catch-arm fan-out, classifier-on-error pattern, partitioner.class deferral surface, ownership-as-readonly rustdoc
type: feedback
---

## Java exception class hierarchy is a catch-arm contract — translate the WHOLE chain, not the union

When a Java try-catch has multiple `catch (FooException e)` arms with
different bodies, the Rust translation MUST split per-arm. Collapsing
them into "any error" handling silently changes behaviour for users who
register both a callback AND await the future:

- Java's `KafkaProducer.doSend` has FOUR catch arms (line 1056-1081):
  1. `ApiException` — fires user callback + `interceptors.onSendError`,
     returns `FutureFailure`.
  2. `InterruptedException` — interceptor only, rethrows wrapped.
  3. `KafkaException` — interceptor only, rethrows.
  4. `Exception` — interceptor only, rethrows.

Rust has no "rethrow-vs-return" distinction (every Err flows the same
way), so the equivalent is "fire user callback ONLY when the error is
an `ApiException` subclass". Add `KafkaError::is_api_exception()` as a
classifier and gate the user-callback fire on it.

**Why:** I initially translated as "every Err fires both callback +
interceptor". Critic 7 caught it as a behavioural divergence: a user
holding both a callback and a `.await` would observe non-API errors
(`Serialization`, `Config`, `IllegalState`) twice — once via the
callback, once via the returned `Err`. Java's per-arm rule prevents
exactly this.

**How to apply:**
- When translating Java try/catch chains with multiple arms, audit each
  arm's body for distinct behaviour (callback fire? rethrow? return-vs-
  raise?). If they differ, split the Rust catch into `match` on a
  classifier; do not collapse.
- For Kafka's specific case: `KafkaError::is_api_exception()` returns
  `true` for `ApiException` subclasses (Timeout, RecordTooLarge,
  InvalidTopic, all the wire-coded ones, all retriables, all
  authn/authz, all transactional), `false` for direct `KafkaException`
  subclasses (`Serialization`, `Config`, `Interrupt`, `Generic`) and
  stdlib `RuntimeException` (`IllegalArgument`, `IllegalState`,
  `UnsupportedOperation`).
- Verify class hierarchies against the Java source — `SerializationException`
  is `extends KafkaException` (NOT ApiException), but `DisconnectException`
  is `extends RetriableException extends ApiException`. Don't trust
  intuition; check the file.

## Test-pin ApiException + non-ApiException paths separately

A test that triggers `RecordTooLarge` (an ApiException) and asserts
"interceptor fired once" is NOT enough — the divergence I introduced
fired the user callback for non-ApiException errors. The pre-fix
behaviour passed an ApiException-only test because Java's behaviour
agrees with the buggy Rust on the ApiException path.

**Why:** A test that doesn't pin the divergent path can't catch the
divergence. Adding a second test that uses an `IllegalState` (close()
+ send()) closes the gap — the pre-fix code would fire the callback
once; the post-fix asserts zero callback fires.

**How to apply:** When the catch arm has per-arm behaviour, write at
least two tests — one per behaviour class. Assert the count, not just
"is_some". The user-callback test must be a counter (Arc<AtomicUsize>)
that fires from the closure-form `Callback` (which Rust supports via
the blanket impl in `producer/callback.rs`).

## Phase deferrals MUST log a warn at the deferral point

When a config key is "defined in schema, not consumed by the current
Phase" (e.g. `partitioner.class` in Phase 7c-7d), `config.log_unused()`
will NOT warn the user — the key is in the schema, just not touched.
This is silent until Phase X wires it up. Two options:

1. Make the constructor reject the config with `KafkaError::Config(...)`
   if the key is non-default.
2. Emit a `log::warn!` at construction with a Phase-X marker so the
   deferral is operator-visible.

**Why:** Critic 7 caught `partitioner.class` as a "regression risk into
Phase 7e" — once the public `new(props)` becomes production, a user
with a custom partitioner gets silently dropped. Without the warn, no
operator would notice until they realise their partitioner never ran.

**How to apply:**
- For each "defined in schema but not consumed by this Phase" config
  key, add a `if config.inner().originals().contains_key(KEY) { warn!(...) }`
  block at the construction site.
- Update the carry-over note in NOTES.md to make explicit that the next
  Phase MUST either reject or wire the key (not "consider wiring").
- The check is `originals().contains_key()` — `get_class()` returns
  `Err` on a Null value but a user-supplied class FQCN populates
  `originals` directly, regardless of how it parses through the schema.

## Rust-ownership-equivalent-of-Java-flag pattern (rustdoc-only fix)

Java's `setReadOnly(record.headers())` flips a flag on `RecordHeaders`
to prevent post-`partition()` mutation. Translating this 1:1 in Rust
would add a public `set_read_only()` method on `Headers` for no
behavioural reason — the equivalent guarantee comes for free from
`do_send`'s by-value receiver: the user no longer holds a reference
to the original `Headers` after the move.

**Why:** Critic 7 flagged this as "either translate setReadOnly or
document why it's unneeded". The rustdoc-only path is correct because
Rust's ownership model is strictly stronger than Java's read-only flag
for this case.

**How to apply:**
- When a Java mutation-prevention API (`setReadOnly`, `final`,
  `Collections.unmodifiableList`) maps to a free Rust ownership
  guarantee, add an inline comment naming the Java construct and
  explaining why the Rust ownership semantics suffice. Do NOT add a
  Rust API method just for parity.
- The comment shape that worked for the Critic: "Java line N: foo()
  flips X. Rust's ownership model provides the same guarantee for
  free: <specific reason involving move/by-value>. No `foo()` call is
  needed."

## Co-existence of behaviour-fix + test commits → fixup against the test commit

Phase 7d had three target commits:
- `53d8cf4` — original do_send body (pre-fix behaviour).
- `34341b2` — original tests (couldn't catch the divergence).
- `b696f5d` — Phase 7c constructor (where partitioner is initialised).

The behaviour fix is a fixup against `53d8cf4` (it modifies do_send),
the test pin is a fixup against `34341b2` (it modifies the test file),
the partitioner.class warn is a fixup against `b696f5d` (it modifies
the constructor).

`git commit --fixup=<sha> -m "<additional body>"` works: `--fixup`
generates the `fixup! <subject>` line and the `-m` arg becomes the
body. Confirmed via `git log -1 --format=fuller`.
