---
name: Phase 7b Round 2 review patterns
description: Verified-good fix shapes for trait-method-removal + sync→async migration on a public trait skeleton
type: reference
---

# Phase 7b Round 2 review patterns

Context: Phase 7b shipped a `Producer<K, V>` trait skeleton with two Round 1 Suggestions:
(1) two methods on the trait did not exist on Java's interface; (2) four blocking-Java
methods were declared as sync `fn -> Result` instead of `async fn`. The Round 2 fixup
addressed both.

## Verified-good fix shapes

### Trait-method removal — audit before deletion

When the Critic suggests removing trait methods because they "don't exist on Java's
interface", the Actor's fixup must include an explicit *re-audit* of:
- The Java interface file (here: `Producer.java`).
- The Java concrete class (here: `KafkaProducer.java`) — methods may exist as
  inherent methods even when absent from the interface.
- Internal package-private classes that the public class delegates to (here:
  `TransactionManager.java`).

If the methods exist as inherent methods on the concrete class, the right action is
**translate them as inherent `impl` methods on the Rust struct, not trait methods**
on the trait. The Phase 7c carry-over note in `NOTES.md` is the correct mechanism to
hand off this audit obligation. Verify both:
- The methods are actually absent from `KafkaProducer.java` *now* (not just from the
  interface).
- The carry-over note tells the next phase to re-verify and translate as inherent
  if back-ported.

### Sync→async-fn-in-trait migration — three acceptable forms

For Java-blocking → Rust-async migration on a *public trait*:

| Form | Verdict | Notes |
|---|---|---|
| `async fn foo(&self) -> Result<...>` | Preferred | Cleanest. But forfeits dyn-compat unless every async fn has explicit Send bound elsewhere. |
| `fn foo(&self) -> impl Future<Output = Result<...>> + Send` | Acceptable | Verbose but self-documenting on Send-ability. Used by Phase 7b. |
| `fn foo(&self) -> Pin<Box<dyn Future<...>>>` | **Forbidden** | Per-call heap alloc; violates CLAUDE.md rule 11. Reject as Blocking. |

The mixed form (trait declares `impl Future + Send`, impl block uses `async fn`) is
fine — the impl-block syntax sugars to the same return type.

### Send-bound preservation checklist for trait-method async-ification

Every async-shaped trait method on a `Send + Sync` trait should carry `+ Send` on
the returned future. Without it:
- A caller holding `Box<dyn Producer>` (when dyn-compat returns) cannot send the
  future across `tokio::spawn`.
- Even with `impl Trait` returns (no dyn), generic call sites lose Send through
  missing bounds.

When the trait declares `+ Send` explicitly on every async method, the impl-block
auto-inference of Send for `async fn` bodies is contracted, not implicit. This is
the right shape.

## Patterns from this round

1. **No production callers yet** is a good cue that signature changes are cheap —
   verify via `grep -rn <method>` over `src/` (excluding the trait file itself).
   If the grep is empty, the migration is non-breaking.
2. **Test-count invariance during signature migration** — Suggestion 2 fixup
   converted four sync trait methods to async without changing the test count.
   The right shape is rename-and-relocate-existing-asserts (here: the four
   `is_err()` lines moved from `..._dispatches` (sync test) into
   `async_methods_dispatch_through_trait` (existing async test) and the original
   test was renamed `..._dispatches_sync` and scoped to `metrics()`).
3. **Archive integrity check pattern** — when the Round 1 had only Suggestion-tier
   issues and they all closed in one fixup, the archive commit is small (one file:
   `COMMENTS.DONE.<N>.md`) and `COMMENTS.<N>.md` should *not* retain a Phase-7b
   stub section — it should look as if Phase 7b had no Round 1 review at all.
   Verify the Round 1 Verdict line is preserved in the DONE file with the fixup
   SHA cited inline.
