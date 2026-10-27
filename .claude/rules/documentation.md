# Documentation rules

This file details CLAUDE.md §6 ("Comments and documentation"): what the doc comment
of a public API item must contain, how it relates to the Javadoc of the Java member it
translates, and how `cargo xtask lint-custom` (`check-api-docs`) enforces that. It
supplements CLAUDE.md; where the two disagree, CLAUDE.md wins.

Each numbered section is one decision: the rule, **Why**, and **How to apply**.

## 1. Scope

The checks of §4 apply to the **public API documentation**:

- every Rust item visible in the published API: a public item reachable through
  public modules (the notion `public_items()` uses in `rust/xtask/src/lint_custom.rs`
  for the other public-API lints), the methods and associated consts of public
  traits and of inherent impls of public types, and the variants of public enums;
- every C export, an `#[unsafe(no_mangle)]` fn. Its text becomes the C header's.

An item finds its Javadoc through its Java marker `#[doc(alias = "org.apache.kafka.…")]`;
a C export through the Rust item it calls (the mapping `check-ffi-translation`
derives). An item with no marker (`rust-only`, `public-in-rust`, or not translated
from a member) has no Javadoc: the size check (§5) does not apply to it, the others do.

Exemptions:
- `self`, in Rust, and the C `self_` and trailing `out_err` parameters: CLAUDE.md §4
  fixes their meaning for every function, so no entry repeats it. A `_cb` twin's `cb`
  and `opaque` are not exempt: they differ in what `cb` receives, so each needs an entry
  (`doc-fill` writes them, §8).
- A trait-impl method without a doc of its own: rustdoc shows the trait method's doc.
  If it has a doc of its own, it is checked.
- `#[doc(hidden)]` items.

`java-line` (§7) applies to every comment in `rust/src` and `rust/tests`, public or not.
Python docstrings are out of scope for now.

**Why:** the published API is what users read; the marker already ties each item to
its Java member, so the check needs no name guessing.

## 2. Layout

```
/// <contract: Java's description, copied and translated (§3)>
///
/// # Arguments            (C: `# Parameters`)
///
/// - `name` - …           one entry per parameter
///
/// # Returns              (or a summary sentence starting "Returns …")
///
/// # Errors               each Javadoc @throws as its Rust error (`XError`); every `Result` fn has it
///
/// # Panics               Java precondition throws kept as panics (CLAUDE.md §12.5)
///
/// # Translation notes    Rust- or C-only remarks; `WARNING:` lines here
///
/// # Safety               C exports and other `unsafe fn`s only; handle ownership and lifetime
```

- The **original documentation** is everything before `# Translation notes` (or before
  `# Safety` when there are no notes): Java's contract and the sections that come from
  its block tags (§3), plus the entries for parameters, returns and errors that Rust
  adds (an `options` parameter, a `Result` where Java throws unchecked). It holds no
  remark about the translation itself.
- `# Translation notes` appears at most once, after the original documentation, and
  holds everything that is about Rust or C rather than about the contract. It never
  appears on an item whose Java member has no Javadoc: such an item keeps its doc as
  plain text.
- `# Safety` comes last. Besides the preconditions clippy's `missing_safety_doc`
  requires, it holds a C handle's ownership and lifetime: who frees a returned handle,
  and how long a borrowed pointer stays valid. After `# Translation notes`, it is the
  only heading allowed.
- The translation boilerplate "Translated from", "Corresponds to", "Mirrors Java",
  "CLAUDE.md" and "DoD #" is allowed only in the notes; the marker already records the
  origin, so it usually goes.

**Why:** a reader sees Java's contract first and unchanged, and finds every deviation
in one place.

## 3. Translating Javadoc

The lint and the Actors use the same translator: `cargo xtask javadoc <item>` prints its
output for an item, as a draft to start from.

| Javadoc | rustdoc |
|---|---|
| body text, `<p>` | paragraphs |
| `<ul>/<ol>/<li>` | `-` / `1.` lists |
| `<h3>X</h3>` (any level) | `# X` |
| `<pre>`, `{@code x}`, `<code>x</code>` | code block, `` `x` `` |
| `<b>`, `<i>`, `<em>`, `<strong>` | `**x**`, `*x*` |
| `{@link C#fooBar(..) label}`, `{@linkplain ..}` | `label` |
| `{@link C#fooBar(..)}` | `` `C::foo_bar` ``; `` `Self::foo_bar` `` for `#fooBar` (an intra-doc link `` [`C::foo_bar`] `` when it resolves) |
| `@param name text` | `# Arguments` list: `` - `name` - text `` (name snake_cased) |
| `@return text` | `# Returns` paragraph |
| `@throws X text`, `@exception X text` | `# Errors` (or `# Panics` where Rust keeps Java's throw as a panic, CLAUDE.md §12.5) list: `` - `XError` text `` |
| `@see ref` | `# See also` list |
| `{@inheritDoc}` | the overridden member's doc |
| `@since`, `@author`, `@deprecated` | dropped |
| `@apiNote`, `@implNote` | the text is kept as a paragraph |

Names are translated with the CLAUDE.md §2 rules: an `Exception` class becomes its
`Error` name (a Java built-in gets the `Local` prefix, `IllegalStateException` ->
`LocalIllegalStateError`; a class outside `common` its package prefix,
`OffsetOutOfRangeException` in `consumer` -> `ConsumerOffsetOutOfRangeError`), Java
method names become snake_case, `null` becomes `None`. Java's own wording stays, except
the word "exception", which neither Rust nor C code may use (CLAUDE.md §2, §4): "the
exception" becomes "the error", "completes exceptionally" "completes with an error".

A C export's doc is C, not Rust: `null` becomes `NULL`, a `Self::foo` reference the C
export of the same class (`kafka_consumer_Consumer_poll`) or else the plain name, and an
intra-doc link its code span, since a C reader has neither `Option` nor rustdoc links.

## 4. What counts as documented

| Finding | Fires when |
|---|---|
| `missing` | The item has no doc at all. |
| `param` | A parameter is not listed as `` `name` `` (or `` `_name` ``) in an `# Arguments` or `# Parameters` section. The message offers Java's `@param` text for it, matched by snake-cased name. Patterns that are not a plain name are skipped. |
| `returns` | The fn returns something other than `()` or `Result<(), _>` and has neither a `# Returns` section nor a first paragraph starting with "Returns". For an `async fn`, a fn returning `impl Future<Output = T>` or a boxed `dyn Future<Output = T>`, `T` is the return type. A constructor is exempt, as a Java constructor has no `@return`: an item whose marker names a Java constructor (`C#C(..)`), or an associated fn without `self` returning `Self` or its own type, directly or in a `Result`. A C constructor states who frees the handle under `# Safety` (§2). Nothing else is exempt: a builder setter returning `Self` (`*OptionsBuilder::set_*`, `MockAdminClient`'s `Builder`) says what it returns, and a `bool` predicate (`Error::is_*_error`) whose summary starts "Whether" still needs a `# Returns` section or a summary starting "Returns". |
| `errors` | A fn returning `Result` (directly or as the future's output), or a fallible C export (a trailing `out_err`, or a `_cb` twin, whose completion receives the error, CLAUDE.md §4), with no `# Errors` section; or a Javadoc `@throws X` not documented. For a fn returning `Result`, `X` is documented only when `# Errors` names its **predicate**, derived from its Rust name (§3) per CLAUDE.md §12.4: `is_producer_fenced_error`, `Error::is_producer_fenced_error`, or inside the C symbol `kafka_common_Error_is_producer_fenced_error`. The type (`ProducerFencedError`), the variant (`Error::ProducerFenced`) or a description in words does not count, because users classify errors with the predicates, not by matching variants (CLAUDE.md §3). The predicate must exist on `Error` (`ErrorHierarchy`); when it does not, the finding says so and the gap is the predicate's, not the doc's. A fallible C export names its throws under `# Errors` by the C predicate, except a throw the Rust method it wraps keeps as a panic (the method returns no `Result`, or its `# Panics` names the throw and its `# Errors` does not, CLAUDE.md §12.5), which goes under `# Panics`. For a fn that does not return `Result`, a throw becomes a panic and is named in `# Panics` as the type, variant or predicate. A JDK exception (a `Local…` name) kept as a panic (CLAUDE.md §12.5) only needs a `# Panics` section describing it. `NullPointerException` is exempt: Rust has no null to pass. A throw the item does not keep is covered by its accepted contract change (§6) when a `WARNING:` paragraph names it as `` `ArithmeticException` ``. |
| `size` | §5. |
| `layout` | A second `# Translation notes`; a heading other than `# Safety` after the notes; `# Safety` before the notes; a notes heading on an item with no Javadoc; a `WARNING` outside the notes (except on an item with no Javadoc, which has no notes); boilerplate (§2) outside the notes. |
| `warning`, `contract-stale`, `contract-format` | §6. |
| `java-line` | §7. |

When the marker names an overload (`#m(T1,T2)`), that overload's Javadoc is used.
Otherwise the overload whose `@param` names best match the Rust parameters is used for
`param` and `errors`, and every marker on the item counts.

A Javadoc that only points elsewhere is replaced by the Javadoc it points to: one with
no text but `@see` tags (`Consumer.commitSync()`: `@see KafkaConsumer#commitSync()`;
`Consumer`: `@see KafkaConsumer`, `@see MockConsumer`), or whose text is only "See" and
`{@link}`s (`Producer.close()`: `See {@link KafkaProducer#close()}`). The referenced
class is found by simple name, the item's own package first. `cargo xtask javadoc`
prints the Javadoc after this step.

## 5. Size: at most 150% of the Javadoc

- Words are runs of letters, digits and `_`, after Markdown links are reduced to their
  text, and a code span is one word, so a symbol (`kafka_common_Error_is_kafka_error`,
  `Self::foo_bar`) costs one. Heading lines are not counted, on either side, so a
  mandatory heading costs nothing.
- The Java words are the translated Javadoc (§3, body plus block tags). The Rust words
  counted are only what has a Java counterpart, so a tag Java lacks costs nothing:
  - the description (the text before the first heading, and any section not listed
    below, such as `# See also` or a heading of Java's body);
  - `# Translation notes`;
  - each `# Arguments` / `# Parameters` entry of a parameter Java has an `@param` for;
  - `# Returns`, only when Java has an `@return`;
  - each `# Errors` / `# Panics` entry naming the error of a Java `@throws`.

  Not counted: the entries of Rust-only parameters and of the C `self_`, `out_err`,
  `cb` and `opaque`, a `# Returns` Java has no `@return` for, errors Java does not
  declare, and `# Safety`.
- The counted words must be at most 1.5 times the Java words. When several Javadocs
  apply (overloads, several markers), the item passes when it fits one of them.
- An item whose Java member has no Javadoc has no limit, and no `# Translation notes`
  (a `layout` finding): its doc is complete but not verbose. This is not checked.

**Why:** the doc carries Java's contract plus what Rust needs to add; past 150% the
additions crowd out the contract.

## 6. Contract changes: `WARNING`

- When the Rust or C item does not keep the Java contract, its notes contain a line
  starting `WARNING:` that says how it differs: behaviour, error kind, nullability,
  blocking, ordering.
- The lint fails on every `WARNING` (`warning` finding) unless the item has an
  **accepted** entry in `design/current/doc-contract-changes.md`.
- The entry must be listed, and accepted, before the doc is changed. The flow:
  1. The agent adds an entry with an empty `Accepted`, which means proposed.
  2. The agent leaves the `warning` finding in the baseline (§8).
  3. Only the user fills `Accepted`.
- An accepted entry whose item no longer exists, or whose doc no longer has a
  `WARNING`, is a `contract-stale` finding. A malformed entry is a `contract-format`
  finding.

The entry format (a numbered list, never a table):

```markdown
1. `crate::producer::ProducerRecord::with_options`
   - Java member: `org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long,Object,Object,Iterable)`
   - Contract change: <what differs from the Java contract>
   - Why: <rationale>
   - Accepted: <empty = proposed; the user writes `YYYY-MM-DD Name`>
```

The title is the Rust path (`crate::…`) or the C symbol. The four sub-items are
required, in this order, and no others are allowed. Titles are unique.

**Why:** CLAUDE.md §6 says never to change the contract of public API. A change must
therefore be visible, reviewed, and accepted by the user, never slipped into prose.

## 7. No Java file:line references

- No comment refers to a Java source line: not a doc comment, not a `//` comment, and
  not in test code. This bans `Foo.java:123` and `Foo.java:12-34`, as well as the short
  form `` `:142` `` that points back to a file named earlier.
- Refer to Java members by fully qualified name instead:
  `org.apache.kafka.clients.producer.ProducerRecord#partition` for a method,
  `org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long,Object,Object,Iterable)`
  for an overload, `org.apache.kafka.clients.producer.ProducerRecord` for a class. Inside
  a doc, the short `ProducerRecord#partition` is fine when the class is unambiguous.
- `check-api-docs` checks `rust/src` (including `ffi/`) and `rust/tests` (`java-line`
  finding, keyed by file). C tests and Python follow the same rule but are not linted.

**Why:** line numbers go stale with every Kafka upgrade; member names survive.

## 8. Baseline and helpers

- `rust/xtask/doc-baseline.txt` lists the finding keys still to burn down: `<kind> <item>`,
  and `java-line <file>`, which waives a whole file. A line that matches no finding is
  itself a finding, so the baseline only shrinks. `cargo xtask doc-baseline` regenerates
  it, and `cargo xtask lint-custom check-api-docs --no-baseline` shows everything. New
  code never adds lines to it, except a `warning` for a proposed contract change (§6).
- `cargo xtask javadoc <marker|rust-path>` prints the translated Javadoc of an item.
- `cargo xtask doc-report` prints the open findings per module and per kind.
- `cargo xtask doc-fill [--write] <path>…` inserts, into the docs of the items under the
  given files or directories, only what is missing and Java has: `# Arguments` /
  `# Parameters` entries from `@param`, `# Returns` from `@return`, and an entry per
  `@throws` not documented, translated (§3): under `# Errors` for a fn returning a
  `Result` or a fallible C export, naming the error's predicate
  (`Error::is_producer_fenced_error`, the C symbol for a C export), and only when that
  predicate exists on `Error`; under `# Panics` otherwise, and for a throw the wrapped
  Rust method keeps as a panic (§4), naming the error type. Java's text is reworded as §3
  says: "exception" as "error" everywhere, and for a C export `NULL`, C symbols and no
  intra-doc links. A `_cb` twin's `cb` and `opaque` get their fixed entries, `cb` saying
  whether the completion receives a value besides the error (from its `_cb_t` typedef). An entry joins the end of the section's list; a new section
  goes in the §2 order, before `# Translation notes` and `# Safety`; every other line stays
  as it is, so a second run inserts nothing. It never rewrites existing prose and never
  writes a placeholder (the `cb` / `opaque` entries aside), so a parameter Java does not have and a `Result` fn's own errors
  stay findings for the author. It skips, and lists, an item with no doc or a doc set by
  `#[doc = ..]` or a macro. Without `--write` it prints what it would insert. Review what
  it inserts against what the item does: `# Errors` versus `# Panics`, `None` versus
  `null`, renamed parameters, and a throw the prose already describes without naming it,
  which then reads twice; the added Java text can push the doc over §5's size, and the
  baseline lines it closes must go (`cargo xtask doc-baseline`).
