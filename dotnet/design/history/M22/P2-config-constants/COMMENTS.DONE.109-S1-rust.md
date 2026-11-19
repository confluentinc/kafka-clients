# COMMENTS.DONE.109 — M22/P2, Rust lane

## 109-S1-1 — the evaluator turns a non-ASCII Java string literal into mojibake, and generation does not stop

- **Severity**: Low (latent).
- **Commit**: d43285062. The code is older (d7c33aed2, #280, the Python lane). S1 extends it to C#.
- **Where**: `rust/xtask/src/config_consts.rs:898`, in `tokenize`'s string branch: `v.push(b[i] as char)` pushes
  each UTF-8 **byte** as a `char`.
- **What is wrong**:
  - A literal `"café — x"` in a Java source evaluates to `"cafÃ© â\u{80}\u{94} x"`. `cs_str` then emits
    `"cafÃ© â\u0080\u0094 x"` into a public `const string`. Nothing stops: it is neither an
    evaluation nor the stop that the module doc promises (`config_consts.rs:24`, "Anything it cannot evaluate
    stops generation instead of being guessed").
  - Java reads the source as UTF-8, so its value is the 8-character string. Python's output has the same
    mojibake, because the evaluator is shared.
  - S1's guards do not catch it:
    - `test_cs_string_literals` passes `é`, `—` and `😀` straight to `cs_str`, so it never goes through this
      path.
    - `test_cs_outputs_are_generated_and_checked` asserts `is_ascii()`, which the `Ã` escapes satisfy.
    - `check-generated` pins whatever the generator emits.
  - So PLAN §9 R3 ("S1's xtask test and `check-generated` pin them") does not hold for a non-ASCII `_DOC`. S3's
    literal check would catch only the single-literal fields, not a concatenated `_DOC`.
- **Evidence**:
  - Probe in a scratch copy of `rust/xtask`, with one added test and the tracked tree untouched: a fake
    `org.example.Fake` with `public static final String X = "café — x";`. `constants_of` returned `Ok`, with
    value `"cafÃ© â\u{80}\u{94} x"`. The test's `assert_eq!` against `"café — x"` failed. The 9 real
    `config_consts` tests passed in the same run.
  - Today's impact is nil. The Java sources the generator reads contain no byte above 0x7F (the eight classes,
    `AbstractConfig` and `CommonClientConfigs`). The nine `.g.cs` files hold no `\u` escape and no non-ASCII
    character.
  - The bug triggers on a Kafka upgrade, the case `test_cs_constant_counts` exists for, if a doc gains a curly
    quote or a dash.
- **Fix**:
  - Decode the literal by `char`, not by byte. Either push `s[i..].chars().next()` and advance `i` by its
    `len_utf8()`, or collect the raw bytes into a `Vec<u8>` and call `String::from_utf8` at the closing quote.
  - Add a test that a non-ASCII Java literal evaluates to itself through `constants_of` (the probe above), so the
    non-ASCII cases of `test_cs_string_literals` are covered end to end.
  - The Python and C# outputs stay byte-identical today.

- **Resolved** in ac394a2c4 (fixup of d43285062): `tokenize` decodes literals by UTF-8 character; `test_non_ascii_literal_evaluates_to_itself` checks value, C# and Python literals through `constants_of` (fails on the old code); generated files unchanged by this item.
