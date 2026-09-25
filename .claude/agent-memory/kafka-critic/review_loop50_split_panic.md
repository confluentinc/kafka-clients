---
name: review-loop50-split-panic
description: Loop 50 (§9.18 build() idempotence) review — clean on behaviour; the four findings were all stale-reference / audit-drift, and the techniques that found them
metadata:
  type: project
---

Loop 50 fixed PLAN §9.18 (split-on-`MESSAGE_TOO_LARGE` panicked) by deleting
`MemoryRecordsBuilder::take_built_records` and pointing `ProducerBatch::records()`
at `build()`. Reviewed `ee1a72ec..HEAD`; **no behavioural defect**. All four
findings were documentation/test-completeness.

**Why:** the fix was a delete-the-special-case fix, so the code risk was low and
the residual risk was entirely in *claims* — comments, audit blocks and PLAN
sections that referenced the now-fixed defect.

**How to apply — techniques that paid off here, in rough order of yield:**

- **Grep the whole repo for the PLAN section number a fix closes.** The Actor
  swept `sender.rs` / `producer_batch.rs` / `record_accumulator.rs` / `PLAN.md`
  but missed `src/mock_client.rs` and `design/current/status.md`. The
  `mock_client.rs` one was the best finding: it cited §9.18 as the reason for a
  workaround whose real cause is `ProduceRequestBuilder::build_version`'s
  `mem::replace` — a *different, still-live* move. Marking §9.18 FIXED turned a
  merely-wrong comment into a trap.
- **Extract and run the shipped derivation, then check its *decomposition*, not
  just its total.** `sender.rs`'s `SenderTest` accounting block ships an
  `awk`/`comm` program; it reproduced exactly (53/55/empty). But the prose
  arithmetic beside it ("+ 3 blocked") had gone stale, and the total still summed
  only because the "unblocked (3)" section re-lists an entry already inside the
  16. A right total for the wrong reason is the failure mode these blocks exist
  to prevent — check group headings against the sentence.
- **"I ran the shipped derivation" is only as strong as *which* derivations are
  shipped, and as wide as each one's domain.** The pass-2 finding, and the reason
  pass 1 missed a defect: that accounting block describes four sweeps, ships two
  as code, and states two as bare results ("55 pairs, 0 mismatches" / "102
  headers, 0 mismatches"). Both stated numbers were stale — direct evidence they
  were not being re-run. Worse, the *shipped* header-range program asserts over
  `cited`, a header set read from a hardcoded `PREFIX` commit, so its domain was
  101 of the file's 105 headers and the 4 excluded were exactly the 4 that were
  wrong. So: (1) treat a bare "N, 0 mismatches" in a comment as unverified no
  matter how rigorous the surrounding prose; (2) after running a shipped program,
  probe its own variables for the denominator (`len(cited)` vs `len(now)`) rather
  than trusting the number in the prose; (3) a name-set `comm` check is
  structurally blind to line numbers — it can never surface a wrong citation.
- **A gate's domain is the artifact *seen through a classifier*. Probe the
  classifier, not just the domain expression.** The pass-3 finding. Pass 2 moved
  the gate from a historical snapshot to `now = headers(open(RUST).read())` —
  correct, and it bites. But `headers()` required `` `Class.method` `` inside the
  backticks, and `sender.rs` carries five headers of the form
  `` Translated from `testX` (Java a-b) `` with the class only in prose. **All
  five cited the `@Test` line instead of the declaration line** — the exact defect
  the convention exists to catch, every instance of it outside the denominator.
  Procedure that found it: enumerate the artifact's population *independently of
  the gate's regex* (count every `Translated from` rustdoc block, then subtract
  what the gate matches) and hand-resolve the residue. Also check dict-keyed
  collections for collapsed duplicates — `len(dict)` is a key count, not a
  population count (106 blocks, 105 keys here).
- **Quote the code line, not the line its comment sits on** — and read the
  dependency's teardown path before claiming a proposed fix does not cover
  something. My one false positive of this loop (pass 3, §9.17 volumes; recorded
  in `COMMENTS.FP.md`) was both errors at once: `kafka_cluster.rs:53` is a doc
  comment reading "(a Docker volume)" and `:54` is a plain path constant, and
  `testcontainers` `Client::rm` passes `.v(true)`, so volumes leak exactly when
  containers do. `sed -n '53,54p'` and ~15 lines of vendored source would each
  have killed it. The parts of that same item that survived were the ones resting
  on observation (the hang as a distinct symptom) rather than on an inferred
  mechanism.
- **Sweep commit messages against their own trees — nothing else checks them.**
  Pass 5. One commit claimed "the eight axes are now enumerated in the block"; the
  edit had aborted and nothing was written, and the claim survived a full review
  pass because a commit message is outside every gate. Two cheap sweeps:
  `git show <c>:<file> | grep -c "<claimed phrase>"` for each "X is now in Y"
  sentence, and a token pass checking every backticked identifier in the message
  exists in that commit's tree (a deletion claim correctly shows absent).
- **A name resolver that assumes uniqueness is wrong wherever Java overloads.**
  I nearly filed two false mismatches because my "first declaration matching the
  name" resolver picked overload #1 while the citation pointed at #3 —
  `SenderTest.setupWithTransactionState` has six, `sendIdempotentProducerResponse`
  three. The overload-tolerant predicate is "the cited start line declares this
  name AND the cited end line is `    }`". Read the Java before writing the
  finding.
- **When a doc says "N deviations", count them.** `drive_split_batch_and_send`
  claimed two; Java's driver also has three `assertTrue(client.isReady(node, ..))`
  the Rust drops. Test that the omission is *forced* before accepting it —
  `MockClient::is_ready` exists and is used 4× in that same file, so it wasn't.
- **For "environmental" perf claims, derive the magnitude rather than accept the
  re-run.** `producer_perf_test`: 24 bytes/batch at 100 rps vs a 1 ms-resolution
  p99 over 1000 samples against a fresh Docker broker. Mechanism + resolution
  settles it; "it passed on re-run" does not.
- **A shared-global test flake is not automatically a production concern.** Check
  whether the sharing is Java-faithful first. `CompressionRatioEstimator` is a
  static map in *both* languages, and Rust's read is per-batch
  (`ProducerBatch::new`), not per-record — so CLAUDE.md §11 is not engaged and a
  test-side mutex is the correct fix, not a mask.

**Substantive things confirmed clean (don't re-derive):** `MemoryRecords` is
`bytes::Bytes` with no `&mut` accessor, so the now-shared handle can't be mutated;
no `try_into_mut`/`BytesMut::from`/`is_unique` anywhere in `src/`;
`is_closed()` returns the separate `closed` flag, so after the fix
`closed ⇔ built_records.is_some()` holds and matches Java's
`builtRecords != null`; `CompressionType::rate()` is 1.0 in both languages, which
is why the driver's compression-ratio assertion lands inside its 0.01 tolerance;
`take_batch_data`'s finalisation copy stays a justified deviation even though
`BytesMut::from_vec` is public in bytes 1.11, because adopting the pooled `Vec`
would break `BufferPool`'s `take_buffer` reclaim.

See [[review_m8_phase16_zerocopy]] for the earlier "stale-justification comment
trap" instance, and [[review_m11_phase8]] for the "derive every column or ship
none" rule this loop's accounting block half-broke.
