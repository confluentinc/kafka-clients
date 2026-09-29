# Critic 78 — resolved (decode hardening)

Review of `f00b57df` (plan §2.1), `1ebbed96` (§2.2) and `7e171207` (§2.3)
against the decode-hardening plan. One finding and four notes. The Manager
decided to act on all four notes; each is recorded below with how it was
handled (Actor 78, round 2).

## Issue: The control-batch D3 test passes without the control-batch D3 check
- **File**: `src/consumer/internals/completed_fetch.rs` (`test_negative_record_count_in_a_control_batch_is_invalid`, :2789; check under test at :1042-1048)
- **Severity**: Missing Requirement (test coverage, low; the production code is correct)
- **Java Reference**: `CompletedFetch.java:376-386` (`containsAbortMarker` → `batch.iterator()`), `DefaultRecordBatch.java:321-326`, `:584-587`
- **Description**: The test is meant to pin plan §6 item 5, the
  `records_count < 0` rejection inside `contains_abort_marker`. It cannot detect that
  check's removal.
  - Its fixture `control_batch(2, 1, 1000)` writes a record with a null key
    (`append_with_offset_bytes(offset, 0, None, ..)`, :1906), so without the check
    `contains_abort_marker` returns `Ok(false)`.
  - The fetch has no aborted transactions, so the batch is not skipped.
  - The batch then reaches the install-point D3 check (:1261-1272), which raises the
    identical `Found invalid record count -1 in magic v2 batch`.

  I verified this by mutation in a scratch copy of the crate: I changed :1046 to
  `if false && records_count < 0 {` and the test still passes.

  The check only makes a difference for a non-ABORT control batch whose producer id is in
  the aborted set. Without the check, that batch is silently skipped by the `is_transactional
  && aborted_producer_ids.contains(..)` arm, where Java throws. I wrote a scratch test for
  exactly that case:
  - buffer: batch 1, then `control_batch(2, 1, 1000)` with count −1 and the CRC recomputed;
  - partition data: `partition_data_with_aborted_txn(buf, 1000, 2)`;
  - config: READ_COMMITTED, `check.crcs=true`.

  It passes on the real code. On the mutant it fails: the second `fetch_records` returns
  `[]` instead of the error.
- **Expected**: A test that fails when the `contains_abort_marker` D3 check is removed.
  Either build the existing test's `CompletedFetch` with
  `partition_data_with_aborted_txn(buf, 1000, 2)` (via `CompletedFetch::new_full`, as
  `test_negative_record_count_in_an_aborted_batch_is_skipped` does), or add the scenario
  above as its own test. Keep asserting the exact message.
- **Actual**: The only test for the control-batch path passes because of the
  install-point check, so a regression in `contains_abort_marker` would go unnoticed.

**Resolution (Actor 78, round 2 — `fixup!` of `f00b57df`).** The existing test
was rebuilt as the Critic described rather than adding a second one: the fetch
is built with `CompletedFetch::new_full` over
`partition_data_with_aborted_txn(buf, 1000, 2)`, READ_COMMITTED,
`check.crcs=true`, so the control batch (a non-ABORT marker: its record has no
key) belongs to a producer id in the aborted set. The exact message
`Found invalid record count -1 in magic v2 batch` is still asserted, and the
first call now asserts offsets `[0, 1]` rather than a count. Its doc comment
says why the aborted transaction is there.

Teeth: with the check in `contains_abort_marker` (`completed_fetch.rs:1046` at
`f00b57df`, `:1056` after this fixup) changed to `if false && records_count < 0 {`
the test fails — the second `fetch_records` returns `Ok([])` (the batch is
skipped as aborted) where the error is expected; with the line restored it
passes. Plan §6 item 5 records why only that fetch can see the check.

## Notes

### Note 3 — the class-name prefix outside `load_next_batch`

Handled in the same `fixup!` of `f00b57df`. `peek_current_record` and
`contains_abort_marker` now append the cause's `message()` instead of its
`Display` (which prefixes `InvalidRecordError: `). A third wrapper of the same
shape — the headers wrap in `fetch_records` — got the same one-token change.
No existing test asserted the prefixed text; two new tests pin the first two
wrappers' texts exactly (`test_malformed_record_is_reported_by_the_cause_message`,
`test_malformed_control_record_is_reported_by_the_cause_message`) and fail with
the `Display` form put back. The wrapper structure and error class are
unchanged; plan §6 item 3 records that Java propagates the
`InvalidRecordException` unwrapped at these sites (a pre-existing structural
deviation outside this change) and why the headers wrap has no test (its only
trigger, a header key that is not UTF-8, is itself a pre-existing divergence:
Java decodes it with replacement characters).

### Note 1 — the D4 rationale measured the wrong quantity

Handled in the `fixup!` of `1ebbed96`. The `MAX_DECOMPRESSED_BATCH_BYTES` doc and
the plan's D4 row now argue from decompressed size: a stored batch decompresses
to its compressed size, at most the broker's `message.max.bytes` (default
1 MiB + 12, `ServerLogConfigs.java:177`), times a compression ratio that has no
bound in principle. 1 GiB on a default broker takes a ratio of roughly 1000:1,
which realistic data does not produce; a broker with a raised
`message.max.bytes` serving highly compressible topics could reach it, and there
a Java consumer streams the batch while this client fails it — the case the
"promote to a config if a user hits it" escape hatch is for. The value is
unchanged. The plan's D5 row, which called its constant "the same broker default
as D4", now names `socket.request.max.bytes` directly.

### Note 4 — the owned compressed path pinned a non-Java text

Handled in the `fixup!` of `1ebbed96`: the code now produces Java's text. The
condition the test hits is a compressed stream that ends where the next
record's size should be. Java's codec streams (`ChunkedBytesStream`) return
`-1` there; `ByteUtils.readVarint(InputStream)` reads it as a continuation byte
and throws `IllegalArgumentException`, which `StreamRecordIterator.readNext`
maps to `Incorrect declared batch size, premature EOF reached`
(`DefaultRecordBatch.java:636-644`), not to `Failed to decompress record
stream` (4.3.1 has no `readVarint(DataInput)`; the `DataInput` in the javadoc is
stale). `DefaultRecord::read_from_stream` now maps an `UnexpectedEof` in the
size to that text; `iter_records`'s return type is unchanged. The test covers
every codec, the Java-translated `test_invalid_record_count_too_many_*_v2`
tests now assert the message instead of `is_err()`, and a new unit test pins the
mapping at its site. The three that exercise the mapping (the `i32::MAX` test,
the compressed too-many test and the unit test) fail with it disabled. Plan §6 item 11
lists the texts on this path that still differ from Java's (all pre-existing).

### Note 2 — the SASL client receive was still unlimited

Handled in `fix(security): cap the SASL client receive`, new behaviour rather
than a fixup. `receive_response_or_token`
now builds `NetworkReceive::with_max_size(SASL_CLIENT_AUTHENTICATOR_MAX_RECEIVE_SIZE,
..)`, a `pub(crate)` constant of 524288 documented against Java's own policy for
the same exchange, `BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE`
(`BrokerSecurityConfigs.java:118-119`, the `sasl.server.max.receive.size`
default). It is the only production receive without a cap (every other one goes
through `KafkaChannel`'s `max_receive_size`), and nothing else needed plumbing.
An oversized receive is an `InvalidReceiveError`, which the selector treats as a
disconnect and closes the connection, not as an authentication failure.
`test_sasl_receive_is_capped` covers `524288 + 1` (rejected through
`authenticate_impl` with `Invalid receive (size = 524289 larger than 524288)`,
no payload buffer, not an authentication error) and exactly `524288` (read in
full through `receive_response_or_token`); it fails with the unlimited receive
put back. Recorded as a Rust-only deviation in plan §4 (Java's client is
`UNLIMITED`, `SaslClientAuthenticator.java:475`, `:570`) and as §6 item 15.

## Pass 2 (Critic 78 over `53298153`, `e6b7dd28`, `38945ca0`): zero findings

Gates re-run at `d2995f75`: lib `4013 passed; 0 failed; 2 ignored`, ffi `206
passed`, build / ffi build / format-check / lint clean. Every "fails with the old
code put back" claim from round 2 was re-verified by mutation in a scratch copy;
each mutation broke exactly the tests named and no others. The autosquash onto
`d7f38f14` was re-run in a scratch clone (conflict-free, identical tree) and each
squashed commit built and passed `cargo test --lib`, so the squashed history
bisects.

One note, not a finding: the 512 KiB SASL receive cap is safe only while
client-side re-authentication (KIP-368) stays unimplemented, because Java reads
parked responses to earlier requests (`Fetch` included) through the same receive
during re-authentication (`SaslClientAuthenticator.java:345`, `:586-593`).
Resolution (Manager, direct — a one-paragraph doc change): the constant's rustdoc
now states the assumption and what a future re-authentication implementation
must do (`fixup! fix(security): cap the SASL client receive`).
