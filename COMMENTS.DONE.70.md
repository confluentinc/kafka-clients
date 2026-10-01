# Critic 70 review of `8796564b..26b61e06` — resolved

### C70-1 — Python `create_acls` with an undefined enum code hangs forever (the F8 key change)
- **Severity**: Bug (Medium: a hang, which CLAUDE.md §5 forbids).
- **Introduced in**: `80091d49` (F8).
- **Files**: `src/ffi/admin.rs` `kafka_admin_AdminClient_create_acls_async` (keys = `acl_binding_key` of the
  validated bindings); `bindings/python/admin.py:2780-2796` `_keyed_acl_binding_void_cb`
  (`futures[_to_acl_binding(key_tuple)]`); `admin.py:1679-1687` `AclBinding.__init__` (plain `int(...)`,
  so any integer is accepted); `_confluentkafka.c:6515` `count_distinct_acls` (counts raw codes).
- **Java reference**: `AclOperation.fromCode` / `ResourceType.fromCode` / `PatternType.fromCode` /
  `AclPermissionType.fromCode` map an undefined code to `UNKNOWN`. `KafkaAdminClient.createAcls` then fails
  that binding's future with `InvalidRequestException("Invalid ACL creation: ...")`. It never hangs.
- **Description**: F8 correctly delivers the callback under the normalised binding, which is what the core
  keys by. The commit message says "Python passes only defined enum codes", and that is false: Python's
  `AclBinding` accepts any int. For `AclBinding(TOPIC, "t", LITERAL, "User:a", "*", 99, ALLOW)`, the FFI fires
  one callback keyed with `operation=0`. `futures[...]` raises `KeyError` inside the callback (the exception is
  printed and swallowed), and the Future keyed with `operation=99` **never resolves**. Probe output:
  ```
  KeyError: AclBinding(resource_type=2, ..., operation=0, permission_type=3)
  3 KafkaError Not implemented yet
  99 TimeoutError        # f.result(timeout=3)
  ```
  (Before F8 the failure mode was different, not absent: a synthetic error for key 99, plus a second callback
  for key 0. That meant two `Py_DECREF(cb)` against one incref, a use-after-free risk.)
  `delete_acls` has **the same hang today**. Its keys have been normalised filters since before this round
  (`kafka_admin_AdminClient_delete_acls_async`), and I reproduced it with `AclBindingFilter(..., 99, ...)`. The
  same fix should cover both.
- **Expected**: every Future that admin.py returns resolves exactly once.
- **Fix** (this belongs in the Python layer, which the round allows to adapt. The FFI now matches what the
  core produces):
  - Normalise the lookup key the way `enum_code_or_unknown` + `from_code` do: any code that is not a defined
    member becomes 0. Do this for each of the four enum fields.
  - Resolve every requested key whose normalised form equals the callback's key.
  - Compute the incref count over the *normalised* distinct bindings. Otherwise two raw bindings that
    normalise to one binding would over-incref and leak `cb`.
  - Apply the same change to `delete_acls`.
  - The public dict stays keyed by the user's own objects, so the return shape does not change.
  - Add a Python test with an undefined operation code for `create_acls` and `delete_acls`, asserting that the
    Future resolves and asserting its exact message.

- **Resolution** (FIXED):
  - `create_acls` is fixed in the Python layer, as the Manager decided. The change is `amend!` commit `c1840205`
    for `80091d49`; autosquash replaces F8's message.
    - `admin.py` now has `_normalized_acl_key`, which does what `enum_code_or_unknown` + `fromCode` do: any code
      that is not a defined member of the four enums becomes UNKNOWN, 0.
    - The per-key callback looks up every caller key whose normalised form equals the callback's key
      (`_futures_by_normalized_acl_key`), and converts the error once for the whole group.
    - The rows sent to the C layer are the distinct *normalised* bindings, so `count_distinct_acls` equals the
      number of callbacks the FFI fires. The returned dict stays keyed by the caller's own objects.
  - The false claim in F8's commit message ("Python passes only defined enum codes") is corrected in the
    `amend!` message, which squashes into 80091d49.
  - `delete_acls` gets the same fix in its own commit, `a082ca4c`. It is not a fixup, because it predates F8.
    The filter value handle is drained once per group.
  - Tests: `test_create_acls_resolves_a_binding_with_an_undefined_enum_code`, its async twin,
    `test_delete_acls_resolves_a_filter_with_an_undefined_enum_code`, and its async twin. Each uses operation
    or resource/pattern codes 99/98 and checks two keys that normalise to one. Every Future resolves within a
    5 s timeout with the exact message "Not implemented yet".

### C70-2 — F10: the synthesised absent-replica error raises `AttributeError` on `txn_requires_abort`
- **Severity**: Behavior Mismatch (Low; the Python surface regressed).
- **Introduced in**: `3ccadaae` (F10).
- **Files**: `bindings/python/admin.py:2722` (`KafkaError._from_parts(_ABSENT_KEY_ERROR_CODE, ...)`);
  `bindings/python/producer.py:36-48` `_from_parts` never sets `_txn_requires_abort`.
- **Description**: before F10, the absent replica's error came from `KafkaError._from_c`, which sets all five
  fields. Now it is built by `_from_parts`, so `error.txn_requires_abort` raises
  `AttributeError: 'KafkaError' object has no attribute '_txn_requires_abort'`. I reproduced this against
  `describe_replica_log_dirs([TopicPartitionReplica("nope", 0, 0)])`. Code, message, `is_retriable` and
  `is_fatal` are unchanged, so the existing test does not notice. The same latent bug affects the other
  `_from_parts` callers (`admin.py:1956`, `:3226`, `:3514`); that part is pre-existing.
- **Fix**: set `ret._txn_requires_abort = False` in `KafkaError._from_parts` (or add a parameter). Then extend
  the F10 test to read `txn_requires_abort` (expect `False`).

- **Resolution** (FIXED, fixup `7ae28d3c` -> `3ccadaae`):
  - `KafkaError._from_parts` now sets `_txn_requires_abort = False`. This also fixes its pre-existing callers.
  - The unknown-replica test now reads `is_retriable`, `is_fatal` and `txn_requires_abort`, and expects all
    three to be `False`.

### C70-3 — Python `ClusterDescription.cluster_id` changes from `""` to `None` on the Metadata fallback path
- **Severity**: Missing Requirement (Low; this breaks the round's rule that Python return shapes must not
  change).
- **Introduced in**: `477f13c7`.
- **Files**: `bindings/python/_confluentkafka.c:4665` (`Py_BuildValue("(sNNN)", kafka_admin_DescribeClusterResult_cluster_id(r), ...)`).
  `"s"` with a NULL pointer yields `None`.
- **Description**: the core and FFI change is Java-exact (`MetadataResponse.clusterId()` is nullable), and I
  have no objection to it. But the commit states that Python now returns `None` on that path with "no Python
  code change". The brief says the Python layer must only adapt to keep working, and that its public interface
  and return shapes must not change in this round. This value was a `str` on every path before. (The gRPC
  translator passes it as a proto kwarg, where `None` means "unset", so nothing crashes.)
- **Fix**: pick one.
  - Keep the old Python value for this round: map a NULL id to `""` in the C extension. That is one line, and
    note it for the later Java-shape plan.
  - Get the user's explicit sign-off on the `None` change, and state it in `COMMENTS.DONE.70.md`.

- **Resolution** (FIXED, fixup `53848496` -> `477f13c7`):
  - The core and FFI stay Java-exact: the cluster id is nullable.
  - `_confluentkafka.c` maps the FFI's NULL cluster id to `""`, so Python's `ClusterDescription.cluster_id`
    is still `""` on the Metadata fallback. A comment at that site says the Python interface change is
    deferred to the Python Java-shape plan.
  - 477f13c7's commit message still says Python returns `None` on that path. After the squash that
    statement is superseded by this fixup; the fixup's own message records the correction.

### C70-4 — stale test-module comment says listTransactions "stays joined"
- **Severity**: Doc (Low).
- **Introduced in**: `b5e3bdd7` (F5 left it behind).
- **File**: `src/ffi/admin.rs:30661-30663`. The comment reads "`listTransactions` is deliberately NOT
  converted ... it stays joined."
- **Fix**: say that the async path is now the F5 two-stage `byBrokerId()` delivery and that only the sync
  entry point is joined.

- **Resolution** (FIXED, fixup `4836ce91` -> `b5e3bdd7`): the comment now says that the async entry point
  delivers Java's `byBrokerId()` in two stages (`deliver_list_transactions_by_broker_id`), and that only the
  sync entry point joins.

### C70-5 — F8 test: doc comment spliced into its neighbour, and a non-exact message assertion
- **Severity**: Test quality (Low; DoD #3).
- **Introduced in**: `80091d49`.
- **File**: `src/ffi/admin.rs:29588-29599`.
  - The new test was inserted *inside* the doc comment of
    `create_acls_async_reports_the_real_unsupported_error_per_binding`. As a result, that test lost its doc, and
    the new test carries both docs.
  - The new test asserts `assert_ne!(message, "the requested key was not present ...")`, not the expected text.
    The Rust mock's message is fixed: `"Not implemented yet"` (`mock_admin_client.rs:1729`).
- **Fix**:
  - Move the `/// End-to-end ... round-trip every field.` block back above its own test.
  - Use `assert_eq!(message, "Not implemented yet")`.

- **Resolution** (FIXED, fixup `bb525f22` -> `80091d49`):
  - The `/// End-to-end ... round-trip every field.` doc is back above
    `create_acls_async_reports_the_real_unsupported_error_per_binding`.
  - The new test asserts `assert_eq!(message.as_deref(), Some("Not implemented yet"))`.

## C70-6 — Python `DeletedAcl` docstring contradicts what `delete_acls` delivers
- **Severity**: Doc (Low). The public docstring is wrong, but the code is right.
- **Commit to fix up**: `d6269abd`, the doc-correction commit. It fixed the same claim in the FFI rustdoc
  and in `admin.py`'s txn docstring, but missed this one. The docstring itself predates this round.
- **File**: `bindings/python/admin.py:1783-1784`. It reads: "Exactly one of the two is set: `binding` when
  the ACL was deleted, `error` when the filter matched it but deleting it failed."
- **Java reference**: `KafkaAdminClient.java:2705-2708` builds
  `new FilterResult(aclBinding, aclError.exception(message))`. The Rust core does the same
  (`kafka_admin_client.rs:1776-1782`, `FilterResult::new(binding, error)`, where `binding` is always
  decoded). `py_DeleteAclsFilterResults_drain` copies both fields.
- **Description**: a failed deletion therefore reaches Python as `DeletedAcl(binding=<the matched ACL>,
  error=<KafkaError>)`, with **both** fields set. `binding` is `None` only if the broker's binding cannot be
  decoded. A caller who trusts the docstring would treat any non-`None` `binding` as "deleted" and miss the
  failure.
- **Fix**: say that `binding` is the ACL the filter matched, and that `error` is set (in addition to
  `binding`) when deleting it failed. This mirrors the corrected FFI doc. It is docstring-only, so the
  interface and return shape do not change.

- **Resolution** (FIXED, fixup `5b2abb87` -> `d6269abd`): the `DeletedAcl`
  docstring now says that `binding` is the ACL the filter matched and that `error` is set when deleting it
  failed. It states that the two are not exclusive, citing `KafkaAdminClient.java:2705-2708`, tells callers
  to test `error` to tell a deleted ACL from a failed one, and notes that `binding` is `None` only if the
  broker's binding could not be decoded. This mirrors the FFI doc corrected in d6269abd.

## Not acted on (the user's decision)

- A synchronous NumberFormatException for describeConfigs / incrementalAlterConfigs. This would need both
  methods to return `Result<_, Error>`, which changes the trait signature. Per the Manager, it is left to the
  user; the Critic accepted the current documented deviation.
