# Critic 85: resolved comments (branch `fix/password-type-redaction`)

Java references point to Apache Kafka at `26b251a451ce941d3d7a55e6487bcb7f16b5ad48`, under `clients/src/main/java/org/apache/kafka/`.

## Issue: `ProducerConfig`'s `Debug` still prints secrets from `originals` that Java never prints
- **File**: `src/producer/producer_config.rs:330-381` (`impl fmt::Debug for ProducerConfig`, the `originals` view). Its two predicates are `SslConfigs::is_password_config` (`src/common/config/ssl_configs.rs:136`) and `SaslConfigs::is_password_config` (`src/common/config/sasl_configs.rs:79`).
- **Severity**: Behavior Mismatch.
  - The leak is latent: nothing in the crate Debug-prints a `ProducerConfig`.
  - But it is the exact leak class this phase set out to close, and it sits in one of this phase's deliverables.
  - The gap comes from the plan itself (§4.3, §10.2), so the Manager should choose the fix.
- **Java Reference**:
  - `common/config/AbstractConfig.java:118`: `values = definition.parse(originals)` keeps only the keys the `ConfigDef` defines.
  - `AbstractConfig.java:371-385`: `logAll()` prints only `values`, and a `Type.PASSWORD` value prints as `[hidden]`.
  - `AbstractConfig.java:390-395`: `logUnused()` prints key names only.
  - `common/utils/ConfigUtils.java:36-59`: `configMapToRedactedString` prints `(redacted)` for any key that is unknown to the `ConfigDef` or has a sensitive type (`:44`).
  - `common/config/SaslConfigs.java:392` and `:402` define two more `Type.PASSWORD` keys. Java's producer includes them through `clients/producer/ProducerConfig.java:525-526` (`withClientSslSupport()` / `withClientSaslSupport()`).
- **Description**:
  - `new()` copies every user key into `originals` unchanged (`originals: props.clone()`, `:547`). An unknown key falls through to the `_ =>` arm (`:733-734`), which logs the key name and keeps the value.
  - The `Debug` impl hides a value only when `SslConfigs::is_password_config(key) || SaslConfigs::is_password_config(key)` is true. That covers the six SSL keys and `sasl.jaas.config`. Every other value prints as-is, including two kinds of secret:
    - **Two Java `Type.PASSWORD` keys**: `sasl.oauthbearer.client.credentials.client.secret` and `sasl.oauthbearer.assertion.private.key.passphrase`. Java's producer defines both as `Type.PASSWORD`, so Java never prints their values in plaintext. The Rust parser does not recognise them, because OAuth is not translated. They are still stored in `originals` and printed.
    - **Any key the Rust producer does not define.** An example is a serializer credential such as `basic.auth.user.info`. Users supply such keys precisely because `originals` is forwarded to the serializer's `configure`. Java never prints the value of an undefined key: `logAll()` iterates only defined keys, and `logUnused()` prints only names.
  - Plan §4.3 assumes that "the `originals` values that are secrets are exactly the `Type.PASSWORD` keys". But the predicates cover only the translated subset (the restriction in §3.1), while `originals` holds every key the user passed.
  - §10.2 justifies redacting by key as "closer to Java's `logAll()`". That does not hold for unknown keys, because `logAll()` never prints them at all.
  - Two rustdoc comments state a contract the code does not meet:
    - The impl's doc (`:326-327`) says the value is hidden "for every key Java defines as `ConfigDef.Type.PASSWORD`".
    - `SaslConfigs::is_password_config`'s doc says it lets the holder of `originals` "hide exactly the values Java hides".
  - I confirmed this by running it. A scratch binary outside the repo called `ProducerConfig::new` with these props and printed `{:?}`:
    ```
    originals: {"basic.auth.user.info": "SR-KEY:SR-SECRET-4", "bootstrap.servers": "broker-visible:9092", "sasl.jaas.config": "[hidden]", "sasl.oauthbearer.assertion.private.key.passphrase": "PASSPHRASE-SECRET-2", "sasl.oauthbearer.client.credentials.client.secret": "OAUTH-SECRET-1", "sasl.password": "UNKNOWN-KEY-SECRET-3", "ssl.keystore.password": "[hidden]"}
    ```
    The two keys the predicates cover are hidden. The other four secrets are printed.
  - The new comments on `ConsumerConfig` (`src/consumer/consumer_config.rs:50`) and `AdminClientConfig` (`src/admin/admin_client_config.rs:33`) tell any future `originals` map to reuse the same two predicates, so the same gap would spread.
- **Expected**: `{:?}` of `ProducerConfig` never prints a value that Java would not print.
  - **Minimum:** also hide the two Java `Type.PASSWORD` SASL keys. Translate their constants (`SaslConfigs.java:188`, `:285`) onto `SaslConfigs` and include them in `SaslConfigs::is_password_config`. The predicate then mirrors all three `Type.PASSWORD` markings in `addClientSaslSupport`, not just the translated subset.
  - **Java-faithful:** also hide the value of any key the Rust producer does not recognise (the `configMapToRedactedString` rule). Alternatively, print only the keys of `originals`, which is the option plan §10.2 already lists. Either way, keys the Rust producer does recognise stay readable through their typed fields.
  - Fix the two rustdoc claims to match.
  - Add a regression test that puts a distinctive secret under at least one OAuth `Type.PASSWORD` key and one unknown key, and asserts both are absent from the output.
- **Actual**: Only the six translated SSL keys and `sasl.jaas.config` are hidden. `test_debug_redacts_password_configs` (`:1786`) sets only those seven secrets, so the gap is untested.

**Resolved in `868ea49d`** (a fixup of `11348821`, also fixing `5e28201b`). Per the Manager's decision, the fix is the key-set alternative that plan §10.2 already lists, not the "minimum":
- `impl fmt::Debug for ProducerConfig` renders `originals` as its sorted key set (a borrowed `BTreeSet<&str>` built in `fmt`), with no values at all. The impl's rustdoc now says why: Java never prints a raw `originals` value (`AbstractConfig.java:371-385`, `logAll()`, prints only parsed known values, with `Type.PASSWORD` values as `[hidden]`; `:390-395`, `logUnused()`, prints key names only), and every known non-secret value is already visible through the typed fields. It also says why hiding by key cannot match Java. The struct-level rustdoc matches.
- `SslConfigs::is_password_config` and `SaslConfigs::is_password_config` are removed, with their constant tables, both-directions tests and source-scan tests. Keys-only rendering left them with no caller. Both `impl` blocks are back to their text before `5e28201b`. `apply_ssl_config_key`'s rustdoc now cites the Java lines instead of the removed predicate. So both rustdoc claims the finding quoted are gone.
- The `ConsumerConfig` and `AdminClientConfig` comments now say that a future `originals` map must render as its key set, like `ProducerConfig`'s.
- `test_debug_redacts_password_configs` is replaced by `test_debug_hides_secrets_and_renders_originals_as_key_set`. The props carry the seven `Type.PASSWORD` keys this client parses, the two OAuth `Type.PASSWORD` keys, and `basic.auth.user.info`, each with a distinctive secret literal, plus `bootstrap.servers`. In both `{:?}` and `{:#?}` the test asserts:
  - no secret literal appears;
  - every key name appears;
  - `[hidden]` appears exactly seven times, once per typed `Password` field;
  - the `bootstrap.servers` value appears exactly once, through its typed field.
  The `{:?}` rendering must also end with the exact sorted key set. I teeth-checked it with three mutations: rendering the values, hiding every value, and dropping the key set. Each one turned the test red at the intended assertion. The field-order test is unchanged.

## Issue: `DescribeDelegationTokenResponseOptions` exposes token ids through its derived `Debug`
- **File**: `rust/src/common/requests/describe_delegation_token_response.rs` (`DescribeDelegationTokenResponseOptions<'a>`; lines 55–66 of `src/common/requests/describe_delegation_token_response.rs` at `58622a55`, before the merge moved the crate under `rust/`).
- **Origin**: the review of the pull request (an inline comment on this file at `58622a55`), relayed by the Manager as round 4 item 1 and verified against the tree.
- **Severity**: Behavior Mismatch.
  - Latent: nothing in the crate Debug-prints the options. The type was also crate-internal: `common::requests` has been `pub(crate)` (`rust/src/common/mod.rs:59`) since #209 (`383f3d30`), which came into the branch with merge `091f6f9d` before this round, so no user could print the ids. (Corrected in round 6, item 10: this line used to say that the type is public.)
  - It is the Phase 2 defect class in a Rust-only `*Options` type, the case plan §11.4 rule 1 names. §11.2 item 11 covered only the Create options.
- **Java Reference**:
  - `common/requests/DescribeDelegationTokenResponse.java:131-140`: `toString()` masks a copy of the data, setting each token's id to `"REDACTED"` and its HMAC to `new byte[0]`.
  - `common/security/token/delegation/DelegationToken.java:71-76`: `toString()` hides the HMAC (`[*******]`) but prints `tokenInformation`, whose `toString()` prints `tokenId`.
- **Description**: the struct was `#[derive(Debug, Clone, Copy)]` and holds `tokens: &'a [DelegationToken]`. Its `{:?}` rendered each token through `DelegationToken`'s `Debug`, which delegates to the `Display` mirroring Java's `toString()`, and so printed each token id in plain text (`tokenId='…'`). The response these options build renders neither the id nor the HMAC, so the options printed what the response itself hides.
- **Expected**: a hand-written `Debug`, exhaustively destructured like `CreateDelegationTokenResponseOptions`'s. The tokens render either as a count, or each with its id as `REDACTED` and its HMAC omitted, following the response's masking rather than `DelegationToken`'s. A regression test builds the options from a real `DelegationToken` and asserts both directions: the `{:?}` rendering holds neither the token id nor any HMAC byte, and non-secret fields such as the owner still appear.
- **Actual**: the derived `Debug`, and no test of it.

**Resolved in `21be3984`** (a fixup of `35d399fa`, answering the review comment on the pull request):
- The struct derives `Clone` and `Copy` only. Its new `Debug` destructures `Self` exhaustively, so a field added to the struct fails to compile until it is considered there.
  - `version`, `throttle_time_ms` and `error` render as the derive rendered them.
  - Each token renders field by field, as `DelegationToken { token_information: TokenInformation { owner, token_requester, renewers, issue_timestamp, max_timestamp, expiry_timestamp, token_id: "REDACTED" }, hmac: [] }`. This is the response's masking, and the `hmac: []` form matches `CreateDelegationTokenResponseOptions`.
  - The token fields are read through accessors, since they are private to their module, and neither secret is read at all. A field later added to `DelegationToken` or `TokenInformation` therefore stays hidden until it is listed.
  - `std::fmt::from_fn` builds the nested renderings, so no helper type was added (DoD #7) and nothing is allocated.
  - The struct's rustdoc now says that `Debug` renders no token's id or HMAC, and the impl's rustdoc cites both Java `toString()`s.
- The new test `options_debug_redacts_token_id_and_hmac` uses the file's `token(..)` helper, whose HMAC text contains the token id.
  - It first asserts that `DelegationToken`'s own `Debug` prints the id, so the leak it guards against is real.
  - It pins the exact `{:?}` rendering. In `{:#?}` it requires `token_id: "REDACTED",` and `hmac: [],`.
  - In both forms it asserts that neither the token id, nor the HMAC's byte-list `Debug`, nor its Base64 string appears. It also asserts that the owner (`"alice"`), the requester (`"requester"`) and the renewer (`"bob"`) still appear.
- Teeth-checked with four mutations, each restored afterwards:
  - restoring the derive;
  - rendering the real token id;
  - rendering the real HMAC;
  - dropping the owner.

  Each turned the test red at the exact-rendering assertion. With that assertion (and the two pretty-form checks) removed, the id and HMAC mutations still fail at "token id leaked" / "hmac leaked", and the owner mutation at "owner missing".

## Issue: `Password` is public, but Java declares its package unsupported, so CI's `check-public-audience` fails
- **File**:
  - `rust/src/common/config/mod.rs:23` (`pub mod types;`)
  - `rust/src/common/config/types/mod.rs:19` (`pub use password::Password;`)
  - `rust/src/common/config/types/password.rs:40` (`pub struct Password`)
- **Severity**: Missing Requirement (CLAUDE.md §2). It blocks CI.
- **Rule / Java reference**:
  - `CLAUDE.md:30` (§2): "Classes whose package contains the disclamer "This module is not a supported API"
    MUST NOT be public".
  - `common/config/types/package-info.java:19` carries that disclaimer at 4.3.1 (`26b251a4`) and at
    4.4.0-rc3 (`a6e87dfe`, the lint's `AUDIENCE_REF`): "This package is not a supported Kafka API; the
    implementation may change without warning between minor or patch releases."
- **Description**: the class does not meet all three §2 conditions, and the merge carried its visibility across unchanged.
  - `check-public-audience` (`audience_failures`, `rust/xtask/src/lint_custom.rs:1472`) requires all three §2
    conditions. Line 278 of `design/current/interface-audience-public-4.4.txt` satisfies only the third.
  - The disclaimer fails the second condition (`:1479`), and no line of `rust/xtask/public-audience-allowlist.txt` matches.
  - So the brief's premise ("stays `pub` legitimately: line 278") does not hold.
  - The merge kept the branch's pre-merge `pub mod types;` unchanged. Master had meanwhile added both this §2 rule and the
    lint.
- **Evidence (executed in a scratch copy outside the repository; the repository was not touched)**:
  - I rebuilt what the three Java-dependent rules read, as a throwaway git repo standing in for `kafka/`:
    - working tree: `clients/src/{main,test}/java` at `26b251a4`;
    - tag `4.3.1`: that same tree;
    - tag `4.4.0-rc3`: the same directories at `a6e87dfe`, the commit that tag dereferences to.

    That is the working tree plus `clients/src/main/java` at the two refs `fetch-java-refs` fetches.
  - I ran `xtask lint-custom` on `git archive` copies of HEAD and of master:
    - **HEAD `3e869523`: exit 1.** One finding:
      `src/common/config/types/password.rs: `crate::common::config::types::Password` is public but translates
      `org.apache.kafka.common.config.types.Password`: its package `common.config.types` is not a supported Kafka API`
    - **master `4eb87db1`: exit 0.** All six rules pass, so the merge introduces the failure.
  - On push, CI's Verify Rust job therefore fails at `lint`:
    - the job runs `fetch-java-refs` and then `make verify-rust` (`.semaphore/semaphore.yml:122-123`, `Makefile:253`);
    - `rust/xtask/src/main.rs:438` runs `lint-custom` first, so the clippy passes never run.
  - The local tracking ref `origin/fix/password-type-redaction` is at `58622a55`, so CI has not seen the merge yet.
- **Why `pub(crate)` loses nothing**:
  - No public item takes, returns or exposes `Password`. Every field holding one is `pub(crate)`, and no public
    signature names it; `unnameable_types` stays quiet with the fix.
  - Keeping it public only commits the crate to an API that Java calls unsupported.
  - Master already uses the same shape:
    - `common::security` is `pub`, with `pub(crate) mod authenticator;` / `pub(crate) mod ssl;`
      (`rust/src/common/security/mod.rs:18,20`);
    - `common::record` has `pub(crate) mod internal;` (`rust/src/common/record/mod.rs:27`).
- **Fix** (steps 1 and 2 were trial-applied to the scratch copy only):
  1. `rust/src/common/config/mod.rs:23`: `pub(crate) mod types;`.
  2. Turn the five intra-doc links from public docs into plain code spans. Otherwise `cargo doc` fails on
     `rustdoc::private_intra_doc_links`: `#![deny(warnings)]` at `rust/src/lib.rs:16` makes it an error, and CI's separate
     `doc-check (Linux amd64)` job would fail. The five links:
     - `rust/src/admin/user_scram_credential_upsertion.rs:31` (`[`Password::HIDDEN`]`)
     - `rust/src/common/config/ssl_configs.rs:129` (`[`Password`]`)
     - `rust/src/producer/producer_config.rs:59` (`[`Password`]`), `:350` (`[`Password`]`) and `:351` (`[`Password::HIDDEN`]`)

     Links in docs of crate-private items can stay: `ssl_configs.rs:229`, `sasl_configs.rs:91` and `:104`.
  3. Scratch-copy results with 1 + 2 applied and the Java input above:
     - `cargo xtask lint` passed end to end (exit 0): six of six `lint-custom` rules, doc hygiene, module-path
       hygiene and all clippy passes.
     - The rule counts equal master's: check-no-public-field 231, check-no-deprecated-translation 1324,
       check-public-audience 2331.
     - `doc-check` (`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`), `format-check` and
       the doctests (5 passed, 7 ignored) pass. So do the 58 lib tests matching `password` / `redact` / `debug_`.
     - With step 1 alone, `cargo doc` fails with exactly the five errors listed in step 2.
  4. Record the change in PLAN.md §12:
     - §12.1 (`PLAN.md:512`) says "The public path `common::config::types::Password` is unchanged";
     - §5 (`:248`) shows `+ pub mod types;`.

     The merge body says the same but is history, so leave it.
  - Not recommended: allow-listing `org.apache.kafka.common.config.types.Password`. It turns the lint green, but it
    contradicts §2's MUST NOT for no user-visible gain. It would need an explicit Manager or user decision.

**Resolved in `3d11391f`** (a fixup of `5e28201b`, answering Critic 85's round-4 item 1). Per the Manager's decision, the fix is the one above, with no allowlist entry:
- `rust/src/common/config/mod.rs:23` declares `pub(crate) mod types;`, as master declares `common::security::{authenticator, ssl}`. Inside the module, `Password` and its `HIDDEN`, `new` and `value` stay `pub` and keep their `doc(alias)` markers. The crate path `crate::common::config::types::Password` is unchanged, so no importer changed. The `types` module doc now says why the module is crate-private.
- The five public-doc links listed in step 2 are code spans now. The links in the docs of crate-private items stay: `ssl_configs.rs:229`, `sasl_configs.rs:91` and `:104`, `types/mod.rs:19`, and `Password`'s own docs. No other public doc links to `Password`.
- PLAN.md no longer calls `Password` public. The header notes the fix, §5's `types` line and §12.1's `Password` bullet are amended, and the new §12.4 records the cause, the fix and its verification. `design/current/structure.md`'s crate-private list now names `common::config::types`. `status.md` and `MILESTONES.md` make no claim about its visibility, so they are unchanged.
- Verified on the real tree: workspace 4469 / ffi 4533 / all-features (with `--skip __grpc`) 4756 passed, 0 failed. `format-check`, `check-generated` (200 files) and `cargo doc` with `-D warnings` are clean, and so are `lint-custom`'s three source-only rules, `doc-hygiene` and the three clippy passes.
- Verified with the Java sources, by the recipe above: a throwaway stand-in for `kafka/` holding 4.3.1 and 4.4.0-rc3, in copies of the tree outside the repository, with the submodule left uninitialised.
  - On `3d11391f` all six `lint-custom` rules pass: `check-no-data-carrying-enum-variants` 817, `check-no-public-field` 231, `check-java-name` 8378, `check-no-deprecated-translation` 1324, `check-public-audience` 2331 and `check-dyn-compatible` 22 items checked. The whole `cargo xtask lint` passes end to end.
  - Master `4eb87db1` gives the same counts, except `check-java-name`'s 8373. The branch's five more are `Password`'s three markers, its file and the `pub use` in `types/mod.rs`, which that rule checks although they are crate-private.
  - With only the module change, `cargo doc` fails with exactly the five link errors.

The entries below are round 6: the pull-request review, relayed by the Manager on 2026-10-12. Their line numbers are at `384b9c2a`, the user's merge of `origin/master` `ca5d4f3f`.

## Issue: `ConfigEntry`, `ConfigSynonym` and `AlterConfigOp` `Display` claim to mirror Java's `toString()` but print Rust `Debug` names
- **File**: `rust/src/admin/config_entry.rs:523-547` (`impl Display for ConfigEntry`), `:172-182`
  (`impl Display for ConfigSynonym`); `rust/src/admin/alter_config_op.rs:109-119` (`impl Display for
  AlterConfigOp`), test `:192`.
- **Origin**: PR #220 thread 4246667956 (pranavrth, on `config_entry.rs:526`).
- **Severity**: Behavior Mismatch. The output predates this PR, but this PR added the "Mirrors Java's
  `toString()`" comments and tests that lock the non-Java output in (`alter_config_op.rs:192` asserts
  `opType=Set`).
- **Java Reference**:
  - `clients/admin/ConfigEntry.java:183-193`: `toString()` concatenates `source`, `synonyms` (a
    `List<ConfigSynonym>`, so `[e1, e2]` through `AbstractCollection.toString()`) and `type`.
  - `ConfigEntry.java:199-225`: `ConfigType` and `ConfigSource` are plain enums with no `toString()`
    override, so they print their constant names (`DYNAMIC_BROKER_CONFIG`, `PASSWORD`, `UNKNOWN`, ...).
  - `ConfigEntry.java:285-291`: `ConfigSynonym.toString()` →
    `ConfigSynonym(name=a, value=b, source=DYNAMIC_BROKER_CONFIG)`.
  - `clients/admin/AlterConfigOp.java:118-124`; `OpType` (`:46-67`) has no `toString()` override, so
    `opType=SET`.
- **Description**: `ConfigEntry`'s `Display` formats `source={:?}`, `synonyms={:?}` and `type={:?}`. It
  prints `source=DynamicBrokerConfig`, `synonyms=[ConfigSynonym { name: "a", value: Some("b"), source:
  DynamicBrokerConfig }]` and `type=Password`. `ConfigSynonym`'s `Display` (`:176`) also formats
  `source={:?}`. `AlterConfigOp`'s `Display` prints `opType=Set`.
- **Expected**: the output is identical to Java's `toString()`.
  - `ConfigSource`, `ConfigType` and `OpType` render their Java constant names. Follow the crate
    precedent `rust/src/admin/transaction_state.rs:44-83` (private `name()` plus `impl Display`, the
    translation of Java's `Enum.toString()`).
  - Synonyms render as Java's list: `[`, each `ConfigSynonym` through its `Display`, joined by `, `,
    then `]`. An empty list is `[]`.
  - Tests assert exact Java strings. Cover an entry with at least one synonym and a non-default source
    and type. Update `alter_config_op.rs:192` to `opType=SET`, and check every other test that renders
    these types (`config.rs`, `config_entry.rs`, `mock_admin_client.rs`).
  - Scope: only these three types. The same `{:?}`-for-an-enum pattern exists in other admin `Display`
    impls (for example `scram_credential_info.rs:59`, `feature_update.rs:124`). Leave those alone, and
    list them as a follow-up in the PLAN.
- **Actual**: as described.

**Resolved in `e1ebf780`** (a fixup of `35d399fa`). The three `Display` impls now print what Java's `toString()` prints:
- `ConfigType`, `ConfigSource` and `OpType` gain a private `name()` that returns the Java constant name, and an `impl Display` over it. That is the translation of Java's `Enum.toString()`, after the precedent in `transaction_state.rs`.
- `ConfigSynonym`'s `Display` renders the source by its Java name. `ConfigEntry`'s renders the source and the type by their Java names, and the synonyms as Java's `AbstractCollection.toString()` renders a list (`[a, b]`, or `[]` when empty), each through `ConfigSynonym`'s `Display`. `AlterConfigOp`'s renders `opType` by its Java name.
- The tests assert exact Java strings: every `ConfigType`, `ConfigSource` and `OpType` constant, a `ConfigSynonym` with and without a value, an entry with two synonyms and a non-default source and type, the defaults and a sensitive entry, and a `DELETE` operation. The `AlterConfigOp` redaction test now asserts the exact string, with `opType=SET`.
- Teeth-checked with nine mutations, each of which turned the tests red: rendering the entry's source, the entry's type, a synonym's source or the op type with `{:?}`; joining the synonyms without the space; dropping the list's brackets; and the Rust spelling of `PASSWORD`, `DEFAULT_CONFIG` and `DELETE`.
- The other admin `Display` impls that render an enum, an `Option` or a collection with `{:?}` are left as they are, per the Manager's scope. Plan §13.3 lists them.

## Issue: `AlterUserScramCredentialsRequest` and `IncrementalAlterConfigsRequest` `Display` print counts; Java prints the masked data
- **File**: `rust/src/common/requests/alter_user_scram_credentials_request.rs:128-138` (tests `:299`,
  `:314`); `rust/src/common/requests/incremental_alter_configs_request.rs:124-135` (tests `:260`, `:272`).
- **Origin**: PR #220 thread 4246667956 ("the SCRAM request comment at line 129 (we print counts, Java
  prints the masked data)"). `IncrementalAlterConfigsRequest` has the same defect; the Manager found it
  while verifying the thread.
- **Severity**: Behavior Mismatch. The comments claim a match, and the tests lock the counts in.
- **Java Reference**:
  - `common/requests/AlterUserScramCredentialsRequest.java:85-98`: `toString()` returns `maskData(data)`,
    the generated `toString()` of a copy whose salts and salted passwords are emptied.
  - `common/requests/IncrementalAlterConfigsRequest.java:111-124`: `toString()` returns `maskData(data)`,
    a copy whose config values are all `"REDACTED"`.
- **Description**: both `Display` impls print `...Request(version=0, deletions=1, upsertions=1)` /
  `...Request(version=1, resources=2)`. Each file already has a `mask_data` (`:111-125` and `:105-121`)
  that its Builder's `Display` renders.
- **Expected**: each request's `Display` renders its masked data, in the crate's request shape that the
  sibling requests in this PR use (`renew_delegation_token_request.rs:98-108`,
  `expire_delegation_token_request.rs:117-127`: `XxxRequest(version={}, data={:?})` of `mask_data`).
  Fix the comments. Rework the tests to assert three things:
  - the masked fields are empty or `REDACTED`;
  - the non-secret fields (user name, mechanism, iterations, resource and config names) are present;
  - no secret literal appears.
  The `network_client.rs` test (`:2797`) compares against `sent.to_string()`, and must keep passing.
- **Actual**: as described.

**Resolved in `d15326ad`** (a fixup of `35d399fa`). Both requests' `Display` renders the masked data, as Java's `toString()` renders `maskData(data)`:
- `AlterUserScramCredentialsRequest` and `IncrementalAlterConfigsRequest` render `XxxRequest(version=…, data=…)` of their `mask_data`, the shape the renew- and expire-delegation-token requests use. The salts and salted passwords render empty, and every config value, the null one of a deleted config included, renders as `"REDACTED"`. The user names, mechanisms, iterations, resource names and config names still show.
- The comments cite the Java `toString()`, and each `mask_data` doc says that the request's `toString()` and `Builder.toString()` share it, as in Java.
- Each request's `Display` test asserts the exact rendering against the masked copy of its fixture, that the masked fields are empty or `REDACTED`, that the other fields show, and that no secret literal appears. The `Debug` tests assert the same in `{:?}` and `{:#?}`.
- Teeth-checked with nine mutations, each of which turned the tests red. For SCRAM: rendering the unmasked data, the old count form or no `version=`, keeping the salted password, and masking the user name too. For incremental alter configs: rendering the unmasked data or the count form, skipping the null config value, and masking the resource name too.
- This is the follow-up that Critic 85's round 3 offered ("Point 4" of `critic-85-final-review.md`).

## Issue: `NetworkClient` — stale `allow(dead_code)` comment; the redaction test checks the helper, not the logged message
- **File**: `rust/src/network_client.rs:67-91` (`loggable_request` and its doc), `:1203-1231` (the two
  log lines), `:2783-2797` (the test and its doc).
- **Origin**: PR #220 thread 4246667960 (pranavrth).
- **Severity**: Test Gap; stale comment.
- **Java Reference**: `clients/NetworkClient.java:403-416` (the cancelled-in-flight DEBUG / INFO lines).
- **Description**:
  - The doc at `:82-84` says the `deny(dead_code)` "overrides this module's blanket `allow(dead_code)`".
    Master changed `:15` to `#![cfg_attr(not(test), expect(dead_code))]`.
  - The test asserts on `loggable_request(...)`, not on the message the log line formats. If someone
    puts `{:?}` back into the log line, every test still passes; only the non-test build fails, on dead
    code.
- **Expected**: one private function builds the whole DEBUG message (every field of Java's line, with
  the request rendered through its redacting `Display`). The `kafka_debug!` line calls it, and the test
  asserts on its output: the exact message for a known in-flight request, and no secret literal.
  - Keep the call inside the `log_enabled!(Debug)` branch, so the message is built only when DEBUG is on.
  - Make both doc comments accurate about the module's `expect(dead_code)` and any item-level `deny`.
  - Teeth check: switching the request to `{:?}` inside the new function must turn the test red.
- **Actual**: as described.

**Resolved in `e42bb1ed`** (a fixup of `35d399fa`). One private function builds the whole DEBUG message, and the tests assert on its output:
- `cancelled_in_flight_request_debug_message` replaces `loggable_request`. It builds the message of `NetworkClient.java:405-409`: every field of Java's line, in Java's order, with the request rendered through its redacting `Display` and a missing request as `null`. It returns a lazily formatted `Display` (`std::fmt::from_fn`), so it allocates nothing.
- The `kafka_debug!` line calls it, still inside the `log_enabled!(Debug)` branch, so the message is built only when DEBUG is on. The INFO line is unchanged.
- The doc describes the module's `#![cfg_attr(not(test), expect(dead_code))]`, which would let the function fall out of use in a non-test build without a word, and the item-level `deny(dead_code)` that overrides it. Checked: with the log line no longer calling the function, `cargo check --lib` fails on the unused function; with the `deny` removed as well, the same change builds.
- `test_cancelled_in_flight_request_debug_message_redacts_request` sends a SCRAM upsertion with correlation id 7, created at 100 and sent at 250, with 40ms of throttle time and a 30000ms timeout. It asserts the exact message at 1000, where every number is distinct and so pinned to its place; that no secret literal appears; and that a disconnect then cancels the request. A second test asserts the exact message, ending in `null`, for an in-flight request built like `InFlightRequestsTest.addRequest`'s (`InFlightRequestsTest.java:122-124`).
- Teeth-checked with eight mutations, each of which turned the tests red: rendering the request with `{:?}` (the item's own check), rendering the `Option` with `{:?}`, printing the unmasked data, swapping the correlation id and the node, swapping the two elapsed times, dropping the throttle time, swapping the throttle time and the timeout, and rendering a missing request as `None`. A second pass removed the exact assertion: the no-secret check alone still catches the unmasked data.

## Issue: `UserScramCredentialUpsertion`'s `Debug` prints `"[hidden]"` in quotes
- **File**: `rust/src/admin/user_scram_credential_upsertion.rs:100-111`; test `:178`.
- **Origin**: PR #220 thread 4246667975 (pranavrth, on `:108`, the salt). The password field on `:109`
  has the same defect (found by the Manager).
- **Severity**: Minor Behavior Mismatch. It is inconsistent with `Password`'s own rendering.
- **Description**: `.field("salt", &Password::HIDDEN)` debug-formats a `&str`, so it prints
  `salt: "[hidden]"`. `Password`'s `Debug` prints `[hidden]` with no quotes
  (`rust/src/common/config/types/password.rs:78-86`). The test at `:178` locks in the quotes for both
  fields.
- **Expected**: `&format_args!("{}", Password::HIDDEN)` for both `salt` and `password`. The test asserts
  `salt: [hidden], password: [hidden]` without quotes, in both `{:?}` and `{:#?}`.
- **Actual**: as described.

**Resolved in `fd48956e` and `937ea026`** (fixups of `11348821`). Both fields render `&format_args!("{}", Password::HIDDEN)`, so each reads `[hidden]` with no quotes, as a `Password`'s own `Debug` does. The impl's doc says so.
- The test asserts the exact `{:?}` rendering with `salt: [hidden], password: [hidden]`; that `{:#?}` ends with the two fields unquoted; that `[hidden]` appears twice and `"[hidden]"` never; and that no secret literal appears in either form.
- Teeth-checked with four mutations, each of which turned the test red: quoting both fields again, quoting only the salt, quoting only the password, and rendering the real salt. A second pass removed the exact assertions: the remaining checks still catch quoting both fields (the check that `"[hidden]"` never appears) and the real salt (the byte-list check).
- `937ea026` turns the two intra-doc links that `fd48956e` added to the impl's doc, to `Password` and `Password::HIDDEN`, into code spans. `Password` is crate-private (plan §12.4), so rustdoc's `private_intra_doc_links` lint rejected them under `-D warnings`, which CI's doc-check job runs. Item 7's doc check found them. Checked on that commit: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features` and `make doc-check` passed.

## Issue: the delegation-token response tests skip Java's `hmac=[]` assertion and the two-token count
- **File**: `rust/src/common/requests/create_delegation_token_response.rs` (the `Display` test ending at
  `:496`, and `debug_redacts_token_id_and_hmac`); `rust/src/common/requests/describe_delegation_token_response.rs:467-505`.
- **Origin**: PR #220 thread 4246667982 (pranavrth).
- **Severity**: Test Gap (DoD #3: Java's assertions are translated in full).
- **Java Reference**: `clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java`:
  - `:3185-3201`, `createCreateTokenResponse`: asserts `tokenId='REDACTED'` and `hmac=[]`.
  - `:3241-3265`, `createDescribeTokenResponse`: two tokens; asserts exactly two `tokenId='REDACTED'` and
    exactly two `hmac=[]`.
- **Description**: the Rust tests check only that `REDACTED` appears and that the secret literal does
  not. They never check that the HMAC renders empty, and the describe test uses one token.
- **Expected**:
  - The create test asserts the masked id and the empty HMAC in the exact Rust rendering.
  - The describe test uses two tokens and asserts that each marker appears exactly twice.
  - Both tests cite the Java tests.
- **Actual**: as described.

**Resolved in `a8cf51fe`** (a fixup of `11348821`). Both tests translate Java's rendering assertions in full:
- Create (`RequestResponseTest.createCreateTokenResponse`, `:3185-3201`): the rendering holds exactly one `token_id: "REDACTED"` and one `hmac: []`, the Rust form of Java's `tokenId='REDACTED'` and `hmac=[]`. It holds neither the token id nor the HMAC, as text or as its byte list.
- Describe (`createDescribeTokenResponse`, `:3241-3265`): two tokens, exactly two of each marker, and neither token's id nor HMAC.
- Each `Display` test cites its Java test, and each `Debug` test cites it as its counterpart. The `Debug` tests run in `{:?}` and `{:#?}`.
- Teeth-checked with six mutations, each of which turned the tests red: for create, keeping the HMAC, setting it to a non-empty value, and masking the id with another word; for describe, masking only the first token, keeping the HMACs, and setting them to a non-empty value.

## Issue: `config.rs` tests say "no Java original", but `ConfigTest.shouldImplementToStringProperly` exists and is not translated
- **File**: `rust/src/admin/config.rs:119-131`, `:132-145`.
- **Origin**: PR #220 thread 4246667981 (pranavrth).
- **Severity**: Test Gap; wrong citation.
- **Java Reference**: `clients/src/test/java/org/apache/kafka/clients/admin/ConfigTest.java:77-81`
  asserts that `config.toString()` contains `E1.toString()` and `E2.toString()`.
- **Expected**: translate Java's two assertions (the rendering contains each entry's `to_string()`) into
  the `Display` test, and cite `ConfigTest.shouldImplementToStringProperly`. The `Debug` test cites it
  as its `Debug` counterpart.
  - The other `ConfigTest` methods (`shouldGetEntry` ... `testEquals`) are untranslated too, but that
    gap predates this PR (Milestone 11). Do not translate them here; list the gap as a follow-up in the
    PLAN.
- **Actual**: as described.

**Resolved in `a20fa3cb`** (a fixup of `35d399fa`). `ConfigTest.shouldImplementToStringProperly` (`ConfigTest.java:77-81`) is translated:
- The `Display` test makes its two assertions: the rendering contains each entry's `to_string()`. Java's fixture holds two plain entries; this one keeps its sensitive entry, so the test still shows that the secret is redacted. The doc cites the Java test.
- The `Debug` test cites it as its `Debug` counterpart, and asserts in `{:?}` and `{:#?}` that the rendering contains every entry's `to_string()`.
- Teeth-checked with three mutations, each of which turned the tests red: rendering each entry by its name only, dropping an entry from the `Display`, and making `ConfigEntry`'s `Debug` stop delegating to its `Display`.
- The other `ConfigTest` methods are untranslated too, a gap from Milestone 11. Per the Manager's scope they are left; plan §13.3 lists them.

## Issue: `SaslConfigs`'s `password` doc says "set programmatically"
- **File**: `rust/src/common/config/sasl_configs.rs:100-105`.
- **Origin**: PR #220 thread 4246667966 (pranavrth).
- **Severity**: Documentation.
- **Description**: since #209 (`383f3d30`) the field is `pub(crate)`. Only tests set it; non-test code
  only defaults it to `None` (`:115`).
- **Expected**: "Rust-only. No config key is parsed into it; tests set it directly." (the reviewer's
  wording). Keep the `[hidden]` and `resolve_password` sentences.
- **Actual**: as described.

**Resolved in `937dad94`** (a fixup of `5e28201b`). The doc now reads "Rust-only. No config key is parsed into it; tests set it directly.", the reviewer's wording, and keeps the `[hidden]` and `resolve_password` sentences.
- Checked: outside the test modules, only `SaslConfigs::default()` writes the field (`password: None`).
- Doc-only, so there is no test to teeth-check.

## Issue: `status.md` and `MILESTONES.md` say `UserScramCredentialUpsertion` delegates `Debug` to `Display`; "two options types"; `sasl.password` called a key
- **File**: `design/current/status.md:10-13`; `design/history/MILESTONES.md:73`.
- **Origin**: PR #220 threads 4246667936 (pranavrth), 4182501519 and 4182501591 (Copilot).
- **Severity**: Documentation.
- **Description**:
  - `UserScramCredentialUpsertion` has no `Display`, because Java has no `toString()` there. Its `Debug`
    is hand-written and hides the salt and password (`user_scram_credential_upsertion.rs:97-111`). Only
    `DelegationToken` and the six request / response wrappers delegate `Debug` to `Display`.
  - `MILESTONES.md:73` says "two Rust-only options types". There are three: `ConfigEntryOptions`,
    `CreateDelegationTokenResponseOptions`, and `DescribeDelegationTokenResponseOptions` (round 4).
  - It also lists "the Rust-only `sasl.password`" with the config keys, but no key is parsed into that
    field.
- **Expected**: describe the two mechanisms separately in both files, say "three", and call it the
  Rust-only `SaslConfigs` `password` field. Re-check every other count and claim in both paragraphs
  against the tree.
- **Actual**: as described.

**Resolved in `4f58998c`** (a fixup of `a963fda2`, which wrote both paragraphs; `58622a55` added the Phase 2 sentence). Both paragraphs now match the tree:
- They describe the two mechanisms apart. `DelegationToken` and the six SASL / delegation-token request and response wrappers delegate `Debug` to their redacting `Display`. `UserScramCredentialUpsertion` has no `Display`, as Java has no `toString()` there, and hides its salt and password in a hand-written `Debug`.
- `password` is called what it is: the Rust-only `SaslConfigs` field, which no config key is parsed into.
- `MILESTONES.md` counts three Rust-only options types and names them: `ConfigEntryOptions`, `CreateDelegationTokenResponseOptions` and, since round 4, `DescribeDelegationTokenResponseOptions`. `status.md` gives the count without the names.
- Every other claim in the two paragraphs was re-checked against the tree; the commit body lists them. `MILESTONES.md` now also says that Java's two OAuth `PASSWORD` keys are not translated.
- The milestone keeps the number 15 in this commit; item 8 renumbers it.

## Issue: agent memory — one machine's setup in a public repo; the `--no-verify` guidance must require the user's OK
- **File**: `.claude/agent-memory/actor-executor/m15_merge_review_followup_notes.md:20-23` and
  `:116-128`; `.claude/agent-memory/kafka-critic/review_lint_java_rules_repro.md:27-32`;
  `.claude/agent-memory/actor-executor/commit_hook_timeout.md`,
  `.claude/agent-memory/actor-executor/workflow_docker_wedged_no_verify.md`, and their index lines
  `actor-executor/MEMORY.md:167`, `:179`.
- **Origin**: PR #220 thread 4246667949 (pranavrth, not blocking). The `--no-verify` decision is the
  user's (2026-10-12).
- **Severity**: Process; Documentation.
- **Expected**:
  - Drop what describes one machine or person: the "~61 GB" root `target/` and the `.r2-rules/` exclude
    advice; the "Shell gotchas (zsh and the BSD userland on this Mac)" list; the "Shell traps" list in
    `review_lint_java_rules_repro.md`, including "`g` is a zsh alias in this user's shell" and "`grep` is
    ugrep here". Also drop the "Attribution" paragraph (`:125-128`): it records one session's harness
    mismatch, not reusable guidance. A portable fact may stay only if phrased generically, for example
    "after the `rust/` move, a pre-move root `target/` is not ignored".
  - Rewrite `commit_hook_timeout.md`, `workflow_docker_wedged_no_verify.md` and their two index lines so
    an agent never uses `--no-verify` on its own. It must first ask the user (through the Manager when
    it runs as a subagent), and must say which checks it still owes. Keep the useful facts: what the
    hook runs, that it takes about 10 minutes, to run the commit in the background, and that it needs
    Docker.
  - Fixup the commits that added or extended these files.
- **Actual**: as described.

**Resolved in `ce1140d8`, `9a99cdea` and `9bbd226a`** (fixups of `354f8838`, `3e869523` and `afbef61f`, the branch's commits that wrote or extended these files). The memory keeps the reusable facts and drops one machine's setup:
- `ce1140d8` rewrites `commit_hook_timeout.md` and `workflow_docker_wedged_no_verify.md` so that an agent never commits with `--no-verify` on its own. It stops and asks the user, through the Manager when it runs as a subagent, and says which of the hook's checks it ran and which are still owed. Only the user's own OK allows the bypass.
  - They keep what the hook runs, that it takes about 10 minutes and needs Docker, how to run the commit in the background, the re-run-then-retry rule for a test the change cannot affect, and the `hooksPath` caveat.
  - The dated count of orphaned containers and volumes is replaced by a generic description.
  - The two `MEMORY.md` index lines say the same, and the two notes that pointed at the bypass (`python_bindings_local_build_test_macos.md`, `python_soak_client_notes.md`) now point at the rule.
  - `python_bindings_local_build_test_macos.md` also drops its one-machine wording about the Docker multilanguage harness: "(avoid — it has wedged this machine)" now reads "(avoid running it locally: it can wedge Docker)".
- `9a99cdea`: `m15_merge_review_followup_notes.md` drops the cache size, the `.git/info/exclude` advice, the zsh and BSD shell-gotcha list and the "Attribution" paragraph. It keeps the portable fact: after the `rust/` move, a pre-move root `target/` is not ignored.
- `9bbd226a`: `kafka-critic/review_lint_java_rules_repro.md` drops its "Shell traps hit while doing this" list, which `afbef61f` added and `3e444252` extended. The recipe and its round-5 section are unchanged.
- The user's decisions on the hook (2026-10-12) are in plan §13.4, and §13.5 records how each commit of this round was verified. Seven older memory files still describe one machine; plan §13.3 lists them as a follow-up.
