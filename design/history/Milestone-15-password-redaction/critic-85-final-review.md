# Critic 85: final review record for `5e28201b`, `11348821` and fixup `868ea49d` (branch `fix/password-type-redaction`)

Java references point to Apache Kafka at `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` (the commit the plan pins), under `clients/src/main/java/org/apache/kafka/`.

The one in-scope issue was resolved in `868ea49d` and moved to `COMMENTS.DONE.85.md`. No in-scope issue is open.

## Out of phase scope — candidates for Phase 2 (not to be fixed in this phase)

I checked each line independently against the Java `toString()` at the pinned commit.

The first three are **live, not latent**. At DEBUG level they print whenever a request of that type is still in flight and one of these happens:
- the broker connection drops;
- the request times out;
- the node is disconnected or closed.

The same log path is why the plan's §2 "latent" claim was wrong for `RenewDelegationTokenRequest` and `ExpireDelegationTokenRequest`. This phase's delegating `Debug` closed those two.

- **LIVE, the log sink:** `src/network_client.rs:1161-1175` formats the in-flight request with `{:?}`. It is reached from request timeouts (`:991` → `:1018` → `:1121`), `disconnect` (`:1534`) and `close_connection` (`:1543`). Java logs `request.request` through its redacting `toString()` (`NetworkClient.java:405-409`). Rendering it through the `Display` translation would neutralise the two items below in one place.
- **LIVE (via the sink above):** `AlterUserScramCredentialsRequest` (`src/common/requests/alter_user_scram_credentials_request.rs:34`, `#[derive(Debug, Clone)]`). `{:?}` prints each upsertion's `salt` and `salted_password` bytes, and for SCRAM the salted password is password-equivalent. I confirmed this with a scratch binary: `{:?}` of `Some(ConcreteRequest::AlterUserScramCredentials(..))` contained both byte sequences, while the `Display` used by the send log printed only counts. Java masks both in `toString()` via `maskData` (`AlterUserScramCredentialsRequest.java:85-91`, `:96-97`).
- **LIVE (via the sink above):** `IncrementalAlterConfigsRequest` (`src/common/requests/incremental_alter_configs_request.rs:31`, `#[derive(Debug, Clone)]`). `{:?}` prints every config value, for example a dynamic listener's `ssl.keystore.password` or `sasl.jaas.config` while it is being altered. Java's `toString()` replaces each value with `"REDACTED"` (`IncrementalAlterConfigsRequest.java:111-118`, `:122-123`).
- `AlterUserScramCredentialsRequestBuilder` (`alter_user_scram_credentials_request.rs:123`): derived `Debug` over `data`. Java's `Builder.toString()` masks via `maskData` (`AlterUserScramCredentialsRequest.java:45-46`). Latent: builders are held as `Box<dyn RequestBuilder>`, and that trait has no `Debug` bound.
- `IncrementalAlterConfigsRequestBuilder` (`incremental_alter_configs_request.rs:122`): derived `Debug` over `data`. Java's `Builder.toString()` masks via `maskData` (`IncrementalAlterConfigsRequest.java:74-75`). Latent.
- `SaslAuthenticateRequestBuilder` (`sasl_authenticate_request.rs:119`): derived `Debug` over `data.auth_bytes`. Its `Display` (`:158`) already matches Java's `Builder.toString()`, which returns `"(type=SaslAuthenticateRequest)"` (`SaslAuthenticateRequest.java:49-50`). Delegating `Debug` to that `Display` fixes it. Latent.
- `RenewDelegationTokenRequestBuilder` (`renew_delegation_token_request.rs:103`): derived `Debug` over `data.hmac`, and no `Display`. Java's `Builder.toString()` masks the HMAC via `maskData` (`RenewDelegationTokenRequest.java:66-67`, `:71-74`). Latent.
- `ExpireDelegationTokenRequestBuilder` (`expire_delegation_token_request.rs:120`): same shape as the Renew builder. Java's `Builder.toString()` masks the HMAC via `maskData` (`ExpireDelegationTokenRequest.java:76-77`, `:81-84`). Latent.
- `ConfigEntry` (`src/admin/config_entry.rs:180`): derived `Debug`, while its `Display` (`:455`) redacts when `is_sensitive`. Java's `toString()` prints `value=Redacted` for a sensitive entry (`ConfigEntry.java:183-186`). The derived `Debug` of `Config` (`src/admin/config.rs:26`) and of `AlterConfigOp` (`src/admin/alter_config_op.rs:75`) also reach it; Java's `Config.toString()` (`Config.java:74-75`) and `AlterConfigOp.toString()` (`AlterConfigOp.java:119-124`) go through the redacting `ConfigEntry.toString()`. Latent.
- `ConfigEntryOptions` (`config_entry.rs:208`): a Rust-only options type with derived `Debug` over `value` and `is_sensitive`. It has no Java counterpart: Java never prints these constructor parameters, and `ConfigEntry.toString()` hides a sensitive value (`ConfigEntry.java:183-186`). Latent.
- `CreateDelegationTokenResponseOptions` (`src/common/requests/create_delegation_token_response.rs:58`): a Rust-only options type with derived `Debug` over `token_id` and `hmac`. It has no Java counterpart, and Java's `CreateDelegationTokenResponse.toString()` redacts both (`CreateDelegationTokenResponse.java:109-113`). Latent.
- `MockAdminClient` (`src/admin/mock_admin_client.rs:171`): derived `Debug`. Its `State` (`:113`) holds the mock's broker, client-metrics, group and topic config values unredacted. Java's `MockAdminClient` defines no `toString()`, so Java has no way to print them; the plan applied the same reasoning to `UserScramCredentialUpsertion`. Latent, a test helper, lowest priority.

Checked and not candidates:
- The generated `*Data` types: Java's generated `toString()` prints the secret bytes too, which is why the Java wrappers mask.
- `DescribeDelegationTokenResponseOptions`: its `DelegationToken`s now redact.
- The NetworkClient send log (`src/network_client.rs:713-721`): it already uses `Display`.
- `NewTopic`'s `Display`: Java prints the configs too.

## Round 2: review of `868ea49d` (fixup of `11348821`, also fixing `5e28201b`)

No new issue. No `## Issue` entry is open in this file.

### Verified correct
- **No `originals` value can reach `{:?}` or `{:#?}`.**
  - In `impl fmt::Debug for ProducerConfig` (`src/producer/producer_config.rs:338-433`), the only use of `originals` is `originals.keys()` (`:387`), rendered as a borrowed `BTreeSet<&str>`.
  - `ProducerConfig` has no other formatting impl.
  - The rustdoc's claim that every known non-secret value is visible through the typed fields holds. Every arm of the `match` in `new()` (`:549-735`) stores into a typed field. Unknown keys and unknown `ssl.*` keys are only logged (`:732-734`, `src/common/config/ssl_configs.rs:269-271`).
- **The rustdoc's Java citations match the pinned source.**
  - `AbstractConfig.java:118`: `values = definition.parse(originals)`.
  - `:371-385`: `logAll()` iterates `TreeMap(values)`.
  - `:390-395`: `logUnused()` prints key names.
- **The predicates are fully removed.**
  - No `is_password_config`, constant table or their tests remain anywhere under `src/`, `tests/`, `generator/` or `xtask/`.
  - The `impl SslConfigs` and `impl SaslConfigs` blocks are byte-identical to `master`.
  - The branch adds no `allow(dead_code)`. Both files already carry `#![allow(dead_code)]` at `:15` on `master`, so the commit's reason for deleting the predicates rather than keeping them holds.
  - `test_debug_redacts_password_fields` and `test_debug_redacts_jaas_config_and_password` are intact and pass.
- **The new test catches regressions.** I ran it on a copy of `868ea49d` outside the repo, made with `git archive` and built into its own target directory; the working tree was not touched.
  - **Unmutated:** passes.
  - **M1**, the round-1 behaviour restored (hide the seven parsed `Type.PASSWORD` keys by name, print every other value): fails at the leak assertion, with "the value of sasl.oauthbearer.client.credentials.client.secret leaked".
  - **M2**, the key set rendered unsorted (`HashSet<&str>`): fails at the `ends_with(expected_originals)` assertion.
  - The field-order test passes under M1 and still checks all 39 fields.
- **The `[hidden]` count of 7 is right for the test's input.**
  - Eight `Option<Password>` fields are reachable from `ProducerConfig`: six in `SslConfig`, plus `SaslConfig::jaas_config` and `SaslConfig::password`.
  - The props can set only seven of them. `SaslConfig::password` has no config key and renders `None`.
  - The `{:?}` tail assertion depends only on `originals` being the last field and on `BTreeSet`'s `Debug` format, not on `Password`'s.
  - The `Some([hidden])` assertions depend on `Password`'s `Debug` being exactly `[hidden]`, which `password.rs` pins.
- **No regressions from the earlier commits.**
  - The fixup touches only five files.
  - All 24 Debug/Display redaction tests in the tree (matched by name) pass.
  - The `ConsumerConfig` and `AdminClientConfig` edits are comment-only, and neither struct has a map field.
- **The internal-identifier grep is clean.** The brief's case-insensitive grep for tracker keys and internal URLs returns 0 on the branch diff, 0 on the commit messages, and 0 on both COMMENTS files.
- **Reruns on the committed tree (`868ea49d`):**
  - `cargo test --workspace`: 4373 passed, 0 failed, 9 ignored.
  - `cargo xtask lint`: clean.
  - `cargo xtask format-check`: clean.

### Notes (no Actor action)
- **Wording, optional.**
  - The test comment says "`[hidden]` comes from the seven typed `Password` fields only". The DONE note says "once per typed `Password` field", and the commit message says "(the typed Password fields)".
  - Both read as if `ProducerConfig` had exactly seven typed `Password` fields. It has eight, and the props can set seven of them (see above).
  - The assertion itself is correct, so this does not need a fixup.
- **For the Manager's §9.4 handoff.** `design/history/Milestone-15-password-redaction/PLAN.md` still describes the by-key design that was replaced:
  - §4.3 (its title and the predicates), §5, §6 step 3, §7.2, §8 item 7 and §9.3 describe the `is_password_config` predicates.
  - §10.2 still says "The by-key form is closer to Java's `logAll()` behaviour", which round 1 refuted.
  - A future reviewer checking the branch against the plan would report the missing predicates as undelivered.
  - A one-line amendment at §4.3 and §10.2 pointing at the key-set decision would prevent that. `cbe48f20` ("correct PLAN §9.1 and mark it fixed") is the precedent.

## Suggested CLAUDE.md / rules updates

Neither `CLAUDE.md` nor any file under `.claude/rules/` mentions `toString`, `Display`, `Debug` or redaction (checked with grep). The rule this milestone enforces exists only in its plan (§3.3), so the next translation of a class whose Java `toString()` redacts has nothing to follow. Each suggestion below is backed by something this milestone found. `COMMENTS.FP.md` and `COMMENTS.FN.md` do not exist, so there is no reviewer feedback to fold in.

1. **Java `toString()` redaction must carry over to Rust `Debug`.** For CLAUDE.md §2, beside the other Java-to-Rust mappings, or a new `.claude/rules/` file if the Manager prefers.
   - **What this milestone found:**
     - The `Display` impls translated Java's redacting `toString()` faithfully, for example on `SaslAuthenticateRequest` and `DelegationToken`. But a `#[derive(Debug)]` next to them printed the secrets.
     - The Phase 2 list above still holds eight types whose Java `toString()` masks while Rust derives `Debug`, five of them builders. It also holds two Rust-only options types that carry the same values.
   - **Proposed text:** "Java `toString()` → Rust `Display`. Where the Java `toString()` masks, omits or replaces a value (`Password.HIDDEN`, `maskData(...)`, `"REDACTED"`, `Redacted`, `(redacted)`), the Rust type MUST NOT derive `Debug`. Delegate `Debug` to that `Display` (`fmt::Display::fmt(self, f)`), or hand-write it with the same masking. This includes nested `Builder` classes and Rust-only types (e.g. `*Options` structs) that carry the same values. A key that Java defines as `ConfigDef.Type.PASSWORD` is held as `Option<Password>`, never `Option<String>`."
2. **Log lines use `{}` where Java logs `toString()`.** Place it beside rule 1.
   - **What this milestone found:**
     - `src/network_client.rs:1161-1175` formats the in-flight request with `{:?}`, while Java logs the same value through `toString()` (`NetworkClient.java:405-409`).
     - `ConcreteRequest`'s `Debug` forwards to each variant's derived `Debug`. So at DEBUG level, a disconnect or timeout with such a request in flight prints its secrets.
     - This is why the plan's "latent" claim was wrong for two request types in round 1.
     - The send log at `:713-721` already uses `{}`.
   - **Proposed text:** "A translated log line formats a value the way the Java line does. Where Java relies on `toString()` (a `{}` placeholder bound to a request, response, builder, config or credential-bearing object), Rust uses `{}`, the `Display` translation, not `{:?}`."
3. **A raw user-property map renders as its key set.** One sentence appended to rule 1.
   - **What this milestone found:** in round 1, the plan's by-key filter over `originals` leaked two kinds of value: Java's two OAuth `Type.PASSWORD` keys, which this client does not parse, and every unknown key. Java prints no raw `originals` value (`AbstractConfig.java:118`, `:371-385`, `:390-395`).
   - Today the rule lives only in comments on `ConsumerConfig` and `AdminClientConfig` and in `ProducerConfig`'s rustdoc.
   - **Proposed text:** "A struct holding Java's `originals()` (the raw user properties) renders that map in `Debug` as its key set only: the map holds keys this client does not parse, so no per-key filter can match what Java hides."
