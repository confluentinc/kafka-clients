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
  - Latent: nothing in the crate Debug-prints the options. The type is public, though, so a user's `{:?}` would print the ids.
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
