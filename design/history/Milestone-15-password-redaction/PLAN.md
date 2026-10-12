# Milestone 15 — `Password` type and secret redaction

**Status:** COMPLETE (2026-09-29) on the branch below; the user opens the PR. Phase 1 done via
Actor/Critic 85: Commit A `5e28201b`, Commit B `11348821`, fixup `868ea49d` after one Critic finding
(§4.3 amendment, §10.2, `COMMENTS.DONE.85.md` in this directory). **Phase 2 (§11) COMPLETE 2026-09-30**
(commit `35d399fa`, Actor/Critic 85, same branch). **`master` merged and the pull-request review
answered (§12) 2026-10-02** (merge `091f6f9d`, fixup `21be3984`, Actor 85; the branch is pushed and
its pull request is open). **Critic 85's round-4 blocker fixed (§12.4) 2026-10-02:** `Password` is
crate-private (fixup `3d11391f`).
**Origin:** security code-review finding (severity Low): secret-bearing structs derive `Debug`.
**Baseline:** `master` at `b76de2e1`. Java reference: Apache Kafka 4.3.1 — the `kafka/` submodule
is pinned at `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` but is **not checked out in this clone** (§10.4).
**Scope:** Rust only. No C FFI / Python / gRPC change is needed (§3.4).
**Agent numbers:** 85 (Actor 85 / Critic 85). Highest number used anywhere in history is 84.
**Branch:** `fix/password-type-redaction` off `master`. The Actor commits there; nobody pushes
or opens a PR until the user says so.

---

## 1. What this milestone delivers

The finding reports that secret-bearing structs use `#[derive(Debug)]`, so any `{:?}` — a `debug!`
line, an error wrapper, a panic message — prints credentials verbatim. Nothing does so today
(verified, §2), so the defect is latent, but it is one line away from a leak.

Rather than hand-redacting each struct, this milestone **aligns with Java**. Java never holds a client
secret as a bare `String`: every `ConfigDef.Type.PASSWORD` value is wrapped in
`org.apache.kafka.common.config.types.Password`, whose `toString()` returns `"[hidden]"`
(`Password.java:24`, `:55-56`), and every wire wrapper that carries a secret overrides `toString()`
to redact it. The Rust translation dropped `Password` and typed those values as `Option<String>`,
which is exactly where the redaction was lost.

Deliverables:

1. `common::config::types::Password` — a full translation of `Password.java`.
2. Every config field Java defines as `Type.PASSWORD` becomes `Option<Password>` in `SslConfig` and
   `SaslConfig`. The *derived* `Debug` of those two structs, and of `ConsumerConfig` and
   `AdminClientConfig` which embed them, is then safe by construction.
3. Hand-written `Debug` where Java redacts in `toString()` but Rust still derives: `ProducerConfig`
   (its verbatim `originals` map), `DelegationToken`, six request/response wrappers, and
   `UserScramCredentialUpsertion`.
4. A test per struct asserting the `{:?}` rendering does not contain the secret.

## 2. Verified baseline — the finding, corrected

Every claim below was checked against `b76de2e1`; the finding's own line numbers are from `a6e6b71`
and `4880d36`, both ancestors of HEAD, so some have shifted.

**Reported variants, all still present:**

| Struct | Where | Secret fields printed by derived `Debug` |
|---|---|---|
| `SaslConfig` | `src/common/config/sasl_configs.rs:71` | `jaas_config`, `password` |
| `SslConfig` | `src/common/config/ssl_configs.rs:128` | `truststore_password`, `keystore_password`, `keystore_key` (PEM private key), `key_password` |
| `DelegationToken` | `src/common/security/token/delegation/delegation_token.rs:56` | `hmac` — its hand-written `Display` redacts (`hmac=[*******]`, tested at `:153`), derived `Debug` bypasses it |
| `ProducerConfig` | `src/producer/producer_config.rs:53` | embeds both sub-configs **and** `originals: HashMap<String, String>` (`:266`), the verbatim user map |
| `ConsumerConfig` | `src/consumer/consumer_config.rs:47` | embeds both sub-configs |

**Correction:** `ConsumerConfig` has **no** `originals` map (the finding says it does). Only
`ProducerConfig` carries one. The consumer stays in scope through the embedded sub-configs.

**Same defect, not in the finding — added to scope:**

- `AdminClientConfig` (`src/admin/admin_client_config.rs:30`) derives `Debug` and embeds `sasl_config`
  / `ssl_config` as private fields (`:51`, `:54`).
- `UserScramCredentialUpsertion` (`src/admin/user_scram_credential_upsertion.rs:26`) derives `Debug`
  over `password: Vec<u8>` and `salt`. Java defines no `toString()` on it at all
  (`UserScramCredentialUpsertion.java:31-34`), so Java has no printing path; the Rust derive adds one.
- Six wrappers with a redacting `Display` (mirroring Java's `toString()`) that **also**
  `#[derive(Debug, Clone)]`, the `DelegationToken` pattern exactly:
  `SaslAuthenticateRequest`, `SaslAuthenticateResponse`, `CreateDelegationTokenResponse`,
  `DescribeDelegationTokenResponse`, `RenewDelegationTokenRequest`, `ExpireDelegationTokenRequest`
  (all under `src/common/requests/`).
- `MockAdminClient` derives `Debug` over `State.all_tokens: Vec<DelegationToken>`, so it inherits
  the HMAC leak. Fixed transitively by the `DelegationToken` change; gets a regression test only.

**"Latent" confirmed.** No production code Debug-formats any of these types (`src`, `src/ffi`,
workspace crates, `tests`, Python bindings' `__repr__`s all checked). `KafkaProducer`,
`AsyncKafkaConsumer`, `KafkaAdminClient` do not derive `Debug`. `SaslChannelBuilder` already has the
safe pattern: a hand-written `Debug` printing only `sasl_config.mechanism` and ending with
`finish_non_exhaustive()` (`src/common/network/sasl_channel_builder.rs:52`). `SaslClientAuthenticator`,
which holds the plaintext password, has no `Debug`.

**Correction (Critic 85, 2026-09-29): "latent" was wrong for two of the six wrappers.**
`src/network_client.rs:1161-1175` logs every in-flight request with `{:?}` when a connection drops, a
request times out, or a node is disconnected or closed (reached from `:991` → `:1018` → `:1121`,
`disconnect` `:1534`, `close_connection` `:1543`). Java logs the redacting `toString()` there
(`NetworkClient.java:405-409`). So `RenewDelegationTokenRequest` and `ExpireDelegationTokenRequest`
leaked their HMAC at DEBUG level on that path until Commit B made their `Debug` delegate to `Display`.
The same path still prints `AlterUserScramCredentialsRequest` (salt and salted password) and
`IncrementalAlterConfigsRequest` (config values); both are Phase 2 (§11). The search behind the
original claim looked for `{:?}` applied to the *config* and *token* types, not to the request enum
that wraps them.

## 3. Scope decisions

### 3.1 Which fields become `Password` — exactly Java's `Type.PASSWORD` set

From `SslConfigs.addClientSslSupport` (`SslConfigs.java:133-140`) and
`SaslConfigs.addClientSaslSupport` (`SaslConfigs.java:380`), restricted to keys the Rust client parses:

| Java key | Rust field | Note |
|---|---|---|
| `ssl.truststore.password` | `SslConfig::truststore_password` | |
| `ssl.truststore.certificates` | `SslConfig::truststore_certificates` | public certs, but Java hides them — we follow Java |
| `ssl.keystore.password` | `SslConfig::keystore_password` | |
| `ssl.keystore.key` | `SslConfig::keystore_key` | PEM private key |
| `ssl.keystore.certificate.chain` | `SslConfig::keystore_certificate_chain` | public certs, but Java hides them — we follow Java |
| `ssl.key.password` | `SslConfig::key_password` | |
| `sasl.jaas.config` | `SaslConfig::jaas_config` | embeds the password |

Java also types `sasl.oauthbearer.client.credentials.client.secret` and
`sasl.oauthbearer.assertion.private.key.passphrase` as PASSWORD (`SaslConfigs.java:392`, `:402`);
OAuth is not translated (Milestone 13 exclusion), so nothing to type here.

### 3.2 Rust-only convenience fields

`SaslConfig::username` / `SaslConfig::password` have no Java counterpart (they are set
programmatically; no `sasl.username`/`sasl.password` key is parsed anywhere in `src`).
`password` becomes `Option<Password>` — it is a password. `username` stays `Option<String>`.
`resolve_username()` / `resolve_password()` keep their `Option<&str>` signatures: the authenticator
must send the plaintext, exactly as Java's `PlainLoginModule` yields a plain `String`. They read
`.value()` on the `Password` internally.

### 3.3 Where Java redacts in `toString()`, Rust's `Debug` must redact too

Java has a single stringification (`toString()`); Rust has two. For `DelegationToken` and the six
wrappers the existing `Display` is the faithful `toString()` translation, so `Debug` is implemented
by **delegating to `Display`** (`fmt::Display::fmt(self, f)`). No second redaction logic to keep in
sync. `Clone` (and `PartialEq, Eq, Hash` on `DelegationToken`) stay derived.

### 3.4 Bindings are unaffected

The C FFI (`src/ffi/*.rs`) and Python bindings build configs from property maps and never touch
these fields (verified by grep). `cargo xtask check-bindings` is a CPython format-arity checker,
unrelated to Rust type changes. No header, `.py`, or gRPC change. `make verify` covers what matters.

### 3.5 Explicitly out of scope

- Zeroizing secrets on drop (would need a new crate; Java does not do it either).
- Translating `ConfigDef` / `AbstractConfig` (Milestone 13 exclusion stands). §4.3 carries the one
  piece of `ConfigDef` knowledge this milestone needs.
- `Deref<Target = str>`, `From<&str>` / `From<String>`, or `Default` on `Password` — none exist in
  Java, and each would make an accidental plaintext path easier. Construction is explicit:
  `Password::new(..)`.
- Removing `Debug` from the client configs altogether (Java's `AbstractConfig` has no `toString()`).
  Rust users expect `{:?}` to work; a redacted `Debug` is what the finding asks for.

## 4. Design

### 4.1 `Password` (`src/common/config/types/password.rs`)

Translation of `Password.java` (Apache 2.0 header, Confluent Inc.), rustdoc citing the Java class:

```rust
#[derive(Clone, PartialEq, Eq, Hash)]           // Java: equals/hashCode on `value`
pub struct Password { value: String }

impl Password {
    pub const HIDDEN: &'static str = "[hidden]";   // Java: Password.HIDDEN
    pub fn new(value: impl Into<String>) -> Self;  // Java: Password(String value)
    pub fn value(&self) -> &str;                   // Java: value()
}
impl fmt::Display for Password { /* writes Self::HIDDEN */ }   // Java: toString()
impl fmt::Debug   for Password { /* writes Self::HIDDEN */ }   // Rust has two; both redact
```

Both formatters write exactly `[hidden]` — no type name, no length (length is information too).

### 4.2 Field typing and its ripple

Parse sites (wrap with `Password::new(value)`):
- `SslConfig::apply_ssl_config_key` (`ssl_configs.rs:206-246`) — the six SSL arms.
- The three parents' `sasl.jaas.config` arms: `producer_config.rs:603`, `consumer_config.rs:811`,
  `admin_client_config.rs:131`.

Read sites (call `.value()`):
- `src/common/security/ssl/ssl_factory.rs:211`, `:248`, `:284` (`truststore_certificates`,
  `keystore_certificate_chain`, `keystore_key` PEM parsing).
- `SaslConfig::resolve_username` / `resolve_password` (`sasl_configs.rs:233`, `:245`).
- `SaslChannelBuilder` reads only through `resolve_*` (`sasl_channel_builder.rs:117-167`) — unchanged.

Construction sites in tests (`Some("x".to_owned())` → `Some(Password::new("x"))`): `ssl_configs.rs`
(~24), `sasl_configs.rs` (~26), `sasl_channel_builder.rs` tests (~8), `channel_builders.rs`,
`ssl_transport_layer.rs`, `tests/integration/ssl_sasl_test.rs:68`, `:101`. The compiler finds them all.

Public API note: `SslConfig` / `SaslConfig` fields are `pub`, so this is a breaking change to their
type. Allowed — CLAUDE.md: the public API is not stable below 1.0.

### 4.3 `ProducerConfig`: hand-written `Debug`, `originals` redacted by key

> **Amended after Critic 85 round 1 (fixup `868ea49d`).** The by-key design below was implemented in
> Commit A/B and then replaced. Critic 85 showed it cannot match Java: `originals` holds **every** key
> the user passed, so the predicates hid only the translated subset of Java's `Type.PASSWORD` keys.
> Java's two OAuth `Type.PASSWORD` keys (`SaslConfigs.java:392`, `:402`), which this client does not
> parse, and every unknown key (for example a serializer credential such as `basic.auth.user.info`)
> printed in plaintext. Java never prints a raw `originals` value: `AbstractConfig.logAll()`
> (`AbstractConfig.java:371-385`) prints only the values parsed for keys the `ConfigDef` defines, with
> `Type.PASSWORD` values as `[hidden]`, and `logUnused()` (`:390-395`) prints key names only.
>
> **Decision (Manager): render `originals` as its sorted key set, no values** — the alternative §10.2
> already listed. Every known non-secret value is already visible through the typed fields of the same
> `Debug` output, so the raw map adds only *which* keys were supplied. The two predicates
> `SslConfigs::is_password_config` / `SaslConfigs::is_password_config` then have no caller and were
> removed with their tables and tests (both files carry a file-level `allow(dead_code)`, so lint would
> never have flagged them). This closes the class permanently instead of chasing a key list; a future
> `originals` map on `ConsumerConfig` / `AdminClientConfig` must render the same way (comments on both
> structs say so). The paragraphs below are kept as the record of the superseded design.

Java's `originals()` is the raw user map too, but Java never prints it; Rust's derive would. The
`originals` values that are secrets are exactly the `Type.PASSWORD` keys — knowledge Java keeps in
`ConfigDef`, which is out of scope. Minimal carrier of that knowledge, hosted on the structs that
already own the key constants (CLAUDE.md §2: statics live on the defining struct):

```rust
impl SslConfigs  { pub(crate) fn is_password_config(key: &str) -> bool }  // the 6 SSL keys of §3.1
impl SaslConfigs { pub(crate) fn is_password_config(key: &str) -> bool }  // sasl.jaas.config
```

each documented as the translation of the `Type.PASSWORD` markings in `addClientSslSupport` /
`addClientSaslSupport`, with the Java line cited. **DoD #7 justification:** two `pub(crate)` free
functions, no new struct or trait; they exist because `ConfigDef` is not translated.

`impl fmt::Debug for ProducerConfig` uses `debug_struct` over **every** field in declaration order
(so the rendering stays a useful debugging aid), rendering `originals` through a temporary
`BTreeMap<&str, &str>` whose value is `Password::HIDDEN` wherever either predicate is true.
`BTreeMap` makes the output deterministic for the test. Omitting a future field from this impl is
safe (nothing leaks), so the maintenance cost is accepted; the Critic checks completeness once.

`ConsumerConfig` and `AdminClientConfig` keep `#[derive(Debug)]` — safe once §4.2 lands — and get
regression tests. If either ever gains an `originals` map it must follow §4.3; say so in a comment
on each struct.

### 4.4 Delegating `Debug` (§3.3)

`DelegationToken` + the six wrappers: drop `Debug` from the derive list, add
`impl fmt::Debug for T { fn fmt(&self, f) -> fmt::Result { fmt::Display::fmt(self, f) } }`.

### 4.5 `UserScramCredentialUpsertion`

Java has no `toString()`. Hand-written `Debug` via `debug_struct("UserScramCredentialUpsertion")`
printing `user` and `info`, then `.field("salt", &Password::HIDDEN).field("password", &Password::HIDDEN)`
(salt is keyed material; hide both), `finish()`.

## 5. Module layout

```
src/common/config/
  mod.rs                 # + pub(crate) mod types;   (pub until §12.4)
  types/
    mod.rs               # mod password; pub use password::Password;
    password.rs          # Password (Java: common.config.types.Password)
  sasl_configs.rs        # jaas_config/password: Option<Password>  (is_password_config removed, §4.3)
  ssl_configs.rs         # six fields: Option<Password>            (is_password_config removed, §4.3)
```

Import path per CLAUDE.md §2: `use crate::common::config::types::Password;` (parent-module
re-export, never `types::password::Password`).

> **Amended in round 4 (§12.4, fixup `3d11391f`).** Phase 1 declared `pub mod types;`. Java marks
> `org.apache.kafka.common.config.types` "not a supported Kafka API" (`package-info.java:19`), so
> CLAUDE.md §2 forbids a public `Password`, and the module is now `pub(crate)`. The import path above is
> unchanged.

## 6. Phase 1 — single phase, two commits (Actor/Critic 85)

**Commit A — `Password` and the typed fields.**
1. Add `Password` + unit tests (§7.1).
2. Retype the seven fields (§3.1) plus `SaslConfig::password`; wrap at parse sites; `.value()` at read
   sites; fix every test construction site.
3. Add `SslConfigs::is_password_config` / `SaslConfigs::is_password_config` + tests (both directions
   over every key constant the struct defines, precedent: `test_retriable_errors_match_java_hierarchy`).
4. `Debug` redaction tests for `SaslConfig`, `SslConfig`, `ConsumerConfig`, `AdminClientConfig` (§7.2).

**Commit B — hand-written `Debug` impls.**
5. `ProducerConfig` (§4.3) + test.
6. `DelegationToken` + six wrappers (§4.4) + tests; `MockAdminClient` regression test.
7. `UserScramCredentialUpsertion` (§4.5) + test.
8. `SaslChannelBuilder` regression test on the existing manual `Debug`.
9. Comments on `ConsumerConfig` / `AdminClientConfig` per §4.3 last paragraph.

Both commits: `make verify` green (build, format-check, lint, workspace tests, check-bindings).

**Fixup `868ea49d`** (after Critic 85 round 1, targets Commit B and also corrects Commit A): `originals`
rendered as its sorted key set; both predicates removed; rustdoc on the impl, the struct,
`apply_ssl_config_key`, `ConsumerConfig` and `AdminClientConfig` rewritten; test
`test_debug_hides_secrets_and_renders_originals_as_key_set` replaces `test_debug_redacts_password_configs`
(seven parsed PASSWORD keys + two OAuth keys + `basic.auth.user.info`, each with its own secret literal,
asserted absent in `{:?}` and `{:#?}`; every key name present; `[hidden]` exactly seven times; the
`bootstrap.servers` value once, through its typed field). Squashing note: `git rebase --autosquash` folds
the fixup into B, so A would still add the predicates that B removes; the final tree is identical.

**Verification actually run** (all green): `make verify` could not run as one target because its
`build-c` step initialises the `kafka/` submodule (§10.4), so each Rust arm ran individually:
`cargo build --all-features --release`, `cargo test --workspace`, `cargo test --features ffi`,
`cargo test --all-features -- --skip __grpc` (includes the integration suite), `make check-bindings`,
`cargo xtask format-check`, `cargo xtask lint`. C and Python suites were not re-run (no binding change, §3.4).

## 7. Tests

### 7.1 Java tests to translate
None. `clients/src/test/java/org/apache/kafka/common/config/types/PasswordTest.java` does **not**
exist at the pinned SHA (HTTP 404 verified). Java exercises `Password` only through `ConfigDefTest`
/ `AbstractConfigTest`, both out of scope with `ConfigDef`. Rust-side tests below are additive.

### 7.2 Rust tests (all assert on the rendered string; every secret is a distinctive literal)
- `password.rs`: `value()` round-trips; `Display` and `Debug` both render exactly `[hidden]`; two
  passwords of different lengths render identically; `PartialEq`/`Hash` follow `value`; `Clone`.
- `sasl_configs.rs`: the finding's requested test — `format!("{:?}", cfg)` contains neither the password nor the
  JAAS string, does contain `mechanism` and `username`; `resolve_password()` still yields plaintext.
- `ssl_configs.rs`: `{:?}` hides all six §3.1 fields, shows `truststore_location` / `keystore_type`.
- `producer_config.rs`: build from props holding all seven §3.1 keys plus a non-secret key; `{:?}`
  contains none of the seven values, contains the non-secret value, contains `[hidden]`; also hides
  the typed sub-config fields. *As landed* (§6 fixup): the props also carry the two OAuth
  `Type.PASSWORD` keys and an unknown key, and `originals` must render as exactly its sorted key set.
- `consumer_config.rs`, `admin_client_config.rs`: `{:?}` hides JAAS + SSL secrets from props.
- ~~`is_password_config` (both structs): exhaustive over every `pub const` key on the struct.~~ Removed
  with the predicates (§4.3 amendment).
- `delegation_token.rs`: `debug_redacts_hmac` mirroring `display_redacts_hmac`.
- Each of the six wrappers: `debug_redacted` mirroring the existing `test_display_redacted` /
  `display_redacts_*`.
- `user_scram_credential_upsertion.rs`: `{:?}` shows `user`, hides password bytes and salt.
- `mock_admin_client.rs`: `{:?}` of a mock holding a token does not contain the HMAC.
- `sasl_channel_builder.rs`: `{:?}` of a builder with credentials contains neither.

## 8. Definition of Done (per `definition-of-done.md`)

1. CLAUDE.md + rules: naming (`types::Password`, `new`, `value`), license header, rustdoc from the
   Java class, no "exception" wording, statics on the defining struct.
2. All `Password` methods translated: `HIDDEN`, constructor, `hashCode`/`equals` (derives),
   `toString` (`Display` + `Debug`), `value`.
3. Tests per §7; `@RepeatedTest`/wire tests: N/A (no wire type changes).
4. Blockers: none — no new dependency.
5. `make verify` green; `cargo test --workspace` green. *As run:* every Rust arm of `make verify`
   individually (§6), because the `build-c` arm initialises the submodule.
6. No duplicate: the only other `Password` identifier is the unrelated `admin::ConfigType::Password`
   enum variant (a `DescribeConfigs` type tag) — coexistence is fine, different modules.
7. Items not in Java: none remain — the two predicates were removed in the §6 fixup (§4.3 amendment).
   `Debug` impls are Rust-required (not new types).
8. No TODO/FIXME.
9. C/Python test suites: not re-run — no binding source changes (§3.4); `check-bindings` runs in
   `make verify`.
10. Hot-path audit: **N/A** — config parsing is startup-time; `Password::value()` returns `&str`
    with no allocation; the authenticator path is unchanged. Stated here per admin-client.md §10.
11. Consumer trait surface: N/A (no trait touched).
12. Fixture fidelity: N/A.

## 9. Manager loop

1. On approval: create branch `fix/password-type-redaction`; commit this PLAN.md as its first commit.
2. Spawn **Actor 85** with §6, §7, §8 and the Java-source instructions of §10.4.
3. Spawn **Critic 85** to review each commit; findings to `COMMENTS.85.md`. Critic focus list:
   every §3.1 key maps to a `Password` field; no `Option<String>` secret remains in either
   sub-config; `Password` renders neither value nor length; no `Deref`/`From`/`Default` added;
   `ProducerConfig::Debug` lists every field; `is_password_config` tests are exhaustive both ways;
   wrapper `Debug` delegates to `Display` rather than re-implementing; `resolve_*` still return
   plaintext; all test-construction sites use `Password::new` (no helper that hides it).
4. Fix loop until `COMMENTS.85.md` is empty; then handoff: update `design/current/structure.md`
   (config listing, line 100) and `design/current/status.md`; add Milestone 14 and 15 entries to
   `design/history/MILESTONES.md` (14 is currently missing); copy `COMMENTS.DONE.85.md` here.
5. The user opens the PR. Repo artifacts (plan, branch, commits, PR text) carry no internal tracker identifiers.

**Outcome:** one Critic round with one finding (§4.3 amendment), one fixup, second Critic round clean.
Handoff done on 2026-09-29: `structure.md`, `status.md`, `MILESTONES.md` (14 and 15), `COMMENTS.DONE.85.md`
copied here, this plan amended. Phase 2 (§11) was approved by the user on 2026-09-30 and runs the same
loop (Actor 85 → Critic 85 → fix → handoff) on the same branch.

## 10. Assumptions and open points for the reviewer

1. **Certificates are hidden too** (`ssl.keystore.certificate.chain`, `ssl.truststore.certificates`)
   because Java types them PASSWORD. Say so if you would rather keep them visible.
2. **`originals` redaction** is by key (§4.3). Alternative: print only the keys of `originals`. The
   by-key form is closer to Java's `logAll()` behaviour and more useful when debugging.
   **Decided 2026-09-29: keys only** (§4.3 amendment). The "closer to `logAll()`" claim did not hold:
   `logAll()` never prints an unknown key's value at all, and the by-key form printed it.
3. **`Password` `Debug` == `Display` == `[hidden]`**, no type-name wrapper.
4. **Java source access.** `kafka/` is not checked out. Agents will read the needed Java files
   read-only from `https://raw.githubusercontent.com/apache/kafka/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/...`
   (`common/config/types/Password.java`, `common/config/SslConfigs.java`,
   `common/config/SaslConfigs.java`, `common/config/AbstractConfig.java`,
   `common/security/token/delegation/DelegationToken.java`, `common/requests/SaslAuthenticateRequest.java`
   and siblings, `clients/admin/UserScramCredentialUpsertion.java`) and will **not** run
   `git submodule update --init kafka`. If you prefer the local tree, initialise the submodule before
   approving and they will use it.
5. `resolve_password()` keeps returning `Option<&str>` (§3.2).
6. No zeroize-on-drop (§3.5).
7. Breaking the `pub` field types of `SslConfig` / `SaslConfig` is acceptable below 1.0.

## 11. Phase 2 (COMPLETE 2026-09-30, commit `35d399fa`) — the same defect beyond the Phase-1 list

Found by Actor 85 and independently verified by Critic 85 against the Java `toString()` at the
pinned commit. Same technique as Phase 1 (§3.3): drop `Debug` from the derive list and delegate to
the existing redacting `Display`, or hand-write it where no `Display` exists. Same agent number 85,
one commit, Critic review, `COMMENTS.85.md` loop.

### 11.1 Correction to §2 — two leaks were live, not latent

`src/network_client.rs:1161-1175` logs the in-flight request with `{:?}` whenever a connection
drops, a request times out, or a node is disconnected or closed. Java logs the redacting
`toString()` (`NetworkClient.java:405-409`). Through that line, at DEBUG level:

- `RenewDelegationTokenRequest` / `ExpireDelegationTokenRequest` leaked the HMAC until Phase 1
  Commit B made their `Debug` delegate to `Display`. §2's "latent" claim was wrong for these two.
- `AlterUserScramCredentialsRequest` **still** prints the salt and salted password — enough to
  authenticate as that SCRAM user. Java masks both (`AlterUserScramCredentialsRequest.java:85-91`, `:96-97`).
- `IncrementalAlterConfigsRequest` **still** prints every config value being altered. Java replaces
  each with `"REDACTED"` (`IncrementalAlterConfigsRequest.java:111-118`, `:122-123`).

### 11.2 Work items, in priority order

| # | Item | Java reference | State |
|---|---|---|---|
| 1 | `network_client.rs:1161-1175`: log the request with `{}` (Display), as Java logs `toString()` | `NetworkClient.java:405-409` | **live path** |
| 2 | `AlterUserScramCredentialsRequest`: `Debug` delegates to redacting `Display` | `AlterUserScramCredentialsRequest.java:85-97` | **live via #1** |
| 3 | `IncrementalAlterConfigsRequest`: `Debug` delegates to redacting `Display` | `IncrementalAlterConfigsRequest.java:111-123` | **live via #1** |
| 4 | `AlterUserScramCredentialsRequestBuilder`: `Debug` delegates to a new `Display` rendering `maskData(data)` as Java's `Builder.toString()` does (this row first said "prints type only"; Java masks, it does not omit) | `AlterUserScramCredentialsRequest.java:45-46` | latent |
| 5 | `IncrementalAlterConfigsRequestBuilder`: same, every config value rendered `REDACTED` | `IncrementalAlterConfigsRequest.java:74-75` | latent |
| 6 | `SaslAuthenticateRequestBuilder`: `Debug` delegates to its existing Java-matching `Display` | `SaslAuthenticateRequest.java:49-50` | latent |
| 7 | `RenewDelegationTokenRequestBuilder`: same pattern | `RenewDelegationTokenRequest.java:66-74` | latent |
| 8 | `ExpireDelegationTokenRequestBuilder`: same pattern | `ExpireDelegationTokenRequest.java:76-84` | latent |
| 9 | `ConfigEntry`: `Debug` delegates to its redacting `Display`; this also fixes the derived `Debug` of `Config` and `AlterConfigOp`, which embed it | `ConfigEntry.java:183-186`, `Config.java:74-75`, `AlterConfigOp.java:119-124` | latent |
| 10 | `ConfigEntryOptions` (Rust-only): hide `value` when `is_sensitive` | `ConfigEntry.java:183-186` | latent |
| 11 | `CreateDelegationTokenResponseOptions` (Rust-only): hide `token_id` and `hmac` | `CreateDelegationTokenResponse.java:109-113` | latent |
| 12 | `MockAdminClient` state holds mock config values unredacted; Java has no `toString()` | — | lowest; the maps hold raw strings, so `State` and `TopicMetadata` got hand-written `Debug` rendering every config map as its key set (this row first said "document and skip") |

Tests: one `Debug` redaction test per item, same shape as Phase 1 (§7.2): distinctive secret literal
absent, a non-secret field present, byte secrets asserted in their `Debug` rendering. For #1, a test
that the disconnect/timeout log path formats requests through `Display` (assert on the rendered
string of a request holding a secret).

Not in Phase 2: the generated `*Data` types print their bytes, but so does Java's generated
`toString()` (`Arrays.toString` in `MessageDataGenerator.generateFieldToString`); Java redacts only
in the wrappers, and so does Rust after #1–#3.

### 11.3 Also recorded

When OAuth configuration is translated, its two `Type.PASSWORD` keys (`SaslConfigs.java:392`,
`:402`) become `Password` fields like §3.1. `ProducerConfig`'s `originals` rendering no longer
depends on a key list (§4.3 as amended), so nothing else needs updating for them.

### 11.4 Rule updates Critic 85 suggested (for the user to decide; nothing in `CLAUDE.md` or `.claude/rules/` covers redaction today)

1. **Java `toString()` redaction carries over to Rust `Debug`.** Where Java's `toString()` masks, omits or
   replaces a value (`Password.HIDDEN`, `maskData(...)`, `"REDACTED"`, `Redacted`, `(redacted)`), the Rust
   type must not derive `Debug`: delegate `Debug` to the redacting `Display`, or hand-write it with the same
   masking, including nested `Builder` classes and Rust-only `*Options` types carrying the same values. A key
   Java defines as `ConfigDef.Type.PASSWORD` is held as `Option<Password>`, never `Option<String>`.
2. **Log lines use `{}` where Java logs `toString()`.** A translated log line formats a value the way the
   Java line does; where Java relies on `toString()`, Rust uses `{}` (the `Display` translation), not `{:?}`.
   Found at `src/network_client.rs:1161-1175` versus `NetworkClient.java:405-409`.
3. **A raw user-property map renders as its key set.** A struct holding Java's `originals()` renders that
   map in `Debug` as its key set only, because the map holds keys this client does not parse, so no per-key
   filter can match what Java hides.

4. **Testing a log line whose logger cannot be captured** (Critic 85, round 3): pass the logged value
   through a private helper returning `&dyn Display`, mark it `#[deny(dead_code)]` so a file-level
   `allow(dead_code)` cannot hide a reverted call site, and assert on the helper's output. Used for item 1.

The full rationale, with the evidence from this milestone, is in `critic-85-final-review.md` in this
directory (the final state of Critic 85's `COMMENTS.85.md`: round-2 verification, the Phase 2 candidates
with line references, and these rule suggestions; that filename is git-ignored everywhere by design).

### 11.5 As landed (commit `35d399fa`, Critic 85 round 3 clean)

- All twelve items done, one `Debug` redaction test each, 25 new tests. Verified arm by arm as in §6:
  workspace 4398 / ffi 4467 / all-features 4691 passed, 0 failed; release build, `check-bindings`,
  `format-check`, `lint` clean. Critic 85 reran the workspace tests, lint and format-check at the same tree.
- **Item 1 shape.** The cancel log formats the in-flight request through a private `loggable_request`
  helper returning `&dyn Display`, so `{:?}` on it cannot compile. A missing request (only the tests build
  one, mirroring `InFlightRequestsTest.java:123-124`) renders as `null`, as SLF4J prints a null argument.
  The helper carries `#[deny(dead_code)]` because `network_client.rs` has a file-level `allow(dead_code)`:
  reverting the log line to `{:?}` passed lint until the attribute was added. The test asserts on the helper's
  output for a request holding SCRAM secrets, not on captured log text, because the process-wide `log`
  logger is already installed by `src/ffi/common.rs` in the `--features ffi` test binary (§11.4 item 4).
- **Found and fixed on the way:** the metadata send log (`NetworkClient.java:1342`) also used `{:?}`; it now
  uses `{}` through a new `Display` on `MetadataRequestBuilder` mirroring `Builder.toString()`
  (`MetadataRequest.java:146-147`). The two `{:?}` left in the file format an `Errors` value and a caught
  error, not a request. `in_flight_requests.rs` has no log lines.
- **Builders 4, 5, 7, 8** got a `Display` translating `Builder.toString()` = `maskData(data)` through a
  private `mask_data` on each request that clones and masks the copy (salt and salted password emptied;
  every config value, null included, set to `REDACTED`; HMAC emptied). Renew/Expire's request `Display`
  now shares it; output unchanged, the Phase 1 tests still pass.
- **Accepted pre-existing deviation (Critic 85 verdict):** the `Display` of `AlterUserScramCredentialsRequest`
  and `IncrementalAlterConfigsRequest` prints counts only (from `3e84b9bd`, PR #127), while Java's request
  `toString()` prints `maskData(data)` (`AlterUserScramCredentialsRequest.java:96-97`,
  `IncrementalAlterConfigsRequest.java:122-123`). It hides more than Java, so nothing leaks; the DEBUG line
  loses the names Java prints. Optional follow-up: render `mask_data` in both `Display`s (the Renew/Expire
  shape), updating the two tests and two comments that pin the current strings.
- **Item 12** also gave `TopicMetadata` a hand-written `Debug` (it is inside `State`), and removed two
  `allow(dead_code)` on `State.controller` / `State.cluster_id`.
- **Sweep result (Critic 85):** nine Java request/response classes have a masking `toString()`; eight are now
  covered in Rust. The ninth, `AlterConfigsRequest`, has no Rust translation.
- **Notes for later, all pre-existing and none leaking a secret:** (a) 38 setter docs in 13 files say
  "panics if it was not set" while `build()` returns `Err`, a doc-only sweep; (b) the version-mismatch log
  prints the API key name where Java prints the builder and the error (`NetworkClient.java:586-587`), fixing
  it needs a `Display` bound on `RequestBuilder`; (c) `Errors` logged with `{:?}` shows the Rust variant
  name instead of Java's constant name.

## 12. Merge of `master` and the pull-request review follow-up (2026-10-02, Actor 85)

Two commits on the same branch, made after it was pushed and its pull request opened: a merge of
`master`, and a fixup answering the one review comment on the pull request. No history was rewritten.

### 12.1 Merge of `master` (`091f6f9d`)

`origin/master` at `4eb87db1` was merged into the branch at `79db2f0f`. Master's changes are kept, and so
is every Milestone 15 redaction. The merge commit's body lists each conflicted file and its resolution.

- **§1–§11 predate the merge and are left as written**, except §5's `types` line, which §12.4 amends.
  Master moved the crate under `rust/`, renamed
  `SslConfig` / `SaslConfig` to `SslConfigs` / `SaslConfigs`, `ConcreteRequest` to `AbstractRequest` and
  `ConfigResourceType` to `config_resource::Type`, nested each request builder as `Builder` in its request
  module, made config fields `pub(crate)`, and added `#[non_exhaustive]` and
  `#[doc(alias = "org.apache.kafka...")]` markers. So read the paths above relative to `rust/` (their line
  numbers are those of the pre-merge tree), the two config types in the plural, and each
  `XxxRequestBuilder` as `xxx_request::Builder`. The `RequestBuilder` trait keeps its name.
- **`Password`** lives at `rust/src/common/config/types/password.rs`. The merge left the branch's `types/`
  directory under the old top-level `src/`, which master no longer has, so it moved beside the rest of
  `common::config`. Its crate path `crate::common::config::types::Password` is unchanged. The merge kept
  the module `pub`, which master's new `check-public-audience` rule rejects; §12.4 makes it crate-private.
  It carries master's `doc(alias)` markers on the struct, `new` and `value`, and none on `HIDDEN`, as
  master marks no constant.
- **`ProducerConfig` derives nothing.** Master dropped `Clone`, because its new `partitioner` field holds a
  `Box<dyn Any + Send + Sync>`, and §4.3 had already dropped `Debug`. The hand-written `Debug` still
  destructures every field. It adds master's `client_dns_lookup`, `partitioner_type` and `partitioner` in
  declaration order (`partitioner_class` is gone). `partitioner` renders through `ConfiguredPartitioner`'s
  derived `Debug`, which prints the boxed partitioner as `Any { .. }` beside its type name. `originals`
  still renders as its key set.
- **Builders and the cancel log.** The `Display` / `Debug` impls of §11.2 items 4–8, and §11.5's `Display`
  on the metadata builder, now sit on each module's `Builder`. `loggable_request` (§11.5) takes
  `Option<&AbstractRequest>`.
- **`ssl_sasl_test.rs` is master's version.** Its helpers build `SslConfigs` / `SaslConfigs` through
  `AdminClientConfig`'s property parser, which wraps the secrets in `Password`, so the branch's
  struct-literal wrapping had no site left.
- **Verified:** workspace 4468 / ffi 4532 / all-features (with `--skip __grpc`) 4755 passed, 0 failed.
  `format-check`, `check-generated`, `cargo doc` with `-D warnings` and the lint checks listed in §12.3 are
  clean.

### 12.2 Review follow-up (`21be3984`, a fixup of `35d399fa`)

The review of the pull request found that `DescribeDelegationTokenResponseOptions`, a Rust-only type,
still derived `Debug`. It holds `tokens: &[DelegationToken]`. `DelegationToken`'s `Debug` mirrors Java's
`toString()`, which hides the HMAC but prints the token id (`DelegationToken.java:71-76`), while the
response the options build masks both (`DescribeDelegationTokenResponse.java:131-140`). This is the
Rust-only `*Options` case §11.4 rule 1 names; §11.2 item 11 covered only the Create options. The leak was
latent, since nothing in the crate Debug-prints the options, but the type is public.

- **Fix.** The struct derives `Clone` and `Copy` only. A hand-written `Debug`, destructured exhaustively,
  renders each token as `DelegationToken { token_information: TokenInformation { owner, token_requester,
  renewers, issue_timestamp, max_timestamp, expiry_timestamp, token_id: "REDACTED" }, hmac: [] }`. That is
  the response's masking, with `hmac: []` as `CreateDelegationTokenResponseOptions` renders it.
  - The token fields are read through accessors and neither secret is read, so a field later added to
    `DelegationToken` or `TokenInformation` stays hidden until it is listed.
  - `std::fmt::from_fn` (stable on the pinned toolchain) builds the nested renderings, so no helper type is
    added (DoD #7) and nothing is allocated.
- **Test `options_debug_redacts_token_id_and_hmac`.**
  - It first asserts that `DelegationToken`'s own `Debug` prints the id, so the leak it guards against is
    real.
  - It pins the exact `{:?}` rendering.
  - In both `{:?}` and `{:#?}` it asserts that the id, the HMAC's byte-list `Debug` and its Base64 string
    are absent, and that the owner, requester and renewer are present.
  - Four mutations each turned it red: restoring the derive, rendering the real id, rendering the real
    HMAC, and dropping the owner.
- **Sweep for the same miss.** Nothing else renders a token id or HMAC that Java hides.
  - The eight Rust wrappers of the Java classes with a masking `toString()` (§11.5), their five nested
    `Builder`s and both `*ResponseOptions` types all have a hand-written `Debug`. The two
    `*ResponseOptionsBuilder` types have no `Debug` at all.
  - The other structs holding a `DelegationToken`:
    - The two admin results derive `Debug` over `KafkaFuture`, whose `Debug` prints only `is_done`.
    - The FFI holder has no `Debug`.
    - `MockAdminClient`'s `State` renders `all_tokens` through `DelegationToken`'s `Debug`, so the id
      shows and the HMAC is hidden, as in Java's `DelegationToken.toString()`. That is item 12's documented
      choice, and it is unchanged.
- **Verified:** workspace 4469 / ffi 4533 / all-features 4756 passed, 0 failed; that is the new test once in
  each arm. The checks listed in §12.1 are clean. `COMMENTS.DONE.85.md` in this directory records the item
  and its resolution.

### 12.3 Lint without the `kafka/` sources

`cargo xtask lint` runs `lint-custom` first. Three of its six rules read the Java sources under `kafka/`,
which this clone does not check out (§10.4): `check-java-name`, `check-no-deprecated-translation` and
`check-public-audience`. They report "cannot run", so `lint` stops before its remaining steps. Those steps
were run one by one instead, and all are clean:

- the other three `lint-custom` rules: `check-no-data-carrying-enum-variants`, `check-no-public-field` and
  `check-dyn-compatible`;
- `doc-hygiene`;
- the module-path hygiene check;
- the three clippy passes (workspace, workspace with `--all-features`, and `xtask`), each with
  `-D warnings`.

CI's Verify Rust job runs `cargo xtask fetch-java-refs` before `make verify-rust`, whose `lint` step runs
all six rules.

### 12.4 Round-4 review: `Password` is crate-private (fixup `3d11391f`)

Critic 85's round-4 review found that the branch fails CI's lint, although every step §12.3 could run is
clean. CLAUDE.md §2 lets an item be public only if the Java class it translates is outside `internal*`
packages, outside packages whose `package-info.java` says "not a supported Kafka API", and annotated
`@InterfaceAudience.Public` in Kafka 4.4. Master's `check-public-audience` rule enforces this. `Password`
is on the 4.4 Public list (`design/current/interface-audience-public-4.4.txt`) and is not in an internal
package, but its package carries the disclaimer (`common/config/types/package-info.java:19`, at 4.3.1 and
at 4.4.0-rc3). Phase 1 declared `pub mod types;` (§5), and the merge kept it (§12.1). The rule reads the
Java sources, so it is one of the three that §12.3 could not run.

- **Fix.** `common::config` declares `pub(crate) mod types;`, as master declares
  `common::security::{authenticator, ssl}`. Inside the module, `Password` and its `HIDDEN`, `new` and
  `value` stay `pub` and keep their `doc(alias)` markers, as master's items in those modules do. No
  allowlist entry was added: CLAUDE.md §2 says the type MUST NOT be public, and nothing needs it to be.
  - The crate path `crate::common::config::types::Password` is unchanged, so no importer changed.
  - No public signature names `Password`. The fields that hold one are `pub(crate)`, and rustc's
    `unnameable_types`, which `src/lib.rs` enables, would reject a public signature that did. A user meets
    a `Password` only as the `[hidden]` that a config's `Debug` renders in its place.
  - rustdoc rejects public documentation that links to a private item (`private_intra_doc_links`, an
    error under `-D warnings`). So the five such links became code spans: one each in the docs of
    `SslConfigs`, `UserScramCredentialUpsertion` and `ProducerConfig`, and two in the doc of
    `ProducerConfig`'s `Debug` impl. Links in the docs of crate-private items stay. The `types` module doc
    now says why the module is crate-private.
- **Verified on the real tree:** workspace 4469 / ffi 4533 / all-features (with `--skip __grpc`) 4756
  passed, 0 failed, the same counts as §12.2. `format-check`, `check-generated` and `cargo doc` with
  `-D warnings` are clean, and so are the three `lint-custom` rules, `doc-hygiene` and the three clippy
  passes that §12.3 lists.
- **Verified with the Java sources.** A throwaway git repository held `clients/src/{main,test}/java` from
  Apache Kafka 4.3.1 (the pinned commit) and 4.4.0-rc3, each tagged as `lint-custom` expects. It stood in
  for `kafka/` in copies of the tree made outside the repository, so the submodule stayed uninitialised.
  Items checked by each `lint-custom` rule:

  | Rule | master `4eb87db1` | before the fix (`3e869523`) | after the fix (`3d11391f`) |
  |---|---|---|---|
  | `check-no-data-carrying-enum-variants` | 817 | 817 | 817 |
  | `check-no-public-field` | 231 | 232 | 231 |
  | `check-java-name` | 8373 | 8378 | 8378 |
  | `check-no-deprecated-translation` | 1324 | 1327 | 1324 |
  | `check-public-audience` | 2331 | 1 finding, `Password` | 2331 |
  | `check-dyn-compatible` | 22 | 22 | 22 |

  - Every cell but the finding is a pass. Before the fix, `Password` was one more public struct for
    `check-no-public-field` and three more public markers for `check-no-deprecated-translation`.
    `check-java-name` also checks crate-private items; its five over master are `Password`'s three
    markers, its file and the `pub use` in `types/mod.rs`.
  - After the fix, the whole `cargo xtask lint` passes: all six rules, doc hygiene, module-path hygiene
    and the three clippy passes. `cargo doc` with `-D warnings` passes there too.
  - With the module change alone, and none of the five link changes, `cargo doc` fails with exactly those
    five errors.
- `COMMENTS.DONE.85.md` in this directory records the item and its resolution.
