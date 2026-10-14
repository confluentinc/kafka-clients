# M15 / P7 — SCRAM credentials, delegation tokens, features (.NET binding)

> **Status:** ✅ **APPROVED 2026-09-22 — implementation AUTHORIZED.** The plan was
> approved as written and **D44 was RULED** (§7): stay Mode A, record the
> `RESOURCE_NOT_FOUND` collapse as a known divergence in the code and here, defer
> the real fix to a Rust-core slice tracked at P9. **Do not widen D44's scope
> beyond that.** `dotnet-actor` N=77 spawned 2026-09-22; `dotnet-critic` N=77 runs
> **exactly once**, after all eight RPCs are complete and green (§6).
> **Agent number:** **N = 77** (re-derived from the filesystem; §0.1)
> **Mode:** **A** — verified, not inherited (§0.2). Zero Rust-core change, zero
> C-ABI change, zero generated-header change.
> **Branch:** work continues on `prashah_dev_dotnet_binding`. Do **NOT** create a
> branch, do **NOT** merge, rebase or squash.
> **Scope:** `DescribeUserScramCredentials`, `AlterUserScramCredentials`,
> `CreateDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken`,
> `DescribeDelegationToken`, `DescribeFeatures`, `UpdateFeatures` —
> **8 RPCs, 29 net-new C# types** (30 declarations incl. one nested enum).
> **Roadmap row:** `design/current/PLAN-M15-admin-client.md` §8, M15/P7.

Bookkeeping (agent-number rule, environment traps, DoD gates, comment-file
mechanics) lives in the roadmap — this plan cites it rather than restating it.
What follows is overwhelmingly the ABI read, the shapes, and the type design.

---

## §0 — Ledger and mode (terse)

### §0.1 Agent number: **N = 77**, re-derived from the filesystem

Per roadmap §8.1 (*"re-derive from `find -name 'COMMENTS*.md'`, never from this
table alone"*). Repo-wide enumeration excluding `kafka/` returns a maximum of
**76** (M15/P6, closed and archived). **No `COMMENTS.77.*` exists anywhere.**
77 matches the roadmap ledger's prediction; nothing downstream shifts.

Comments land in `bindings/dotnet/COMMENTS.77.md`; resolved items move to
`COMMENTS.DONE.77.md`. Neither is ever `git add`ed (roadmap §12).

### §0.2 Mode A — verified

| Check | Evidence |
|---|---|
| All 8 RPCs exist in the Rust core | `src/admin/mod.rs:981` `describe_user_scram_credentials_with_users_options`, `:1001` `alter_user_scram_credentials_with_options`, `:1018` `create_delegation_token_with_options`, `:1034` `renew_…`, `:1051` `expire_…`, `:1068` `describe_delegation_token_with_options`, `:1083` `describe_features_with_options`, `:1107` `update_features_with_options` |
| All 8 have sync **and** `_async` C entry points | `h:8794/8823`, `8896/8935`, `8987/9022`, `9061/9091`, `9128/9158`, `9195/9227`, `9260/9288`, `9340/9375` — 16 declarations |
| All 8 `*Result_t` typedefs + `_destroy` | `h:1171/1191/1211/1231/1251/1271/1292/1311`; destroys at `h:9472/9518/9539/9560/9582/9615/9736/9780` |
| All 8 callback typedefs | `h:1182/1202/1222/1242/1262/1283/1302/1321` |
| Value-type handles already exported | `kafka_common_KafkaPrincipal_t` `h:1141`, `kafka_common_TokenInformation_t` `h:1152`, `kafka_common_DelegationToken_t` `h:1164` |
| Working tree carries no Rust/ABI edit | `git status --porcelain -- src/ cbindgen.toml generator/` → **empty** |

**Conclusion: the whole delta is C# under `bindings/dotnet/`.** Discharge the
Mode-A proof at phase end with `git diff <base>..HEAD -- src/ cbindgen.toml
generator/` (**not** `target/include/confluent_kafka.h` — it is gitignored, so
citing it proves nothing; STATUS.md:122 records that correction).

⚠ **Build with `cargo build --features ffi`.** A bare `cargo build` exits 0 while
producing a symbol-less dylib and overwriting the good header.

### §0.3 Nothing in this phase already exists

A sweep of `src/Confluent.Kafka/` (92 flat `Admin/*.cs`, zero subdirectories) for
`scram|delegat|token|feature|principal|version` returns **0 files**. All 29 types
are net-new; no prior-phase type is reopened. `AclOperation`-style reuse does not
apply here.

---

## §1 — THE ABI READ (roadmap §8: *"load-bearing, not a formality"*)

### §1.0 The eight accessor sets, classified against §4.4's six shapes

| RPC | C accessor set (`h:`) | Shape | Java's **stored field** (the contract) |
|---|---|---|---|
| `describeUserScramCredentials` | `count` :9393, `get_user` :9404, `get_error` :9420, **`get_credential_count(i)`** :9432, **`get_credential_mechanism(i,j)`** :9445, **`get_credential_iterations(i,j)`** :9458, `destroy` :9472 | **1 + nested inline value (N1)** | `KafkaFuture<DescribeUserScramCredentialsResponseData>` `:37` — **raw protocol data, NOT a map** (N5) |
| `alterUserScramCredentials` | `count` :9482, `get_user` :9493, `get_error` :9505, `destroy` :9518 | **2** | `Map<String, KafkaFuture<Void>>` `:31` + `all()` `:53` — genuine shape 2 |
| `createDelegationToken` | **`get_token`** :9529, `destroy` :9539 — **no `count`, no index** | **NONE — single value (N3)** | `KafkaFuture<DelegationToken>` `:27`; only `delegationToken()` `:36` |
| `renewDelegationToken` | **`expiry_timestamp` → `int64_t`** :9550, `destroy` :9560 — no count, no index | **NONE — single inline scalar (N3)** | `KafkaFuture<Long>` `:26`; only `expiryTimestamp()` `:35` |
| `expireDelegationToken` | **`expiry_timestamp`** :9572, `destroy` :9582 — identical to renew | **NONE — single inline scalar (N3)** | `KafkaFuture<Long>` `:26`; only `expiryTimestamp()` `:35` |
| `describeDelegationToken` | `count` :9592, `get_token(i)` :9603, `destroy` :9615 — **no `get_error`** | **3b** | `KafkaFuture<List<DelegationToken>>` `:29`; only `delegationTokens()` `:38` |
| `describeFeatures` | `finalized_count` :9625, `get_finalized_feature(i)` :9636, `get_finalized_min_version_level(i)` :9648, `get_finalized_max_version_level(i)` :9660, **`finalized_features_epoch(out)` → `bool`** :9677, `supported_count` :9690, `get_supported_feature(i)` :9701, `get_supported_min_version(i)` :9713, `get_supported_max_version(i)` :9725, `destroy` :9736 | **NONE — two non-co-indexed maps + optional scalar (N4)** | `KafkaFuture<FeatureMetadata>` `:28` — **ONE future over a composite**; only `featureMetadata()` `:34` |
| `updateFeatures` | `count` :9746, `get_feature` :9757, `get_error` :9769, `destroy` :9780 | **2** | `Map<String, KafkaFuture<Void>>` `:29` + `all()` `:46` — genuine shape 2 |

**Three of the eight accessor sets match none of §4.4's six shapes** — more than
any prior M15 phase. But unlike P2 (which had to *generalize the walker's type
parameters*), every one of them resolves **below** the walker seam; see §1.5.

⚠ **Two RPCs have byte-identical accessor sets** (`renew` / `expire`: `count`-less,
one `int64_t expiry_timestamp`, one destroy) **and two more do** (`alterUserScram`
/ `updateFeatures`: `count`/`get_X`/`get_error`/`destroy`, identical to P6's
`CreateAcls` and `AlterClientQuotas`). A cross-wired reader returns a plausible
answer in both pairs — this is P5 finding 70.12's class and gets the T-N8 wiring
guard.

### §1.1 Finding N1 — `DescribeUserScramCredentials` is a two-level nest whose inner values are **inline scalars**

Structurally close to P6's `DeleteAclsResult` (N2 there) but **not** the same
arity — the inner level carries no error channel and no handle:

```
count                            -> users                         (outer)
  get_user(i)                    -> string                        KEY
  get_error(i)                   -> that user's future failing    FAULT  (borrowed)
  get_credential_count(i)        -> |credentialInfos()|           (inner)
    get_credential_mechanism(i,j)  -> int32, ScramMechanism.type() VALUE (inline)
    get_credential_iterations(i,j) -> int32                        VALUE (inline)
```

Both inner accessors return **`-1` when either index is out of range**
(`h:9437-9440`, `:9452-9455`), and `get_credential_count` returns **0** for a
failed user (`h:9424-9425`). So the inner walk is bounded by
`get_credential_count(i)` and never needs a sentinel check — success/failure is
driven off `get_error(i) != null`, exactly as sub-shape 1c mandates.

**No walker edit.** `Complete<TKey,TValue>` (`:279`) takes `readValue` as a
reader over `(result, index)`; the `(i, j)` loop lives inside that reader, which
is the identical judgment P6 made for `DeleteAcls` and is explicitly sanctioned by
`KeyedResultMarshal.cs:270-276`.

### §1.2 Finding N2 — `DelegationToken.hmac` is a **length-delimited borrowed byte slice**, which the roadmap says Admin does not have

Roadmap §3 states, under *"Reuse unchanged"*:

> *"every Admin string is UTF-8, **NUL-terminated**, callee-owned … There are
> **no** length-delimited slices in Admin; that form is the consumer fetch-batch
> path only."*

**That is false as of P7.** `h:8754`:

```c
const uint8_t *kafka_common_DelegationToken_hmac(const kafka_common_DelegationToken_t *token,
                                                 int32_t *out_len);
```

with the header stating verbatim (`h:8740-8746`): *"the raw MAC bytes, borrowed,
with the length written to `out_len` … The bytes are **not** NUL-terminated and
may contain NUL, so `out_len` is the only way to know how many there are."*

Two consequences, both load-bearing:

1. **This is the first length-delimited read on the Admin surface.** It is a
   *byte* slice, not a string, so ffi §B3's NUL-scan prohibition applies in its
   sharpest form: a NUL-scan here silently truncates an hmac at its first zero
   byte. An hmac is essentially uniform random bytes, so a 32-byte SHA-256 MAC
   contains a zero byte roughly **12%** of the time — frequent enough to be a live
   production defect, rare enough to pass a hand-written test fixture.
2. **The truncation is silently round-trippable.** The header directs the caller
   to *"pass these bytes back verbatim"* to `renew`/`expire`, whose inputs are
   `(const uint8_t *hmac, int32_t hmac_len)` — so a truncated read produces a
   well-formed request that the **broker** rejects, not the binding. Assert on the
   marshalled byte array, not on call success (T-N2).

The roadmap sentence is corrected in P9's doc-sync, not edited here (P6/§1.3
precedent, `DeletedAcl`).

### §1.3 Finding N3 — four RPCs have **no table at all**, and they need no walker

`CreateDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken` and
`DescribeFeatures` have **no `count` and no index parameter** on any accessor.
They are not shape 3 (one future over a *map*) and not 3b (one future over a
*collection*) — they are one future over **one value read from the root**.

The binding already has the vehicle, and it is not the walker:
`SingleAdminOperation<TValue>` (P2b's G3, `Internal/AdminOperation.cs:393`)
exposes `SetResult(value)`, which `CompleteList` and `CompleteAggregate` both call
as their last statement. For these four the trampoline calls it **directly**:

```csharp
// no count, no loop, no Accessors bundle
operation.SetResult(DelegationTokenMarshal.Read(CreateDelegationTokenResult_get_token(result)));
operation.SetResult(RenewDelegationTokenResult_expiry_timestamp(result));   // int64 -> long
```

**`KeyedResultMarshal` is not involved for these four, and gains no callable.**
It stays at **5 callables** (`Complete<TKey,TValue>` :279, `Complete<TKey>` :351,
`CompleteAggregate<TKey,TValue>` :407, `CompleteList<TValue>` :479,
`CompleteTwoLists<TFirst,TSecond>` :554). **P7 adds ZERO and edits ZERO.**

⚠ **Do not add a `CompleteSingle` "for symmetry."** It would be a one-line
pass-through to `SetResult` with no table to walk, i.e. a callable whose entire
body is the thing it wraps — and the walker's value is precisely that each
callable *names a distinct result arity* (P2b's closing rule). A wrapper with no
arity of its own dilutes that.

### §1.4 Finding N4 — `DescribeFeaturesResult` is one Java future over **two non-co-indexed maps plus an `Optional<Long>`**

The header is explicit that the two tables are independent (`h:9682-9684`):
*"Features are sorted by name, and are **not** co-indexed with the finalized ones:
the two maps can differ in both size and contents."* So this is **not**
`CompleteTwoLists` (sub-shape 3c, which resolves **two** awaiters) — Java has
**one** `KafkaFuture<FeatureMetadata>` (`:28`), and `FeatureMetadata` is a
composite of three members (`:32`, `:34`, `:36`).

The precedent is **P3's `DescribeCluster`** (shape 5), which reads the whole root
into `Internal/DescribeClusterSnapshot.cs` and completes from it. P7 mirrors that
shape, with one difference: `DescribeCluster` fans the snapshot out to **four**
`Task`s (Java has four futures), whereas `DescribeFeatures` resolves **one** —
because Java publishes one. Read the accessor, not the analogy.

The epoch is the milestone's **third** `bool`-returning out-param accessor
(after `ListOffsetsResultInfo_leader_epoch` and P6's `get_quota_value`), and the
header gives the reason verbatim (`h:9666-9669`): *"A nullable number needs an
explicit discriminant: every `int64_t`, including 0 and -1, is a legal epoch, so
no sentinel would work."* → C# `long?`, never a sentinel.

### §1.5 The phase's central technical judgment — one phase, no sub-phase

Three accessor sets match none of the six shapes (§1.0), which is exactly the
trigger roadmap §8 names for considering a sub-phase. **It does not fire here**,
for the reason P6 recorded: the bar is *"generalize the walker's own type
parameters … i.e. edit P1's reviewed foundation"* (P2's D7 split). P7:

| | P2 (split) | P6 (not split) | **P7** |
|---|---|---|---|
| Walker type params changed | ✅ `<TValue>`→`<TKey,TValue>` | ❌ | ❌ |
| Walker callables added | — | 0 | **0** |
| Existing callables edited | ✅ | 0 | **0** |
| New work confined to leaf readers | ❌ | ✅ | ✅ |

N1 lives in a `readValue` reader; N3 bypasses the walker via an already-shipped
`SetResult`; N4 follows a shipped snapshot precedent. **P7 is one phase.**

⚠ **The one condition that changes this answer**, carried verbatim from P6 §1.2:
if mid-implementation the Actor finds `Complete<TKey,TValue>`'s signature cannot
express N1's nested value without a signature change, that is a **foundation edit**
and becomes an **escalation to the Manager**, not a unilateral refactor.

### §1.6 Finding N5 — the roadmap's type list has two errors (the P6/§1.3 class)

| Roadmap M15/P7 row says | Java actually has |
|---|---|
| `UpgradeType` as a top-level type | **Nested**: `FeatureUpdate.UpgradeType` (`FeatureUpdate.java:28`) — ships as a nested C# enum (D17/`ReplicaLogDirInfo` precedent) |
| — (omitted) | **`UserScramCredentialAlteration`** (`:27`), the abstract base. `alterUserScramCredentials` takes `List<UserScramCredentialAlteration>`, so the base type **is** the public parameter type and cannot be skipped |

Corrected in §3; the roadmap row is fixed in P9 doc-sync, not edited here.

### §1.7 ⚠ Finding N6 — a **semantic divergence the binding cannot fix in Mode A**. RULED (D44).

Java's `DescribeUserScramCredentialsResult` stores raw protocol data (`:37`) and
derives three accessors with **three different `RESOURCE_NOT_FOUND` behaviours**:

| Java accessor | On `RESOURCE_NOT_FOUND` for a user |
|---|---|
| `all()` `:54` | treated as success but the user is **omitted from the map** (`:60-67`, `:71-75`) |
| `users()` `:92` | the user is **filtered out of the list** (`:98-100`) |
| `description(String)` `:114` | completes **exceptionally**, `ResourceNotFoundException("No such user: " + userName)` (`:124-125`) |

The ABI has already collapsed that distinction. `h:9409-9413`, verbatim:

> *"A user the broker reports as `RESOURCE_NOT_FOUND` is **not** an error here: it
> is a successfully described user with zero credentials, which is what Java's
> `all()` also does."*

**The header's justification is wrong on its own terms.** Java's `all()` does
treat it as non-erroring, but it then *removes the entry*; the ABI *keeps* it with
`get_credential_count(i) == 0` and `get_error(i) == null`. So the flattened table
gives the binding **no way to tell** a `RESOURCE_NOT_FOUND` user from a genuinely
zero-credential one, and therefore no way to reproduce any of the three Java
behaviours faithfully. Observable consequences:

- `All()` **contains** a key Java would omit → `All().ContainsKey(u)` differs.
- `Users()` **contains** a name Java would filter out.
- `Description(u)` **succeeds with zero credentials** where Java faults.

This is not a style question and not a marshalling bug — closing it needs a
discriminant at the ABI, i.e. a **Rust-core slice (Mode B)**, which
`bindings/dotnet/CLAUDE.md §8.1` puts outside `dotnet-actor`'s scope.

**Disposition — RULED 2026-09-22 (D44):** stay Mode A,
ship the ABI's behaviour, and record it as a deviation at the site and in the
close-out, joining `LogDirDescription.isCordoned()` (P3/D15) as a tracked
milestone gap carried to P9. **Do not** invent a managed heuristic — "zero
credentials means not-found" is a *guess*, it is wrong for any user who genuinely
has none, and `bindings/CLAUDE.md §2.6` forbids the binding adding what the core
lacks.

---

## §2 — FOUR explicit discriminants (P6 §2's hazard class, at higher arity)

P6 carried two null-vs-absent models. P7 carries **four**, all already modelled
honestly by the ABI. Mirror them; do **not** re-derive, and do **not** collapse
any into a nullable.

| # | Discriminant | ABI | Java | C# |
|---|---|---|---|---|
| 1 | **salt supplied vs generated** | `has_salts[i]` (`h:8865-8872`) | 3-arg ctor `UserScramCredentialUpsertion.java:54` **generates** a random salt; 4-arg `:66` takes it verbatim | `byte[]? Salt` on `UserScramCredentialUpsertion`, `null` ⇒ `has_salts[i]=false` |
| 2 | **owner filter unset vs empty** | `has_owners_filter` (`h:9177-9181`) | `DescribeDelegationTokenOptions.owners` `:28` has **no initializer** → `null` when unset | `IReadOnlyList<KafkaPrincipal>? Owners`, `null` ⇒ `false` |
| 3 | **node id unset** | `has_node_id` / `node_id` (`h:9261-9262`) | `DescribeFeaturesOptions.nodeId` `:25` = `OptionalInt.empty()` | `int? NodeId` |
| 4 | **finalized epoch absent** | `bool ..._finalized_features_epoch(out int64_t)` (`h:9677`) | `FeatureMetadata.finalizedFeaturesEpoch()` `:59` → `Optional<Long>` | `long?` |

**Discriminant 1 is the one with a security-relevant failure mode.** The header
(`h:8865-8872`): *"`has_salts[i] == false` selects Java's three-argument
constructor, which **generates** a random salt; `true` selects the four-argument
one, which takes the supplied salt verbatim — **including a zero-length one**,
which `Objects.requireNonNull(salt)` accepts. This is an explicit discriminant
because a length of 0 cannot distinguish 'generate one' from 'use this empty
one'."* A C# model that maps `Salt = Array.Empty<byte>()` to `has_salts=false`
silently upgrades an explicit empty salt into a generated one — **and the reverse
mapping is worse**: `null → has_salts=true` with a zero-length salt would store a
credential with **no salt at all**. Single-site rule (§4.2), pinned by T-N3.

**Discriminant 2's Java asymmetry must not be "tidied".**
`CreateDelegationTokenOptions.renewers` `:31` **does** initialize to an empty
`LinkedList` (never null); `DescribeDelegationTokenOptions.owners` `:28` does
**not**. That is deliberate — for `create`, an empty renewer list is a meaningful
request; for `describe`, null means "everything I may see" and empty means "filter
by nothing". Mirror both exactly.

⚠ **`CreateDelegationTokenOptions` has a setter/getter type asymmetry**
(`:43` takes a raw `KafkaPrincipal`, `:48` returns `Optional<KafkaPrincipal>`) and
**two `@Deprecated`-since-4.0 members** — `maxlifeTimeMs(long)` `:56` and
`long maxlifeTimeMs()` `:70`, lowercase `l`, sharing the backing field with the
correctly-spelled `maxLifetimeMs` `:61`/`:74`. **Bind only the correct spelling**;
the deprecated pair is a Java typo kept for compatibility, and
`bindings/CLAUDE.md §2.6` gives no reason to carry a misspelling forward.

---

## §3 — Types: what ships, and where

### §3.1 Placement (P6 §3.1 convention — `Admin/` is flat, `common` types at root)

| Java package | → namespace | → folder |
|---|---|---|
| `org.apache.kafka.common.security.{auth,token.delegation}` | `Confluent.Kafka` | project root |
| `org.apache.kafka.clients.admin` | `Confluent.Kafka.Admin` | `Admin/` (flat) |

### §3.2 The 29 net-new types

**Root namespace `Confluent.Kafka` — 3:**

| Type | Java | Shape |
|---|---|---|
| `KafkaPrincipal` | `common/security/auth/KafkaPrincipal` | ctor `(string principalType, string name)` `:51` + `(…, bool tokenAuthenticated)` `:55`; `PrincipalType` `:88` / `Name` `:84` / `TokenAuthenticated` `:96`; const **`UserType = "User"`** `:44` (⚠ Java's constant is `USER_TYPE`, **not** `PRINCIPAL_TYPE`); `ToString()` ⇒ `"{type}:{name}"` `:62`; **value equality over (type, name) only — excludes `tokenAuthenticated`** `:67`/`:77`. No `FromString` (Java parses via `SecurityUtils.parseKafkaPrincipal`, out of scope) |
| `TokenInformation` | `common/security/token/delegation/TokenInformation` | `TokenId` `:102`, `Owner` `:62`, `TokenRequester` `:70`, `Renewers` `:78`, `IssueTimestamp` `:90`, `ExpiryTimestamp` `:94`, `MaxTimestamp` `:106`; `OwnerAsString` `:66` / `TokenRequesterAsString` `:74` / `RenewersAsString` `:82`; `OwnerOrRenewer(KafkaPrincipal)` `:110` |
| `DelegationToken` | `common/security/token/delegation/DelegationToken` | ctor `(TokenInformation, byte[] hmac)` `:32`; **`TokenInfo`** `:37` (⚠ accessor name ≠ field name), `Hmac` `:41`, `HmacAsBase64String` `:45`; equality uses a **constant-time** hmac compare `:50`; `ToString()` **masks the hmac as `[*******]`** `:71` |

⚠ **`TokenInformation`'s Java equality contract is internally inconsistent** —
`equals` `:128` **excludes** `expiryTimestamp` while `hashCode` `:148`
**includes** it. That is a Java bug, and mirroring it would ship two objects that
are `Equals` but have different hash codes — which **corrupts any hash container**
they enter. Neither type is a dictionary key on P7's surface (§4.3), so the safe
and reviewable choice is **D43**: ship value equality over the *same* field set in
both members (including `ExpiryTimestamp`), and record the deliberate
non-mirroring at the site. Also note `TokenInformation.expiryTimestamp` and
`KafkaPrincipal.tokenAuthenticated` are **mutable** in Java; the C# types are
**immutable** — a binding-layer tightening, recorded, not a shape change.

**`Confluent.Kafka.Admin` — 10 data types:**

| Type | Java | Shape |
|---|---|---|
| `ScramMechanism` | `admin/ScramMechanism` | enum; **`Unknown=0, ScramSha256=1, ScramSha512=2`** (`:33-35`, `byte` type codes; ABI confirms `h:9448`). `MechanismName` ⇒ `"SCRAM-SHA-256"`/`"SCRAM-SHA-512"`/`"UNKNOWN"` (`:90`, `'_'`→`'-'`); `FromType(byte)` `:44` and `FromMechanismName(string)` `:60`, **both falling back to `Unknown`, never throwing**. ⚠ Distinct from `common/security/scram/internals/ScramMechanism` (no `UNKNOWN`) — bind the **admin** one |
| `ScramCredentialInfo` | `admin/ScramCredentialInfo` | ctor `(ScramMechanism, int iterations)` `:36` (mechanism non-null); `Mechanism` `:45`, `Iterations` `:53`; **value equality** `:66`/`:75` |
| `UserScramCredentialAlteration` | `admin/UserScramCredentialAlteration` | **abstract base** `:27`; `User` `:42`; protected ctor `:34`. ⚠ Omitted from the roadmap row (N5) but it **is** the public parameter type |
| `UserScramCredentialUpsertion` | `admin/UserScramCredentialUpsertion` | 3 ctors: `(user, info, string password)` `:43`, `(user, info, byte[] password)` `:54`, `(user, info, byte[] password, byte[] salt)` `:66` — **only the last sets a salt**; `CredentialInfo` `:77`, `Salt` `:85`, `Password` `:93`. See D42 on salt generation |
| `UserScramCredentialDeletion` | `admin/UserScramCredentialDeletion` | ctor `(user, ScramMechanism)` `:34`; `Mechanism` `:43` |
| `UserScramCredentialsDescription` | `admin/UserScramCredentialsDescription` | ctor `(string name, IReadOnlyList<ScramCredentialInfo>)` `:60`; `Name` `:69`, `CredentialInfos` `:77`; **value equality** `:34`/`:43` |
| `FeatureMetadata` | `admin/FeatureMetadata` | `FinalizedFeatures` → `IReadOnlyDictionary<string, FinalizedVersionRange>` `:51`, **`FinalizedFeaturesEpoch` → `long?`** `:59`, `SupportedFeatures` `:68`; value equality `:73`/`:88`. Java's ctor is **package-private** `:38` → C# `internal` |
| `FinalizedVersionRange` | `admin/FinalizedVersionRange` | ctor `(short minVersionLevel, short maxVersionLevel)` `:38`; **throws** unless `min>=0 && max>=0 && max>=min` `:39` (⚠ the javadoc says `>=1`, the **code** enforces `>=0` — follow the code); `MinVersionLevel` `:50`, `MaxVersionLevel` `:54`; value equality |
| `SupportedVersionRange` | `admin/SupportedVersionRange` | ctor `(short minVersion, short maxVersion)` `:38`, throws unless `0<=min<=max` `:39`; `MinVersion` `:50`, `MaxVersion` `:54`; value equality. ⚠ **Different member names from `FinalizedVersionRange`** (`MinVersion` vs `MinVersionLevel`) — Java's asymmetry, preserve it |
| `FeatureUpdate` (+ nested `UpgradeType`) | `admin/FeatureUpdate` | ctor `(short maxVersionLevel, UpgradeType)` `:67`; **throws** if `maxVersionLevel==0 && upgradeType==Upgrade` `:68`, and if `maxVersionLevel<0` `:73`; `MaxVersionLevel` `:80`, `UpgradeType` `:84`; value equality. Nested enum `:28`: **`Unknown=0, Upgrade=1, SafeDowngrade=2, UnsafeDowngrade=3`**, `Code` `:40` (`byte`), `FromCode(int)` `:44` → `Unknown` fallback |

**All `short`, not `int`.** The whole features family is `int16_t` at the ABI
(`h:9648`, `:9660`, `:9713`, `:9725`, and `const int16_t *max_version_levels`
`h:9342`) and `short` in Java. This is the **inverse** of P1's
`ReplicationFactor` correction (where `short` was the *request* side and `int` the
*result* side); here it is `short` on both. Read the accessor.

**`Confluent.Kafka.Admin` — 8 results + 8 options:**

`DescribeUserScramCredentialsResult`, `AlterUserScramCredentialsResult`,
`CreateDelegationTokenResult`, `RenewDelegationTokenResult`,
`ExpireDelegationTokenResult`, `DescribeDelegationTokenResult`,
`DescribeFeaturesResult`, `UpdateFeaturesResult`; and the matching `*Options`.
Only four options carry a member beyond `TimeoutMs`:

| Options | Extra members |
|---|---|
| `DescribeUserScramCredentialsOptions` `:25` | **none** (timeout only) |
| `AlterUserScramCredentialsOptions` `:25` | **none** |
| `CreateDelegationTokenOptions` | `Renewers` `:39` (**never null**, default empty), `Owner` `:48` (`KafkaPrincipal?`), `MaxLifetimeMs` `:74` (default **`-1`**) |
| `RenewDelegationTokenOptions` | `RenewTimePeriodMs` `:31` (default `-1`) |
| `ExpireDelegationTokenOptions` | `ExpiryTimePeriodMs` `:38` (default `-1`) |
| `DescribeDelegationTokenOptions` | `Owners` `:41` (**`null` when unset** — §2 discriminant 2) |
| `DescribeFeaturesOptions` | `NodeId` `:39` (`int?`) |
| `UpdateFeaturesOptions` | `ValidateOnly` `:27` (default `false`) |

⚠ Those `-1` defaults are **Java's own sentinels on the options object**, not the
ABI's `timeout_ms` "unset" convention (roadmap §7 gate 3). Do not conflate them:
`MaxLifetimeMs = -1` is passed through to the ABI's `int64_t max_lifetime_ms`
verbatim, whereas a null `TimeSpan?` timeout maps to a **negative `timeout_ms`**.

---

## §4 — Per-RPC surface and the marshalling design

### §4.1 The five input shapes

| RPC | Input (`h:`) | Precedent |
|---|---|---|
| `describe_user_scram_credentials` | `char*const* users` + `count` (:8794) | P5 group-name arrays |
| **`alter_user_scram_credentials`** | **10 parallel arrays** + `count` (:8896): `users`, `bool* is_deletions`, `int32* mechanisms`, `int32* iterations`, `uint8*const* passwords`, `int32* password_lens`, `uint8*const* salts`, `int32* salt_lens`, `bool* has_salts` | P6's 7-array `AclRowMarshal`, extended — **the phase's largest marshalling job** |
| `create_delegation_token` | 2 renewer arrays + `renewer_count` + 2 nullable owner scalars + `int64 max_lifetime_ms` (:8987) | P6 `describe_acls` scalars |
| `renew` / `expire_delegation_token` | `uint8* hmac` + `int32 hmac_len` + `int64 period` (:9061, :9128) | producer send-path byte pin (ffi §A4) |
| `describe_delegation_token` | `bool has_owners_filter` + 2 owner arrays + `owner_count` (:9195) | §2 discriminant 2 |
| `describe_features` | `bool has_node_id` + `int32 node_id` (:9260) | trivial |
| `update_features` | `char*const* features`, **`int16* max_version_levels`**, `int32* upgrade_types`, `count`, `bool validate_only` (:9340) | P3 config arrays |

`[MarshalAs(UnmanagedType.I1)]` on **every** `bool` — there are five scalar ones
(`has_owners_filter`, `has_node_id`, `validate_only`, plus the `bool` **return**
of `finalized_features_epoch`) and **two `bool` arrays** (`is_deletions`,
`has_salts`). ⚠ The arrays cross as `const bool *`: marshal each as a
**`byte[]` of 0/1**, never a `bool[]` — .NET's `bool` is 4 bytes under the default
marshaller and 1 byte only in a blittable context (the identical call P6 made for
`op_has_values`).

### §4.2 `AlterUserScramCredentialsMarshal` — one row-projector, two row kinds

`UserScramCredentialAlteration` is a closed 2-case hierarchy, so one projector
covers both, with the deletion columns left unset:

```
row i            <- UserScramCredentialUpsertion      | UserScramCredentialDeletion
  users[i]          User                              | User
  is_deletions[i]   false                             | true
  mechanisms[i]     CredentialInfo.Mechanism.Type     | Mechanism.Type
  iterations[i]     CredentialInfo.Iterations         | (ignored by the ABI)
  passwords[i]      Password bytes  (pinned)          | NULL
  password_lens[i]  Password.Length                   | 0
  salts[i]          Salt bytes (pinned) or NULL       | NULL
  salt_lens[i]      Salt.Length or 0                  | 0
  has_salts[i]      Salt is not null                  | false
```

- One `Pin(count)` scope: **four blittable arrays** (`byte[] is_deletions`,
  `int[] mechanisms`, `int[] iterations`, `byte[] has_salts` — plus
  `int[] password_lens`, `int[] salt_lens`) and **three `IntPtr[]`** of per-row
  pinned buffers (`users`, `passwords`, `salts`).
- **Call-scoped pin, unpinned in a `finally` after the submit returns** — the ABI
  copies the rows out during the call and materialises the result before the
  callback fires. Do **not** hold a pin across the `Task` (ffi §A4).
- **`has_salts` is set from `Salt is not null`, at this one site**, so the §2
  discriminant-1 rule cannot diverge between construction and marshalling.

⚠ **Secret material.** `passwords[i]` carries a raw SCRAM password. Two
obligations, neither of which the ABI provides: (a) **never** log, trace or put
password or salt bytes in an exception message — this includes the
`ArgumentException` texts for the precondition checks below; and (b) follow
Java's own lead on `ToString()` — `DelegationToken.toString()` masks its hmac
(`:71`), so `UserScramCredentialUpsertion.ToString()` **must** mask both
`Password` and `Salt`. Java's `UserScramCredentialUpsertion` has **no**
`toString()` override at all, so C#'s default `object.ToString()` (type name only)
is already safe — the hazard is an *added* one. Pinned by T-N4.

⚠ **An empty password is NOT rejected at the ABI, deliberately**
(`h:8859-8864`): Java *"records `UnacceptableCredentialException("Password must
not be empty")` against that user and still sends every other user's alteration
(`KafkaAdminClient.java:4414-4416`), so the error arrives through
`..._get_error` for that user."* **Do not add a C# precondition that throws on an
empty password** — that would convert a per-key failure into a whole-call throw
and drop the other users' alterations. This is the **opposite** call from P6's
D38, and the difference is exactly that Java itself validates per-key here.

⚠ **Duplicate users are passed through, not rejected** (`h:8877-8886`) — the
inverse of P6's `alter_client_quotas`, and the header explains why: a SCRAM user
is a plain string the caller already holds, so the collapsed outcome row is
re-derivable. **Do not "fix" the asymmetry.**

### §4.3 Result shapes → C# surface

```csharp
// N1 — shape 1, nested inline-scalar value. Java stores raw protocol data (:37),
// so all three accessors are DERIVED from one flattened table (see N6 / D44).
public sealed class DescribeUserScramCredentialsResult {
    public Task<IReadOnlyDictionary<string, UserScramCredentialsDescription>> All();   // Java all()        :54
    public Task<IReadOnlyList<string>> Users();                                        // Java users()      :92
    public Task<UserScramCredentialsDescription> Description(string userName);         // Java description():114
}

// shape 2
public sealed class AlterUserScramCredentialsResult {
    public IReadOnlyDictionary<string, Task> Values { get; }    // Java values() :46
    public Task All();                                          // Java all()    :53
}

// N3 — single value, no table
public sealed class CreateDelegationTokenResult {
    public Task<DelegationToken> DelegationToken();             // Java delegationToken() :36
}
public sealed class RenewDelegationTokenResult {
    public Task<long> ExpiryTimestamp();                        // Java expiryTimestamp() :35
}
public sealed class ExpireDelegationTokenResult {
    public Task<long> ExpiryTimestamp();                        // Java expiryTimestamp() :35
}

// 3b — one future over a collection; Java has NO all()
public sealed class DescribeDelegationTokenResult {
    public Task<IReadOnlyList<DelegationToken>> DelegationTokens();  // Java delegationTokens() :38
}

// N4 — ONE future over a composite (not two futures)
public sealed class DescribeFeaturesResult {
    public Task<FeatureMetadata> FeatureMetadata();             // Java featureMetadata() :34
}

// shape 2
public sealed class UpdateFeaturesResult {
    public IReadOnlyDictionary<string, Task> Values { get; }    // Java values() :39
    public Task All();                                          // Java all()    :46
}
```

**No handle-typed dictionary keys in this phase.** Both keyed results are keyed by
`string`, so P6's D39 value-equality obligation does **not** recur. The value
types still get value equality where Java has it (§3.2) — that is Java fidelity,
not a dictionary-key requirement.

⚠ **`Description(string)` is a method, not an indexer or a dictionary** — Java's
`:114` takes a user name and returns a future, resolving from the same table
`All()` uses. It must fault with a `ResourceNotFoundException`-equivalent for a
user genuinely absent from the table (Java `:124-125`, message *"No such user: "*
+ name), asserted as a string per DoD §3. See N6 for the case it **cannot**
reproduce.

### §4.4 `DelegationTokenMarshal` — the nested read, and the length-delimited hmac

```
DelegationToken_token_info(t)          -> TokenInformation_t*   (borrowed)
  TokenInformation_token_id            -> const char*  (NUL-terminated)
  TokenInformation_owner / _token_requester -> KafkaPrincipal_t* (borrowed)
      KafkaPrincipal_principal_type / _name -> const char*
      KafkaPrincipal_token_authenticated    -> bool
  TokenInformation_renewer_count       -> int32
    TokenInformation_get_renewer(i)    -> KafkaPrincipal_t* (borrowed)
  _issue_timestamp / _expiry_timestamp / _max_timestamp -> int64
DelegationToken_hmac(t, &len)          -> const uint8*   ** LENGTH-DELIMITED (N2) **
```

Every pointer here is `const` → **borrowed, never destroyed**; all of it dies with
the `*Result_t` root, so the marshaller copies out fully before the trampoline's
`finally` destroys the root. The hmac copies into an owned `byte[]` using
`out_len` — **never** a NUL-scan (§1.2). `hmacAsBase64String` is **derived in C#**
from those bytes rather than read through the ABI, so the two cannot disagree.

---

## §5 — Tests (the phase-specific ones; the standing battery is roadmap §9)

Every roadmap §9 mandatory test applies per phase and is not restated.

| # | Test | Pins |
|---|---|---|
| **T-N1** | `DescribeUserScramCredentials` with **two users, one failing**, the succeeding one carrying **two** credential infos — the inner `(i,j)` walk is bounded by `get_credential_count(i)` and the failing user's `Task` faults while the other's completes. Mutation: hardcode the inner bound to the outer `count`, confirm RED. | §1.1 |
| **T-N2** | **hmac round-trip through a zero byte.** A token whose hmac contains an interior `0x00` marshals to the **full** byte array (assert length **and** content), and feeding it back to `Renew` submits those exact bytes at the **submit seam**. Must be shown RED if the reader NUL-scans. | §1.2 — the 12%-of-the-time truncation |
| **T-N3** | `Salt = null` vs `Salt = Array.Empty<byte>()` submit **different** rows — `has_salts[i]` is `0` vs `1`, asserted at the submit seam. Both directions mutation-checked (collapsing either way must go RED). | §2 discriminant 1 |
| **T-N4** | **Secrets never surface.** `UserScramCredentialUpsertion.ToString()` contains neither the password nor the salt bytes; no precondition `ArgumentException` message contains them; `DelegationToken.ToString()` masks the hmac (`[*******]`, Java `:71`). | §4.2 |
| **T-N5** | The three remaining discriminants each submit/read **two distinct** outcomes: `Owners=null` vs `Owners=[]` → `has_owners_filter` 0 vs 1; `NodeId=null` vs `NodeId=0` → `has_node_id` 0 vs 1 (⚠ `0` is a **legal broker id**, so a sentinel model turns this RED); `finalized_features_epoch` absent vs present-with-value-`0` → `null` vs `0L`. | §2 discriminants 2–4 |
| **T-N6** | `DescribeFeatures` with **finalized and supported maps of different sizes and different key sets** — each walked by **its own** count, no co-indexing. Mutation: drive the supported loop with `finalized_count`, confirm RED. | §1.4 |
| **T-N7** | `FinalizedVersionRange(min,max)` / `SupportedVersionRange` / `FeatureUpdate` constructors **throw** on Java's exact invalid inputs with the **message asserted as a string** — incl. `FeatureUpdate(0, Upgrade)` `:68` and `FinalizedVersionRange` accepting `0` (the **code**'s bound, not the javadoc's `>=1`). | §3.2 |
| **T-N8** | **Wiring guard** (P5 finding 70.12 / P6 T-N9): `RenewDelegationTokenResult` ⇄ `ExpireDelegationTokenResult` and `AlterUserScramCredentialsResult` ⇄ `UpdateFeaturesResult` have byte-identical accessor sets — and the latter pair is **also** identical to P6's `CreateAclsResult`/`AlterClientQuotasResult`. Every reader gets an `[InlineData]` row + a guard entry **in the same commit**, with a deliberate cross-wire injection confirmed RED before committing. | §1.0 |
| **T-N9** | **Per-key error never destroyed** — injection on `DescribeUserScramCredentials` and `UpdateFeatures`: destroy the borrowed `get_error(i)` deliberately, confirm RED, revert. A double free aborts the process; no managed assertion catches it. | roadmap risk 1 |
| **T-N10** | **The two SCRAM RPCs' unsupported path** — against `MockAdminClient` both fault with the `UnsupportedVersion` code **and the exact message `"Not implemented yet"`**, per-user for `alter`. This is the *correct* behaviour, not a gap (§5.1), and asserting it prevents a later phase "fixing" it. | §5.1 |

### §5.1 ⚠ Mock coverage — better than P5/P6, with one sharp exception

Verified directly against `src/admin/mock_admin_client.rs` and Java's
`MockAdminClient.java`:

| RPC | Rust mock | Java mock |
|---|---|---|
| `createDelegationToken` | real logic `:1804` | real `:641` |
| `renewDelegationToken` | real `:1849` | real `:661` |
| `expireDelegationToken` | real `:1876` | real `:683` |
| `describeDelegationToken` | real `:1911` | real `:709` |
| `describeFeatures` | real `:1936` | real `:1269` |
| `updateFeatures` | real `:1963` | real `:1286` |
| **`describeUserScramCredentials`** | **`unsupported_version("Not implemented yet")`** `:1772` | **throws `UnsupportedOperationException("Not implemented yet")`** `:1254` |
| **`alterUserScramCredentials`** | **`unsupported_version(…)` per user** `:1786` | **throws** `:1259` |

Six of eight have a real broker-free happy path — a marked improvement on P5
(six of nine with *none*). The two SCRAM RPCs correctly mirror Java's own mock per
`admin-client.md` §9, each citing the exact Java line. **This is faithful, not a
defect — do not implement around it.**

**Consequence, and it is the phase's testing constraint:** the RPC with the
**largest input marshalling job** (`alterUserScramCredentials`, 10 arrays) has
**no mock happy path**. Every assertion about its rows — T-N3, T-N4, the
`is_deletions` split, duplicate-user pass-through — must be made **at the submit
seam** (P5's D19 / P6's precedent), not through the mock. Report this in the first
commit body.

---

## §6 — Coordination (VERBATIM from the maintainer; P5/P6 §6 is the precedent)

1. **Exactly ONE Critic review pass for the whole phase, run only after the Actor
   has fully finished implementing all eight RPCs.** No mid-phase or interim Critic
   reviews. This mirrors the M15/P5 and M15/P6 rulings (roadmap D6: *"P5 ships as
   ONE phase, no internal sub-stages, Critic once at the end"*) — the identical
   discipline applies to P7. Do **not** propose a sub-phase split (e.g. SCRAM vs
   tokens vs features) as a way to get more than one Critic pass; if the ABI-read
   exercise genuinely surfaces a new mechanism that needs its own sub-phase, flag
   it explicitly as an **escalation to the maintainer** rather than deciding it
   yourself.
2. **The Actor MAY use internal checkpoints / resumable stages for its own
   progress tracking** (e.g. landing one RPC per session/checkpoint, the way Actor
   75 did for M15/P5 after repeatedly dying to context exhaustion) — this is fine
   and even encouraged, since the maintainer is short on tokens and wants
   resilience against context/autocompact exhaustion. **But a checkpoint is a
   resume point, not a Critic-reviewable stage — it must never trigger an interim
   Critic pass.**
3. **The plan document itself prioritizes CODE AND LOGIC content over process
   narrative.** Bookkeeping sections are pointers to the roadmap, not restatements.

**Agent numbers:** Actor = `dotnet-actor` N=77; Critic = `dotnet-critic` N=77.
**Personas:** `dotnet-actor` / `dotnet-critic` — **never** `actor-executor` /
`kafka-critic`, which are Rust-shaped and review against the wrong ground truth.
**Fix cycles** after the single first review are unbounded and unstaged, until
`COMMENTS.77.md` is empty.

### §6.1 Suggested checkpoint boundaries (Actor's own bookkeeping only)

Ordered so the new mechanisms land late, on top of proven readers, and so the
mock-less RPC lands once the submit-seam harness already exists:

`KafkaPrincipal`/`TokenInformation`/`DelegationToken` + `DelegationTokenMarshal`
(incl. the N2 hmac) → `CreateDelegationToken` (N3 single value) →
`Renew`/`ExpireDelegationToken` (N3 inline scalar + the T-N8 twin guard) →
`DescribeDelegationToken` (3b) → features type family →
`UpdateFeatures` (shape 2) → `DescribeFeatures` (N4 composite) →
SCRAM type family → `DescribeUserScramCredentials` (N1 nested) →
**`AlterUserScramCredentials` last** (10-array marshalling, no mock happy path).

### §6.2 Environment traps — VERBATIM in every Actor/Critic brief

This sandbox has a broken shell init that fabricates **false passes**:

1. **`PATH` is clobbered.** Every Bash call must start with
   `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"`.
   `command -v` is **not** reliable here.
2. **`grep` is aliased to `ugrep`** — rejects some patterns and emits nothing,
   reading as a pass. Use `/usr/bin/grep` for anything relied on as evidence.
3. **`sed` may be missing** — use `awk` or `/usr/bin/sed` explicitly.
4. **`cat` is shadowed by a missing `bat` alias** — `cat > file <<'EOF'` silently
   writes a 0-byte file. Use `/bin/cat`.
5. **This is zsh** — unquoted `$var` is not word-split; unquoted globs abort with
   "no matches found". Always quote.
6. **A test filter matching zero tests exits 0** — always confirm the expected
   **count**, never the exit code.

**Standing context-budget carry-overs (P5 §3.A / P6 §6.2):** grep rather than read
the generated header (it is ~15k lines); **range-read this plan, never whole-file
it**; defer per-RPC Java-source reads until that RPC is being written; build with
`cargo build --features ffi`; **bound every test invocation's output** (an
unbounded `dotnet test` is ~1.16 MB in one call).

---

## §7 — Design decisions (continuing P6's numbering; P6 ended at D39)

**D40 — the four `count`-less results bypass `KeyedResultMarshal` entirely and add
NO walker callable.** `Create`/`Renew`/`ExpireDelegationToken` and
`DescribeFeatures` have no table to walk, so their trampolines call
`SingleAdminOperation<T>.SetResult(...)` directly (§1.3). A `CompleteSingle`
wrapper is explicitly **rejected**: its body would be the call it wraps, and the
walker's value is that each callable names a distinct result **arity**.

**D41 — `DescribeFeaturesResult` publishes ONE `Task<FeatureMetadata>`, not a
fan-out.** Java's stored field is a single `KafkaFuture<FeatureMetadata>` (`:28`)
with one accessor (`:34`), even though the ABI exposes two tables plus a scalar.
The `DescribeCluster`/`DescribeClusterSnapshot` precedent (P3, shape 5) supplies
the read-the-whole-root mechanism; it does **not** supply the fan-out, because
`DescribeCluster` has four Java futures and this has one. Read the accessor.

**D42 — C# does NOT generate a salt; a null `Salt` is passed through as
`has_salts[i] = false`.** Java's 3-arg upsertion ctor (`:54`) generates one via
`ScramFormatter.secureRandomBytes`, but that generation happens **in the core**
when `has_salts` is false (`h:8865-8868`). Generating one in C# as well would be
the binding adding behaviour (`bindings/CLAUDE.md §2.6`) **and** would make the
discriminant unreachable — the ABI would never see `false`. So the C# 3-arg
ctors leave `Salt` null and the marshaller reports `false`; the shape a user
writes is identical to Java's.

**D43 — `TokenInformation` ships CONSISTENT value equality, deliberately NOT
mirroring Java's.** Java's `equals` `:128` excludes `expiryTimestamp` while
`hashCode` `:148` includes it — two instances can be `Equals` with different hash
codes, which corrupts any hash container. `bindings/CLAUDE.md §8.2` makes the Java
**public API shape** the ground truth, not a defect in its implementation, and
nothing on P7's surface uses the type as a key (§4.3), so nothing observable
depends on mirroring the bug. Ship both members over the same field set and record
the non-mirroring at the site per `definition-of-done.md` §7.

**D44 — ✅ RULED 2026-09-22. `RESOURCE_NOT_FOUND` users (N6): stay Mode A, record
the divergence, defer the real fix to P9.** The ABI folds Java's three distinct
`RESOURCE_NOT_FOUND` behaviours into "success with zero credentials" and leaves
the binding no discriminant, so `All()`/`Users()`/`Description()` cannot all be
Java-faithful (§1.7). **The ruling:** ship the ABI's behaviour, record it as a
**known divergence** in a short note at the site *and* in the close-out, and carry
it to P9 as a tracked milestone gap alongside `LogDirDescription.isCordoned()`
(P3/D15). The real fix — a discriminant at the ABI — is a **Rust-core slice
(Mode B)**, outside `dotnet-actor`'s scope per `bindings/dotnet/CLAUDE.md §8.1`,
and is **deferred to P9**, not attempted here.

⚠ **Do NOT widen D44's scope beyond that.** Specifically: no managed heuristic
("zero credentials ⇒ not found" is wrong for a user who genuinely has none), no
ABI or Rust-core edit, no extra C# type or flag invented to model the missing
state, and no test asserting a Java behaviour the ABI cannot produce. Ship the
three accessors over the flattened table, note the divergence, move on.

**D45 — bind `maxLifetimeMs`, not Java's deprecated `maxlifeTimeMs` typo.**
`CreateDelegationTokenOptions` carries both spellings over one field (`:56`/`:70`
deprecated since 4.0, `:61`/`:74` current). Carrying a misspelling into a new
public surface has no compatibility argument behind it here, since no .NET caller
exists yet.

**Open for the maintainer: none.** D40–D45 are all settled. The single item the
Actor may hit mid-phase and must escalate rather than absorb is §1.5's
walker-signature condition.

---

## §8 — Definition of Done

Roadmap §9 in full, per phase. P7-specific additions:

- **Mode-A proof:** `git diff <base>..HEAD -- src/ cbindgen.toml generator/`
  empty, with a control-positive file count under `bindings/dotnet/` in the same
  command. **Do not cite `target/include/confluent_kafka.h`** — it is gitignored
  and proves nothing (STATUS.md:122). A header hash-compare after a clean
  `cargo build --features ffi` is the fallback if ever disputed.
- **Exhaustiveness walk:** the 8 RPCs checked off against `src/admin/mod.rs`'s 46
  methods — a walk, not a recollection. After P7 the milestone stands at
  **40 of 46**, leaving only P8's six.
- **`MockAdminClient` audit** for these 8 per `admin-client.md` §9 — already
  performed in §5.1: six implemented, two surfacing `UnsupportedVersion` with the
  exact Java line cited. Re-verify, do not re-derive.
- **DoD §10 (hot-path allocation audit): N/A** — stated explicitly, never silently
  skipped (`admin-client.md` §10).
- **DoD §11:** N/A to `IAdmin`, but verify the 8 new methods stayed plain `fn`,
  only `Close` returns `Task`, and no `async` bled into the marshallers.
- **Shape justification per RPC:** one line in each commit body citing the **Java
  file and line of the stored field** (§1.0's table is the source). Eight lines,
  eight cites — the Critic verifies all eight.
- **Secrets sweep:** grep the phase diff for any log/trace/exception-message site
  that could reach `Password`, `Salt` or `Hmac` bytes (§4.2). Zero hits required.
- Gates are **CI-only** for Docker/rustfmt/clippy in this sandbox; do not block
  loop closure on them.

---

## §9 — Risks specific to P7

| # | Risk | Mitigation |
|---|---|---|
| 1 | **hmac NUL-scanned instead of length-read** → silently truncated ~12% of the time; the truncated value round-trips into a well-formed request the *broker* rejects, so the binding's own tests pass. | §1.2's `out_len` rule; **T-N2** with an interior-zero fixture, RED-confirmed. |
| 2 | **`has_salts` collapsed with salt length** → an explicit empty salt becomes a generated one, or worse, a null salt becomes a real zero-length salt and the credential ships unsalted. | **D42** + the single-site rule (§4.2); **T-N3**, both directions mutated. |
| 3 | **Password/salt bytes leak into a log, trace or exception message.** No ABI or compiler check catches this. | §4.2's two obligations; **T-N4**; the §8 secrets sweep over the whole diff. |
| 4 | **Cross-wired reader** between `Renew`/`Expire` (identical sets) or `AlterUserScram`/`UpdateFeatures` (identical to each other **and** to two P6 results) returns a plausible answer. | **T-N8** wiring guard + injection, same commit. |
| 5 | **N6 shipped silently** — the `RESOURCE_NOT_FOUND` divergence is invisible against a mock that does not implement the RPC at all, so nothing will surface it in test. | **D44** escalated **before** implementation; recorded at the site and carried to P9. |
| 6 | **`alterUserScramCredentials` has the biggest marshalling job and no mock happy path**, so a row-projection defect is invisible to any result-level assertion. | §5.1's submit-seam mandate; sequenced **last** (§6.1) so the seam harness already exists. |
| 7 | **`short` silently widened to `int`** across the features family (9 ABI sites) — compiles, and round-trips for small values. | §3.2's read-the-accessor rule; the `[InlineData]` rows in T-N8 carry a value above `int16` range. |
| 8 | **Walker signature turns out insufficient for N1's nested value** → a mid-phase rewrite of P1's reviewed foundation. | §1.5's escalation condition, flagged up front rather than discovered late. |

---

## §10 — Authorization

✅ **APPROVED 2026-09-22, as written, with D44 ruled** (§7: stay Mode A, record the
divergence, defer the real fix to P9 — and do not widen that scope).
Implementation is **authorized**. `dotnet-actor` N=77 spawned 2026-09-22 with the
full eight-RPC scope in one continuous pass.

**The §6 coordination constraints are part of the APPROVED PLAN, not preconditions
that expired at approval.** An agent proposing an interim Critic review gets §6.1
as the citation. Restated: one continuous Actor pass; internal checkpoints are
permitted for **resumability only** and never constitute a review gate;
`dotnet-critic` N=77 is spawned **exactly once**, after the Actor reports all
eight RPCs implemented, built and green; only then are fix cycles unbounded until
`COMMENTS.77.md` is empty.

**Token economy is a first-class constraint for this phase** (maintainer, on
approval): the Actor optimizes for **code, behavioural correctness and Java-shape
fidelity**, not documentation. Minimal comments — only where a genuinely
non-obvious constraint needs recording (the D44 divergence, the §1.2 hmac
length-delimited rule, the §2 discriminants). No large docstrings, no prose
restating what the code already says; the same restraint applies to commit
messages. **The review bar is correctness and fidelity, not documentation
completeness.**

---

## §11 — Plan defects found during execution (Manager-owned; recorded, not rewritten)

1. **§1.0/§1.1 classify `describeUserScramCredentials` as result shape 1** (the per-key
   walker, with the `Complete<TKey,TValue>` bridge). That is wrong, and the plan
   contradicts itself: its own **§4.3** already shows the published surface as one
   aggregate future with no per-key `Task`.

   **What is actually true.** Java stores a single
   `KafkaFuture<DescribeUserScramCredentialsResponseData>`
   (`DescribeUserScramCredentialsResult.java:37`) and derives `all()` (`:54`), `users()`
   (`:92`) and `description(String)` (`:114`) from it — none is a per-key future. The ABI
   forces the same conclusion independently: an empty or NULL `users` array describes
   **every** user (`confluent_kafka.h:8784-8785`), so the key set is not knowable at
   submit time and a shape-1 bridge — which mints one `TaskCompletionSource` per key
   *before* the submit — cannot be constructed at all.

   **The shipped code is right.** Actor 77 implemented it as shape 3
   (`SingleAdminOperation` + `CompleteListRpc`, with the three accessors derived from one
   snapshot), adding and editing **zero** walker callables. Confirmed independently by
   Critic 77 against both the Java source and the ABI header, and by the Manager. No code
   change follows from this entry; it exists so the P9 doc-sync corrects §1.0/§1.1.
