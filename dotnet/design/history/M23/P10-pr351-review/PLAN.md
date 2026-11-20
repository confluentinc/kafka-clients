# Plan: the owner's decisions on the review of PR #351

Status: in progress.

Milestone 23, Phase 10. Agent number N=133. Mode A (.NET only): no Rust, C ABI, header, Python or proto change, and
no new P/Invoke. Branch `esabellico/dotnet_async_serdes_and_zero_copy`, at `8be49a80e` (a merge of `origin/master`
into M23/P7 and M23/P8), in the worktree `kafka-clients-dotnet-serdes`. Never pushed by an agent. All paths below are
under `dotnet/`.

## Context

Pranav Shah reviewed PR #351 (M23/P7 asynchronous serdes and zero-copy deserialization, M23/P8 clients built by
`Create`). The owner answered his comments on 2026-11-20. This phase applies the four answers; the minor findings of
`COMMENTS.131.md` are not part of it.

## The decisions

1. **`CancellationToken` defaults** (`IAsyncDeserializer.cs`, `IAsyncSerializer.cs`). Every public asynchronous serde
   method takes `CancellationToken cancellationToken = default`: `Serialize` / `Deserialize` and `Configure` of every
   public asynchronous serde interface, the new ones included, and every public `Async*Serializer` /
   `Async*Deserializer` class, since C# applies an interface's default only to a call made through the interface type.
   Other public methods whose token has no default are listed, with the reason they keep it, under "Deviations and
   open items".
2. **`IAsyncSerializer<T>` split like the synchronous serializers.** `IAsyncSerializer<T>.Serialize(string topic, T?
   data, CancellationToken cancellationToken = default)` is Java's `serialize(String, T)`;
   `IAsyncSerializerWithHeaders<T> : IAsyncSerializer<T>` adds `Serialize(string topic, Headers headers, T? data,
   CancellationToken cancellationToken = default)`, Java's default `serialize(String, Headers, T)`. The extension
   `AsyncSerializerExtensions.Serialize(this IAsyncSerializer<T>, string, Headers, T?, CancellationToken)` plays Java's
   default, as `SerializerExtensions.Serialize` does. The producers choose the form once, when they are built
   (`Internal/AsyncSerializerCall.cs`). The mocks call the headers form with a new, empty `Headers` that they drop,
   and the plain form otherwise, as the synchronous mocks do. The guard refuses, as on the synchronous side, an
   asynchronous serializer that declares a public headers `Serialize` without the interface.
3. **`IAsyncDeserializer<T>` split the same way.** `IAsyncDeserializer<T>.Deserialize(string topic,
   ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)`; `IAsyncDeserializerWithHeaders<T> :
   IAsyncDeserializer<T>` adds `Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data,
   CancellationToken cancellationToken = default)`. `IAsyncDeserializerWithRetention<T>` extends
   `IAsyncDeserializerWithHeaders<T>`, still memberless; its permission applies to the `data` of the headers form,
   which is the form the consumers call on it. The consumers choose the form once
   (`Internal/AsyncDeserializerCall.cs`). The copy-out contracts, zero-copy and retention, the revocation of `data`
   after the call, and cancellation and rewind are unchanged.
4. **The drafted rule text is applied.** P7's drafts R1–R5 (`dotnet/CLAUDE.md` R1–R3,
   `dotnet/.claude/rules/ffi-marshalling.md` R4–R5) and P8's R1–R14 (`dotnet/CLAUDE.md`, R12 the README), each
   located by its quoted text, the asynchronous serde shape written as decisions 1–3 make it, and decisions 1–3 added
   where the rule files describe that shape. Each of the two plans marks its drafts applied. No rule file outside
   `dotnet/` is touched.

## Ledger rows

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 584 | Owner 2026-11-20 | Token default on the asynchronous serdes | every public asynchronous serde interface method (`Serialize`, `Deserialize`, `Configure`) and every public `Async*Serializer` / `Async*Deserializer` class method takes `CancellationToken cancellationToken = default`. Supersedes ledger 572's "required on the interface method" | Agreed |
| 585 | Owner 2026-11-20 | `IAsyncSerializerWithHeaders<T>` | `IAsyncSerializer<T>.Serialize(topic, data, token)` is Java's `serialize(String, T)`; `IAsyncSerializerWithHeaders<T> : IAsyncSerializer<T>` adds the headers form, Java's default `serialize(String, Headers, T)`; the producers choose once, at construction; mirrors `ISerializer<T>` / `ISerializerWithHeaders<T>`. Supersedes, for serializers, P7's "one method each … the `Headers` always passed" (its draft R2, the shape behind ledger 539) | Agreed |
| 586 | Owner 2026-11-20 | `IAsyncDeserializerWithHeaders<T>` | `IAsyncDeserializer<T>.Deserialize(topic, data, token)`; `IAsyncDeserializerWithHeaders<T> : IAsyncDeserializer<T>` adds the headers form; the consumers choose once; mirrors `IDeserializer<T>` / `IDeserializerWithHeaders<T>`. Supersedes the same text for deserializers | Agreed |
| 587 | Owner 2026-11-20 | Retention | `IAsyncDeserializerWithRetention<T>` extends `IAsyncDeserializerWithHeaders<T>`, has no member, and its permission applies to the `data` of the headers form | Agreed |
| 588 | Owner 2026-11-20 | Rule text | P7's drafts R1–R5 and P8's R1–R14 are applied to `dotnet/CLAUDE.md`, `dotnet/.claude/rules/ffi-marshalling.md` and `dotnet/README.md`, the asynchronous serde shape written as rows 584–587 make it | Agreed |

## Steps (one commit each, `git commit --no-verify`)

- **S0** this plan and the `STATUS.md` entry.
- **S1** the token defaults on the existing asynchronous serde interfaces and built-in classes.
- **S2** the serializer split: interfaces, extension dispatch, `AsyncSerializerCall`, `SerializedProducerRecord`,
  the guard, the built-ins, every caller and implementer (`src/`, `tests/`, `grpc-server/`, `soak/`, `examples/`,
  `tools/AotSmoke/`), the analyzer lists, and tests pinning the dispatch on both producers.
- **S3** the deserializer split, the same way, and tests pinning the dispatch on both consumers, retention included.
- **S4** the rule text, `README.md`, `examples/README.md` and the XML docs; the two phase plans mark their drafts
  applied.
- **S5** the full verification and any fixups; this plan's "Deviations and open items" and the `STATUS.md` entry
  completed.

## Gates per step

From `dotnet/`, the native library built by `cargo build --features ffi` in `rust/` (debug profile, so every
`dotnet` command takes `-p:CargoProfileDir=debug`): `dotnet build Confluent.Kafka.sln -c Release` 0 warnings / 0
errors, `dotnet format --verify-no-changes`, the unit tests on net8.0. At S5 also the unit tests on net10.0, the gRPC
server unit tests, `make -C dotnet test-aot-dotnet` and, if no Docker conflict stops it,
`make -C dotnet test-grpc-server-dotnet`. `make verify-dotnet` is the owner's.

## Deviations and open items

Filled in at S5.
