# Critic 110: review of Phase 1, the `KafkaException` base (working tree) — resolved

Moved from `COMMENTS.110.md` after the fix; the finding ends with how and where it was resolved.

## 1. Low: `KafkaFuture.ToString()` builds Java's `toString()` of the error by hand, and with D22 it prints `"<type>: "` where Java prints `"<type>"`

- **File:** `dotnet/src/Confluent.Kafka/Internal/KafkaFutureImpl.cs:505`:
  `string exception = error is null ? "null" : error.GetType().FullName + ": " + error.Message;`
- **Java:** `org.apache.kafka.common.internals.KafkaFutureImpl#toString` is
  `String.format("KafkaFuture{value=%s,exception=%s,done=%b}", value, exception, …)`. A `%s` of a
  `Throwable` is its `toString()`, which adds `": " + message` only when `getMessage()` is non-null. Java's
  own `org.apache.kafka.common.KafkaFutureTest#testToString` asserts the bare form for a null message:
  `KafkaFuture{value=null,exception=java.util.concurrent.CancellationException,done=true}`.
- **What is wrong:** the line is the body of the deleted `CauseMessage`, inlined. D21 says
  `JavaText.ToString` "replaces `CauseMessage`", but this copy was not switched. Under D22 a
  `KafkaException` whose Java message is `null` has `Message == ""`. So a future failed with
  `new KafkaException()`, `new KafkaException((string?)null)` or `new SerializationException()` now
  prints `KafkaFuture{value=null,exception=Confluent.Kafka.Common.KafkaException: ,done=true}`.
  - Before Phase 1, .NET's default text followed the colon.
  - Java, and .NET under D21 (a), print `Confluent.Kafka.Common.KafkaException` with no colon.

  The trailing `": "` matches neither. Once Phase 2 generates `QuotaViolationException`, this line will
  also skip D16's override. No later phase touches this method: Phase 6 changes only `Get`, `GetNow` and
  the timeout in `KafkaFutureImpl`.
- **Fix:**
  - Use `string exception = error is null ? "null" : JavaText.ToString(error);`. A BCL exception's
    `Message` is never null, so its output does not change, and the existing `TestToString` assertions
    still pass.
  - Add a case to `KafkaFutureTestBase.TestToString`: a future failed with `new KafkaException()` prints
    `"KafkaFuture{value=null,exception=Confluent.Kafka.Common.KafkaException,done=true}"`.
  - Manager: add `KafkaFuture.ToString` to D21's list of callers of `JavaText.ToString`.

Resolved: `dotnet/src/Confluent.Kafka/Internal/KafkaFutureImpl.cs`, `ToString`, now reads `string exception = error is null ? "null" : JavaText.ToString(error);`, so a null Java message prints the bare type name and a class that overrides `JavaToString` keeps its override. `dotnet/tests/Confluent.Kafka.UnitTests/KafkaFutureTestBase.cs`, `TestToString`, gains the case: a future failed with `new KafkaException()` prints `"KafkaFuture{value=null,exception=Confluent.Kafka.Common.KafkaException,done=true}"`, on every view the base's subclasses supply. The existing `TestToString` assertions are unchanged and pass. Adding `KafkaFuture.ToString` to D21's caller list is the Manager's.

The claim 5 correction needs no code change: no comment or doc in the Phase 1 files says `Headers` keeps its own `ToString()`; the claim was only in the Actor's report.
