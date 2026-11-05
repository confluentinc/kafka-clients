# COMMENTS.DONE.105 — M21/P1

## 1. `b6611c36` — three ACL constructor messages still name the old member `Any`, not Java's `ANY`

- **Where:** `dotnet/src/Confluent.Kafka/Common/Acl/AccessControlEntry.cs:65` ("operation must not be Any"),
  `:70` ("permissionType must not be Any"); `dotnet/src/Confluent.Kafka/Common/Resource/ResourcePattern.cs:71`
  ("resourceType must not be Any").
- **What is wrong:** each literal sits under the comment "Java's message, with the C# member name"
  (`AccessControlEntry.cs:64`, `ResourcePattern.cs:70`). S1b renamed the C# members to `AclOperation.ANY`,
  `AclPermissionType.ANY` and `ResourceType.ANY`, so the deviation's premise is gone and the text now names
  no member at all. PLAN §3.1 ("every text the binding prints must equal Java's") covers it. The
  neighbouring check in the same constructor now prints Java's text (`ResourcePattern.cs:77`, "patternType
  must not be ANY"/"MATCH", which the commit message lists as fixed), so one class gives two spellings.
- **Evidence:**
  - Java `org.apache.kafka.common.acl.AccessControlEntry` constructor: "operation must not be ANY",
    "permissionType must not be ANY"; `org.apache.kafka.common.resource.ResourcePattern` constructor:
    "resourceType must not be ANY".
  - Python `python/confluent_kafka/common/acl/access_control_entry.py:56`, `:60` and
    `python/confluent_kafka/common/resource/resource_pattern.py:61`: the same texts with `ANY`. The core
    and C agree (`rust/src/common/acl/access_control_entry.rs:54`, `:57`;
    `rust/src/common/resource/resource_pattern.rs:59`; `c/tests/test_mock_admin.c:4029-4051`).
  - The text goes out on the gRPC wire. `dotnet/grpc-server/TranslateAdmin.cs:989-1010` builds these
    types from the request, so a CreateAcls binding with an `ANY` operation is sent as -3 "operation must
    not be Any", where Python's server sends "operation must not be ANY".
  - Five tests pin the stale text with `StartsWith`: `dotnet/tests/Confluent.Kafka.UnitTests/PublicAclTypesTests.cs:99`,
    `:246`, `:256`; `PublicAclUndefinedCodeTests.cs:189`, `:194`.
- **Fix:** write `ANY` in the three literals and drop the two "with the C# member name" comments. Update the
  five asserts to Java's full text (`Assert.Equal` on the message before " (Parameter '…')", as
  `PublicProducerRecordTests.WithoutDotNetSuffix` does).

  Resolved in fef7a81c6.

## 2. `b6611c36` — `GroupListing.ToString` drops Java's two closing quotes, and the new tests pin it

- **Where:** `dotnet/src/Confluent.Kafka/Admin/GroupListing.cs:135`, the format
  `"(groupId='{0}, type={1}, protocol='{2}, groupState={3})"`. New tests in
  `dotnet/tests/Confluent.Kafka.UnitTests/PublicJavaToStringEnumTextTests.cs:83`, `:92`, `:104`, `:113`
  assert this text in full, under test names that say "PrintsJavasToString".
- **What is wrong:** Java prints `(groupId='g', type=…, protocol='consumer', groupState=…)`. .NET prints
  `(groupId='g, type=…, protocol='consumer, groupState=…)`. The defect predates the phase (the format
  dates from `0204437a`). But PLAN §3.1 lists `src/Admin/GroupListing.cs:136-138` in the S1 inventory of
  printed texts that "must equal Java's". S1b rewrote the arguments of this `string.Format` and added the
  four assertions, and those assertions now pin text that is not Java's. Nothing in the ledger or the design
  records a deviation.
- **Evidence:** Java `org.apache.kafka.clients.admin.GroupListing#toString`: `"groupId='" + groupId + '\''`
  and `", protocol='" + protocol + '\''`. Python `python/confluent_kafka/admin/group_listing.py:83`, `:85`
  has both quotes, and `python/test/unit/test_admin_value_types.py:760` asserts
  `"(groupId='g', type=none, protocol='p', groupState=none)"`.
- **Fix:** use the format `"(groupId='{0}', type={1}, protocol='{2}', groupState={3})"` and update the four
  test expectations. A `GroupListing` test with the Python vector above would pin it.

  Resolved in e621a17f2.

## Comment-only notes applied in b9c50706

The 11 notes Critic 105 left for the comment-only commit, applied in `b9c50706` (`docs(dotnet): comment-only cleanup for M21/P1`), as written:

- `dotnet/src/Confluent.Kafka/Common/Acl/AccessControlEntry.cs:130-132`: "print with their .NET names (`Read`, `Allow`), where Java prints its constants" is stale. After S1b they print Java's constants.
- `dotnet/src/Confluent.Kafka/Common/Acl/AccessControlEntryFilter.cs:127-129`: the same stale sentence.
- `dotnet/src/Confluent.Kafka/Common/Resource/ResourcePattern.cs:124-126`: "the types print with their .NET names (`Topic`, `Literal`)" is stale.
- `dotnet/src/Confluent.Kafka/Common/Resource/ResourcePatternFilter.cs:105-106`: the same stale sentence.
- `dotnet/src/Confluent.Kafka/Common/Acl/AclBinding.cs:80` and `AclBindingFilter.cs:86`: "with the enums' .NET names" is stale.
- `dotnet/src/Confluent.Kafka/Admin/ScramCredentialInfo.cs:59-60`: "prints with its .NET name (`ScramSha256`), where Java prints its constant (`SCRAM_SHA_256`)" is stale. It now prints `SCRAM_SHA_256`.
- `dotnet/src/Confluent.Kafka/Admin/UserScramCredentialsDescription.cs:77-78`: "with its .NET name" is stale.
- `dotnet/src/Confluent.Kafka/Admin/ConsumerGroupDescription.cs:231-233` and `ClassicGroupDescription.cs:239-241`: "render with their C# member names (`[Read, Write]` where Java prints `[READ, WRITE]`) … a recorded deviation" is stale. The deviation is gone.
- `dotnet/src/Confluent.Kafka/Common/ClassicGroupState.cs:27`: `<c>Assigning</c>` and `<c>Reconciling</c>` should be `GroupState.ASSIGNING` / `RECONCILING`.
- `dotnet/tests/Confluent.Kafka.UnitTests/PublicAclTypesTests.cs:26` and `PublicAclUndefinedCodeTests.cs:24-26`: `<c>Any</c>` / `<c>Match</c>` / `<c>Unknown</c>` should be the new member names.
- The six enums whose Java `toString()` is overridden (`TransactionState.cs`, `GroupState.cs`, `GroupType.cs`, `ClassicGroupState.cs`, `IsolationLevel.cs`, `TimestampType.cs`): none of their `<remarks>` says that the enum's own `ToString()` prints the constant, not Java's `toString()`. That is the recorded deviation of PLAN §2.2 #2. `dotnet/CLAUDE.md` §4 Comments rule 3 allows one concise sentence for it.

How each was applied:

- The eight `ToString` docs (the ACL, resource-pattern and SCRAM notes): the stale sentence is removed, the rest of the summary kept.
- `ConsumerGroupDescription` / `ClassicGroupDescription`: the `<remarks>` on `ToString` is removed.
- `ClassicGroupState`: `<see cref="GroupState.ASSIGNING"/>` and `<see cref="GroupState.RECONCILING"/>`.
- The two test summaries: `<c>ANY</c>` / `<c>MATCH</c>` / `<c>UNKNOWN</c>`.
- The six enums: one remarks sentence each, "`ToString()` gives the Java constant name (`X`), not Java's `toString()` text (`\"Xx\"`): a recorded deviation." (a new `<para>` where the remarks had paragraphs; appended to `IsolationLevel`'s one-line remarks; a new `<remarks>` on `TimestampType`).
- The sweep of `51f453af..e621a17f` for comments naming the old members also found `<c>Unknown</c>` in `IAdmin.cs` (3), `IAsyncAdmin.cs` (3), the `IsUnknown` docs of `AccessControlEntry`, `AccessControlEntryFilter`, `AclBinding`, `AclBindingFilter`, `ResourcePattern`, `ResourcePatternFilter`, and in `Internal/AclEnumCodes.cs` and `Internal/Interop/GroupMarshal.cs`; all now `<c>UNKNOWN</c>`. No comment names an old member qualified (`Enum.OldName`, PCRE grep with a control positive), and none of the comments the phase added contradicts its code.
- Comment-only proof: with the comment lines removed, each of the 22 files is byte-identical to its parent. Gates G1–G6 green: 5092 / 2 per TFM, gRPC 189, soak 198, perf 71.

## 3. `b6611c36` — a test comment still says Java's constant name is not the .NET member name

- **Where:** `dotnet/tests/Confluent.Kafka.UnitTests/PublicAdminP3ShapeParityTests.cs:197`:

  ```csharp
  // Java's constant name, not the .NET member name.
  Assert.Equal("ConfigResource(type=TOPIC, name='t')", topic.ToString());
  ```

- **Why it is false:** S1b (`b6611c36`) renamed `ConfigResourceType.Topic` to `TOPIC` in this very
  file (the lines around it now read `ConfigResourceType.TOPIC` / `.BROKER`, `:174-177`, `:194`,
  `:199`). The .NET member name is now `TOPIC`, the text asserted. So "not the .NET member name"
  says the two differ when they are the same. Reference: Java's `ConfigResource.Type.TOPIC` and
  `ConfigResource#toString` (`"ConfigResource(type=" + type + ", name='" + name + "')"`).
- **Fix:** delete the comment (`dotnet/CLAUDE.md` §4 Comments rule 3: it records no decision), or
  keep only "Java's constant name." Comment-only; nothing else changes.
- **Why it was missed:** my pass 1 notes missed it, and so did the commit's sweep, which looked for
  comments that name an old member. This one names no member, it only contrasts the two names. A
  sweep of comment lines for `\.NET (member|name)` / `C# (member|name)` / `constant name` over
  `dotnet/{src,tests,grpc-server,soak}` at `b9c50706` finds it and nothing else stale. The other
  hits are true: `TranslateAdmin.cs:1286`, the six new remarks, `AlterConfigOp.cs:94`,
  `ConfigEntry.cs:207` and `:441`, `FeatureUpdate.cs:108`, `ConfigResource.cs:82`,
  `PublicJavaToStringEnumTextTests.cs:27`.

  Resolved in 93687a3cb.
