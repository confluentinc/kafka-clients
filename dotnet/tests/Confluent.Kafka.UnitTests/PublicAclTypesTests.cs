// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the ACL value-type family (<see cref="ResourceType"/>, <see cref="PatternType"/>,
/// <see cref="AclPermissionType"/>, <see cref="ResourcePattern"/>,
/// <see cref="ResourcePatternFilter"/>, <see cref="AccessControlEntry"/>,
/// <see cref="AccessControlEntryFilter"/>, <see cref="AclBinding"/>,
/// <see cref="AclBindingFilter"/>) against the Java classes it restores.
/// </summary>
/// <remarks>
/// <para>
/// Two properties carry most of the weight. <b>(1) The concrete/filter split:</b> concrete types
/// reject null strings and the <c>Any</c>/<c>Match</c> sentinels, filters accept both — and the
/// rejection <em>messages</em> are asserted as strings (<c>definition-of-done.md</c> §3).
/// <b>(2) <c>null</c> is not <c>""</c>:</b> on a filter, <c>null</c> means "match any" while
/// <c>""</c> filters on the literal empty name, and collapsing the two would later delete ACLs
/// the user never asked to delete (PLAN §2.1).
/// </para>
/// <para>
/// Value equality is asserted through a real dictionary lookup, because that is how these types
/// are used on the ACL RPC results — a wrong <c>GetHashCode</c> fails there and nowhere else
/// (PLAN D39).
/// </para>
/// <para>Everything here is pure managed state: no native handle, no broker.</para>
/// </remarks>
public sealed class PublicAclTypesTests
{
    // ---- enums: values are Kafka wire codes, not C# ordinals ----

    /// <summary>Every <see cref="ResourceType"/> member carries Java's wire code.</summary>
    /// <param name="value">The member.</param>
    /// <param name="code">Java's <c>code()</c>.</param>
    [Theory]
    [InlineData(ResourceType.Unknown, 0)]
    [InlineData(ResourceType.Any, 1)]
    [InlineData(ResourceType.Topic, 2)]
    [InlineData(ResourceType.Group, 3)]
    [InlineData(ResourceType.Cluster, 4)]
    [InlineData(ResourceType.TransactionalId, 5)]
    [InlineData(ResourceType.DelegationToken, 6)]
    [InlineData(ResourceType.User, 7)]
    public void ResourceType_MemberValuesAreJavaWireCodes(ResourceType value, int code) =>
        Assert.Equal(code, (int)value);

    /// <summary>Every <see cref="PatternType"/> member carries Java's wire code.</summary>
    /// <param name="value">The member.</param>
    /// <param name="code">Java's <c>code()</c>.</param>
    [Theory]
    [InlineData(PatternType.Unknown, 0)]
    [InlineData(PatternType.Any, 1)]
    [InlineData(PatternType.Match, 2)]
    [InlineData(PatternType.Literal, 3)]
    [InlineData(PatternType.Prefixed, 4)]
    public void PatternType_MemberValuesAreJavaWireCodes(PatternType value, int code) =>
        Assert.Equal(code, (int)value);

    /// <summary>Every <see cref="AclPermissionType"/> member carries Java's wire code.</summary>
    /// <param name="value">The member.</param>
    /// <param name="code">Java's <c>code()</c>.</param>
    [Theory]
    [InlineData(AclPermissionType.Unknown, 0)]
    [InlineData(AclPermissionType.Any, 1)]
    [InlineData(AclPermissionType.Deny, 2)]
    [InlineData(AclPermissionType.Allow, 3)]
    public void AclPermissionType_MemberValuesAreJavaWireCodes(AclPermissionType value, int code) =>
        Assert.Equal(code, (int)value);

    /// <summary>The three enums declare exactly the members Java declares — no more.</summary>
    [Fact]
    public void Enums_DeclareExactlyJavasMemberCounts()
    {
        Assert.Equal(8, Enum.GetValues(typeof(ResourceType)).Length);
        Assert.Equal(5, Enum.GetValues(typeof(PatternType)).Length);
        Assert.Equal(4, Enum.GetValues(typeof(AclPermissionType)).Length);
    }

    // ---- ResourcePattern: concrete, so it rejects ----

    /// <summary>A valid pattern exposes exactly what it was given.</summary>
    [Fact]
    public void ResourcePattern_ExposesItsComponents()
    {
        ResourcePattern pattern = new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal);

        Assert.Equal(ResourceType.Topic, pattern.ResourceType);
        Assert.Equal("orders", pattern.Name);
        Assert.Equal(PatternType.Literal, pattern.PatternType);
        Assert.False(pattern.IsUnknown);
    }

    /// <summary>Java's wildcard resource name is <c>*</c> (<c>ResourcePattern.java:30</c>).</summary>
    [Fact]
    public void ResourcePattern_WildcardResourceIsStar() =>
        Assert.Equal("*", ResourcePattern.WildcardResource);

    /// <summary>A null name is rejected — Java's <c>Objects.requireNonNull(name)</c> (<c>:45</c>).</summary>
    [Fact]
    public void ResourcePattern_NullName_Throws()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ResourcePattern(ResourceType.Topic, null!, PatternType.Literal));

        Assert.Equal("name", ex.ParamName);
    }

    /// <summary>
    /// <see cref="ResourceType.Any"/> is rejected with Java's message (<c>:48-50</c>).
    /// </summary>
    [Fact]
    public void ResourcePattern_AnyResourceType_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new ResourcePattern(ResourceType.Any, "orders", PatternType.Literal));

        Assert.StartsWith("resourceType must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("resourceType", ex.ParamName);
    }

    /// <summary>
    /// The two filter-only pattern types are rejected with Java's message (<c>:52-54</c>), which
    /// names the offending value.
    /// </summary>
    /// <param name="patternType">The filter-only pattern type.</param>
    /// <param name="expected">The exact message text.</param>
    [Theory]
    [InlineData(PatternType.Any, "patternType must not be Any")]
    [InlineData(PatternType.Match, "patternType must not be Match")]
    public void ResourcePattern_FilterOnlyPatternType_ThrowsWithJavasMessage(
        PatternType patternType,
        string expected)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new ResourcePattern(ResourceType.Topic, "orders", patternType));

        Assert.StartsWith(expected, ex.Message, StringComparison.Ordinal);
        Assert.Equal("patternType", ex.ParamName);
    }

    /// <summary>An empty name is a legal concrete name — only <c>null</c> is rejected.</summary>
    [Fact]
    public void ResourcePattern_EmptyName_IsAccepted() =>
        Assert.Equal(string.Empty, new ResourcePattern(ResourceType.Topic, string.Empty, PatternType.Literal).Name);

    /// <summary><c>Unknown</c> components make the pattern unknown (Java's <c>:93</c>).</summary>
    [Fact]
    public void ResourcePattern_IsUnknown_TracksEitherComponent()
    {
        Assert.True(new ResourcePattern(ResourceType.Unknown, "orders", PatternType.Literal).IsUnknown);
        Assert.True(new ResourcePattern(ResourceType.Topic, "orders", PatternType.Unknown).IsUnknown);
        Assert.False(new ResourcePattern(ResourceType.Topic, "orders", PatternType.Prefixed).IsUnknown);
    }

    /// <summary>Java's <c>toFilter()</c> (<c>:81</c>) carries all three components across.</summary>
    [Fact]
    public void ResourcePattern_ToFilter_CarriesEveryComponent()
    {
        ResourcePatternFilter filter =
            new ResourcePattern(ResourceType.Group, "g1", PatternType.Prefixed).ToFilter();

        Assert.Equal(new ResourcePatternFilter(ResourceType.Group, "g1", PatternType.Prefixed), filter);
    }

    /// <summary>Java's <c>toString()</c> rendering (<c>:86</c>).</summary>
    [Fact]
    public void ResourcePattern_ToString_MatchesJava() =>
        Assert.Equal(
            "ResourcePattern(resourceType=Topic, name=orders, patternType=Literal)",
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal).ToString());

    // ---- ResourcePatternFilter: accepts everything, and null != "" ----

    /// <summary>
    /// A filter accepts every combination the concrete type rejects (Java's <c>:52</c> has no
    /// validation at all).
    /// </summary>
    [Fact]
    public void ResourcePatternFilter_AcceptsAnyAndMatchAndNullName()
    {
        ResourcePatternFilter filter = new ResourcePatternFilter(ResourceType.Any, null, PatternType.Match);

        Assert.Equal(ResourceType.Any, filter.ResourceType);
        Assert.Null(filter.Name);
        Assert.Equal(PatternType.Match, filter.PatternType);
    }

    /// <summary>Java's <c>ANY</c> constant (<c>:31</c>) is the match-everything filter.</summary>
    [Fact]
    public void ResourcePatternFilter_Any_IsTheMatchEverythingFilter()
    {
        Assert.Equal(ResourceType.Any, ResourcePatternFilter.Any.ResourceType);
        Assert.Null(ResourcePatternFilter.Any.Name);
        Assert.Equal(PatternType.Any, ResourcePatternFilter.Any.PatternType);
    }

    /// <summary>
    /// ⚠ A <c>null</c> name (match any) and an empty name (the literal name <c>""</c>) are
    /// different values and must never compare equal — PLAN §2.1.
    /// </summary>
    [Fact]
    public void ResourcePatternFilter_NullName_IsNotEmptyName()
    {
        ResourcePatternFilter wildcard = new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal);
        ResourcePatternFilter empty =
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal);

        Assert.NotEqual(wildcard, empty);
        Assert.Null(wildcard.Name);
        Assert.Equal(string.Empty, empty.Name);

        // The distinction has to survive a dictionary, which is where these types are used.
        Dictionary<ResourcePatternFilter, string> byFilter = new Dictionary<ResourcePatternFilter, string>
        {
            [wildcard] = "wildcard",
            [empty] = "empty",
        };

        Assert.Equal(2, byFilter.Count);
        Assert.Equal("wildcard", byFilter[new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal)]);
        Assert.Equal(
            "empty",
            byFilter[new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal)]);
    }

    /// <summary>Equal filters agree on <c>GetHashCode</c>; differing ones are not equal.</summary>
    [Fact]
    public void ResourcePatternFilter_ValueEquality()
    {
        ResourcePatternFilter a = new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Prefixed);
        ResourcePatternFilter b = new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Prefixed);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Group, "orders", PatternType.Prefixed));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Topic, "Orders", PatternType.Prefixed));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Literal));
        Assert.False(a.Equals("not a filter"));
    }

    /// <summary>
    /// Java's <c>toString()</c> (<c>:144</c>) renders a null name as <c>&lt;any&gt;</c> and keeps
    /// its (quirky) <c>ResourcePattern(…)</c> label.
    /// </summary>
    [Fact]
    public void ResourcePatternFilter_ToString_MatchesJava()
    {
        Assert.Equal(
            "ResourcePattern(resourceType=Any, name=<any>, patternType=Any)",
            ResourcePatternFilter.Any.ToString());
        Assert.Equal(
            "ResourcePattern(resourceType=Topic, name=, patternType=Literal)",
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal).ToString());
    }

    // ---- AccessControlEntry: concrete, so it rejects ----

    /// <summary>A valid entry exposes exactly what it was given.</summary>
    [Fact]
    public void AccessControlEntry_ExposesItsComponents()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);

        Assert.Equal("User:alice", entry.Principal);
        Assert.Equal("*", entry.Host);
        Assert.Equal(AclOperation.Read, entry.Operation);
        Assert.Equal(AclPermissionType.Allow, entry.PermissionType);
        Assert.False(entry.IsUnknown);
    }

    /// <summary>
    /// A null principal or host is rejected — Java's <c>Objects.requireNonNull</c> (<c>:37-38</c>).
    /// </summary>
    [Fact]
    public void AccessControlEntry_NullStrings_Throw()
    {
        Assert.Equal(
            "principal",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry(null!, "*", AclOperation.Read, AclPermissionType.Allow)).ParamName);

        Assert.Equal(
            "host",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry("User:alice", null!, AclOperation.Read, AclPermissionType.Allow))
                .ParamName);
    }

    /// <summary>
    /// <see cref="AclOperation.Any"/> is rejected with Java's message (<c>:40-41</c>).
    /// </summary>
    [Fact]
    public void AccessControlEntry_AnyOperation_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.Any, AclPermissionType.Allow));

        Assert.StartsWith("operation must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("operation", ex.ParamName);
    }

    /// <summary>
    /// <see cref="AclPermissionType.Any"/> is rejected with Java's message (<c>:43-44</c>).
    /// </summary>
    [Fact]
    public void AccessControlEntry_AnyPermissionType_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Any));

        Assert.StartsWith("permissionType must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("permissionType", ex.ParamName);
    }

    /// <summary>Empty principal / host are legal concrete values — only <c>null</c> is rejected.</summary>
    [Fact]
    public void AccessControlEntry_EmptyStrings_AreAccepted()
    {
        AccessControlEntry entry = new AccessControlEntry(
            string.Empty, string.Empty, AclOperation.Describe, AclPermissionType.Deny);

        Assert.Equal(string.Empty, entry.Principal);
        Assert.Equal(string.Empty, entry.Host);
    }

    /// <summary><c>Unknown</c> components make the entry unknown (Java's <c>:91</c>).</summary>
    [Fact]
    public void AccessControlEntry_IsUnknown_TracksEitherEnum()
    {
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.Unknown, AclPermissionType.Allow).IsUnknown);
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Unknown).IsUnknown);
    }

    /// <summary>Java's <c>toFilter()</c> (<c>:79</c>) carries all four components across.</summary>
    [Fact]
    public void AccessControlEntry_ToFilter_CarriesEveryComponent()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntry("User:alice", "10.0.0.1", AclOperation.Write, AclPermissionType.Deny).ToFilter();

        Assert.Equal(
            new AccessControlEntryFilter("User:alice", "10.0.0.1", AclOperation.Write, AclPermissionType.Deny),
            filter);
    }

    /// <summary>Java's <c>toString()</c> rendering (<c>AccessControlEntryData.java:76</c>).</summary>
    [Fact]
    public void AccessControlEntry_ToString_MatchesJava() =>
        Assert.Equal(
            "(principal=User:alice, host=*, operation=Read, permissionType=Allow)",
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow).ToString());

    // ---- AccessControlEntryFilter: accepts everything, and null != "" ----

    /// <summary>A filter accepts the nulls and the <c>Any</c> sentinels (Java's <c>:42</c>).</summary>
    [Fact]
    public void AccessControlEntryFilter_AcceptsNullsAndAny()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any);

        Assert.Null(filter.Principal);
        Assert.Null(filter.Host);
        Assert.Equal(AclOperation.Any, filter.Operation);
        Assert.Equal(AclPermissionType.Any, filter.PermissionType);
    }

    /// <summary>Java's <c>ANY</c> constant (<c>:31</c>) is the match-everything filter.</summary>
    [Fact]
    public void AccessControlEntryFilter_Any_IsTheMatchEverythingFilter() =>
        Assert.Equal(
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any),
            AccessControlEntryFilter.Any);

    /// <summary>
    /// ⚠ <c>null</c> (match any) and <c>""</c> (the literal empty principal/host) are different
    /// values on both nullable fields — PLAN §2.1.
    /// </summary>
    [Fact]
    public void AccessControlEntryFilter_NullStrings_AreNotEmptyStrings()
    {
        AccessControlEntryFilter nullPrincipal =
            new AccessControlEntryFilter(null, "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter emptyPrincipal =
            new AccessControlEntryFilter(string.Empty, "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter nullHost =
            new AccessControlEntryFilter("User:a", null, AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter emptyHost =
            new AccessControlEntryFilter("User:a", string.Empty, AclOperation.Read, AclPermissionType.Allow);

        Assert.NotEqual(nullPrincipal, emptyPrincipal);
        Assert.NotEqual(nullHost, emptyHost);

        Dictionary<AccessControlEntryFilter, string> byFilter = new Dictionary<AccessControlEntryFilter, string>
        {
            [nullPrincipal] = "null-principal",
            [emptyPrincipal] = "empty-principal",
            [nullHost] = "null-host",
            [emptyHost] = "empty-host",
        };

        Assert.Equal(4, byFilter.Count);
        Assert.Equal(
            "empty-principal",
            byFilter[new AccessControlEntryFilter(string.Empty, "*", AclOperation.Read, AclPermissionType.Allow)]);
    }

    /// <summary>Equal filters agree on <c>GetHashCode</c>; differing ones are not equal.</summary>
    [Fact]
    public void AccessControlEntryFilter_ValueEquality()
    {
        AccessControlEntryFilter a =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter b =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new AccessControlEntryFilter("User:bob", "*", AclOperation.Read, AclPermissionType.Allow));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Write, AclPermissionType.Allow));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Deny));
        Assert.False(a.Equals(42));
    }

    /// <summary>
    /// Java's <c>toString()</c> renders null components as <c>&lt;any&gt;</c>
    /// (<c>AccessControlEntryData.java:77-78</c>).
    /// </summary>
    [Fact]
    public void AccessControlEntryFilter_ToString_RendersNullAsAny() =>
        Assert.Equal(
            "(principal=<any>, host=<any>, operation=Any, permissionType=Any)",
            AccessControlEntryFilter.Any.ToString());

    // ---- AclBinding / AclBindingFilter: nested over the flat ABI ----

    /// <summary>
    /// The binding is <b>nested</b>: the pattern and the entry are reachable as real types, not
    /// flattened onto the binding (PLAN D36).
    /// </summary>
    [Fact]
    public void AclBinding_IsNested_NotFlattened()
    {
        AclBinding binding = NewBinding();

        Assert.Equal("orders", binding.Pattern.Name);
        Assert.Equal(ResourceType.Topic, binding.Pattern.ResourceType);
        Assert.Equal(PatternType.Literal, binding.Pattern.PatternType);
        Assert.Equal("User:alice", binding.Entry.Principal);
        Assert.Equal("*", binding.Entry.Host);
        Assert.Equal(AclOperation.Read, binding.Entry.Operation);
        Assert.Equal(AclPermissionType.Allow, binding.Entry.PermissionType);
        Assert.False(binding.IsUnknown);
    }

    /// <summary>Null halves are rejected — Java's <c>:38-39</c>.</summary>
    [Fact]
    public void AclBinding_NullHalves_Throw()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);
        ResourcePattern pattern = new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal);

        Assert.Equal("pattern", Assert.Throws<ArgumentNullException>(() => new AclBinding(null!, entry)).ParamName);
        Assert.Equal("entry", Assert.Throws<ArgumentNullException>(() => new AclBinding(pattern, null!)).ParamName);
    }

    /// <summary><c>IsUnknown</c> propagates from either half (Java's <c>:45</c>).</summary>
    [Fact]
    public void AclBinding_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.Unknown, "orders", PatternType.Literal),
            new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Allow)).IsUnknown);

        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal),
            new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Unknown)).IsUnknown);
    }

    /// <summary>Java's <c>toFilter()</c> (<c>:66</c>) filters both halves down to this binding.</summary>
    [Fact]
    public void AclBinding_ToFilter_CarriesBothHalves()
    {
        AclBindingFilter filter = NewBinding().ToFilter();

        Assert.Equal(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Literal),
                new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow)),
            filter);
    }

    /// <summary>Java's <c>toString()</c> rendering (<c>:71</c>), nesting both halves.</summary>
    [Fact]
    public void AclBinding_ToString_MatchesJava() =>
        Assert.Equal(
            "(pattern=ResourcePattern(resourceType=Topic, name=orders, patternType=Literal), "
                + "entry=(principal=User:alice, host=*, operation=Read, permissionType=Allow))",
            NewBinding().ToString());

    /// <summary>
    /// ⚠ The dictionary-key round-trip (PLAN D39): a binding built independently but equal by
    /// value must find the entry a differently-constructed equal binding stored. Without a
    /// matching <c>GetHashCode</c> this misses silently.
    /// </summary>
    [Fact]
    public void AclBinding_IsUsableAsADictionaryKey()
    {
        Dictionary<AclBinding, string> byBinding = new Dictionary<AclBinding, string>
        {
            [NewBinding()] = "created",
        };

        Assert.True(byBinding.ContainsKey(NewBinding()));
        Assert.Equal("created", byBinding[NewBinding()]);
        Assert.Equal(NewBinding().GetHashCode(), NewBinding().GetHashCode());

        AclBinding other = new AclBinding(
            new ResourcePattern(ResourceType.Topic, "payments", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

        Assert.False(byBinding.ContainsKey(other));
        Assert.NotEqual(NewBinding(), other);
        Assert.False(NewBinding().Equals("not a binding"));
    }

    /// <summary>Java's <c>ANY</c> constant (<c>:34</c>) is built from the two half-filters' <c>ANY</c>.</summary>
    [Fact]
    public void AclBindingFilter_Any_IsBothHalvesAny()
    {
        Assert.Equal(ResourcePatternFilter.Any, AclBindingFilter.Any.PatternFilter);
        Assert.Equal(AccessControlEntryFilter.Any, AclBindingFilter.Any.EntryFilter);
        Assert.Equal(
            new AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any),
            AclBindingFilter.Any);
    }

    /// <summary>Null halves are rejected — Java's <c>:43-44</c>.</summary>
    [Fact]
    public void AclBindingFilter_NullHalves_Throw()
    {
        Assert.Equal(
            "patternFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(null!, AccessControlEntryFilter.Any)).ParamName);

        Assert.Equal(
            "entryFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(ResourcePatternFilter.Any, null!)).ParamName);
    }

    /// <summary><c>IsUnknown</c> propagates from either half (Java's <c>:50</c>).</summary>
    [Fact]
    public void AclBindingFilter_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.False(AclBindingFilter.Any.IsUnknown);

        Assert.True(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Unknown, null, PatternType.Any),
            AccessControlEntryFilter.Any).IsUnknown);

        Assert.True(new AclBindingFilter(
            ResourcePatternFilter.Any,
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Unknown)).IsUnknown);
    }

    /// <summary>
    /// ⚠ The binding filter is also a dictionary key, and the <c>null</c>/<c>""</c> distinction has
    /// to survive nested inside it (PLAN §2.1 + D39).
    /// </summary>
    [Fact]
    public void AclBindingFilter_IsUsableAsADictionaryKey_AndKeepsNullApartFromEmpty()
    {
        AclBindingFilter wildcardName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal),
            AccessControlEntryFilter.Any);
        AclBindingFilter emptyName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal),
            AccessControlEntryFilter.Any);

        Assert.NotEqual(wildcardName, emptyName);

        Dictionary<AclBindingFilter, string> byFilter = new Dictionary<AclBindingFilter, string>
        {
            [wildcardName] = "wildcard",
            [emptyName] = "empty",
        };

        Assert.Equal(2, byFilter.Count);
        Assert.Equal(
            "wildcard",
            byFilter[new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal),
                AccessControlEntryFilter.Any)]);
        Assert.Equal(
            "empty",
            byFilter[new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal),
                AccessControlEntryFilter.Any)]);
        Assert.False(wildcardName.Equals("not a filter"));
    }

    /// <summary>Java's <c>toString()</c> rendering (<c>:69</c>), nesting both halves.</summary>
    [Fact]
    public void AclBindingFilter_ToString_MatchesJava() =>
        Assert.Equal(
            "(patternFilter=ResourcePattern(resourceType=Any, name=<any>, patternType=Any), "
                + "entryFilter=(principal=<any>, host=<any>, operation=Any, permissionType=Any))",
            AclBindingFilter.Any.ToString());

    /// <summary>
    /// A binding and its filter round-trip: <c>binding.ToFilter()</c> equals a filter assembled
    /// component-by-component, which is what the submit path will do.
    /// </summary>
    [Fact]
    public void AclBinding_And_Filter_RoundTripThroughTheirComponents()
    {
        AclBinding binding = NewBinding();
        AclBindingFilter filter = binding.ToFilter();

        Assert.Equal(binding.Pattern.ResourceType, filter.PatternFilter.ResourceType);
        Assert.Equal(binding.Pattern.Name, filter.PatternFilter.Name);
        Assert.Equal(binding.Pattern.PatternType, filter.PatternFilter.PatternType);
        Assert.Equal(binding.Entry.Principal, filter.EntryFilter.Principal);
        Assert.Equal(binding.Entry.Host, filter.EntryFilter.Host);
        Assert.Equal(binding.Entry.Operation, filter.EntryFilter.Operation);
        Assert.Equal(binding.Entry.PermissionType, filter.EntryFilter.PermissionType);
    }

    private static AclBinding NewBinding() =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
