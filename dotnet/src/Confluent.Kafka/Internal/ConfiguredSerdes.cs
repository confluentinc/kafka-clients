// Copyright 2026 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The key and value serializers, or deserializers, of a real client, as its <c>Create</c> resolves them: each one is
/// the argument or, for a null argument, built from the configuration.
/// </summary>
/// <remarks>
/// This is the non-generic face <see cref="NativeProducer"/> and <see cref="NativeConsumer"/> drive while they create
/// the client, with a synchronous entry for the synchronous clients and an awaited one for the asynchronous clients;
/// the factories below name, for each kind of client, the two configuration keys, Java's message and what the client
/// does with an instance.
/// </remarks>
internal abstract class ConfiguredSerdes
{
    internal static ConfiguredSerdes<ISerializer<TKey>, ISerializer<TValue>> Serializers<TKey, TValue>(
        IReadOnlyDictionary<string, object?> configs, ISerializer<TKey>? keySerializer, ISerializer<TValue>? valueSerializer) =>
        new ConfiguredSerdes<ISerializer<TKey>, ISerializer<TValue>>(
            configs,
            keySerializer,
            valueSerializer,
            new Role<ISerializer<TKey>>(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                serializer => SerdeGuard.Check(serializer, DefaultMethodGuard.Producer, disposes: true),
                SerializerExtensions.Configure),
            new Role<ISerializer<TValue>>(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                serializer => SerdeGuard.Check(serializer, DefaultMethodGuard.Producer, disposes: true),
                SerializerExtensions.Configure),
            NativeProducer.ConstructionFailedMessage,
            asynchronous: false,
            holdsKeyBeforeValue: true);

    internal static ConfiguredSerdes<IDeserializer<TKey>, IDeserializer<TValue>> Deserializers<TKey, TValue>(
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer) =>
        new ConfiguredSerdes<IDeserializer<TKey>, IDeserializer<TValue>>(
            configs,
            keyDeserializer,
            valueDeserializer,
            new Role<IDeserializer<TKey>>(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                deserializer => SerdeGuard.Check(deserializer, DefaultMethodGuard.Consumer, disposes: true),
                DeserializerExtensions.Configure),
            new Role<IDeserializer<TValue>>(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                deserializer => SerdeGuard.Check(deserializer, DefaultMethodGuard.Consumer, disposes: true),
                DeserializerExtensions.Configure),
            NativeConsumer.ConstructionFailedMessage,
            asynchronous: false,
            holdsKeyBeforeValue: false);

    internal static ConfiguredSerdes<IAsyncSerializer<TKey>, IAsyncSerializer<TValue>> AsyncSerializers<TKey, TValue>(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncSerializer<TKey>? keySerializer,
        IAsyncSerializer<TValue>? valueSerializer) =>
        new ConfiguredSerdes<IAsyncSerializer<TKey>, IAsyncSerializer<TValue>>(
            configs,
            keySerializer,
            valueSerializer,
            new Role<IAsyncSerializer<TKey>>(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                serializer => SerdeGuard.Check(serializer, DefaultMethodGuard.Producer, disposes: true),
                AsyncSerializerExtensions.Configure),
            new Role<IAsyncSerializer<TValue>>(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                serializer => SerdeGuard.Check(serializer, DefaultMethodGuard.Producer, disposes: true),
                AsyncSerializerExtensions.Configure),
            NativeProducer.ConstructionFailedMessage,
            asynchronous: true,
            holdsKeyBeforeValue: true);

    internal static ConfiguredSerdes<IAsyncDeserializer<TKey>, IAsyncDeserializer<TValue>> AsyncDeserializers<TKey, TValue>(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncDeserializer<TKey>? keyDeserializer,
        IAsyncDeserializer<TValue>? valueDeserializer) =>
        new ConfiguredSerdes<IAsyncDeserializer<TKey>, IAsyncDeserializer<TValue>>(
            configs,
            keyDeserializer,
            valueDeserializer,
            new Role<IAsyncDeserializer<TKey>>(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                deserializer => SerdeGuard.Check(deserializer, DefaultMethodGuard.Consumer, disposes: true),
                AsyncDeserializerExtensions.Configure),
            new Role<IAsyncDeserializer<TValue>>(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                deserializer => SerdeGuard.Check(deserializer, DefaultMethodGuard.Consumer, disposes: true),
                AsyncDeserializerExtensions.Configure),
            NativeConsumer.ConstructionFailedMessage,
            asynchronous: true,
            holdsKeyBeforeValue: false);

    /// <summary>
    /// The part inside Java's constructor <c>try</c>, for a synchronous client: builds and configures each serde that
    /// was not given, the key's first. It runs once the whole configuration was validated and before the core builds
    /// the client.
    /// </summary>
    /// <returns>The pair's disposal, given and built ones alike, which the client owns from here on.</returns>
    /// <exception cref="KafkaException">
    /// Java's "Failed to construct kafka …", with the cause inside: a function returned null or threw, or the instance
    /// was refused or its <c>Configure</c> threw. A serde built here is disposed, the one that failed first; a given
    /// key is disposed when the value fails only on a producer, whose Java constructor holds the key by then and
    /// closes it, and not on a consumer, whose Java constructor holds neither deserializer yet.
    /// </exception>
    internal abstract SerdeDisposal Build();

    /// <summary>
    /// <see cref="Build"/> for an asynchronous client: each <c>Configure</c> is awaited, with the token, before the
    /// next step, and what a failure disposes is awaited too. A token cancelled before a function is called, or a
    /// <c>Configure</c> that ends with <see cref="OperationCanceledException"/> while the token is cancelled, ends the
    /// build with that exception as it is, once the serdes built so far are disposed.
    /// </summary>
    internal abstract ValueTask<SerdeDisposal> BuildAwaited(CancellationToken cancellationToken);

    /// <summary>
    /// Disposes the serdes a build built, and not the given ones: for a failure after it that Java raises outside its
    /// <c>try</c>, which leaves a given serde to its owner, while a built one has no other owner.
    /// </summary>
    internal abstract void DisposeBuilt();

    /// <summary><see cref="DisposeBuilt"/> for an asynchronous client, which awaits each disposal.</summary>
    internal abstract ValueTask DisposeBuiltAwaited();

    /// <summary>
    /// What a client does with one of its two serdes: the configuration key that holds its function, the client's
    /// check of an instance, and the instance's <c>Configure</c>, in the form the client calls.
    /// </summary>
    internal sealed class Role<TSerde>
    {
        internal Role(string config, Action<TSerde> check, Action<TSerde, IReadOnlyDictionary<string, object?>, bool> configure)
        {
            Config = config;
            Check = check;
            Configure = configure;
        }

        internal Role(
            string config,
            Action<TSerde> check,
            Func<TSerde, IReadOnlyDictionary<string, object?>, bool, CancellationToken, ValueTask> configureAwaited)
        {
            Config = config;
            Check = check;
            ConfigureAwaited = configureAwaited;
        }

        internal string Config { get; }

        internal Action<TSerde> Check { get; }

        internal Action<TSerde, IReadOnlyDictionary<string, object?>, bool>? Configure { get; }

        internal Func<TSerde, IReadOnlyDictionary<string, object?>, bool, CancellationToken, ValueTask>? ConfigureAwaited { get; }
    }
}

/// <summary>
/// The pair for one client, Java's <c>KafkaProducer(Map, Serializer, Serializer)</c> /
/// <c>KafkaConsumer(Map, Deserializer, Deserializer)</c>: a non-null argument is taken as it is and never configured;
/// for a null one the configuration must hold, under the serde's key, a function that returns the instance, where
/// Java's holds a class it builds by reflection.
/// </summary>
/// <remarks>
/// The constructor makes the checks Java makes before its client constructor's <c>try</c>, all bare: a null
/// <c>configs</c>; for a null argument a missing or null key (the key's, then the value's); then, for each serde, the
/// guard of a given one or the kind of value of a configured one. A build is the part inside that <c>try</c>.
/// <para>
/// The constructor also copies <c>configs</c>, once, as Java's <c>appendSerializerToConfig</c> /
/// <c>appendDeserializerToConfig</c> does at the call (<c>new HashMap&lt;&gt;(configs)</c>): its own checks, the
/// client's checks and the core read <see cref="Configs"/>, and each <c>Configure</c> gets its own copy of it. So a
/// change made to the caller's dictionary after the call, by the caller while an asynchronous <c>Create</c> is
/// pending or by a function, reaches none of them.
/// </para>
/// </remarks>
internal sealed class ConfiguredSerdes<TKeySerde, TValueSerde> : ConfiguredSerdes
    where TKeySerde : class
    where TValueSerde : class
{
    // The caller's entries as they were at the call, in its order: a key it holds twice, or a null one, is kept for
    // the client's own checks.
    private readonly KeyValuePair<string, object?>[] _configs;

    private readonly Role<TKeySerde> _keyRole;

    private readonly Role<TValueSerde> _valueRole;

    private readonly string _constructionFailedMessage;

    private readonly bool _asynchronous;

    // Whether Java's constructor holds the key serde, given or built, while it builds the value serde. KafkaProducer
    // does: it assigns keySerializerPlugin first, and its catch closes it. The consumer does not: one
    // new Deserializers(config, keyDeserializer, valueDeserializer, metrics) builds both, the field stays null when
    // the value fails, and its catch closes neither.
    private readonly bool _holdsKeyBeforeValue;

    // Null for a serde that was given.
    private readonly Func<TKeySerde>? _keyFunction;

    private readonly Func<TValueSerde>? _valueFunction;

    private TKeySerde? _key;

    private TValueSerde? _value;

    internal ConfiguredSerdes(
        IReadOnlyDictionary<string, object?> configs,
        TKeySerde? key,
        TValueSerde? value,
        Role<TKeySerde> keyRole,
        Role<TValueSerde> valueRole,
        string constructionFailedMessage,
        bool asynchronous,
        bool holdsKeyBeforeValue)
    {
        // Java copies the config map first, so a null one fails before a missing serde.
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        // Java's appendSerializerToConfig / appendDeserializerToConfig: the map is copied once, before anything reads
        // it; then a null argument needs its key, and both are looked at before anything else.
        KeyValuePair<string, object?>[] snapshot = new List<KeyValuePair<string, object?>>(configs).ToArray();
        object? keyValue = key is null ? Required(snapshot, keyRole.Config) : null;
        object? valueValue = value is null ? Required(snapshot, valueRole.Config) : null;
        if (key is null)
        {
            _keyFunction = FunctionOf<TKeySerde>(keyRole.Config, keyValue!);
        }
        else
        {
            keyRole.Check(key);
        }

        if (value is null)
        {
            _valueFunction = FunctionOf<TValueSerde>(valueRole.Config, valueValue!);
        }
        else
        {
            valueRole.Check(value);
        }

        _configs = snapshot;
        _key = key;
        _value = value;
        _keyRole = keyRole;
        _valueRole = valueRole;
        _constructionFailedMessage = constructionFailedMessage;
        _asynchronous = asynchronous;
        _holdsKeyBeforeValue = holdsKeyBeforeValue;
    }

    /// <summary>
    /// The configuration as it was when the client's <c>Create</c> was called: what the client checks and hands to
    /// the core, whatever happens to the caller's dictionary afterwards.
    /// </summary>
    internal IReadOnlyCollection<KeyValuePair<string, object?>> Configs => _configs;

    /// <summary>The key serde: the given one, or, once a build returned, the built one.</summary>
    internal TKeySerde Key => _key ?? throw new InvalidOperationException("The key serde was not built.");

    /// <summary>The value serde: the given one, or, once a build returned, the built one.</summary>
    internal TValueSerde Value => _value ?? throw new InvalidOperationException("The value serde was not built.");

    internal override SerdeDisposal Build()
    {
        if (_key is null)
        {
            try
            {
                _key = Built(_keyFunction!, _keyRole, isKey: true);
            }
            catch (Exception failure)
            {
                // Java's constructor holds neither serde yet, so a given value stays its owner's.
                throw new KafkaException(_constructionFailedMessage, failure);
            }
        }

        if (_value is null)
        {
            try
            {
                _value = Built(_valueFunction!, _valueRole, isKey: false);
            }
            catch (Exception failure)
            {
                // A key built here is disposed; a given one only where Java's constructor holds it by now.
                Disposal(TakeKey(builtOnly: !_holdsKeyBeforeValue), null).DisposeOnce();
                throw new KafkaException(_constructionFailedMessage, failure);
            }
        }

        return Disposal(_key, _value);
    }

    internal override async ValueTask<SerdeDisposal> BuildAwaited(CancellationToken cancellationToken)
    {
        if (_key is null)
        {
            try
            {
                _key = await BuiltAwaited(_keyFunction!, _keyRole, isKey: true, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception failure) when (!IsCancellation(failure, cancellationToken))
            {
                throw new KafkaException(_constructionFailedMessage, failure);
            }
        }

        if (_value is null)
        {
            try
            {
                _value = await BuiltAwaited(_valueFunction!, _valueRole, isKey: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                // A cancellation is no failure of Java's constructor, so it leaves a given key to its owner.
                bool cancelled = IsCancellation(failure, cancellationToken);
                await Disposal(TakeKey(builtOnly: cancelled || !_holdsKeyBeforeValue), null)
                    .DisposeOnceAsync()
                    .ConfigureAwait(false);
                if (cancelled)
                {
                    throw;
                }

                throw new KafkaException(_constructionFailedMessage, failure);
            }
        }

        return Disposal(_key, _value);
    }

    internal override void DisposeBuilt() => Disposal(TakeKey(builtOnly: true), TakeBuiltValue()).DisposeOnce();

    internal override ValueTask DisposeBuiltAwaited() =>
        Disposal(TakeKey(builtOnly: true), TakeBuiltValue()).DisposeOnceAsync();

    private static bool IsCancellation(Exception failure, CancellationToken cancellationToken) =>
        failure is OperationCanceledException && cancellationToken.IsCancellationRequested;

    private SerdeDisposal Disposal(object? key, object? value) =>
        _asynchronous ? SerdeDisposal.OfAsyncSerdes(key, value) : new SerdeDisposal(key, value);

    // The key serde to dispose, which this then no longer holds when it built it: a later disposal skips it. When
    // the value serde fails, a key built here is always disposed, since nothing else holds it (Java's consumer leaks
    // it); a given one only where Java's constructor holds it by then and closes it, on a producer.
    private TKeySerde? TakeKey(bool builtOnly)
    {
        TKeySerde? key = _key;
        if (_keyFunction is null)
        {
            return builtOnly ? null : key;
        }

        _key = null;
        return key;
    }

    private TValueSerde? TakeBuiltValue()
    {
        if (_valueFunction is null)
        {
            return null;
        }

        TValueSerde? value = _value;
        _value = null;
        return value;
    }

    // Java's "Invalid value null for configuration <key>: must be non-null.", for a null argument whose key is missing
    // or null.
    private static object Required(KeyValuePair<string, object?>[] configs, string config) =>
        ValueOf(configs, config) ?? throw new ConfigException(config, null, "must be non-null.");

    // The last entry of that key, as the core keeps the last text of a key; a dictionary whose own comparer is not
    // ordinal is still read by the exact key the core's configuration uses.
    private static object? ValueOf(KeyValuePair<string, object?>[] configs, string config)
    {
        object? value = null;
        foreach (KeyValuePair<string, object?> entry in configs)
        {
            if (string.Equals(entry.Key, config, StringComparison.Ordinal))
            {
                value = entry.Value;
            }
        }

        return value;
    }

    // Func<out TResult> is covariant, so a function declared to return the class that implements the interface is
    // one of the interface; one that returns a struct, or another interface, is not.
    private static Func<TSerde> FunctionOf<TSerde>(string config, object value) =>
        value as Func<TSerde>
            ?? throw new ConfigException(
                config,
                value,
                "Expected value to be a " + DefaultMethodGuard.TypeText(typeof(Func<TSerde>)) + ", but it was a "
                    + DefaultMethodGuard.TypeText(value.GetType()));

    private static TSerde Instance<TSerde>(Func<TSerde> function, string config)
        where TSerde : class =>
        function()
            ?? throw new ConfigException(
                config,
                null,
                "Expected the function to return a " + DefaultMethodGuard.TypeText(typeof(TSerde)) + ", but it returned null");

    // Java builds the instance and configures it; an instance the client refuses, or whose Configure throws, is
    // disposed here, since nothing else will ever hold it.
    private TSerde Built<TSerde>(Func<TSerde> function, Role<TSerde> role, bool isKey)
        where TSerde : class
    {
        TSerde serde = Instance(function, role.Config);
        try
        {
            role.Check(serde);
            role.Configure!(serde, OwnCopy(), isKey);
        }
        catch
        {
            Disposal(serde, null).DisposeOnce();
            throw;
        }

        return serde;
    }

    private async ValueTask<TSerde> BuiltAwaited<TSerde>(
        Func<TSerde> function, Role<TSerde> role, bool isKey, CancellationToken cancellationToken)
        where TSerde : class
    {
        cancellationToken.ThrowIfCancellationRequested();
        TSerde serde = Instance(function, role.Config);
        try
        {
            role.Check(serde);
            await role.ConfigureAwaited!(serde, OwnCopy(), isKey, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await Disposal(serde, null).DisposeOnceAsync().ConfigureAwait(false);
            throw;
        }

        return serde;
    }

    // Java's originals(): each Configure gets its own copy of the entries the call found, so one serde cannot change
    // what the other, or the caller, sees, and a later change to the caller's dictionary is not seen either.
    private Dictionary<string, object?> OwnCopy()
    {
        Dictionary<string, object?> copy = new Dictionary<string, object?>(_configs.Length, StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?> entry in _configs)
        {
            copy[entry.Key] = entry.Value;
        }

        return copy;
    }
}
