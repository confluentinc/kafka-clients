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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The native <c>MockAdminClient.Builder</c> behind both mocks' <c>Builder</c>, and the native forms of Java's two
/// <c>MockAdminClient</c> constructors. Each call is serialized, as the core lets one call at a time use a builder.
/// </summary>
internal sealed class MockAdminClientBuilderCore
{
    private readonly SafeMockAdminClientBuilderHandle _handle;
    private readonly object _lock = new object();

    private MockAdminClientBuilderCore(SafeMockAdminClientBuilderHandle handle)
    {
        _handle = handle;
    }

    internal static MockAdminClientBuilderCore Create()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr builder = NativeMethods.MockAdminClientCreate(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new MockAdminClientBuilderCore(new SafeMockAdminClientBuilderHandle(builder));
    }

    internal static MockAdminClientBuilderCore New()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr builder = NativeMethods.MockAdminClientBuilderNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new MockAdminClientBuilderCore(new SafeMockAdminClientBuilderHandle(builder));
    }

    internal static IntPtr NewMock()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr mock = NativeMethods.MockAdminClientNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return mock;
    }

    internal static IntPtr NewMock(IReadOnlyList<Node> brokers, Node controller)
    {
        Node[] snapshot = SnapshotNodes(brokers, nameof(brokers));
        if (controller is null)
        {
            throw new ArgumentException("The controller node must be in the list of brokers", nameof(controller));
        }

        ValidateNode(controller, nameof(controller));
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        IntPtr mock = NativeMethods.MockAdminClientWithBrokersController(
            arguments.NodeList(snapshot), arguments.Node(controller), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return mock;
    }

    internal void ClusterId(string clusterId)
    {
        if (clusterId is null)
        {
            throw new ArgumentNullException(nameof(clusterId));
        }

        AdminStrings.Validate(clusterId, nameof(clusterId));
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetClusterId(_handle, arguments.Utf8(clusterId), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void Brokers(IReadOnlyList<Node> brokers)
    {
        Node[] snapshot = SnapshotNodes(brokers, nameof(brokers));
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            _ = NativeMethods.MockAdminClientBuilderSetBrokers(_handle, arguments.NodeList(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void NumBrokers(int numBrokers) =>
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            _ = NativeMethods.MockAdminClientBuilderSetNumBrokers(_handle, numBrokers, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void Controller(int index) =>
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            _ = NativeMethods.MockAdminClientBuilderSetController(_handle, index, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void BrokerLogDirs(IReadOnlyList<IReadOnlyList<string>> brokerLogDirs)
    {
        if (brokerLogDirs is null)
        {
            throw new ArgumentNullException(nameof(brokerLogDirs));
        }

        IReadOnlyList<string>[] snapshot = new IReadOnlyList<string>[brokerLogDirs.Count];
        for (int i = 0; i < snapshot.Length; i++)
        {
            IReadOnlyList<string> logDirs = brokerLogDirs[i]
                ?? throw new ArgumentException("The broker log dirs must not contain a null element.", nameof(brokerLogDirs));
            string[] copy = new string[logDirs.Count];
            for (int j = 0; j < copy.Length; j++)
            {
                copy[j] = logDirs[j]
                    ?? throw new ArgumentException("The broker log dirs must not contain a null log dir.", nameof(brokerLogDirs));
                AdminStrings.Validate(copy[j], nameof(brokerLogDirs));
            }

            snapshot[i] = copy;
        }

        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetBrokerLogDirs(_handle, arguments.StringListList(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void DefaultReplicationFactor(int defaultReplicationFactor) =>
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetDefaultReplicationFactor(_handle, defaultReplicationFactor, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void UsingRaftController(bool usingRaftController) =>
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetUsingRaftController(
                _handle, usingRaftController ? (sbyte)1 : (sbyte)0, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void DefaultPartitions(short numPartitions) =>
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetDefaultPartitions(_handle, numPartitions, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });

    internal void FeatureLevels(IReadOnlyDictionary<string, short> featureLevels)
    {
        KeyValuePair<string, short>[] snapshot = SnapshotLevels(featureLevels, nameof(featureLevels));
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetFeatureLevels(_handle, arguments.ShortMap(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void MinSupportedFeatureLevels(IReadOnlyDictionary<string, short> minSupportedFeatureLevels)
    {
        KeyValuePair<string, short>[] snapshot =
            SnapshotLevels(minSupportedFeatureLevels, nameof(minSupportedFeatureLevels));
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetMinSupportedFeatureLevels(
                _handle, arguments.ShortMap(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void MaxSupportedFeatureLevels(IReadOnlyDictionary<string, short> maxSupportedFeatureLevels)
    {
        KeyValuePair<string, short>[] snapshot =
            SnapshotLevels(maxSupportedFeatureLevels, nameof(maxSupportedFeatureLevels));
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetMaxSupportedFeatureLevels(
                _handle, arguments.ShortMap(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal void DefaultGroupConfigs(IReadOnlyDictionary<string, string> defaultGroupConfigs)
    {
        if (defaultGroupConfigs is null)
        {
            throw new ArgumentNullException(nameof(defaultGroupConfigs));
        }

        List<KeyValuePair<string, string>> snapshot = new List<KeyValuePair<string, string>>(defaultGroupConfigs.Count);
        foreach (KeyValuePair<string, string> entry in defaultGroupConfigs)
        {
            ValidateKey(entry.Key, nameof(defaultGroupConfigs));
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    "The default group configs must not contain a null value.", nameof(defaultGroupConfigs));
            }

            AdminStrings.Validate(entry.Value, nameof(defaultGroupConfigs));
            snapshot.Add(entry);
        }

        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MockAdminClientBuilderSetDefaultGroupConfigs(_handle, arguments.StringMap(snapshot), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
    }

    internal IntPtr Build()
    {
        IntPtr mock = IntPtr.Zero;
        Call(arguments =>
        {
            IntPtr outErr = IntPtr.Zero;
            mock = NativeMethods.MockAdminClientBuilderBuild(_handle, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        });
        return mock;
    }

    private static Node[] SnapshotNodes(IReadOnlyList<Node> nodes, string parameterName)
    {
        if (nodes is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        Node[] snapshot = new Node[nodes.Count];
        for (int i = 0; i < snapshot.Length; i++)
        {
            snapshot[i] = nodes[i] ?? throw new ArgumentException("The brokers must not contain a null element.", parameterName);
            ValidateNode(snapshot[i], parameterName);
        }

        return snapshot;
    }

    private static void ValidateNode(Node node, string parameterName)
    {
        if (node is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        AdminStrings.Validate(node.Host, parameterName);
        AdminStrings.Validate(node.Rack, parameterName);
    }

    private static void ValidateKey(string key, string parameterName)
    {
        if (key is null)
        {
            throw new ArgumentException("The map must not contain a null key.", parameterName);
        }

        AdminStrings.Validate(key, parameterName);
    }

    private static KeyValuePair<string, short>[] SnapshotLevels(
        IReadOnlyDictionary<string, short> levels, string parameterName)
    {
        if (levels is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        List<KeyValuePair<string, short>> snapshot = new List<KeyValuePair<string, short>>(levels.Count);
        foreach (KeyValuePair<string, short> entry in levels)
        {
            ValidateKey(entry.Key, parameterName);
            snapshot.Add(entry);
        }

        return snapshot.ToArray();
    }

    private void Call(Action<NativeCallArguments> call)
    {
        lock (_lock)
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            call(arguments);
        }
    }
}
