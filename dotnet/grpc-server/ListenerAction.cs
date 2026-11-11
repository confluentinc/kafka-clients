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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// The scripts of a <c>ListenerActionRequest</c>, which a logging listener runs from inside its methods; the C#
/// port of Python's <c>ListenerAction</c>.
/// </summary>
internal sealed class ListenerAction
{
    internal const string KindListenerAction = "listener_action";

    private static readonly Dictionary<Proto.ListenerStepKind, string> s_stepNames = new Dictionary<Proto.ListenerStepKind, string>
    {
        [Proto.ListenerStepKind.ListenerStepAssignment] = "ASSIGNMENT",
        [Proto.ListenerStepKind.ListenerStepBeginningOffsets] = "BEGINNING_OFFSETS",
        [Proto.ListenerStepKind.ListenerStepPosition] = "POSITION",
        [Proto.ListenerStepKind.ListenerStepSeek] = "SEEK",
        [Proto.ListenerStepKind.ListenerStepPause] = "PAUSE",
        [Proto.ListenerStepKind.ListenerStepSleep] = "SLEEP",
        [Proto.ListenerStepKind.ListenerStepCommitSyncPosition] = "COMMIT_SYNC_POSITION",
        [Proto.ListenerStepKind.ListenerStepFail] = "FAIL",
    };

    private readonly Dictionary<string, Script> _scripts;

    private ListenerAction(Dictionary<string, Script> scripts)
    {
        _scripts = scripts;
    }

    internal static ListenerAction? FromRequest(Proto.ListenerActionRequest request)
    {
        Dictionary<string, Script> scripts = new Dictionary<string, Script>();
        if (request.OnAssigned is not null)
        {
            scripts[Translate.KindAssigned] = new Script(request.OnAssigned);
        }

        if (request.OnRevoked is not null)
        {
            scripts[Translate.KindRevoked] = new Script(request.OnRevoked);
        }

        if (request.OnLost is not null)
        {
            scripts[Translate.KindLost] = new Script(request.OnLost);
        }

        return scripts.Count > 0 ? new ListenerAction(scripts) : null;
    }

    internal void RunSync(
        IConsumer<byte[], byte[]> consumer,
        CallbackLog log,
        ulong consumerId,
        string method,
        IReadOnlyCollection<TopicPartition> partitions)
    {
        Script? script = ScriptFor(method, partitions);
        if (script is null)
        {
            return;
        }

        Dictionary<TopicPartition, long> positions = new Dictionary<TopicPartition, long>();
        foreach (Proto.ListenerStep step in script.Steps)
        {
            StepResult result = new StepResult();
            try
            {
                switch (step.Kind)
                {
                    case Proto.ListenerStepKind.ListenerStepAssignment:
                        result.Assignment = consumer.Assignment();
                        break;
                    case Proto.ListenerStepKind.ListenerStepBeginningOffsets:
                        result.Offsets = OffsetsByKey(consumer.BeginningOffsets(Partitions(step)));
                        break;
                    case Proto.ListenerStepKind.ListenerStepPosition:
                        result.Offsets = Position(step, positions, consumer.Position(Translate.Tp(step.Partition)));
                        break;
                    case Proto.ListenerStepKind.ListenerStepSeek:
                        consumer.Seek(Translate.Tp(step.Partition), step.Value);
                        break;
                    case Proto.ListenerStepKind.ListenerStepPause:
                        consumer.Pause(Partitions(step));
                        break;
                    case Proto.ListenerStepKind.ListenerStepSleep:
                        Thread.Sleep(Translate.DurationOfMs(step.Value));
                        break;
                    case Proto.ListenerStepKind.ListenerStepCommitSyncPosition:
                        Dictionary<TopicPartition, OffsetAndMetadata> offsets = CommitOffsets(step, positions);
                        consumer.Commit(offsets);
                        result.Offsets = CommittedByKey(offsets);
                        break;
                    default:
                        throw new IllegalArgumentException(step.Message);
                }
            }
            catch (Exception e)
            {
                LogStep(log, consumerId, method, partitions, step, new StepResult(), e);
                if (step.Kind == Proto.ListenerStepKind.ListenerStepFail)
                {
                    throw;
                }

                return;
            }

            LogStep(log, consumerId, method, partitions, step, result, null);
        }
    }

    internal async Task RunAsync(
        IAsyncConsumer<byte[], byte[]> consumer,
        CallbackLog log,
        ulong consumerId,
        string method,
        IReadOnlyCollection<TopicPartition> partitions)
    {
        Script? script = ScriptFor(method, partitions);
        if (script is null)
        {
            return;
        }

        Dictionary<TopicPartition, long> positions = new Dictionary<TopicPartition, long>();
        foreach (Proto.ListenerStep step in script.Steps)
        {
            StepResult result = new StepResult();
            try
            {
                switch (step.Kind)
                {
                    case Proto.ListenerStepKind.ListenerStepAssignment:
                        result.Assignment = consumer.Assignment();
                        break;
                    case Proto.ListenerStepKind.ListenerStepBeginningOffsets:
                        result.Offsets = OffsetsByKey(await consumer.BeginningOffsets(Partitions(step)).ConfigureAwait(false));
                        break;
                    case Proto.ListenerStepKind.ListenerStepPosition:
                        long position = await consumer.Position(Translate.Tp(step.Partition)).ConfigureAwait(false);
                        result.Offsets = Position(step, positions, position);
                        break;
                    case Proto.ListenerStepKind.ListenerStepSeek:
                        await consumer.Seek(Translate.Tp(step.Partition), step.Value).ConfigureAwait(false);
                        break;
                    case Proto.ListenerStepKind.ListenerStepPause:
                        await consumer.Pause(Partitions(step)).ConfigureAwait(false);
                        break;
                    case Proto.ListenerStepKind.ListenerStepSleep:
                        await Task.Delay(Translate.DurationOfMs(step.Value)).ConfigureAwait(false);
                        break;
                    case Proto.ListenerStepKind.ListenerStepCommitSyncPosition:
                        Dictionary<TopicPartition, OffsetAndMetadata> offsets = CommitOffsets(step, positions);
                        await consumer.Commit(offsets).ConfigureAwait(false);
                        result.Offsets = CommittedByKey(offsets);
                        break;
                    default:
                        throw new IllegalArgumentException(step.Message);
                }
            }
            catch (Exception e)
            {
                LogStep(log, consumerId, method, partitions, step, new StepResult(), e);
                if (step.Kind == Proto.ListenerStepKind.ListenerStepFail)
                {
                    throw;
                }

                return;
            }

            LogStep(log, consumerId, method, partitions, step, result, null);
        }
    }

    private static List<TopicPartition> Partitions(Proto.ListenerStep step)
    {
        List<TopicPartition> partitions = new List<TopicPartition>(step.Partitions.Count);
        foreach (Proto.TopicPartition partition in step.Partitions)
        {
            partitions.Add(Translate.Tp(partition));
        }

        return partitions;
    }

    private static Dictionary<string, long> Position(Proto.ListenerStep step, Dictionary<TopicPartition, long> positions, long position)
    {
        TopicPartition partition = Translate.Tp(step.Partition);
        positions[partition] = position;
        return new Dictionary<string, long> { [Translate.OffsetKey(partition.Topic(), partition.Partition())] = position };
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> CommitOffsets(Proto.ListenerStep step, Dictionary<TopicPartition, long> positions)
    {
        TopicPartition partition = Translate.Tp(step.Partition);
        if (!positions.TryGetValue(partition, out long position))
        {
            throw new IllegalStateException($"no position was read for {partition} before COMMIT_SYNC_POSITION");
        }

        return new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(position) };
    }

    private static Dictionary<string, long> OffsetsByKey(IReadOnlyDictionary<TopicPartition, long> offsets)
    {
        Dictionary<string, long> byKey = new Dictionary<string, long>(offsets.Count);
        foreach (KeyValuePair<TopicPartition, long> pair in offsets)
        {
            byKey[Translate.OffsetKey(pair.Key.Topic(), pair.Key.Partition())] = pair.Value;
        }

        return byKey;
    }

    private static Dictionary<string, long> CommittedByKey(Dictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        Dictionary<string, long> byKey = new Dictionary<string, long>(offsets.Count);
        foreach (KeyValuePair<TopicPartition, OffsetAndMetadata> pair in offsets)
        {
            byKey[Translate.OffsetKey(pair.Key.Topic(), pair.Key.Partition())] = pair.Value.Offset();
        }

        return byKey;
    }

    private static void LogStep(
        CallbackLog log,
        ulong consumerId,
        string method,
        IReadOnlyCollection<TopicPartition> partitions,
        Proto.ListenerStep step,
        StepResult result,
        Exception? exception)
    {
        List<TopicPartition>? assignment = null;
        if (result.Assignment is not null)
        {
            assignment = new List<TopicPartition>(result.Assignment);
            assignment.Sort((a, b) =>
            {
                int byTopic = string.CompareOrdinal(a.Topic(), b.Topic());
                return byTopic != 0 ? byTopic : a.Partition().CompareTo(b.Partition());
            });
        }

        log.Append(
            consumerId,
            KindListenerAction,
            partitions,
            result.Offsets,
            exception,
            method: method,
            resultPartitions: assignment,
            step: s_stepNames[step.Kind]);
    }

    private Script? ScriptFor(string method, IReadOnlyCollection<TopicPartition> partitions) =>
        _scripts.TryGetValue(method, out Script? script) && script.Triggered(partitions) ? script : null;

    /// <summary>What a step read, logged in its entry.</summary>
    private sealed class StepResult
    {
        internal IReadOnlyCollection<TopicPartition>? Assignment { get; set; }

        internal Dictionary<string, long>? Offsets { get; set; }
    }

    /// <summary>One method's <c>ListenerScript</c>: its trigger, with the trigger's state, and its steps.</summary>
    private sealed class Script
    {
        private readonly Proto.ListenerTriggerKind _trigger;
        private readonly int _firstNCalls;
        private readonly TopicPartition? _partition;
        private int _calls;
        private bool _done;

        internal Script(Proto.ListenerScript proto)
        {
            if (proto.Trigger is not (Proto.ListenerTriggerKind.ListenerTriggerAlways
                or Proto.ListenerTriggerKind.ListenerTriggerFirstNCalls
                or Proto.ListenerTriggerKind.ListenerTriggerWhenContains
                or Proto.ListenerTriggerKind.ListenerTriggerOnceWhenContains))
            {
                throw new IllegalArgumentException($"unknown listener trigger {(int)proto.Trigger}");
            }

            foreach (Proto.ListenerStep step in proto.Steps)
            {
                if (!s_stepNames.ContainsKey(step.Kind))
                {
                    throw new IllegalArgumentException($"unknown listener step {(int)step.Kind}");
                }
            }

            _trigger = proto.Trigger;
            _firstNCalls = proto.FirstNCalls;
            _partition = proto.Partition is null ? (TopicPartition?)null : Translate.Tp(proto.Partition);
            Steps = new List<Proto.ListenerStep>(proto.Steps);
        }

        internal List<Proto.ListenerStep> Steps { get; }

        internal bool Triggered(IReadOnlyCollection<TopicPartition> partitions)
        {
            _calls++;
            switch (_trigger)
            {
                case Proto.ListenerTriggerKind.ListenerTriggerAlways:
                    return true;
                case Proto.ListenerTriggerKind.ListenerTriggerFirstNCalls:
                    return _calls <= _firstNCalls;
            }

            if (_partition is not TopicPartition partition || !Contains(partitions, partition))
            {
                return false;
            }

            if (_trigger == Proto.ListenerTriggerKind.ListenerTriggerOnceWhenContains)
            {
                if (_done)
                {
                    return false;
                }

                _done = true;
            }

            return true;
        }

        private static bool Contains(IReadOnlyCollection<TopicPartition> partitions, TopicPartition partition)
        {
            foreach (TopicPartition candidate in partitions)
            {
                if (candidate.Equals(partition))
                {
                    return true;
                }
            }

            return false;
        }
    }
}

/// <summary>
/// A logging listener that runs a <see cref="ListenerAction"/>'s scripts on its sync consumer; the C# port of
/// Python's <c>ActionLoggingRebalanceListener</c>.
/// </summary>
internal sealed class ActionLoggingRebalanceListener : IConsumerRebalanceListenerWithPartitionsLost
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly ListenerAction _action;

    internal ActionLoggingRebalanceListener(CallbackLog log, ulong consumerId, IConsumer<byte[], byte[]> consumer, ListenerAction action)
    {
        _log = log;
        _consumerId = consumerId;
        _consumer = consumer;
        _action = action;
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindAssigned, partitions);

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindRevoked, partitions);

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindLost, partitions);

    private void Run(string method, IReadOnlyCollection<TopicPartition> partitions)
    {
        _log.Append(_consumerId, method, partitions);
        _action.RunSync(_consumer, _log, _consumerId, method, partitions);
    }
}

/// <summary>
/// The async twin of <see cref="ActionLoggingRebalanceListener"/>; the C# port of Python's
/// <c>AsyncActionLoggingRebalanceListener</c>.
/// </summary>
internal sealed class AsyncActionLoggingRebalanceListener : IAsyncConsumerRebalanceListenerWithPartitionsLost
{
    private readonly CallbackLog _log;
    private readonly ulong _consumerId;
    private readonly IAsyncConsumer<byte[], byte[]> _consumer;
    private readonly ListenerAction _action;

    internal AsyncActionLoggingRebalanceListener(CallbackLog log, ulong consumerId, IAsyncConsumer<byte[], byte[]> consumer, ListenerAction action)
    {
        _log = log;
        _consumerId = consumerId;
        _consumer = consumer;
        _action = action;
    }

    public Task OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindAssigned, partitions);

    public Task OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindRevoked, partitions);

    public Task OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        Run(Translate.KindLost, partitions);

    private Task Run(string method, IReadOnlyCollection<TopicPartition> partitions)
    {
        _log.Append(_consumerId, method, partitions);
        return _action.RunAsync(_consumer, _log, _consumerId, method, partitions);
    }
}
