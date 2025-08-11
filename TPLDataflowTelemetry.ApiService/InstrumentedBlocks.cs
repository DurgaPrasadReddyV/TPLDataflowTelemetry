using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TPLDataflowTelemetry.ApiService;

public static class InstrumentedBlocks
{
    // ---------- Shared helper: rebuild envelope when T changes ----------
    private static InstrumentedMessage<TOut> Map<TIn, TOut>(InstrumentedMessage<TIn> src, TOut value)
    {
        var dst = new InstrumentedMessage<TOut>(
            value,
            src.FlowId,
            src.Seq,
            src.CreatedAt,
            src.ParentContext,
            src.IntegrityHash)
        {
            EnqueuedAtUtc = src.EnqueuedAtUtc,
            DequeuedAtUtc = src.DequeuedAtUtc,
            HopHash = src.HopHash
        };

        // copy tags
        foreach (var kv in src.Tags)
            dst.Tags[kv.Key] = kv.Value;

        return dst;
    }

    // ---------- TransformBlock<TIn,TOut> ----------
    public static TransformBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>> CreateTransform<TIn, TOut>(
        string name,
        ITplTelemetry tel,
        Func<TIn, Task<TOut>> handler,
        ExecutionDataflowBlockOptions? opt = null)
    {
        opt ??= new();

        var block = new TransformBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>>(async im =>
        {
            im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;

            using var _ = tel.BeginBlockExecution(
                name, "transform",
                opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
                im.FlowId, im.Seq, im.ParentContext);

            try
            {
                var result = await handler(im.Value).ConfigureAwait(false);
                var outMsg = Map<TIn, TOut>(im, result);
                tel.RecordProcessed(name, "transform", success: true);
                return outMsg;
            }
            catch (Exception ex)
            {
                tel.RecordProcessed(name, "transform", success: false, ex);
                throw;
            }
        }, opt);

        tel.RegisterQueueLength(name, "transform", () => block.InputCount);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "transform", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- TransformManyBlock<TIn,TOut> ----------
    public static TransformManyBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>> CreateTransformMany<TIn, TOut>(
        string name,
        ITplTelemetry tel,
        Func<TIn, Task<IEnumerable<TOut>>> fanout,
        ExecutionDataflowBlockOptions? opt = null)
    {
        opt ??= new();

        var block = new TransformManyBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>>(async im =>
        {
            im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;

            using var _ = tel.BeginBlockExecution(
                name, "transformmany",
                opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
                im.FlowId, im.Seq, im.ParentContext);

            try
            {
                var outs = (await fanout(im.Value).ConfigureAwait(false)).ToList();
                tel.RecordProcessed(name, "transformmany", success: true, producedCount: outs.Count);

                // map each value to a new envelope
                return outs.Select(o => Map<TIn, TOut>(im, o));
            }
            catch (Exception ex)
            {
                tel.RecordProcessed(name, "transformmany", success: false, ex);
                throw;
            }
        }, opt);

        tel.RegisterQueueLength(name, "transformmany", () => block.InputCount);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "transformmany", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- ActionBlock<T> ----------
    public static ActionBlock<InstrumentedMessage<T>> CreateAction<T>(
        string name,
        ITplTelemetry tel,
        Func<T, Task> action,
        ExecutionDataflowBlockOptions? opt = null)
    {
        opt ??= new();

        var block = new ActionBlock<InstrumentedMessage<T>>(async im =>
        {
            im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;

            using var _ = tel.BeginBlockExecution(
                name, "action",
                opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
                im.FlowId, im.Seq, im.ParentContext);

            try
            {
                await action(im.Value).ConfigureAwait(false);
                tel.RecordProcessed(name, "action", success: true);
            }
            catch (Exception ex)
            {
                tel.RecordProcessed(name, "action", success: false, ex);
                throw;
            }
        }, opt);

        tel.RegisterQueueLength(name, "action", () => block.InputCount);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "action", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- BufferBlock<T> ----------
    public static BufferBlock<InstrumentedMessage<T>> CreateBuffer<T>(
        string name,
        ITplTelemetry tel,
        DataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var block = new BufferBlock<InstrumentedMessage<T>>(opt);

        tel.RegisterQueueLength(name, "buffer", () =>
        {
            try { return block.Count; } catch { return 0; }
        });

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "buffer", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- BroadcastBlock<T> ----------
    public static BroadcastBlock<InstrumentedMessage<T>> CreateBroadcast<T>(
        string name,
        ITplTelemetry tel,
        Func<InstrumentedMessage<T>, InstrumentedMessage<T>> cloner,
        DataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var hasValue = false;
        var block = new BroadcastBlock<InstrumentedMessage<T>>(msg =>
        {
            hasValue = true;
            return cloner(msg);
        }, opt);

        tel.RegisterQueueLength(name, "broadcast", () => hasValue ? 1 : 0);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "broadcast", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- WriteOnceBlock<T> ----------
    public static WriteOnceBlock<InstrumentedMessage<T>> CreateWriteOnce<T>(
        string name,
        ITplTelemetry tel,
        DataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var hasValue = false;
        var block = new WriteOnceBlock<InstrumentedMessage<T>>(msg =>
        {
            hasValue = true;
            return msg;
        }, opt);

        tel.RegisterQueueLength(name, "writeonce", () => hasValue ? 1 : 0);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "writeonce", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- BatchBlock<T> ----------
    public static BatchBlock<InstrumentedMessage<T>> CreateBatch<T>(
        string name,
        ITplTelemetry tel,
        int batchSize,
        GroupingDataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var block = new BatchBlock<InstrumentedMessage<T>>(batchSize, opt);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "batch", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- Batch probe ----------
    public static TransformBlock<InstrumentedMessage<T>[], InstrumentedMessage<T>[]> CreateBatchProbe<T>(
        string name,
        ITplTelemetry tel,
        ExecutionDataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var probe = new TransformBlock<InstrumentedMessage<T>[], InstrumentedMessage<T>[]>(batch =>
        {
            if (batch.Length > 0)
            {
                var firstEnq = batch.Min(m => m.EnqueuedAtUtc ?? m.CreatedAt);
                var waitMs = (DateTimeOffset.UtcNow - firstEnq).TotalMilliseconds;
                tel.RecordBatchOrJoinWait(name, "batch", waitMs, batch.Length);
                tel.RecordProcessed(name, "batch", success: true, producedCount: batch.Length);
            }
            return batch;
        }, opt);

        tel.RegisterQueueLength(name, "batch_probe", () => probe.InputCount);

        return probe;
    }

    // ---------- JoinBlock<T1,T2> ----------
    public static JoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>> CreateJoin<T1, T2>(
        string name,
        ITplTelemetry tel,
        GroupingDataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var block = new JoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>>(opt);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "join", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }

    // ---------- Join probe ----------
    public static TransformBlock<Tuple<InstrumentedMessage<T1>, InstrumentedMessage<T2>>, InstrumentedMessage<(T1, T2)>> CreateJoinProbe<T1, T2>(
        string name,
        ITplTelemetry tel,
        ExecutionDataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var probe = new TransformBlock<Tuple<InstrumentedMessage<T1>, InstrumentedMessage<T2>>, InstrumentedMessage<(T1, T2)>>(pair =>
        {
            var left = pair.Item1;
            var right = pair.Item2;

            var firstEnq = new[] { left.EnqueuedAtUtc ?? left.CreatedAt, right.EnqueuedAtUtc ?? right.CreatedAt }.Min();
            var waitMs = (DateTimeOffset.UtcNow - firstEnq).TotalMilliseconds;
            tel.RecordBatchOrJoinWait(name, "join", waitMs, 2);

            var merged = Map<T1, (T1, T2)>(left, (left.Value, right.Value));
            tel.RecordProcessed(name, "join", success: true);
            return merged;
        }, opt);

        tel.RegisterQueueLength(name, "join_probe", () => probe.InputCount);

        return probe;
    }

    // ---------- BatchedJoinBlock<T1,T2> ----------
    public static BatchedJoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>> CreateBatchedJoin<T1, T2>(
        string name,
        ITplTelemetry tel,
        int batchSize,
        GroupingDataflowBlockOptions? opt = null)
    {
        opt ??= new();
        var block = new BatchedJoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>>(batchSize, opt);

        block.Completion.ContinueWith(t =>
        {
            if (t.IsFaulted)
                tel.RecordProcessed(name, "batchedjoin", success: false, ex: t.Exception?.Flatten());
        });

        return block;
    }
}