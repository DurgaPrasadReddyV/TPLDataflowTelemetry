using System.Threading.Tasks.Dataflow;

namespace TPLDataflowTelemetry.ApiService
{
    public static class InstrumentedBlocks
    {
        // ——— TransformBlock ———
        public static TransformBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>> CreateTransform<TIn, TOut>(
            string name,
            ITplTelemetry tel,
            Func<TIn, CancellationToken, ValueTask<TOut>> handler,
            ExecutionDataflowBlockOptions? opt = null)
        {
            opt ??= new();

            var block = new TransformBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>>(async (im, ct) =>
            {
                im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;
                using var _ = tel.BeginBlockExecution(name, "transform", opt.MaxDegreeOfParallelism, opt.BoundedCapacity, im.FlowId, im.Seq, out var act);
                try
                {
                    // link parent → child
                    if (act is not null && im.ParentContext != default)
                        act.SetParentId(im.ParentContext.TraceId, im.ParentContext.SpanId);

                    var result = await handler(im.Value, ct).ConfigureAwait(false);
                    var outMsg = im with { Value = result }; // keep FlowId, Seq, trace linkage
                    tel.RecordProcessed(name, "transform", success: true);
                    return outMsg;
                }
                catch (Exception ex)
                {
                    tel.RecordProcessed(name, "transform", success: false, ex);
                    throw;
                }
            }, opt);

            // live queue length (TransformBlock supports InputCount)
            tel.RegisterQueueLength(name, "transform", () => block.InputCount);

            // exceptions via Completion
            block.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "transform", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— TransformManyBlock ———
        public static TransformManyBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>> CreateTransformMany<TIn, TOut>(
            string name,
            ITplTelemetry tel,
            Func<TIn, CancellationToken, IAsyncEnumerable<TOut>> fanout,
            ExecutionDataflowBlockOptions? opt = null)
        {
            opt ??= new();

            long producedForGauge = 0;

            var block = new TransformManyBlock<InstrumentedMessage<TIn>, InstrumentedMessage<TOut>>(async (im, ct) =>
            {
                im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;
                using var _ = tel.BeginBlockExecution(name, "transformmany", opt.MaxDegreeOfParallelism, opt.BoundedCapacity, im.FlowId, im.Seq, out var act);
                try
                {
                    var outs = new List<InstrumentedMessage<TOut>>();
                    await foreach (var o in fanout(im.Value, ct).ConfigureAwait(false))
                    {
                        outs.Add(im with { Value = o });
                    }
                    Interlocked.Add(ref producedForGauge, outs.Count);
                    tel.RecordProcessed(name, "transformmany", success: true, producedCount: outs.Count);
                    return outs;
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
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "transformmany", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— ActionBlock (sink) ———
        public static ActionBlock<InstrumentedMessage<T>> CreateAction<T>(
            string name,
            ITplTelemetry tel,
            Func<T, CancellationToken, ValueTask> action,
            ExecutionDataflowBlockOptions? opt = null)
        {
            opt ??= new();

            var block = new ActionBlock<InstrumentedMessage<T>>(async (im) =>
            {
                im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;
                using var _ = tel.BeginBlockExecution(name, "action", opt.MaxDegreeOfParallelism, opt.BoundedCapacity, im.FlowId, im.Seq, out var act);
                try
                {
                    await action(im.Value, default).ConfigureAwait(false);
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
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "action", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— BufferBlock (source buffer) ———
        public static BufferBlock<InstrumentedMessage<T>> CreateBuffer<T>(
            string name,
            ITplTelemetry tel,
            DataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var block = new BufferBlock<InstrumentedMessage<T>>(opt);

            // queue length (BufferBlock exposes Count)
            tel.RegisterQueueLength(name, "buffer", () =>
            {
                try { return block.Count; } catch { return 0; }
            });

            block.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "buffer", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— BroadcastBlock (fanout latest) ———
        public static BroadcastBlock<InstrumentedMessage<T>> CreateBroadcast<T>(
            string name,
            ITplTelemetry tel,
            Func<InstrumentedMessage<T>, InstrumentedMessage<T>> cloner,
            DataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var block = new BroadcastBlock<InstrumentedMessage<T>>(msg => cloner(msg), opt);

            // We can’t get a count; expose 0/1 “has value”
            tel.RegisterQueueLength(name, "broadcast", () => block.HasValue ? 1 : 0);

            block.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "broadcast", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— WriteOnceBlock (set-once cache) ———
        public static WriteOnceBlock<InstrumentedMessage<T>> CreateWriteOnce<T>(
            string name,
            ITplTelemetry tel,
            DataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var firstWrite = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
            var block = new WriteOnceBlock<InstrumentedMessage<T>>(msg =>
            {
                firstWrite.TrySetResult(DateTimeOffset.UtcNow);
                return msg;
            }, opt);

            tel.RegisterQueueLength(name, "writeonce", () => block.HasValue ? 1 : 0);

            // measure latency until first value written (from pipeline start, if needed)
            _ = firstWrite.Task.ContinueWith(_ =>
            {
                // You can emit a custom metric/event here if you want
            });

            block.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted && t.Exception is { } e)
                    tel.RecordProcessed(name, "writeonce", success: false, ex: e.Flatten());
            });

            return block;
        }

        // ——— BatchBlock ———
        public static BatchBlock<InstrumentedMessage<T>> CreateBatch<T>(
            string name,
            ITplTelemetry tel,
            int batchSize,
            GroupingDataflowBlockOptions? opt = null)
        {
            opt ??= new() { BoundedCapacity = DataflowBlockOptions.Unbounded };
            long inCount = 0, outCount = 0;
            var block = new BatchBlock<InstrumentedMessage<T>>(batchSize, opt);

            // Computed backlog (BatchBlock doesn’t expose InputCount)
            tel.RegisterQueueLength(name, "batch", () => Volatile.Read(ref inCount) - Volatile.Read(ref outCount));

            // Hook into offered items via SendAsync wrapper (see helper below)
            // We'll count in MeasureSendAsync; for outCount, add a small pass-through after the batch emits.

            return block;
        }

        // Add a small pass-through to measure batch emit and time-to-fill
        public static TransformBlock<InstrumentedMessage<T>[], InstrumentedMessage<T>[]> CreateBatchProbe<T>(
            string name,
            ITplTelemetry tel,
            GroupingDataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var probe = new TransformBlock<InstrumentedMessage<T>[], InstrumentedMessage<T>[]>(batch =>
            {
                var size = batch.Length;
                var firstEnq = batch.Min(m => m.EnqueuedAtUtc ?? m.CreatedAt);
                var waitMs = (DateTimeOffset.UtcNow - firstEnq).TotalMilliseconds;
                tel.RecordBatchOrJoinWait(name, "batch", waitMs, size);
                tel.RecordProcessed(name, "batch", success: true, producedCount: size);
                return batch;
            }, opt);
            tel.RegisterQueueLength(name, "batch_probe", () => probe.InputCount);
            return probe;
        }

        // ——— JoinBlock (T1,T2) ———
        public static JoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>> CreateJoin<T1, T2>(
            string name,
            ITplTelemetry tel,
            GroupingDataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var block = new JoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>>(opt);

            // No direct backlog; expose a computed observable if you track items_in/out externally.
            tel.RegisterQueueLength(name, "join", () => 0);

            return block;
        }

        // wrap join output to compute wait until pair matched
        public static TransformBlock<Tuple<InstrumentedMessage<T1>, InstrumentedMessage<T2>>, InstrumentedMessage<(T1, T2)>>
            CreateJoinProbe<T1, T2>(string name, ITplTelemetry tel, ExecutionDataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var probe = new TransformBlock<Tuple<InstrumentedMessage<T1>, InstrumentedMessage<T2>>, InstrumentedMessage<(T1, T2)>>(pair =>
            {
                var left = pair.Item1; var right = pair.Item2;
                var firstEnq = new[] { left, right }.Min(m => m.EnqueuedAtUtc ?? m.CreatedAt);
                var waitMs = (DateTimeOffset.UtcNow - firstEnq).TotalMilliseconds;
                tel.RecordBatchOrJoinWait(name, "join", waitMs, size: 2);

                // Merge contexts: prefer left parent, add right as Activity Link if desired (outside of block)
                var merged = new InstrumentedMessage<(T1, T2)>((left.Value, right.Value), left.FlowId, left.Seq, DateTimeOffset.UtcNow, left.ParentContext);
                tel.RecordProcessed(name, "join", success: true);
                return merged;
            }, opt);
            tel.RegisterQueueLength(name, "join_probe", () => probe.InputCount);
            return probe;
        }

        // ——— BatchedJoinBlock (T1,T2) ———
        public static BatchedJoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>> CreateBatchedJoin<T1, T2>(
            string name, ITplTelemetry tel, int batchSize, GroupingDataflowBlockOptions? opt = null)
        {
            opt ??= new();
            var block = new BatchedJoinBlock<InstrumentedMessage<T1>, InstrumentedMessage<T2>>(batchSize, opt);
            tel.RegisterQueueLength(name, "batchedjoin", () => 0);
            return block;
        }
    }

}
