namespace TPLDataflowTelemetry.ApiService
{
    using System.Diagnostics;
    using System.Threading.Tasks.Dataflow;
    using static TplDataflow.Otel.PipelineTracing;
    using static TPLDataflowTelemetry.ApiService.InstrumentedBlocks;

    public static class InstrumentedBlocks
    {
        // Transform<TIn,TOut>
        public static TransformBlock<TraceEnvelope<TIn>, TraceEnvelope<TOut>> Transform<TIn, TOut>(
            PipelineContext ctx,
            string blockName,                   // e.g., "transform"
            Func<TIn, Task<TOut>> body,
            ExecutionDataflowBlockOptions? options = null)
        {
            return new TransformBlock<TraceEnvelope<TIn>, TraceEnvelope<TOut>>(async env =>
            {
                var entry = StartEntry(ctx, env, blockName);
                Stop(entry);

                // Some blocks have no real "execution" (Buffer) – for Transform we do.
                Activity? exec = null;
                try
                {
                    exec = StartExecution(ctx, env, blockName);
                    var result = await body(env.Value);
                    Stop(exec);

                    var exit = StartExit(ctx, env, blockName);
                    Stop(exit);

                    return env.To(result);
                }
                catch (Exception ex)
                {
                    Stop(exec, ex);
                    throw;
                }
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        // Action<T>
        public static ActionBlock<TraceEnvelope<T>> Action<T>(
            PipelineContext ctx,
            string blockName,                   // e.g., "action"
            Func<T, Task> body,
            ExecutionDataflowBlockOptions? options = null)
        {
            return new ActionBlock<TraceEnvelope<T>>(async env =>
            {
                var entry = StartEntry(ctx, env, blockName);
                Stop(entry);

                Activity? exec = null;
                try
                {
                    exec = StartExecution(ctx, env, blockName);
                    await body(env.Value);
                    Stop(exec);

                    var exit = StartExit(ctx, env, blockName);
                    Stop(exit);
                }
                catch (Exception ex)
                {
                    Stop(exec, ex);
                    throw;
                }
                finally
                {
                    // Message span ends when the terminal block completes
                    // (or you can end it right after the last exit)
                    env.MessageActivity?.Stop();
                }
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        // TransformMany<TIn,TOut> (split)
        public static TransformManyBlock<TraceEnvelope<TIn>, TraceEnvelope<TOut>> TransformMany<TIn, TOut>(
            PipelineContext ctx,
            string blockName,                       // e.g., "transformmany"
            Func<TIn, IEnumerable<TOut>> splitter,
            ExecutionDataflowBlockOptions? options = null)
        {
            return new TransformManyBlock<TraceEnvelope<TIn>, TraceEnvelope<TOut>>(env =>
            {
                var entry = StartEntry(ctx, env, blockName);
                Stop(entry);

                var exec = StartExecution(ctx, env, blockName);
                try
                {
                    var outs = splitter(env.Value).ToList();
                    Stop(exec);

                    var exit = StartExit(ctx, env, blockName);
                    Stop(exit);

                    // create child envelopes w/ splitIndex
                    return outs.Select((val, i) => 
                    env.To(val,i));
                }
                catch (Exception ex)
                {
                    Stop(exec, ex);
                    throw;
                }
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        // Broadcast<T> (split into many identical copies)
        public static TransformManyBlock<TraceEnvelope<T>, TraceEnvelope<T>> Broadcast<T>(
            PipelineContext ctx,
            string blockName,                       // e.g., "broadcast"
            int fanout,
            ExecutionDataflowBlockOptions? options = null)
        {
            return new TransformManyBlock<TraceEnvelope<T>, TraceEnvelope<T>>(env =>
            {
                var entry = StartEntry(ctx, env, blockName); Stop(entry);
                var exec = StartExecution(ctx, env, blockName);
                var list = Enumerable.Range(0, fanout).Select(i => CloneForSplit(env, env.Value, i)).ToList();
                Stop(exec);
                var exit = StartExit(ctx, env, blockName); Stop(exit);
                return list;
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        // Batch<T> (aggregate) – emits a batch, links prior message spans
        public static TransformBlock<TraceEnvelope<T>[], TraceEnvelope<T[]>> BatchAggregator<T>(
            PipelineContext ctx,
            string blockName, // e.g., "batch"
            ExecutionDataflowBlockOptions? options = null)
        {
            return new TransformBlock<TraceEnvelope<T>[], TraceEnvelope<T[]>>(batch =>
            {
                // aggregated child span linked to all message spans
                var linkTargets = batch.Select(b => b.MessageActivity).ToList();
                using var aggEntry = StartAggregatedChild<T[]>(ctx,
                    entityNameOrGroup: "aggregate", // <— naming: see notes below
                    blockName: blockName,
                    priorMessageActivities: linkTargets,
                    operation: "entry",
                    batchSize: batch.Length);

                // We “mock” execution for a BatchBlock because actual execution is trivial
                MockZeroDuration(aggEntry, reason: "batch-no-distinct-execution");

                // Aggregated exit
                using var aggExit = StartAggregatedChild<T[]>(ctx, "aggregate", blockName, linkTargets, "exit", batch.Length);

                var newVal = batch.Select(b => b.Value).ToArray();
                var envelope = new TraceEnvelope<T[]>(newVal, EntityName: "aggregate", MessageId: Guid.NewGuid().ToString(), MessageActivity: aggExit);
                return envelope;
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        public static TransformBlock<TraceEnvelope<T>, TraceEnvelope<T>> Buffer<T>(
    PipelineContext ctx,
    string blockName = "buffer",
    ExecutionDataflowBlockOptions? options = null,
    bool emitMockExecution = true,
    bool emitMockExitWhenGreedy = true)
        {
            options ??= new ExecutionDataflowBlockOptions();

            return new TransformBlock<TraceEnvelope<T>, TraceEnvelope<T>>(env =>
            {
                // ENTRY (arrival to buffer)
                using (var entry = StartEntry(ctx, env, blockName))
                { /* zero/near-zero */ }

                // EXECUTION (not applicable for a pure buffer)
                if (emitMockExecution)
                {
                    MockZeroDuration(env.MessageActivity, a =>
                    {
                        a.SetTag("dataflow.block.name", blockName)
                         .SetTag("dataflow.operation", "execution");
                    }, reason: "buffer-no-execution");
                }

                // EXIT (may not be observable when downstream is greedy)
                if (emitMockExitWhenGreedy)
                {
                    MockZeroDuration(env.MessageActivity, a =>
                    {
                        a.SetTag("dataflow.block.name", blockName)
                         .SetTag("dataflow.operation", "exit");
                    }, reason: "greedy-exit-or-unobservable");
                }
                else
                {
                    using var exit = StartExit(ctx, env, blockName);
                }

                return env;
            }, options);
        }

        public static IPropagatorBlock<TraceEnvelope<TIn>, TraceEnvelope<TOut>>
Batch<TIn, TOut>(
    PipelineContext ctx,
    int batchSize,
    Func<TIn[], TOut> projector,
    string resultingEntityName = "aggregate",
    GroupingDataflowBlockOptions? batchOptions = null,
    ExecutionDataflowBlockOptions? transformOptions = null,
    string blockName = "batch")
        {
            batchOptions ??= new GroupingDataflowBlockOptions { Greedy = true };
            transformOptions ??= new ExecutionDataflowBlockOptions();

            var batch = new BatchBlock<TraceEnvelope<TIn>>(batchSize, batchOptions);

            var aggregate = new TransformBlock<TraceEnvelope<TIn>[], TraceEnvelope<TOut>>(arr =>
            {
                var priorActs = arr.Select(a => a.MessageActivity).ToList();
                var aggregateMsg = StartAggregateMessage(ctx, resultingEntityName, priorActs, batchOrJoinSize: arr.Length);

                // Wrap into an envelope so downstream blocks look like normal message children
                var env = new TraceEnvelope<TOut>(
                    Value: projector(arr.Select(e => e.Value).ToArray()),
                    EntityName: resultingEntityName,
                    MessageId: Guid.NewGuid().ToString("N"),
                    MessageActivity: aggregateMsg);

                // ENTRY (aggregated)
                using (var entry = StartAggregatedChild(ctx, env, blockName, "entry")) { }

                // EXECUTION (batch has trivial execution; keep timings if your projector is heavy)
                using (var exec = StartAggregatedChild(ctx, env, blockName, "execution")) { }

                // EXIT (aggregated)
                using (var exit = StartAggregatedChild(ctx, env, blockName, "exit")) { }

                return env;
            }, transformOptions);

            batch.LinkTo(aggregate, new DataflowLinkOptions { PropagateCompletion = true });

            return DataflowBlock.Encapsulate<TraceEnvelope<TIn>, TraceEnvelope<TOut>>(batch, aggregate);
        }


        public sealed class Join2Block<T1, T2, TOut>
        {
            public ITargetBlock<TraceEnvelope<T1>> Input1 { get; }
            public ITargetBlock<TraceEnvelope<T2>> Input2 { get; }
            public ISourceBlock<TraceEnvelope<TOut>> Output { get; }

            internal Join2Block(
                ITargetBlock<TraceEnvelope<T1>> in1,
                ITargetBlock<TraceEnvelope<T2>> in2,
                ISourceBlock<TraceEnvelope<TOut>> outSrc)
            { Input1 = in1; Input2 = in2; Output = outSrc; }
        }

        public sealed class Join3Block<T1, T2, T3, TOut>
        {
            public ITargetBlock<TraceEnvelope<T1>> Input1 { get; }
            public ITargetBlock<TraceEnvelope<T2>> Input2 { get; }
            public ITargetBlock<TraceEnvelope<T3>> Input3 { get; }
            public ISourceBlock<TraceEnvelope<TOut>> Output { get; }

            internal Join3Block(
                ITargetBlock<TraceEnvelope<T1>> in1,
                ITargetBlock<TraceEnvelope<T2>> in2,
                ITargetBlock<TraceEnvelope<T3>> in3,
                ISourceBlock<TraceEnvelope<TOut>> outSrc)
            { Input1 = in1; Input2 = in2; Input3 = in3; Output = outSrc; }
        }

        public static Join2Block<T1, T2, TOut>
        Join2<T1, T2, TOut>(
            PipelineContext ctx,
            Func<T1, T2, TOut> projector,
            string resultingEntityName = "aggregate",
            GroupingDataflowBlockOptions? joinOptions = null,
            ExecutionDataflowBlockOptions? transformOptions = null,
            string blockName = "join")
        {
            joinOptions ??= new GroupingDataflowBlockOptions { Greedy = true };
            transformOptions ??= new ExecutionDataflowBlockOptions();

            var join = new JoinBlock<TraceEnvelope<T1>, TraceEnvelope<T2>>(joinOptions);

            var aggregate = new TransformBlock<Tuple<TraceEnvelope<T1>, TraceEnvelope<T2>>, TraceEnvelope<TOut>>(tuple =>
            {
                var (e1, e2) = (tuple.Item1, tuple.Item2);

                var aggregateMsg = StartAggregateMessage(
                    ctx, resultingEntityName, new[] { e1.MessageActivity, e2.MessageActivity }, batchOrJoinSize: 2);

                var env = new TraceEnvelope<TOut>(
                    Value: projector(e1.Value, e2.Value),
                    EntityName: resultingEntityName,
                    MessageId: Guid.NewGuid().ToString("N"),
                    MessageActivity: aggregateMsg);

                using (var entry = StartAggregatedChild(ctx, env, blockName, "entry")) { }
                using (var exec = StartAggregatedChild(ctx, env, blockName, "execution")) { }
                using (var exit = StartAggregatedChild(ctx, env, blockName, "exit")) { }

                return env;
            }, transformOptions);

            join.LinkTo(aggregate, new DataflowLinkOptions { PropagateCompletion = true });

            return new Join2Block<T1, T2, TOut>(join.Target1, join.Target2, aggregate);
        }

        public static Join3Block<T1, T2, T3, TOut>
    Join3<T1, T2, T3, TOut>(
        PipelineContext ctx,
        Func<T1, T2, T3, TOut> projector,
        string resultingEntityName = "aggregate",
        GroupingDataflowBlockOptions? joinOptions = null,
        ExecutionDataflowBlockOptions? transformOptions = null,
        string blockName = "join")
        {
            joinOptions ??= new GroupingDataflowBlockOptions { Greedy = true };
            transformOptions ??= new ExecutionDataflowBlockOptions();

            var join = new JoinBlock<TraceEnvelope<T1>, TraceEnvelope<T2>, TraceEnvelope<T3>>(joinOptions);

            var aggregate = new TransformBlock<Tuple<TraceEnvelope<T1>, TraceEnvelope<T2>, TraceEnvelope<T3>>, TraceEnvelope<TOut>>(tuple =>
            {
                var (e1, e2, e3) = (tuple.Item1, tuple.Item2, tuple.Item3);

                var aggregateMsg = StartAggregateMessage(
                    ctx, resultingEntityName, new[] { e1.MessageActivity, e2.MessageActivity, e3.MessageActivity }, batchOrJoinSize: 3);

                var env = new TraceEnvelope<TOut>(
                    Value: projector(e1.Value, e2.Value, e3.Value),
                    EntityName: resultingEntityName,
                    MessageId: Guid.NewGuid().ToString("N"),
                    MessageActivity: aggregateMsg);

                using (var entry = StartAggregatedChild(ctx, env, blockName, "entry")) { }
                using (var exec = StartAggregatedChild(ctx, env, blockName, "execution")) { }
                using (var exit = StartAggregatedChild(ctx, env, blockName, "exit")) { }

                return env;
            }, transformOptions);

            join.LinkTo(aggregate, new DataflowLinkOptions { PropagateCompletion = true });

            return new Join3Block<T1, T2, T3, TOut>(join.Target1, join.Target2, join.Target3, aggregate);
        }


    }

}
