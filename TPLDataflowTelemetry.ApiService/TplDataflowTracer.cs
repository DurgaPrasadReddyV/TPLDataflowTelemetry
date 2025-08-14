namespace TPLDataflowTelemetry.ApiService
{
    using OpenTelemetry;
    using OpenTelemetry.Trace;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using System.Threading.Tasks.Dataflow;

    // --------------------------------
    // Concurrency slot helper (DoP tags)
    // --------------------------------
    file sealed class ConcurrencySlots
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<int> _free = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<int, byte> _taken = new();
        public int MaxDoP { get; }
        public ConcurrencySlots(int dop)
        {
            MaxDoP = Math.Max(1, dop);
            for (int i = 0; i < MaxDoP; i++) _free.Enqueue(i);
        }
        public int Rent() => _free.TryDequeue(out var s) && _taken.TryAdd(s, 0) ? s : -1;
        public void Return(int slot)
        {
            if (slot < 0) return;
            if (_taken.TryRemove(slot, out _)) _free.Enqueue(slot);
        }
    }

    // -------------
    // The tracer
    // -------------
    public sealed class TplDataflowTracer
    {
        private readonly ActivitySource _source;
        private readonly Meter _meter;

        // Metrics
        private readonly Counter<long> _execStarted;
        private readonly Counter<long> _execCompleted;
        private readonly UpDownCounter<long> _inflight;

        public TplDataflowTracer(ActivitySource source, Meter meter)
        {
            _source = source;
            _meter = meter;

            _execStarted = _meter.CreateCounter<long>("tpl.block.exec.started");
            _execCompleted = _meter.CreateCounter<long>("tpl.block.exec.completed");
            _inflight = _meter.CreateUpDownCounter<long>("tpl.block.exec.inflight");
        }

        // --------------------------
        // Pipeline + message roots
        // --------------------------
        public Activity? StartPipeline(string name, Action<Activity>? tag = null)
        {
            var a = _source.StartActivity($"pipeline:{name}", ActivityKind.Internal);
            if (a is null) return null;
            a.SetTag("pipeline.name", name);
            a.SetTag("component", "tpl-dataflow");
            tag?.Invoke(a);
            return a;
        }

        /// <summary>
        /// Create a new message whose root is parented to the provided pipeline span.
        /// Pass the Activity returned by StartPipeline here.
        /// </summary>
        public TracedMessage<T> NewMessage<T>(T value, Activity? pipelineSpan, Action<Activity>? tag = null)
        {
            var msgRoot = _source.StartActivity("msg.root",
                kind: ActivityKind.Internal,
                parentContext: pipelineSpan?.Context ?? default,
                tags: null,
                links: null,
                startTime: default);

            tag?.Invoke(msgRoot!);
            var rootCtx = msgRoot?.Context ?? default;
            // First hop == root (so first block entry won’t create a self-link)
            return new TracedMessage<T>(value, rootCtx, rootCtx, Baggage.Current);
        }

        // --------------------------
        // Send wrapper (block-entry on post path)
        // --------------------------
        public async Task<bool> SendWithEntryAsync<T>(
            ITargetBlock<TracedMessage<T>> target,
            string blockName,
            string blockType,
            TracedMessage<T> msg,
            TimeSpan? timeout = null)
        {
            using var entry = msg.StartMessageSpan(_source, $"block.entry:{blockName}", ActivityKind.Consumer);
            TagBlock(entry, blockName, blockType);
            entry?.SetTag("path", "post");

            var ok = await (timeout is null ? target.SendAsync(msg)
                                            : target.SendAsync(msg).WaitAsync(timeout.Value))
                      .ConfigureAwait(false);
            entry?.SetTag("deliver.accepted", ok);
            return ok;
        }

        // --------------------------
        // Lifecycle + metric helpers
        // --------------------------
        private void AttachLifecycleSpan(string blockName, string blockType, IDataflowBlock block)
        {
            var life = _source.StartActivity($"block.lifecycle:{blockName}", ActivityKind.Internal);
            if (life is not null)
            {
                TagBlock(life, blockName, blockType);
                _ = block.Completion.ContinueWith(_ => life.Dispose());
            }
        }

        private static void TagBlock(Activity? a, string name, string type)
        {
            if (a is null) return;
            a.SetTag("block.name", name);
            a.SetTag("block.type", type);
        }

        private void ExecStart(string name) => _execStarted.Add(1, KeyValuePair.Create<string, object?>("block.name", name));
        private void ExecEnd(string name) => _execCompleted.Add(1, KeyValuePair.Create<string, object?>("block.name", name));
        private void InFlight(string name, int delta) => _inflight.Add(delta, KeyValuePair.Create<string, object?>("block.name", name));

        // --------------------------
        // Block factories (signatures mirror Dataflow)
        // --------------------------

        // BufferBlock<T>
        public BufferBlock<TracedMessage<T>> Buffer<T>(string name, DataflowBlockOptions? cfg = null)
        {
            var b = new BufferBlock<TracedMessage<T>>(cfg ?? new DataflowBlockOptions());
            AttachLifecycleSpan(name, "buffer", b);
            return b;
        }

        // BroadcastBlock<T>
        public BroadcastBlock<TracedMessage<T>> Broadcast<T>(string name, Func<T, T> identity, DataflowBlockOptions? cfg = null)
        {
            var b = new BroadcastBlock<TracedMessage<T>>(msg =>
            {
                using var entry = msg.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "broadcast");

                using var exit = msg.StartMessageSpan(_source, $"block.exit:{name}", ActivityKind.Producer);
                TagBlock(exit, name, "broadcast");

                var cloned = identity(msg.Value);
                // same root; last hop becomes this exit
                return new TracedMessage<T>(cloned, msg.RootContext, exit?.Context ?? msg.LastHopContext, msg.Baggage);
            }, cfg ?? new DataflowBlockOptions());

            AttachLifecycleSpan(name, "broadcast", b);
            return b;
        }

        // ActionBlock<T>
        public ActionBlock<TracedMessage<T>> Action<T>(string name, Func<T, Task> act, ExecutionDataflowBlockOptions? cfg = null)
        {
            var opts = cfg ?? new ExecutionDataflowBlockOptions();
            var dop = Math.Max(1, opts.MaxDegreeOfParallelism);
            var slots = new ConcurrencySlots(dop);

            var block = new ActionBlock<TracedMessage<T>>(async msg =>
            {
                using var entry = msg.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "action");

                var slot = slots.Rent(); InFlight(name, +1);
                using var exec = msg.StartMessageSpan(_source, $"block.exec:{name}", ActivityKind.Server);
                if (exec is not null)
                {
                    TagBlock(exec, name, "action");
                    exec.SetTag("dop.max", dop);
                    exec.SetTag("slot.index", slot);
                    exec.SetTag("thread.id", Environment.CurrentManagedThreadId);
                    ExecStart(name);
                }

                try { await act(msg.Value).ConfigureAwait(false); }
                catch (Exception ex) { exec?.SetStatus(ActivityStatusCode.Error, ex.Message); exec?.RecordException(ex); throw; }
                finally { ExecEnd(name); InFlight(name, -1); slots.Return(slot); }

                using var exit = msg.StartMessageSpan(_source, $"block.exit:{name}", ActivityKind.Producer);
                TagBlock(exit, name, "action");
                msg.UpdateLastHop(exit!);
            }, opts);

            AttachLifecycleSpan(name, "action", block);
            return block;
        }

        // TransformBlock<TIn,TOut>
        public TransformBlock<TracedMessage<TIn>, TracedMessage<TOut>> Transform<TIn, TOut>(
            string name,
            Func<TIn, TOut> fn,
            ExecutionDataflowBlockOptions? cfg = null)
        {
            var opts = cfg ?? new ExecutionDataflowBlockOptions();
            var dop = Math.Max(1, opts.MaxDegreeOfParallelism);
            var slots = new ConcurrencySlots(dop);

            var block = new TransformBlock<TracedMessage<TIn>, TracedMessage<TOut>>(async msg =>
            {
                using var entry = msg.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "transform");

                var slot = slots.Rent(); InFlight(name, +1);
                using var exec = msg.StartMessageSpan(_source, $"block.exec:{name}", ActivityKind.Internal);
                if (exec is not null)
                {
                    TagBlock(exec, name, "transform");
                    exec.SetTag("dop.max", dop);
                    exec.SetTag("slot.index", slot);
                    exec.SetTag("thread.id", Environment.CurrentManagedThreadId);
                    ExecStart(name);
                }

                TOut res;
                try { res = await Task.Run(() => fn(msg.Value)).ConfigureAwait(false); }
                catch (Exception ex) { exec?.SetStatus(ActivityStatusCode.Error, ex.Message); exec?.RecordException(ex); throw; }
                finally { ExecEnd(name); InFlight(name, -1); slots.Return(slot); }

                using var exit = msg.StartMessageSpan(_source, $"block.exit:{name}", ActivityKind.Producer);
                TagBlock(exit, name, "transform");

                return new TracedMessage<TOut>(res, msg.RootContext, exit?.Context ?? msg.LastHopContext, msg.Baggage);
            }, opts);

            AttachLifecycleSpan(name, "transform", block);
            return block;
        }

        // TransformManyBlock<TIn,TOut>
        public TransformManyBlock<TracedMessage<TIn>, TracedMessage<TOut>> TransformMany<TIn, TOut>(
            string name,
            Func<TIn, IEnumerable<TOut>> fn,
            ExecutionDataflowBlockOptions? cfg = null)
        {
            var opts = cfg ?? new ExecutionDataflowBlockOptions();
            var dop = Math.Max(1, opts.MaxDegreeOfParallelism);
            var slots = new ConcurrencySlots(dop);

            var block = new TransformManyBlock<TracedMessage<TIn>, TracedMessage<TOut>>(async msg =>
            {
                using var entry = msg.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "transformmany");

                var slot = slots.Rent(); InFlight(name, +1);
                using var exec = msg.StartMessageSpan(_source, $"block.exec:{name}", ActivityKind.Internal);
                if (exec is not null)
                {
                    TagBlock(exec, name, "transformmany");
                    exec.SetTag("dop.max", dop);
                    exec.SetTag("slot.index", slot);
                    exec.SetTag("thread.id", Environment.CurrentManagedThreadId);
                    ExecStart(name);
                }

                List<TOut> outs;
                try { outs = await Task.Run(() => fn(msg.Value).ToList()).ConfigureAwait(false); }
                catch (Exception ex) { exec?.SetStatus(ActivityStatusCode.Error, ex.Message); exec?.RecordException(ex); throw; }
                finally { ExecEnd(name); InFlight(name, -1); slots.Return(slot); }

                using var exit = msg.StartMessageSpan(_source, $"block.exit:{name}", ActivityKind.Producer);
                TagBlock(exit, name, "transformmany");
                exit?.SetTag("split.count", outs.Count);

                var last = exit?.Context ?? msg.LastHopContext;
                return outs.Select(v => new TracedMessage<TOut>(v, msg.RootContext, last, msg.Baggage));
            }, opts);

            AttachLifecycleSpan(name, "transformmany", block);
            return block;
        }

        // BatchBlock<T>
        public BatchBlock<TracedMessage<T>> Batch<T>(string name, int batchSize, GroupingDataflowBlockOptions? cfg = null)
        {
            var b = new BatchBlock<TracedMessage<T>>(batchSize, cfg ?? new GroupingDataflowBlockOptions());
            AttachLifecycleSpan(name, "batch", b);
            return b;
        }

        // --------------------------
        // Merge projector blocks (you wire them)
        // --------------------------

        /// <summary>
        /// Project a BatchBlock's output (TracedMessage&lt;TIn&gt;[]) into ONE TracedMessage&lt;TOut&gt;.
        /// Signatures mirror TransformBlock&lt;TIn[],TOut&gt; (no linking inside tracer).
        /// </summary>
        public TransformBlock<TracedMessage<TIn>[], TracedMessage<TOut>> BatchMergeProjector<TIn, TOut>(
            string name,
            Func<IReadOnlyList<TIn>, TOut> projector,
            ExecutionDataflowBlockOptions? cfg = null)
        {
            var block = new TransformBlock<TracedMessage<TIn>[], TracedMessage<TOut>>(batchArr =>
            {
                var any = batchArr.FirstOrDefault();
                using var entry = any?.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "batch");

                var payload = projector(batchArr.Select(m => m.Value).ToList());

                // Create a NEW message root under the same pipeline as inputs, carrying links to all inputs' last hops.
                var links = batchArr.Select(m => new ActivityLink(m.LastHopContext)).ToArray();
                IEnumerable<KeyValuePair<string, object?>>? tags = null;
                IEnumerable<ActivityLink>? linkEnum = links;

                var pipelineParent = any?.RootContext ?? default;
                using var msgRoot = _source.StartActivity("msg.root",
                    kind: ActivityKind.Internal,
                    parentContext: pipelineParent,
                    tags: null,
                    links: linkEnum, // provenance preserved
                    startTime: default);

                using var mergeExit = _source.StartActivity( $"block.exit:{name}",
                    kind: ActivityKind.Producer,
                    parentContext: msgRoot?.Context ?? default,
                    tags: tags,
                    links: linkEnum,
                    startTime: default);
                TagBlock(mergeExit, name, "batch");
                mergeExit?.SetTag("merge.count", links.Length);

                var root = msgRoot?.Context ?? default;
                var last = mergeExit?.Context ?? root;
                return new TracedMessage<TOut>(payload, root, last, any?.Baggage ?? Baggage.Current);
            }, cfg ?? new ExecutionDataflowBlockOptions());

            AttachLifecycleSpan(name, "batch.merge", block);
            return block;
        }

        // 2-way Join: returns the Join block and a projector block (you wire them)
        public (JoinBlock<TracedMessage<T1>, TracedMessage<T2>>,
                TransformBlock<Tuple<TracedMessage<T1>, TracedMessage<T2>>, TracedMessage<TOut>>)
            JoinWithMergeBlocks<T1, T2, TOut>(
                string name,
                Func<T1, T2, TOut> mergePayload,
                GroupingDataflowBlockOptions? cfgJoin = null,
                ExecutionDataflowBlockOptions? cfgProjector = null)
        {
            var join = new JoinBlock<TracedMessage<T1>, TracedMessage<T2>>(cfgJoin ?? new GroupingDataflowBlockOptions());
            AttachLifecycleSpan(name, "join", join);

            var projector = new TransformBlock<Tuple<TracedMessage<T1>, TracedMessage<T2>>, TracedMessage<TOut>>(pair =>
            {
                var left = pair.Item1; var right = pair.Item2;
                using var entry = left.StartMessageSpan(_source, $"block.entry:{name}", ActivityKind.Consumer);
                TagBlock(entry, name, "join");

                var payload = mergePayload(left.Value, right.Value);

                var links = new[] { new ActivityLink(left.LastHopContext), new ActivityLink(right.LastHopContext) };
                IEnumerable<KeyValuePair<string, object?>>? tags = null;
                IEnumerable<ActivityLink>? linkEnum = links;

                // New message root (parented to pipeline), with links to both inputs
                var pipelineParent = left.RootContext; // both should share same pipeline root lineage
                using var msgRoot = _source.StartActivity("msg.root",
                    kind: ActivityKind.Internal,
                    parentContext: pipelineParent,
                    tags: null,
                    links: linkEnum,
                    startTime: default);

                using var mergeExit = _source.StartActivity( $"block.exit:{name}",
                    kind: ActivityKind.Producer,
                    parentContext: msgRoot?.Context ?? default,
                    tags: tags,
                    links: linkEnum,
                    startTime: default);
                TagBlock(mergeExit, name, "join");
                mergeExit?.SetTag("merge.count", 2);

                var root = msgRoot?.Context ?? default;
                var last = mergeExit?.Context ?? root;
                return new TracedMessage<TOut>(payload, root, last, left.Baggage);
            }, cfgProjector ?? new ExecutionDataflowBlockOptions());

            AttachLifecycleSpan(name, "join.merge", projector);
            return (join, projector);
        }
    }

}
