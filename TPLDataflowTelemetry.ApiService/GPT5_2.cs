namespace TPLDataflowTelemetry.ApiService
{
    using System.Diagnostics;
    using System.Threading.Tasks.Dataflow;
    using static InstrumentedBlocks;
    using static TplDataflow.Otel.PipelineTracing;

    public static class OrderProcessingPipelineGPT
    {
        public static async Task RunAsync()
        {
            using var ctx = StartPipeline(pipelineName: "pipeline", businessCase: "orderprocessing");

            // Document the workflow blocks (topology spans)
            RecordWorkflowBlockTopology(ctx, "buffer", "orderprocessing", "BufferBlock", bounded: true, capacity: 100);
            RecordWorkflowBlockTopology(ctx, "transform", "orderprocessing", "TransformBlock");
            RecordWorkflowBlockTopology(ctx, "transformmany", "orderprocessing", "TransformManyBlock");
            RecordWorkflowBlockTopology(ctx, "batch", "orderprocessing", "BatchBlock");
            RecordWorkflowBlockTopology(ctx, "action", "orderprocessing", "ActionBlock");

            // Ingress: wrap raw T into TraceEnvelope<T> + message span
            var ingress = new TransformBlock<Order, TraceEnvelope<Order>>(order =>
                CreateMessage(ctx, order, entityName: "order", messageId: order.Id.ToString()),
                new ExecutionDataflowBlockOptions { BoundedCapacity = 100 });

            // Buffer (no distinct execution; we "mock" execution in downstream if desired)
            // You can leave this as BufferBlock<TraceEnvelope<Order>> if you want a real queue
            var buffer = new BufferBlock<TraceEnvelope<Order>>(new DataflowBlockOptions { BoundedCapacity = 100 });

            // Transform (enrich order)
            var enrich = Transform<Order, EnrichedOrder>(ctx, "transform", async o =>
            {
                await Task.Yield();
                return new EnrichedOrder(o.Id, o.Lines, "Gold");
            });

            // TransformMany split (one order => many line items)
            var split = TransformMany<EnrichedOrder, LineItem>(ctx, "transformmany", o => o.Lines);

            // Batch 10 items (aggregate + links)
            var batchBlock = new BatchBlock<TraceEnvelope<LineItem>>(10, new GroupingDataflowBlockOptions { Greedy = true });
            var batchAgg = InstrumentedBlocks.BatchAggregator<LineItem>(ctx, "batch");

            // Action (persist batch)
            var persist = InstrumentedBlocks.Action<LineItem[]>(ctx, "action", async items =>
            {
                await SaveLinesAsync(items);
            });

            // Wire
            ingress.LinkTo(buffer, new DataflowLinkOptions { PropagateCompletion = true });

            buffer.LinkTo(enrich, new DataflowLinkOptions { PropagateCompletion = true });

            enrich.LinkTo(split, new DataflowLinkOptions { PropagateCompletion = true });

            split.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            // BatchBlock emits TraceEnvelope<LineItem>[] → aggregate to TraceEnvelope<LineItem[]>
            batchBlock.LinkTo(batchAgg, new DataflowLinkOptions { PropagateCompletion = true });

            batchAgg.LinkTo(persist, new DataflowLinkOptions { PropagateCompletion = true });

            // Feed data
            foreach (var order in SampleOrders())
                ingress.Post(order);

            ingress.Complete();
            await persist.Completion;
        }

        public static async Task ExampleAsync()
        {
            using var ctx = StartPipeline("pipeline", "orderprocessing");

            using var topoBuf = StartWorkflowBlockSpan(ctx, "buffer", "orderprocessing", "BufferBlock", bounded: true, capacity: 100);
            using var topoTr = StartWorkflowBlockSpan(ctx, "transform", "orderprocessing", "TransformBlock");
            using var topoBat = StartWorkflowBlockSpan(ctx, "batch", "orderprocessing", "BatchBlock", greedy: true);
            using var topoJoin = StartWorkflowBlockSpan(ctx, "join", "orderprocessing", "JoinBlock", greedy: true);
            using var topoAct = StartWorkflowBlockSpan(ctx, "action", "orderprocessing", "ActionBlock");

            var ingress = new TransformBlock<Order, TraceEnvelope<Order>>(o => CreateMessage(ctx, o, "order", o.Id.ToString()),
                new ExecutionDataflowBlockOptions { BoundedCapacity = 100 });

            var buffer = Buffer<Order>(ctx,
                options: new ExecutionDataflowBlockOptions { BoundedCapacity = 100 },
                emitMockExecution: true,
                emitMockExitWhenGreedy: true);

            var enrich = Transform<Order, EnrichedOrder>(ctx, "transform", async o =>
            {
                await Task.Yield();
                return new EnrichedOrder(o.Id, o.Lines, "Gold");
            });

            var split = TransformMany<EnrichedOrder, LineItem>(ctx, "transformmany", o => o.Lines);

            var batch = Batch<LineItem, LineItem[]>(
                ctx,
                batchSize: 10,
                projector: items => items,
                resultingEntityName: "orderlines",
                batchOptions: new GroupingDataflowBlockOptions { Greedy = true });

            var join = Join2<LineItem[], CustomerProfile, Shipment>(
                ctx,
                projector: (items, profile) => new Shipment(items, profile),
                resultingEntityName: "shipment");

            var action = Action<Shipment>(ctx, "action", async s => await PersistAsync(s));

            // Wire
            ingress.LinkTo(buffer, new DataflowLinkOptions { PropagateCompletion = true });
            buffer.LinkTo(enrich, new DataflowLinkOptions { PropagateCompletion = true });
            enrich.LinkTo(split, new DataflowLinkOptions { PropagateCompletion = true });
            split.LinkTo(batch, new DataflowLinkOptions { PropagateCompletion = true });

            // external source feeding customer profile branch
            var profileIngress = new TransformBlock<CustomerProfile, TraceEnvelope<CustomerProfile>>(p => CreateMessage(ctx, p, "customerprofile", p.Id));
            profileIngress.LinkTo(join.Input2, new DataflowLinkOptions { PropagateCompletion = true });

            batch.LinkTo(join.Input1, new DataflowLinkOptions { PropagateCompletion = true });
            join.Output.LinkTo(action, new DataflowLinkOptions { PropagateCompletion = true });

            // drive
            foreach (var o in Orders()) ingress.Post(o);
            foreach (var p in Profiles()) profileIngress.Post(p);

            ingress.Complete();
            profileIngress.Complete();
            await action.Completion;
        }


        static IEnumerable<Order> SampleOrders() { /* ... */ yield break; }
        static Task SaveLinesAsync(LineItem[] items) => Task.CompletedTask;

        public record Order(Guid Id, List<LineItem> Lines);
        public record EnrichedOrder : Order
        {
            public EnrichedOrder(Guid Id, List<LineItem> lines, string customerTier = "Std") : base(Id, lines)
            {
                CustomerTier = customerTier;
            }
            public string CustomerTier { get; init; } = "Std";
        }
        public record LineItem(string Sku, int Qty);
        public record CustomerProfile(string Id, string Tier);
        public record Shipment(LineItem[] Items, CustomerProfile Profile);
        static IEnumerable<Order> Orders() => Enumerable.Empty<Order>();
        static IEnumerable<CustomerProfile> Profiles() => Enumerable.Empty<CustomerProfile>();
        static Task PersistAsync(Shipment s) => Task.CompletedTask;
    }

}
