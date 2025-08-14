using System.Threading.Tasks.Dataflow;

namespace TPLDataflowTelemetry.ApiService
{
    // -----------------------------
    // Example domain + pipeline wiring (explicit, outside tracer)
    // -----------------------------
    public sealed class Order { public int Id { get; set; } }
    public sealed class EnrichedOrder { public int Id { get; set; } public string Note { get; set; } = ""; }
    public sealed class Line { public int OrderId { get; set; } public int Qty { get; set; } }

    public static class Demo
    {
        public static async Task RunOrdersPipeline(TplDataflowTracer tracer, CancellationToken ct = default)
        {
            using var pipeline = tracer.StartPipeline("orders", a => a.SetTag("env", "prod"));

            var inbound = tracer.Buffer<Order>("orders.in");
            var enrich = tracer.Transform<Order, EnrichedOrder>(
                "orders.enrich",
                o => new EnrichedOrder { Id = o.Id, Note = "ok" },
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4, BoundedCapacity = 128 });

            var lines = tracer.TransformMany<EnrichedOrder, Line>(
                "orders.lines",
                eo => Enumerable.Range(1, 1 + (eo.Id % 3)).Select(i => new Line { OrderId = eo.Id, Qty = i }),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4, BoundedCapacity = 256 });

            var batch = tracer.Batch<Line>("lines.batch", batchSize: 5);
            var batchMerge = tracer.BatchMergeProjector<Line, Line[]>("lines.batch", xs => xs.ToArray());

            var persist = tracer.Action<Line[]>("lines.persist",
                async xs => { await Task.Delay(10, ct); /* your I/O */ },
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 8, BoundedCapacity = 64 });

            // Wire the graph (outside tracer)
            var linkOpts = new DataflowLinkOptions { PropagateCompletion = true };
            inbound.LinkTo(enrich, linkOpts);
            enrich.LinkTo(lines, linkOpts);
            lines.LinkTo(batch, linkOpts);
            batch.LinkTo(batchMerge, linkOpts);
            batchMerge.LinkTo(persist, linkOpts);

            // Feed messages (create message roots under the pipeline!)
            for (int i = 1; i <= 20; i++)
            {
                var msg = tracer.NewMessage(new Order { Id = i }, pipeline, a => a.SetTag("order.id", i));
                await tracer.SendWithEntryAsync(inbound, "orders.in", "buffer", msg);
            }

            inbound.Complete();
            await persist.Completion;
        }
    }
}
