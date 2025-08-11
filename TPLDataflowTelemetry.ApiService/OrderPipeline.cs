namespace TPLDataflowTelemetry.ApiService
{
    using System.Diagnostics;
    using System.Threading.Tasks.Dataflow;
    using InstrPay = InstrumentedMessage<PaymentEvent>;
    using InstrRule = InstrumentedMessage<RefundRule>;
    using InstrPairs = InstrumentedMessage<(PaymentEvent, RefundRule)[]>;

    using InstrPayList = System.Collections.Generic.IList<InstrumentedMessage<PaymentEvent>>;
    using InstrRuleList = System.Collections.Generic.IList<InstrumentedMessage<RefundRule>>;


    public interface IOrderPipeline
    {
        // Returns the pipeline span id you can search in traces, and a task that completes when both sinks finish.
        Task<(string PipelineSpanId, Task WhenCompleted)> RunScenarioAsync(ScenarioParameters p, CancellationToken ct = default);
    }

    public sealed class OrderPipeline : IOrderPipeline, IAsyncDisposable
    {
        private readonly ITplTelemetry _tel;
        private readonly ActivitySource _src = new("MyCompany.TplDataflow");

        private long _convertCount, _sinkCount;

        public OrderPipeline(ITplTelemetry tel) => _tel = tel;

        public async Task<(string PipelineSpanId, Task WhenCompleted)> RunScenarioAsync(ScenarioParameters p, CancellationToken ct = default)
        {
            using var pipelineActivity = _src.StartActivity("dataflow.pipeline", ActivityKind.Internal);
            pipelineActivity?.SetTag("pipeline", "retail-fulfillment-v1");
            pipelineActivity?.SetTag("orders", p.OrdersCount);
            pipelineActivity?.SetTag("batch.size", p.BatchSize);
            pipelineActivity?.SetTag("join.greedy", p.JoinGreedy);
            pipelineActivity?.SetTag("sink.delay_ms", p.BatchSinkDelayMs);

            var pipelineSpanId = pipelineActivity?.Id ?? Guid.NewGuid().ToString("N");

            // SOURCES (buffers)
            var ordersIngress = InstrumentedBlocks.CreateBuffer<Order>("orders_ingress", _tel, new DataflowBlockOptions { BoundedCapacity = p.IngressCapacity });
            var fxIngress = InstrumentedBlocks.CreateBuffer<FxRate>("fx_ingress", _tel, new DataflowBlockOptions { BoundedCapacity = 1024 });
            var inventoryIngress = InstrumentedBlocks.CreateBuffer<InventorySnapshot>("inventory_ingress", _tel, new DataflowBlockOptions { BoundedCapacity = 1024 });
            var paymentsIngress = InstrumentedBlocks.CreateBuffer<PaymentEvent>("payments_ingress", _tel, new DataflowBlockOptions { BoundedCapacity = 1024 });
            var refundRulesIngress = InstrumentedBlocks.CreateBuffer<RefundRule>("refund_rules_ingress", _tel, new DataflowBlockOptions { BoundedCapacity = 256 });

            // ORDER PATH — parse → expand → taps → join(FX) → convert → batch → probe → compact → broadcast → (upper → sink) & (writeonce)
            var parse = InstrumentedBlocks.CreateTransform<Order, Order>(
                "parse", _tel,
                async o => { await Task.Delay(p.ParseDelayMs); return o; },
                new() { MaxDegreeOfParallelism = p.ParseDop, BoundedCapacity = p.ParseCapacity });

            var expand = InstrumentedBlocks.CreateTransformMany<Order, LineItem>(
                "expand_items", _tel,
                async o =>
                {
                    await Task.Delay(p.ExpandDelayMs);
                    var count = Math.Max(1, Random.Shared.Next(p.AvgItemsPerOrder - 1, p.AvgItemsPerOrder + 2));
                    var list = Enumerable.Range(0, count).Select(i =>
                        new LineItem(o.OrderId, $"SKU-{Random.Shared.Next(1, 500):D4}",
                            Random.Shared.Next(1, 5),
                            Math.Round((decimal)(5 + Random.Shared.NextDouble() * 95), 2),
                            o.Currency)).ToList();
                    return list;
                },
                new() { MaxDegreeOfParallelism = p.ExpandDop, BoundedCapacity = p.ExpandCapacity });

            var tapLine = Taps.Create<LineItem>("tap_line_items", _tel, p.TapLineCapacity);
            var tapFx = Taps.Create<FxRate>("tap_fx_rates", _tel, p.TapFxCapacity);

            var join = InstrumentedBlocks.CreateJoin<LineItem, FxRate>(
                "line_x_fx_join", _tel,
                new GroupingDataflowBlockOptions { Greedy = p.JoinGreedy, BoundedCapacity = p.JoinOutCapacity });

            var joinProbe = InstrumentedBlocks.CreateJoinProbe<LineItem, FxRate>("join_probe", _tel, new() { BoundedCapacity = p.JoinOutCapacity });

            var deadLetters = InstrumentedBlocks.CreateAction<(LineItem Item, Exception Error)>(
                "dead_letters", _tel, pair => { return Task.CompletedTask; }, new () { BoundedCapacity = 512 });

            var convert = new TransformManyBlock<InstrumentedMessage<(LineItem, FxRate)>, InstrumentedMessage<LineItem>>(
                async im =>
                {
                    im.DequeuedAtUtc ??= DateTimeOffset.UtcNow;
                    using var _ = _tel.BeginBlockExecution("convert_currency", "transformmany",
                               dop: p.ConvertDop, boundedCapacity: p.ConvertCapacity,
                               im.FlowId, im.Seq, im.ParentContext);

                    try
                  {
                      await Task.Delay(p.FxConvertDelayMs);
                      var (li, fx) = im.Value;
                      if (p.ThrowEveryNInConvert > 0 &&
                          Interlocked.Increment(ref _convertCount) % p.ThrowEveryNInConvert == 0)
                          throw new InvalidOperationException("Synthetic convert error");

                      var inInr = li.Currency == "INR" ? li.UnitPrice : Math.Round(li.UnitPrice * fx.Rate, 2);
                      var outMsg = new InstrumentedMessage<LineItem>(
                          li with { UnitPrice = inInr, Currency = "INR" },
                          im.FlowId, im.Seq, im.CreatedAt, im.ParentContext)
                      {
                          EnqueuedAtUtc = im.EnqueuedAtUtc,
                          DequeuedAtUtc = im.DequeuedAtUtc
                      };
                      foreach (var kv in im.Tags) outMsg.Tags[kv.Key] = kv.Value;

                      _tel.RecordProcessed("convert_currency", "transformmany", success: true, producedCount: 1);
                      return new[] { outMsg };
                  }
                  catch (Exception ex)
                  {
                      _tel.RecordProcessed("convert_currency", "transformmany", success: false, ex);
                      var err = new InstrumentedMessage<(LineItem, Exception)>((im.Value.Item1, ex),
                                im.FlowId, im.Seq, im.CreatedAt, im.ParentContext)
                      { EnqueuedAtUtc = im.EnqueuedAtUtc, DequeuedAtUtc = im.DequeuedAtUtc };
                      foreach (var kv in im.Tags) err.Tags[kv.Key] = kv.Value;

                      await DataflowSend.SendAsync(deadLetters, _tel, "dead_letters", "action", err);
                      return Enumerable.Empty<InstrumentedMessage<LineItem>>();
                  }
                },new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = p.ConvertDop, BoundedCapacity = p.ConvertCapacity });

            var batch = InstrumentedBlocks.CreateBatch<LineItem>("batch_line_items", _tel, p.BatchSize,
                new GroupingDataflowBlockOptions { BoundedCapacity = p.BatchCapacity, Greedy = true });

            var batchProbe = InstrumentedBlocks.CreateBatchProbe<LineItem>("batch_probe", _tel, new() { BoundedCapacity = p.BatchProbeCapacity });

            // Compact: InstrumentedMessage<LineItem>[]  →  InstrumentedMessage<LineItem[]>
            var compact = new TransformBlock<InstrumentedMessage<LineItem>[], InstrumentedMessage<LineItem[]>>(batchArr =>
            {
                var first = batchArr[0];
                using var _ = _tel.BeginBlockExecution("batch_compact", "transform", dop: 1, boundedCapacity: p.CompactCapacity, first.FlowId, first.Seq, first.ParentContext);
                var arr = batchArr.Select(m => m.Value).ToArray();
                var outMsg = new InstrumentedMessage<LineItem[]>(arr, first.FlowId, first.Seq, first.CreatedAt, first.ParentContext)
                {
                    EnqueuedAtUtc = first.EnqueuedAtUtc,
                    DequeuedAtUtc = first.DequeuedAtUtc
                };
                foreach (var kv in first.Tags) outMsg.Tags[kv.Key] = kv.Value;
                _tel.RecordProcessed("batch_compact", "transform", success: true, producedCount: 1);
                return outMsg;
            }, new ExecutionDataflowBlockOptions { BoundedCapacity = p.CompactCapacity });

            var broadcast = InstrumentedBlocks.CreateBroadcast<LineItem[]>(
                "broadcast_latest_batch", _tel, msg => msg,
                new DataflowBlockOptions { BoundedCapacity = p.BroadcastCapacity });

            var toUpper = InstrumentedBlocks.CreateTransform<LineItem[], LineItem[]>(
                "uppercase_sku", _tel,
                arr => Task.FromResult(arr.Select(li => li with { Sku = li.Sku.ToUpperInvariant() }).ToArray()),
                new() { BoundedCapacity = p.UpperCapacity });

            var sink = InstrumentedBlocks.CreateAction<LineItem[]>(
                "persist_batch", _tel,
                async arr =>
                {
                    await Task.Delay(p.BatchSinkDelayMs);
                    if (p.ThrowEveryNInSink > 0 && Interlocked.Increment(ref _sinkCount) % p.ThrowEveryNInSink == 0)
                        throw new Exception("Synthetic sink fault");
                    // TODO: DB write
                },
                new() { MaxDegreeOfParallelism = p.BatchSinkDop, BoundedCapacity = p.SinkCapacity });

            var firstSeen = InstrumentedBlocks.CreateWriteOnce<LineItem[]>("first_seen_batch", _tel);

            // PAYMENTS ↔ REFUND RULES (BatchedJoin) → reconcile
            var paymentsTap = Taps.Create<PaymentEvent>("tap_payments", _tel, 128);
            var rulesTap = Taps.Create<RefundRule>("tap_refund_rules", _tel, 64);

            var batchedJoin = InstrumentedBlocks.CreateBatchedJoin<PaymentEvent, RefundRule>(
                "payments_x_rules_batchedjoin", _tel, p.PaymentsBatchSize,
                new GroupingDataflowBlockOptions { Greedy = true});

            var postBatchedJoin =
                new TransformBlock<Tuple<InstrPayList, InstrRuleList>, InstrPairs>(tuple =>
                {
                    var leftBatch = tuple.Item1; // IList<InstrumentedMessage<PaymentEvent>>
                    var rightBatch = tuple.Item2; // IList<InstrumentedMessage<RefundRule>>

                    var min = Math.Min(leftBatch.Count, rightBatch.Count);
                    if (min == 0)
                    {
                        return new InstrumentedMessage<(PaymentEvent, RefundRule)[]>(
                            Array.Empty<(PaymentEvent, RefundRule)>(),
                            Guid.NewGuid(), 0, DateTimeOffset.UtcNow, default);
                    }

                    // Safe to use left[0] for context (BatchedJoin only emits when both sides have items)
                    var first = leftBatch[0];

                    using var _ = _tel.BeginBlockExecution(
                        "batchedjoin_probe", "transform",
                        dop: 1, boundedCapacity: 128,
                        first.FlowId, first.Seq, first.ParentContext);

                    // time-to-fill across both sides
                    var firstEnqLeft = leftBatch.Min(m => m.EnqueuedAtUtc ?? m.CreatedAt);
                    var firstEnqRight = rightBatch.Min(m => m.EnqueuedAtUtc ?? m.CreatedAt);
                    var firstEnq = (firstEnqLeft <= firstEnqRight) ? firstEnqLeft : firstEnqRight;

                    _tel.RecordBatchOrJoinWait(
                        "payments_x_rules_batchedjoin",
                        "batchedjoin",
                        (DateTimeOffset.UtcNow - firstEnq).TotalMilliseconds,
                        min);

                    // Pair 1:1 up to min
                    var pairs = new (PaymentEvent, RefundRule)[min];
                    for (int i = 0; i < min; i++)
                        pairs[i] = (leftBatch[i].Value, rightBatch[i].Value);

                    var outMsg = new InstrumentedMessage<(PaymentEvent, RefundRule)[]>(
                        pairs, first.FlowId, first.Seq, first.CreatedAt, first.ParentContext)
                    {
                        EnqueuedAtUtc = first.EnqueuedAtUtc,
                        DequeuedAtUtc = first.DequeuedAtUtc
                    };
                    foreach (var kv in first.Tags) outMsg.Tags[kv.Key] = kv.Value;

                    _tel.RecordProcessed("batchedjoin_probe", "transform", success: true, producedCount: 1);
                    return outMsg;
                }, new ExecutionDataflowBlockOptions { BoundedCapacity = 128 });


            var paymentsSink = InstrumentedBlocks.CreateAction<(PaymentEvent, RefundRule)[]>(
                "reconcile_payments", _tel,
                async pairs => { await Task.Delay(p.PaymentSinkDelayMs); /* reconcile */ },
                new() { BoundedCapacity = 256 });

            // 1) LINKING — rely solely on PropagateCompletion
            var link = new DataflowLinkOptions { PropagateCompletion = true };

            ordersIngress.LinkTo(parse, link);
            parse.LinkTo(expand, link);
            expand.LinkTo(tapLine, link);
            fxIngress.LinkTo(tapFx, link);

            // join path
            tapLine.LinkTo(join.Target1, link);
            tapFx.LinkTo(join.Target2, link);
            join.LinkTo(joinProbe, link);
            joinProbe.LinkTo(convert, link);

            // batch path
            convert.LinkTo(batch, link);
            batch.LinkTo(batchProbe, link);
            batchProbe.LinkTo(compact, link);
            compact.LinkTo(broadcast, link);

            // broadcast branches
            broadcast.LinkTo(toUpper, link);
            broadcast.LinkTo(firstSeen, link);
            toUpper.LinkTo(sink, link);

            // payments/refunds batched join
            paymentsIngress.LinkTo(paymentsTap, link);
            refundRulesIngress.LinkTo(rulesTap, link);
            paymentsTap.LinkTo(batchedJoin.Target1, link);
            rulesTap.LinkTo(batchedJoin.Target2, link);
            batchedJoin.LinkTo(postBatchedJoin, link);
            postBatchedJoin.LinkTo(paymentsSink, link);

            // 2) FEEDERS — bounded duration + grace, do NOT pass external ct into pacing delays
            var runDuration = TimeSpan.FromSeconds(Math.Max(5, p.OrdersCount / Math.Max(1, p.OrderIngestPerSecond) + 2));
            var sideStreamGrace = TimeSpan.FromSeconds(2);

            async Task feedOrders()
            {
                try
                {
                    for (var i = 0; i < p.OrdersCount; i++)
                    {
                        var o = new Order($"ORD-{i:D6}", Random.Shared.Next(0, 2) == 0 ? "USD" : "INR", 0m, Array.Empty<LineItem>());
                        var im = InstrumentedMessage.Create(o);
                        im.Tags["pipeline.span_id"] = pipelineSpanId;

                        await DataflowSend.SendAsync(ordersIngress, _tel, "orders_ingress", "buffer", im); // no request CT
                        await Task.Delay(1000 / Math.Max(1, p.OrderIngestPerSecond));                       // pace without ct
                    }
                }
                finally
                {
                    ordersIngress.Complete();
                }
            }

            async Task feedFx()
            {
                var end = DateTime.UtcNow + runDuration + sideStreamGrace;
                while (DateTime.UtcNow < end)
                {
                    var late = Random.Shared.NextDouble() < p.FxLateProbability ? 12 : 0;
                    await Task.Delay((1000 / Math.Max(1, p.FxPerSecond)) + late);
                    var fx = new FxRate("USD", "INR", (decimal)(82 + Random.Shared.NextDouble() * 2), DateTimeOffset.UtcNow);
                    await DataflowSend.SendAsync(fxIngress, _tel, "fx_ingress", "buffer", InstrumentedMessage.Create(fx));
                }
                fxIngress.Complete();
            }

            async Task feedInventory()
            {
                var end = DateTime.UtcNow + runDuration; // keep it shorter; it’s auxiliary
                while (DateTime.UtcNow < end)
                {
                    var slow = Random.Shared.NextDouble() < p.InventorySlowProbability ? 30 : 0;
                    await Task.Delay((1000 / Math.Max(1, p.InventoryPerSecond)) + slow);
                    var inv = new InventorySnapshot($"SKU-{Random.Shared.Next(1, 500):D4}", Random.Shared.Next(0, 1000), DateTimeOffset.UtcNow);
                    await DataflowSend.SendAsync(inventoryIngress, _tel, "inventory_ingress", "buffer", InstrumentedMessage.Create(inv));
                }
                inventoryIngress.Complete();
            }

            async Task feedPayments()
            {
                try
                {
                    for (var i = 0; i < p.OrdersCount; i++)
                    {
                        await Task.Delay(1000 / Math.Max(1, p.PaymentsPerSecond));
                        var pay = new PaymentEvent($"ORD-{i:D6}", Math.Round((decimal)(50 + Random.Shared.NextDouble() * 500), 2), "CARD", DateTimeOffset.UtcNow);
                        await DataflowSend.SendAsync(paymentsIngress, _tel, "payments_ingress", "buffer", InstrumentedMessage.Create(pay));
                    }
                }
                finally
                {
                    paymentsIngress.Complete();
                }
            }

            async Task feedRefundRules()
            {
                try
                {
                    for (var i = 0; i < Math.Max(1, p.OrdersCount / 20); i++)
                    {
                        await Task.Delay(1000 / Math.Max(1, p.RefundRulesPerSecond));
                        var rr = new RefundRule("CARD", (decimal)(1 + Random.Shared.NextDouble() * 3));
                        await DataflowSend.SendAsync(refundRulesIngress, _tel, "refund_rules_ingress", "buffer", InstrumentedMessage.Create(rr));
                    }
                }
                finally
                {
                    refundRulesIngress.Complete();
                }
            }

            var feeders = Task.WhenAll(feedOrders(), feedFx(), feedInventory(), feedPayments(), feedRefundRules());

            var whenCompleted = Task.WhenAll(sink.Completion, paymentsSink.Completion);

            _ = whenCompleted.ContinueWith(_ => pipelineActivity?.Dispose(), TaskScheduler.Default);

            // await the sources to finish enqueuing
            await feeders;

            // return the sink completion task so the caller can await drains/telemetry
            return (pipelineSpanId, whenCompleted);
        }

        public ValueTask DisposeAsync()
        {
            _src.Dispose();
            return ValueTask.CompletedTask;
        }
    }

}
