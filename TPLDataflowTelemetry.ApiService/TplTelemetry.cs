namespace TPLDataflowTelemetry.ApiService
{
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using System.Threading.Tasks.Dataflow;

    public sealed class TplTelemetryOptions
    {
        public string PipelineName { get; init; } = "default-pipeline";
        public string ActivitySourceName { get; init; } = "MyCompany.TplDataflow";
        public string MeterName { get; init; } = "MyCompany.TplDataflow";
    }

    public interface ITplTelemetry
    {
        // emit when caller is about to SendAsync/Post into a block (captures enqueue wait)
        ValueTask MeasureSendAsync<T>(ISourceBlock<T> sourceOrNull, ITargetBlock<T> target, object? payload, string blockName, string blockType, Func<Task> send);

        // called by blocks around actual execution
        IDisposable BeginBlockExecution(string blockName, string blockType, int? dop, int? boundedCapacity, Guid flowId, long seq, out Activity? activity);

        // record outcomes and counters
        void RecordProcessed(string blockName, string blockType, bool success, Exception? ex = null, long? producedCount = 1);

        // register a queue/backlog observable for a block
        IDisposable RegisterQueueLength(string blockName, string blockType, Func<long> observe);

        // (optional) record batch/join wait time
        void RecordBatchOrJoinWait(string blockName, string kind /*batch|join*/, double waitMs, int size);
    }

    public sealed class TplTelemetry : ITplTelemetry, IDisposable
    {
        private readonly ActivitySource _source;
        private readonly Meter _meter;

        // Common instruments
        private readonly Counter<long> _itemsIn;
        private readonly Counter<long> _itemsOut;
        private readonly Counter<long> _exceptions;
        private readonly Histogram<double> _procMs;
        private readonly Histogram<double> _enqueueWaitMs;
        private readonly UpDownCounter<long> _concurrency;
        private readonly Histogram<double> _batchJoinWaitMs;

        // Keep disposables for observable gauges
        private readonly List<IDisposable> _observables = new();

        public TplTelemetry(TplTelemetryOptions opts, IMeterFactory meterFactory)
        {
            _source = new ActivitySource(opts.ActivitySourceName);
            _meter = meterFactory.Create(opts.MeterName);

            _itemsIn = _meter.CreateCounter<long>("dataflow.items_in", unit: "items", description: "Items accepted by a block");
            _itemsOut = _meter.CreateCounter<long>("dataflow.items_out", unit: "items", description: "Items emitted by a block");
            _exceptions = _meter.CreateCounter<long>("dataflow.exceptions", unit: "events", description: "Exceptions thrown by a block");
            _procMs = _meter.CreateHistogram<double>("dataflow.process_ms", unit: "ms", description: "Processing time per item in a block");
            _enqueueWaitMs = _meter.CreateHistogram<double>("dataflow.enqueue_wait_ms", unit: "ms", description: "Wait time before a block accepted an item");
            _concurrency = _meter.CreateUpDownCounter<long>("dataflow.concurrency", unit: "workers", description: "In-flight handlers per block");
            _batchJoinWaitMs = _meter.CreateHistogram<double>("dataflow.batchjoin_wait_ms", unit: "ms", description: "Time to fill batch or match join");

            PipelineName = opts.PipelineName;
        }

        public string PipelineName { get; }

        static KeyValuePair<string, object?>[] Tags(string pipeline, string blockName, string blockType) => new[]
        {
        new KeyValuePair<string, object?>("pipeline", pipeline),
        new KeyValuePair<string, object?>("block.name", blockName),
        new KeyValuePair<string, object?>("block.type", blockType),
    };

        public async ValueTask MeasureSendAsync<T>(
            ISourceBlock<T> sourceOrNull,
            ITargetBlock<T> target,
            object? payload,
            string blockName,
            string blockType,
            Func<Task> send)
        {
            var start = Stopwatch.GetTimestamp();
            await send().ConfigureAwait(false);
            var end = Stopwatch.GetTimestamp();
            var ms = (end - start) * 1000.0 / Stopwatch.Frequency;

            _itemsIn.Add(1, Tags(PipelineName, blockName, blockType));
            _enqueueWaitMs.Record(ms, Tags(PipelineName, blockName, blockType));
        }

        public IDisposable BeginBlockExecution(
            string blockName, string blockType, int? dop, int? boundedCapacity,
            Guid flowId, long seq, out Activity? activity)
        {
            activity = _source.StartActivity($"dataflow.{blockType}.{blockName}.process", ActivityKind.Internal);
            if (activity is not null)
            {
                activity.SetTag("pipeline", PipelineName);
                activity.SetTag("block.name", blockName);
                activity.SetTag("block.type", blockType);
                if (dop is not null) activity.SetTag("block.dop", dop);
                if (boundedCapacity is not null) activity.SetTag("block.bounded_capacity", boundedCapacity);
                activity.SetTag("item.flow_id", flowId.ToString());
                activity.SetTag("item.seq", seq);
            }

            _concurrency.Add(1, Tags(PipelineName, blockName, blockType));
            var sw = Stopwatch.StartNew();

            return new CallbackDisposable(() =>
            {
                sw.Stop();
                _procMs.Record(sw.Elapsed.TotalMilliseconds, Tags(PipelineName, blockName, blockType));
                _concurrency.Add(-1, Tags(PipelineName, blockName, blockType));
                activity?.Dispose();
            });
        }

        public void RecordProcessed(string blockName, string blockType, bool success, Exception? ex = null, long? producedCount = 1)
        {
            if (!success)
            {
                _exceptions.Add(1, Tags(PipelineName, blockName, blockType));
            }
            else
            {
                if (producedCount is > 0)
                    _itemsOut.Add(producedCount.Value, Tags(PipelineName, blockName, blockType));
            }
        }

        public IDisposable RegisterQueueLength(string blockName, string blockType, Func<long> observe)
        {
            var obs = _meter.CreateObservableGauge("dataflow.queue_length",
                () => new[] { new Measurement<long>(observe(), Tags(PipelineName, blockName, blockType)) },
                unit: "items",
                description: "Items waiting inside a block");
            _observables.Add(obs);
            return obs;
        }

        public void RecordBatchOrJoinWait(string blockName, string kind, double waitMs, int size)
        {
            _batchJoinWaitMs.Record(waitMs, new[]
            {
            new KeyValuePair<string, object?>("pipeline", PipelineName),
            new KeyValuePair<string, object?>("block.name", blockName),
            new KeyValuePair<string, object?>("block.kind", kind),
            new KeyValuePair<string, object?>("batch.size", size),
        });
        }

        public void Dispose()
        {
            foreach (var d in _observables) d.Dispose();
            _observables.Clear();
            _meter.Dispose();
            _source.Dispose();
        }

        private sealed class CallbackDisposable : IDisposable
        {
            private readonly Action _cb;
            public CallbackDisposable(Action cb) => _cb = cb;
            public void Dispose() => _cb();
        }
    }
}
