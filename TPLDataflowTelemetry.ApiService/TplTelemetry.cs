using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

public sealed class TplTelemetryOptions
{
    public string PipelineName { get; init; } = "default-pipeline";
    public string ActivitySourceName { get; init; } = "MyCompany.TplDataflow";
    public string MeterName { get; init; } = "MyCompany.TplDataflow";
}

public readonly struct BlockExecScope : IDisposable
{
    private readonly TplTelemetry _tel;
    private readonly string _blockName;
    private readonly string _blockType;
    private readonly Stopwatch _sw;

    public Activity? Activity { get; }

    internal BlockExecScope(TplTelemetry tel,
        string blockName, string blockType,
        Activity? activity)
    {
        _tel = tel;
        _blockName = blockName;
        _blockType = blockType;
        Activity = activity;
        _sw = Stopwatch.StartNew();
    }

    public void Dispose()
    {
        _sw.Stop();
        _tel.RecordProcessDuration(_blockName, _blockType, _sw.Elapsed.TotalMilliseconds);
        _tel.DecConcurrency(_blockName, _blockType);
        Activity?.Dispose();
    }
}

public interface ITplTelemetry
{
    ValueTask MeasureSendAsync<T>(
        ITargetBlock<T> target,
        string blockName,
        string blockType,
        Func<Task> send);

    BlockExecScope BeginBlockExecution(
        string blockName, string blockType,
        int? dop, int? boundedCapacity,
        Guid flowId, long seq,
        ActivityContext parent);

    void RecordProcessed(string blockName, string blockType, bool success, Exception? ex = null, long producedCount = 1);

    void RegisterQueueLength(string blockName, string blockType, Func<long> observe);

    void RecordBatchOrJoinWait(string blockName, string kind /*batch|join|batchedjoin*/, double waitMs, int size);
}

public sealed class TplTelemetry : ITplTelemetry, IDisposable
{
    private readonly ActivitySource _source;
    private readonly Meter _meter;

    private readonly Counter<long> _itemsIn;
    private readonly Counter<long> _itemsOut;
    private readonly Counter<long> _exceptions;
    private readonly Histogram<double> _procMs;
    private readonly Histogram<double> _enqueueWaitMs;
    private readonly UpDownCounter<long> _concurrency;
    private readonly Histogram<double> _batchJoinWaitMs;

    // keep instruments alive (no need to dispose; just hold references)
    private readonly List<object> _observableInstruments = new();

    public string PipelineName { get; }

    public TplTelemetry(TplTelemetryOptions opts, IMeterFactory meterFactory)
    {
        PipelineName = opts.PipelineName;
        _source = new ActivitySource(opts.ActivitySourceName);
        _meter = meterFactory.Create(opts.MeterName);

        _itemsIn = _meter.CreateCounter<long>("dataflow.items_in", unit: "items", description: "Items accepted by a block");
        _itemsOut = _meter.CreateCounter<long>("dataflow.items_out", unit: "items", description: "Items emitted by a block");
        _exceptions = _meter.CreateCounter<long>("dataflow.exceptions", unit: "events", description: "Exceptions thrown by a block");
        _procMs = _meter.CreateHistogram<double>("dataflow.process_ms", unit: "ms", description: "Processing time per item in a block");
        _enqueueWaitMs = _meter.CreateHistogram<double>("dataflow.enqueue_wait_ms", unit: "ms", description: "Wait before target accepted an item");
        _concurrency = _meter.CreateUpDownCounter<long>("dataflow.concurrency", unit: "workers", description: "In-flight handlers per block");
        _batchJoinWaitMs = _meter.CreateHistogram<double>("dataflow.batchjoin_wait_ms", unit: "ms", description: "Time to fill batch / match join");
    }

    public async ValueTask MeasureSendAsync<T>(
        ITargetBlock<T> target,
        string blockName,
        string blockType,
        Func<Task> send)
    {
        var start = Stopwatch.GetTimestamp();
        await send().ConfigureAwait(false);
        var end = Stopwatch.GetTimestamp();
        var ms = (end - start) * 1000.0 / Stopwatch.Frequency;

        _itemsIn.Add(1, Tags(blockName, blockType));
        _enqueueWaitMs.Record(ms, Tags(blockName, blockType));
    }

    public BlockExecScope BeginBlockExecution(
        string blockName, string blockType,
        int? dop, int? boundedCapacity,
        Guid flowId, long seq,
        ActivityContext parent)
    {
        var act = _source.StartActivity($"dataflow.{blockType}.{blockName}.process",
                                        ActivityKind.Internal,
                                        parent);

        if (act is not null)
        {
            act.SetTag("pipeline", PipelineName);
            act.SetTag("block.name", blockName);
            act.SetTag("block.type", blockType);
            if (dop is not null) act.SetTag("block.dop", dop);
            if (boundedCapacity is not null) act.SetTag("block.bounded_capacity", boundedCapacity);
            act.SetTag("item.flow_id", flowId.ToString());
            act.SetTag("item.seq", seq);
        }

        _concurrency.Add(1, Tags(blockName, blockType));
        return new BlockExecScope(this, blockName, blockType, act);
    }

    public void RecordProcessed(string blockName, string blockType, bool success, Exception? ex = null, long producedCount = 1)
    {
        if (!success)
        {
            _exceptions.Add(1, Tags(blockName, blockType));
        }
        else
        {
            if (producedCount > 0)
                _itemsOut.Add(producedCount, Tags(blockName, blockType));
        }
    }

    public void RegisterQueueLength(string blockName, string blockType, Func<long> observe)
    {
        var instrument = _meter.CreateObservableGauge("dataflow.queue_length",
            () =>
            {
                try
                {
                    long v = observe();
                    return new[] { new Measurement<long>(v, Tags(blockName, blockType)) };
                }
                catch
                {
                    return Array.Empty<Measurement<long>>();
                }
            },
            unit: "items",
            description: "Items waiting inside a block");
        _observableInstruments.Add(instrument);
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

    internal void RecordProcessDuration(string blockName, string blockType, double ms)
        => _procMs.Record(ms, Tags(blockName, blockType));

    internal void DecConcurrency(string blockName, string blockType)
        => _concurrency.Add(-1, Tags(blockName, blockType));

    private KeyValuePair<string, object?>[] Tags(string blockName, string blockType) => new[]
    {
        new KeyValuePair<string, object?>("pipeline", PipelineName),
        new KeyValuePair<string, object?>("block.name", blockName),
        new KeyValuePair<string, object?>("block.type", blockType),
    };

    public void Dispose()
    {
        _meter.Dispose();
        _source.Dispose();
        _observableInstruments.Clear();
    }
}
