using System.Threading.Tasks.Dataflow;

namespace TPLDataflowTelemetry.ApiService
{
    // Send helper: stamps EnqueuedAtUtc and measures enqueue wait/backpressure against the TARGET block.
    public static class DataflowSend
    {
        public static async ValueTask SendAsync<T>(
            ITargetBlock<InstrumentedMessage<T>> target,
            ITplTelemetry tel,
            string blockName,
            string blockType,
            InstrumentedMessage<T> msg,
            CancellationToken ct = default)
        {
            msg.EnqueuedAtUtc ??= DateTimeOffset.UtcNow;
            await tel.MeasureSendAsync(target, blockName, blockType, () => target.SendAsync(msg, ct));
        }
    }

    // Pass-through “tap” to expose per-branch queue/backpressure
    public static class Taps
    {
        public static TransformBlock<InstrumentedMessage<T>, InstrumentedMessage<T>> Create<T>(
            string name, ITplTelemetry tel, int capacity)
            => InstrumentedBlocks.CreateTransform<T, T>(
                name, tel,
                v => Task.FromResult(v),
                new ExecutionDataflowBlockOptions { BoundedCapacity = capacity, MaxDegreeOfParallelism = 1 });
    }

}
