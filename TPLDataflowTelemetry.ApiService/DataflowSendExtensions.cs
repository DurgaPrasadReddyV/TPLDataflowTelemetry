using System.Threading.Tasks.Dataflow;

namespace TPLDataflowTelemetry.ApiService
{
    public static class DataflowSendExtensions
    {
        public static async ValueTask<bool> SendWithTelemetryAsync<T>(
            this ITargetBlock<InstrumentedMessage<T>> target,
            ITplTelemetry tel,
            string blockName,
            string blockType,
            InstrumentedMessage<T> msg,
            CancellationToken ct = default)
        {
            msg.EnqueuedAtUtc ??= DateTimeOffset.UtcNow;
            await tel.MeasureSendAsync<InstrumentedMessage<T>>(sourceOrNull: null, target, msg, blockName, blockType,
                send: () => target.SendAsync(msg, ct));
            return true;
        }

        // Link with backpressure-friendly propagation and a tiny passthrough “probe” (optional)
        public static IDisposable LinkWithName<TOut>(
            this ISourceBlock<InstrumentedMessage<TOut>> source,
            ITargetBlock<InstrumentedMessage<TOut>> target,
            ITplTelemetry tel,
            string targetName,
            string targetType,
            DataflowLinkOptions? linkOptions = null)
        {
            linkOptions ??= new() { PropagateCompletion = true };
            return source.LinkTo(target, linkOptions);
        }
    }

}
