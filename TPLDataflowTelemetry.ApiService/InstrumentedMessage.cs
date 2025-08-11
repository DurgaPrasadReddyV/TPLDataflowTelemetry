using System.Diagnostics;

namespace TPLDataflowTelemetry.ApiService
{
    // Use this wrapper to carry correlation, timing breadcrumbs, and integrity info.
    public sealed record InstrumentedMessage<T>(
        T Value,
        Guid FlowId,
        long Seq,
        DateTimeOffset CreatedAt,
        ActivityContext ParentContext,
        string? IntegrityHash = null)
    {
        // On first enqueue into a block (or SendAsync), set this:
        public DateTimeOffset? EnqueuedAtUtc { get; set; }
        // When a block starts actually processing the item:
        public DateTimeOffset? DequeuedAtUtc { get; set; }
        // Optional: store per-hop hashes if you need tail checks
        public string? HopHash { get; set; }
        // Attach arbitrary tags if helpful
        public Dictionary<string, object?> Tags { get; } = new();
    }

    public static class InstrumentedMessage
    {
        public static InstrumentedMessage<T> Create<T>(T value, long seq = 0)
        {
            var parent = Activity.Current?.Context ?? default;
            return new(value, Guid.NewGuid(), seq, DateTimeOffset.UtcNow, parent);
        }
    }
}
