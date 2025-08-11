namespace TPLDataflowTelemetry.ApiService
{
    public sealed record Order(string OrderId, string Currency, decimal Total, IReadOnlyList<LineItem> Items);
    public sealed record LineItem(string OrderId, string Sku, int Qty, decimal UnitPrice, string Currency);
    public sealed record FxRate(string From, string To, decimal Rate, DateTimeOffset AsOf);
    public sealed record InventorySnapshot(string Sku, int OnHand, DateTimeOffset AsOf);
    public sealed record PaymentEvent(string OrderId, decimal Amount, string Method, DateTimeOffset At);
    public sealed record RefundRule(string Method, decimal FeePct);

    public sealed record ScenarioParameters
    {
        // Workload
        public int OrdersCount { get; init; } = 800;
        public int AvgItemsPerOrder { get; init; } = 4;
        public int OrderIngestPerSecond { get; init; } = 300;

        // Delays (ms) to shape backpressure
        public int ParseDelayMs { get; init; } = 2;
        public int ExpandDelayMs { get; init; } = 2;
        public int FxConvertDelayMs { get; init; } = 3;
        public int BatchSinkDelayMs { get; init; } = 25;
        public int PaymentSinkDelayMs { get; init; } = 10;

        // DOP
        public int ParseDop { get; init; } = Environment.ProcessorCount;
        public int ExpandDop { get; init; } = 4;
        public int ConvertDop { get; init; } = 4;
        public int BatchSinkDop { get; init; } = 2;

        // Capacities (tighten to induce backpressure)
        public int IngressCapacity { get; init; } = 2048;
        public int ParseCapacity { get; init; } = 128;
        public int ExpandCapacity { get; init; } = 64;
        public int TapLineCapacity { get; init; } = 32;
        public int TapFxCapacity { get; init; } = 32;
        public int JoinOutCapacity { get; init; } = 128;
        public int ConvertCapacity { get; init; } = 64;
        public int BatchCapacity { get; init; } = 500;
        public int BatchProbeCapacity { get; init; } = 500;
        public int CompactCapacity { get; init; } = 256;
        public int BroadcastCapacity { get; init; } = 1;
        public int UpperCapacity { get; init; } = 256;
        public int SinkCapacity { get; init; } = 64;

        // Batch / Join / side-stream
        public int BatchSize { get; init; } = 100;
        public bool JoinGreedy { get; init; } = false;      // false => waits for both sides → join wait visible
        public int PaymentsBatchSize { get; init; } = 40;

        // Error injection
        public int ThrowEveryNInConvert { get; init; } = 0; // 0 = never
        public int ThrowEveryNInSink { get; init; } = 0;

        // Skews (to stretch waits)
        public double FxLateProbability { get; init; } = 0.10;
        public double InventorySlowProbability { get; init; } = 0.03;

        // Side-stream rates
        public int FxPerSecond { get; init; } = 400;
        public int InventoryPerSecond { get; init; } = 300;
        public int PaymentsPerSecond { get; init; } = 200;
        public int RefundRulesPerSecond { get; init; } = 50;
    }

}
