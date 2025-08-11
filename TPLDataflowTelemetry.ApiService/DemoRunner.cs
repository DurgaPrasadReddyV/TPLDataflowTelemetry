namespace TPLDataflowTelemetry.ApiService
{
    public sealed class DemoRunner : IHostedService
    {
        private readonly IOrderPipeline _pipeline;
        private readonly ILogger<DemoRunner> _log;
        public DemoRunner(IOrderPipeline pipeline, ILogger<DemoRunner> log) { _pipeline = pipeline; _log = log; }

        public async Task StartAsync(CancellationToken ct)
        {
            var p = new ScenarioParameters
            {
                // turn the knobs to surface ALL the challenges:
                ParseCapacity = 128,
                ExpandCapacity = 64,
                ConvertCapacity = 64,
                SinkCapacity = 64,
                TapLineCapacity = 32,
                TapFxCapacity = 32,
                BatchSize = 100,
                JoinGreedy = false,
                BatchSinkDelayMs = 25,
                FxConvertDelayMs = 3,
                ThrowEveryNInConvert = 200,
                ThrowEveryNInSink = 150,
                FxLateProbability = 0.10,
                PaymentsBatchSize = 40
            };

            var (spanId, whenCompleted) = await _pipeline.RunScenarioAsync(p, CancellationToken.None);
            _log.LogInformation("Retail pipeline started. PipelineSpanId={SpanId}", spanId);

            try
            {
                await whenCompleted;
            }
            catch (AggregateException ex)
            {
                foreach (var inner in ex.Flatten().InnerExceptions)
                {
                    Console.WriteLine($"Exception: {inner.GetType().Name} : {inner.Message}");
                }
            }
        }

        public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
    }

}
