using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace TplDataflowTracing
{
    // Message wrapper to carry tracing context
    public class TracedMessage<T>
    {
        public T Data { get; set; }
        public Activity MessageSpan { get; set; }
        public string EntityName { get; set; }
        public Dictionary<string, object> Metadata { get; set; } = new();

        public TracedMessage(T data, Activity messageSpan, string entityName)
        {
            Data = data;
            MessageSpan = messageSpan;
            EntityName = entityName;
        }
    }

    // Enhanced pipeline context to manage spans
    public class PipelineTraceContext
    {
        public Activity PipelineSpan { get; set; }
        public Activity WorkflowSpan { get; set; }
        public ConcurrentDictionary<string, Activity> BlockSpans { get; set; } = new();
        public string BusinessUseCase { get; set; }

        public PipelineTraceContext(string businessUseCase)
        {
            BusinessUseCase = businessUseCase;
        }

        // Method to get active block span by name
        public Activity GetBlockSpan(string blockName)
        {
            return BlockSpans.TryGetValue(blockName, out var span) ? span : null;
        }
    }

    // Main tracing orchestrator
    public class DataflowTracer
    {
        private static readonly ActivitySource ActivitySource = new("TplDataflow.Pipeline");
        private readonly PipelineTraceContext _context;

        public DataflowTracer(string businessUseCase)
        {
            _context = new PipelineTraceContext(businessUseCase);
        }

        // 1) Create pipeline and workflow spans
        public void StartPipeline()
        {
            // Create or get existing trace
            if (Activity.Current == null)
            {
                _context.PipelineSpan = ActivitySource.StartActivity("pipeline");
            }
            else
            {
                _context.PipelineSpan = Activity.Current;
            }

            // Create workflow span as child of pipeline
            _context.WorkflowSpan = ActivitySource.StartActivity("pipeline.workflow", ActivityKind.Internal, _context.PipelineSpan?.Context ?? default);
        }

        // 2) Create block spans within workflow
        public Activity CreateBlockSpan(string blockName)
        {
            var spanName = $"pipeline.workflow.{blockName}.{_context.BusinessUseCase}";
            var blockSpan = ActivitySource.StartActivity(spanName, ActivityKind.Internal, _context.WorkflowSpan?.Context ?? default);
            blockSpan?.Start(); // Explicitly start the span

            _context.BlockSpans[blockName] = blockSpan;
            return blockSpan;
        }

        // 3) Create message span
        public Activity CreateMessageSpan(string entityName)
        {
            var spanName = $"pipeline.{entityName}";
            var messageSpan = ActivitySource.StartActivity(spanName, ActivityKind.Internal, _context.PipelineSpan?.Context ?? default);
            return messageSpan;
        }

        // Create block operation spans (entry, execution, exit)
        public Activity CreateBlockOperationSpan(string entityName, string blockName,
            string operation, Activity parentSpan, int? splitNumber = null, bool isAggregated = false)
        {
            var spanName = $"pipeline.{entityName}.{blockName}.{operation}";

            if (isAggregated)
            {
                spanName += ".aggregated";
            }

            if (splitNumber.HasValue)
            {
                spanName += $".{splitNumber}";
            }

            var operationSpan = ActivitySource.StartActivity(spanName, ActivityKind.Internal, parentSpan?.Context ?? default);
            return operationSpan;
        }

        public void FinishPipeline()
        {
            // Stop all block spans first
            foreach (var blockSpan in _context.BlockSpans.Values)
            {
                blockSpan?.Stop();
            }

            _context.WorkflowSpan?.Stop();
            _context.PipelineSpan?.Stop();
        }
    }

    // Enhanced blocks with tracing capabilities
    public static class TracedDataflowBlocks
    {
        // Action Block with tracing
        public static ActionBlock<TracedMessage<T>> CreateTracedActionBlock<T>(
            Func<T, Task> action,
            string blockName,
            DataflowTracer tracer,
            ExecutionDataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            return new ActionBlock<TracedMessage<T>>(async tracedMessage =>
            {
                Activity entrySpan = null;
                Activity executionSpan = null;
                Activity exitSpan = null;

                try
                {
                    // Set current activity to block span to ensure proper nesting
                    using var blockScope = blockSpan;

                    // Entry span
                    entrySpan = tracer.CreateBlockOperationSpan(
                        tracedMessage.EntityName, blockName, "entry", tracedMessage.MessageSpan);
                    entrySpan?.Start();

                    // Execution span
                    executionSpan = tracer.CreateBlockOperationSpan(
                        tracedMessage.EntityName, blockName, "execution", tracedMessage.MessageSpan);
                    executionSpan?.Start();

                    await action(tracedMessage.Data);

                    executionSpan?.Stop();

                    // Exit span (if not greedy downstream)
                    if (!IsGreedyDownstream(options))
                    {
                        exitSpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "exit", tracedMessage.MessageSpan);
                        exitSpan?.Start();
                        exitSpan?.Stop();
                    }
                    else
                    {
                        // Mock span with 0 time for greedy scenario
                        exitSpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "exit", tracedMessage.MessageSpan);
                        exitSpan?.Start();
                        exitSpan?.Stop();
                        exitSpan?.SetTag("mock", "greedy_downstream");
                    }
                }
                finally
                {
                    entrySpan?.Stop();
                    executionSpan?.Stop();
                }
            }, options ?? new ExecutionDataflowBlockOptions());
        }

        // Transform Block with tracing
        public static TransformBlock<TracedMessage<TInput>, TracedMessage<TOutput>>
            CreateTracedTransformBlock<TInput, TOutput>(
                Func<TInput, TOutput> transform,
                string blockName,
                DataflowTracer tracer,
                ExecutionDataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            return new TransformBlock<TracedMessage<TInput>, TracedMessage<TOutput>>(
                tracedMessage =>
                {
                    Activity entrySpan = null;
                    Activity executionSpan = null;
                    Activity exitSpan = null;

                    try
                    {
                        // Entry span
                        entrySpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "entry", tracedMessage.MessageSpan);
                        entrySpan?.Start();

                        // Execution span
                        executionSpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "execution", tracedMessage.MessageSpan);
                        executionSpan?.Start();

                        var result = transform(tracedMessage.Data);

                        executionSpan?.Stop();

                        // Exit span
                        exitSpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "exit", tracedMessage.MessageSpan);
                        exitSpan?.Start();
                        exitSpan?.Stop();

                        return new TracedMessage<TOutput>(result, tracedMessage.MessageSpan, tracedMessage.EntityName);
                    }
                    finally
                    {
                        entrySpan?.Stop();
                        executionSpan?.Stop();
                    }
                }, options ?? new ExecutionDataflowBlockOptions());
        }

        // 5) Transform Many Block - splits one message into many
        public static TransformManyBlock<TracedMessage<TInput>, TracedMessage<TOutput>>
            CreateTracedTransformManyBlock<TInput, TOutput>(
                Func<TInput, IEnumerable<TOutput>> transform,
                string blockName,
                DataflowTracer tracer,
                ExecutionDataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            return new TransformManyBlock<TracedMessage<TInput>, TracedMessage<TOutput>>(
                tracedMessage =>
                {
                    var results = new List<TracedMessage<TOutput>>();
                    Activity entrySpan = null;
                    Activity executionSpan = null;

                    try
                    {
                        // Entry span
                        entrySpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "entry", tracedMessage.MessageSpan);
                        entrySpan?.Start();

                        // Execution span
                        executionSpan = tracer.CreateBlockOperationSpan(
                            tracedMessage.EntityName, blockName, "execution", tracedMessage.MessageSpan);
                        executionSpan?.Start();

                        var transformResults = transform(tracedMessage.Data);
                        var splitNumber = 1;

                        foreach (var item in transformResults)
                        {
                            // Create new message span for each split
                            var splitMessageSpan = tracer.CreateMessageSpan(tracedMessage.EntityName);

                            // Add link to parent message span using ActivityLinks
                            if (splitMessageSpan != null && tracedMessage.MessageSpan != null)
                            {
                                splitMessageSpan.AddTag("parent_message_id", tracedMessage.MessageSpan.Id);
                            }

                            // Exit span with split number
                            var exitSpan = tracer.CreateBlockOperationSpan(
                                tracedMessage.EntityName, blockName, "exit", splitMessageSpan, splitNumber);
                            exitSpan?.Start();
                            exitSpan?.Stop();

                            results.Add(new TracedMessage<TOutput>(item, splitMessageSpan, tracedMessage.EntityName));
                            splitNumber++;
                        }

                        executionSpan?.Stop();
                        return results;
                    }
                    finally
                    {
                        entrySpan?.Stop();
                        executionSpan?.Stop();
                    }
                }, options ?? new ExecutionDataflowBlockOptions());
        }

        // 4) Batch Block - aggregates multiple messages into one
        public static BatchBlock<TracedMessage<T>> CreateTracedBatchBlock<T>(
            int batchSize,
            string blockName,
            DataflowTracer tracer,
            GroupingDataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            return new BatchBlock<TracedMessage<T>>(batchSize, options ?? new GroupingDataflowBlockOptions());
        }

        // Helper method to handle batch aggregation spans
        public static TracedMessage<T[]> CreateAggregatedMessage<T>(
            TracedMessage<T>[] batchedMessages,
            string blockName,
            DataflowTracer tracer)
        {
            if (batchedMessages == null || batchedMessages.Length == 0)
                return null;

            var firstMessage = batchedMessages[0];

            // TODO: AGGREGATION DESIGN NEEDS REFINEMENT
            // Current approach creates a single aggregated span, but we might need:
            // 1. Individual entry spans for each message in the batch
            // 2. A single execution span for the batch operation
            // 3. Proper linking of parent message spans to the aggregated result
            // 4. Consider using ActivityLinks for better traceability

            // Create aggregated entry span
            var aggregatedEntrySpan = tracer.CreateBlockOperationSpan(
                firstMessage.EntityName, blockName, "entry", firstMessage.MessageSpan, isAggregated: true);

            // Link to all parent message spans using tags
            if (aggregatedEntrySpan != null)
            {
                for (int i = 0; i < batchedMessages.Length; i++)
                {
                    aggregatedEntrySpan.AddTag($"parent_message_{i}",
                        batchedMessages[i].MessageSpan?.Id ?? "unknown");
                }
            }

            aggregatedEntrySpan?.Start();
            aggregatedEntrySpan?.Stop();

            // Create new message span for aggregated result
            var aggregatedMessageSpan = tracer.CreateMessageSpan(firstMessage.EntityName);

            var aggregatedData = batchedMessages.Select(m => m.Data).ToArray();
            return new TracedMessage<T[]>(aggregatedData, aggregatedMessageSpan, firstMessage.EntityName);
        }

        // Buffer Block - typically no execution, just storage
        public static BufferBlock<TracedMessage<T>> CreateTracedBufferBlock<T>(
            string blockName,
            DataflowTracer tracer,
            DataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            // Note: BufferBlock typically doesn't have execution logic,
            // so we create mock spans for consistency
            var bufferBlock = new BufferBlock<TracedMessage<T>>(options ?? new DataflowBlockOptions());

            // TODO: Consider adding instrumentation for buffer operations if needed
            return bufferBlock;
        }

        // Join Block - joins multiple message types
        public static JoinBlock<TracedMessage<T1>, TracedMessage<T2>> CreateTracedJoinBlock<T1, T2>(
            string blockName,
            DataflowTracer tracer,
            GroupingDataflowBlockOptions options = null)
        {
            var blockSpan = tracer.CreateBlockSpan(blockName);

            // TODO: AGGREGATION DESIGN FOR JOIN BLOCKS
            // Similar to batch blocks, join blocks combine multiple messages
            // We need to handle span relationships properly:
            // 1. Track spans from both input branches
            // 2. Create aggregated spans that link to both inputs
            // 3. Consider using ActivityLinks for proper correlation

            return new JoinBlock<TracedMessage<T1>, TracedMessage<T2>>(
                options ?? new GroupingDataflowBlockOptions());
        }

        // Helper method to check if downstream blocks are greedy
        private static bool IsGreedyDownstream(ExecutionDataflowBlockOptions options)
        {
            // This is a simplification - in reality you'd need to check
            // the actual downstream block configurations
            return false;
        }
    }

    // Usage example with proper linking and batch handling
    public class OrderProcessingPipeline
    {
        private readonly DataflowTracer _tracer;

        public OrderProcessingPipeline()
        {
            _tracer = new DataflowTracer("orderprocessing");
        }

        public async Task ProcessOrdersAsync()
        {
            _tracer.StartPipeline();

            try
            {
                // Create pipeline blocks
                var validateBlock = TracedDataflowBlocks.CreateTracedActionBlock<Order>(
                    ValidateOrder, "validate", _tracer);

                var transformBlock = TracedDataflowBlocks.CreateTracedTransformBlock<Order, ProcessedOrder>(
                    TransformOrder, "transform", _tracer);

                var batchBlock = TracedDataflowBlocks.CreateTracedBatchBlock<ProcessedOrder>(
                    5, "batch", _tracer);

                var processBatchBlock = TracedDataflowBlocks.CreateTracedActionBlock<ProcessedOrder[]>(
                    async batchData => await ProcessBatch(batchData), "processBatch", _tracer);

                // Link blocks (ActionBlock doesn't have LinkTo, but TransformBlock does)
                transformBlock.LinkTo(batchBlock);

                // Handle batch output manually since BatchBlock outputs arrays
                _ = Task.Run(async () =>
                {
                    while (await batchBlock.OutputAvailableAsync())
                    {
                        if (batchBlock.TryReceive(out var batch))
                        {
                            // Create aggregated message for the batch
                            var aggregatedMessage = TracedDataflowBlocks.CreateAggregatedMessage(
                                batch, "batch", _tracer);

                            if (aggregatedMessage != null)
                            {
                                // Create a new TracedMessage for the batch processing block
                                var batchTracedMessage = new TracedMessage<ProcessedOrder[]>(
                                    aggregatedMessage.Data,
                                    aggregatedMessage.MessageSpan,
                                    aggregatedMessage.EntityName);

                                await processBatchBlock.SendAsync(batchTracedMessage);
                            }
                        }
                    }
                    processBatchBlock.Complete();
                });

                // Post initial messages to transform block (since ActionBlock can't be linked to)
                for (int i = 0; i < 10; i++)
                {
                    var messageSpan = _tracer.CreateMessageSpan("order");
                    var tracedOrder = new TracedMessage<Order>(
                        new Order { Id = i, Amount = 100 * i },
                        messageSpan,
                        "order");

                    // Validate first, then send to transform
                    await ValidateOrder(tracedOrder.Data);
                    await transformBlock.SendAsync(tracedOrder);
                }

                transformBlock.Complete();
                await transformBlock.Completion;

                batchBlock.Complete();
                await batchBlock.Completion;

                await processBatchBlock.Completion;
            }
            finally
            {
                _tracer.FinishPipeline();
            }
        }

        private async Task ValidateOrder(Order order)
        {
            await Task.Delay(10); // Simulate work
        }

        private ProcessedOrder TransformOrder(Order order)
        {
            return new ProcessedOrder { Id = order.Id, ProcessedAmount = order.Amount * 1.1m };
        }

        private async Task ProcessBatch(ProcessedOrder[] batchData)
        {
            await Task.Delay(50); // Simulate batch processing
            // Process the batch of orders
            foreach (var order in batchData)
            {
                // Process individual order in batch
                Console.WriteLine($"Processing order {order.Id} with amount {order.ProcessedAmount}");
            }
        }
    }

    // Sample data classes
    public class Order
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    public class ProcessedOrder
    {
        public int Id { get; set; }
        public decimal ProcessedAmount { get; set; }
    }
}