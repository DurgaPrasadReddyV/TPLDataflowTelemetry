
using OpenTelemetry.Resources;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace TplDataflow.Otel
{
    public static class PipelineTracing
    {
        public const string ActivitySourceName = "tpl.dataflow.pipeline";
        public static readonly ActivitySource Source = new(ActivitySourceName);

        public readonly struct CurrentScope : IDisposable
        {
            private readonly Activity? _prev;
            public CurrentScope(Activity? parent)
            {
                _prev = Activity.Current;
                Activity.Current = parent;
            }
            public void Dispose() => Activity.Current = _prev;
        }

        public static CurrentScope Enter(Activity? parent) => new(parent);

        // ---------- Context objects ----------
        public sealed class PipelineContext : IDisposable
        {
            public Activity Pipeline { get; }
            public Activity Workflow { get; }
            public string BusinessCase { get; }
            public string PipelineName { get; }

            public PipelineContext(string pipelineName, string businessCase)
            {
                PipelineName = pipelineName;
                BusinessCase = businessCase;
                
                // ensure a trace exists
                var parent = Activity.Current?.Context ?? default;
                Pipeline = Source.StartActivity("pipeline", ActivityKind.Internal, parent) ?? new Activity("pipeline");
                Pipeline?.SetTag("dataflow.pipeline.name", pipelineName);

                Workflow = Source.StartActivity("pipeline.workflow", ActivityKind.Internal) ?? new Activity("pipeline.workflow");
                Workflow?.SetTag("dataflow.pipeline.name", pipelineName)
                        ?.SetTag("dataflow.workflow.case", businessCase);
            }

            public void Dispose()
            {
                Workflow?.Stop();
                Pipeline?.Stop();
            }
        }

        public sealed record TraceEnvelope<T>(
            T Value,
            string EntityName,
            string MessageId,
            Activity MessageActivity,
            int? SplitIndex = null)
        {
            public TraceEnvelope<TOut> To<TOut>(TOut newValue, int? splitIndex = null)
    => new TraceEnvelope<TOut>(
        newValue,
        EntityName,
        MessageId,
        MessageActivity,
        splitIndex);
            // Maps blockName -> (entry, execution, exit)
            public ConcurrentDictionary<string, (Activity? entry, Activity? exec, Activity? exit)> Spans { get; } = new();
        }

        // ---------- Creation helpers ----------
        public static PipelineContext StartPipeline(string pipelineName, string businessCase)
            => new(pipelineName, businessCase);

        public static TraceEnvelope<T> CreateMessage<T>(PipelineContext ctx, T value, string entityName, string messageId)
        {
            using var _ = Enter(ctx.Pipeline);           // 👈 ensure parent is Current
            var msgAct = Source.StartActivity($"pipeline.{entityName}", ActivityKind.Internal)!;
            msgAct.SetTag("dataflow.pipeline.name", ctx.PipelineName)
                  .SetTag("dataflow.workflow.case", ctx.BusinessCase)
                  .SetTag("dataflow.entity.name", entityName)
                  .SetTag("dataflow.message.id", messageId);
            return new TraceEnvelope<T>(value, entityName, messageId, msgAct);
        }

        public static TraceEnvelope<T> CloneForSplit<T>(TraceEnvelope<T> src, T newValue, int splitIndex)
        {
            // Same parent (message span), track index
            var split = new TraceEnvelope<T>(
                newValue, src.EntityName, src.MessageId, src.MessageActivity, splitIndex);
            return split;
        }

        // ---------- Message child spans (entry/execution/exit) ----------
        public static Activity StartEntry<T>(PipelineContext ctx, TraceEnvelope<T> env, string blockName)
            => StartChild(ctx, env, blockName, "entry");

        public static Activity StartExecution<T>(PipelineContext ctx, TraceEnvelope<T> env, string blockName)
            => StartChild(ctx, env, blockName, "execution");

        public static Activity StartExit<T>(PipelineContext ctx, TraceEnvelope<T> env, string blockName)
            => StartChild(ctx, env, blockName, "exit");

        static Activity StartChild<T>(PipelineContext ctx, TraceEnvelope<T> env, string blockName, string op)
        {
            using var _ = Enter(env.MessageActivity);    // 👈 ensure parent is Current
            var suffix = env.SplitIndex.HasValue ? $".{env.SplitIndex.Value}" : "";
            var name = $"pipeline.{env.EntityName}.{blockName}.{op}{suffix}";
            var act = Source.StartActivity(name, ActivityKind.Internal)!;

            act?.SetTag("dataflow.pipeline.name", ctx.PipelineName)
               ?.SetTag("dataflow.workflow.case", ctx.BusinessCase)
               ?.SetTag("dataflow.entity.name", env.EntityName)
               ?.SetTag("dataflow.message.id", env.MessageId)
               ?.SetTag("dataflow.block.name", blockName)
               ?.SetTag("dataflow.operation", op);

            if (env.SplitIndex.HasValue)
                act?.SetTag("dataflow.split.index", env.SplitIndex.Value);

            env.Spans.AddOrUpdate(
                blockName,
                _ => (op == "entry" ? act : null, op == "execution" ? act : null, op == "exit" ? act : null),
                (_, old) => (
                    op == "entry" ? act : old.entry,
                    op == "execution" ? act : old.exec,
                    op == "exit" ? act : old.exit
                )
            );

            return act;
        }

        public static void Stop(Activity? a, Exception? ex = null)
        {
            if (a is null) return;
            if (ex != null)
            {
                a.SetTag("exception.type", ex.GetType().FullName);
                a.SetTag("exception.message", ex.Message);
                a.SetTag("exception.stacktrace", ex.StackTrace);
            }
            a.Stop();
        }

        public static void MockZeroDuration(Activity parent, Action<Activity>? tagger = null, string reason = "")
        {
            // Create start/stop at practically the same instant
            var now = DateTimeOffset.UtcNow;
            var act = Source.StartActivity(parent.OperationName + ".mock", ActivityKind.Internal, parent.Context, startTime: now);
            if (act != null)
            {
                act.SetTag("dataflow.mock", true);
                if (!string.IsNullOrWhiteSpace(reason)) act.SetTag("dataflow.mock.reason", reason);
                tagger?.Invoke(act);
                act.Stop(); // zero/near-zero duration
            }
        }

        // ---------- Links for aggregation ----------
        public static Activity StartAggregatedChild<TOut>(
            PipelineContext ctx,
            string entityNameOrGroup,
            string blockName,
            IEnumerable<Activity> priorMessageActivities,
            string operation,
            int? batchSize = null)
        {
            var name = $"pipeline.{entityNameOrGroup}.{blockName}.{operation}.aggregated";
            var links = priorMessageActivities
                .Where(a => a != null)
                .Select(a => new ActivityLink(a.Context))
                .ToList();

            var act = Source.StartActivity(name, ActivityKind.Internal, parentContext: ctx.Workflow.Context, tags: null, links: links);
            act?.SetTag("dataflow.pipeline.name", ctx.PipelineName)
               ?.SetTag("dataflow.workflow.case", ctx.BusinessCase)
               ?.SetTag("dataflow.block.name", blockName)
               ?.SetTag("dataflow.operation", operation)
               ?.SetTag("dataflow.batch.size", batchSize);

            return act!;
        }

        // ---------- Topology span for a workflow block ----------
        public static Activity StartWorkflowBlockSpan(PipelineContext ctx, string blockName, string businessCase, string blockKind, bool? bounded = null, int? capacity = null, bool? greedy = null)
        {
            var name = $"pipeline.workflow.{blockName}.{businessCase}";
            var a = Source.StartActivity(name, ActivityKind.Internal, ctx.Workflow.Context);
            a?.SetTag("dataflow.pipeline.name", ctx.PipelineName)
             ?.SetTag("dataflow.workflow.case", businessCase)
             ?.SetTag("dataflow.block.name", blockName)
             ?.SetTag("dataflow.block.kind", blockKind);

            if (bounded.HasValue) a?.SetTag("dataflow.block.options.bounded", bounded.Value);
            if (capacity.HasValue) a?.SetTag("dataflow.block.options.capacity", capacity.Value);
            if (greedy.HasValue) a?.SetTag("dataflow.greedy", greedy.Value);
            return a!;
        }

        // Add alongside StartWorkflowBlockSpan:
        public static void RecordWorkflowBlockTopology(
            PipelineContext ctx, string blockName, string businessCase, string blockKind,
            bool? bounded = null, int? capacity = null, bool? greedy = null)
        {
            using var _ = Enter(ctx.Workflow);
            using var a = Source.StartActivity($"pipeline.workflow.{blockName}.{businessCase}", ActivityKind.Internal);
            a?.SetTag("dataflow.pipeline.name", ctx.PipelineName)
              ?.SetTag("dataflow.workflow.case", businessCase)
              ?.SetTag("dataflow.block.name", blockName)
              ?.SetTag("dataflow.block.kind", blockKind);
            if (bounded.HasValue) a?.SetTag("dataflow.block.options.bounded", bounded.Value);
            if (capacity.HasValue) a?.SetTag("dataflow.block.options.capacity", capacity.Value);
            if (greedy.HasValue) a?.SetTag("dataflow.greedy", greedy.Value);
            // span auto-stops at end of using → near-zero duration
        }

        public static Activity StartAggregateMessage(
        PipelineContext ctx,
        string resultingEntityName,
        IEnumerable<Activity> priorMessageActivities,
        int? batchOrJoinSize = null)
        {
            using var _ = Enter(ctx.Pipeline);           // 👈 ensure pipeline is Current
            var links = priorMessageActivities.Where(a => a != null).Select(a => new ActivityLink(a.Context)).ToList();
            var act = Source.StartActivity($"pipeline.{resultingEntityName}",
                                           kind: ActivityKind.Internal,
                                           parentContext: Activity.Current?.Context ?? default,
                                           tags: null,
                                           links: links)!; // overload uses Current for parent

            act.SetTag("dataflow.pipeline.name", ctx.PipelineName)
               .SetTag("dataflow.workflow.case", ctx.BusinessCase)
               .SetTag("dataflow.entity.name", resultingEntityName);

            if (batchOrJoinSize.HasValue)
                act.SetTag("dataflow.batch.size", batchOrJoinSize.Value);

            return act;
        }

        // ".aggregated" child spans for entry/execution/exit
        public static Activity StartAggregatedChild<T>(
            PipelineContext ctx,
            TraceEnvelope<T> env,
            string blockName,
            string operation)
        {
            using var _ = Enter(env.MessageActivity);    // 👈
            var suffix = env.SplitIndex.HasValue ? $".{env.SplitIndex.Value}" : "";
            var name = $"pipeline.{env.EntityName}.{blockName}.{operation}.aggregated{suffix}";
            var act = Source.StartActivity(name, ActivityKind.Internal)!;

            act.SetTag("dataflow.pipeline.name", ctx.PipelineName)
               .SetTag("dataflow.workflow.case", ctx.BusinessCase)
               .SetTag("dataflow.entity.name", env.EntityName)
               .SetTag("dataflow.message.id", env.MessageId)
               .SetTag("dataflow.block.name", blockName)
               .SetTag("dataflow.operation", operation);

            if (env.SplitIndex.HasValue)
                act.SetTag("dataflow.split.index", env.SplitIndex.Value);

            return act;
        }
    }

}
