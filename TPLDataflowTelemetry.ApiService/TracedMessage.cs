using OpenTelemetry;
using System.Diagnostics;

// -----------------------------
// Message envelope with roots & hop-links
// -----------------------------
public interface ITracedMessage
{
    Guid MessageId { get; }
    ActivityContext RootContext { get; }    // parent for all spans of THIS message
    ActivityContext LastHopContext { get; } // most recent hop (usually previous block.exit)
    Baggage Baggage { get; }
}

public sealed class TracedMessage<T> : ITracedMessage
{
    public Guid MessageId { get; } = Guid.NewGuid();
    public T Value { get; }
    public ActivityContext RootContext { get; }
    public ActivityContext LastHopContext { get; private set; }
    public Baggage Baggage { get; }

    public TracedMessage(T value, ActivityContext root, ActivityContext lastHop, Baggage baggage)
    {
        Value = value;
        RootContext = root;
        LastHopContext = lastHop;
        Baggage = baggage;
    }

    // Parent spans to the message root; optionally link to the last hop.
    public Activity? StartMessageSpan(ActivitySource source, string name, ActivityKind kind, bool linkToLastHop = true)
    {
        IEnumerable<ActivityLink>? links = null;
        if (linkToLastHop && LastHopContext != default && LastHopContext != RootContext)
            links = new[] { new ActivityLink(LastHopContext) };

        var act = source.StartActivity(name,
            kind: kind,
            parentContext: RootContext,
            tags: null,
            links: links,
            startTime: default);

        if (act is null) return null;
        if (Baggage.Current != Baggage) Baggage.Current = Baggage;
        act.SetTag("msg.id", MessageId.ToString());
        return act;
    }

    public void UpdateLastHop(Activity hopExit)
    {
        LastHopContext = hopExit?.Context ?? LastHopContext;
    }
}