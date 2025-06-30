using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using static OpenTelemetry.Proto.Trace.V1.Span.Types;

namespace Signals.Traces;

public class Span(string traceId, string spanId, string? parentSpanId, string name, SpanKind kind, DateTime startTime, DateTime endTime)
{
    [Key]
    public string SpanId { get; set; } = spanId;
    public string? ParentSpanId { get; set; } = parentSpanId;
    public string TraceId { get; set; } = traceId;

    public string Name { get; set; } = name;
    public Scope? Scope { get; set; }

    [NotMapped]
    public TimeSpan Duration { get; set; } = endTime - startTime;
    public DateTime StartTime { get; set; } = startTime;
    public DateTime EndTime { get; set; } = endTime;


    public Span? Parent { get; set; }
    public List<Span> Children { get; set; } = [];

    public SpanKind Kind { get; set; } = kind;
}
