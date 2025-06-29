using System.ComponentModel.DataAnnotations;

namespace Signals.Traces;

public class Span(string traceId, string spanId, string? parentSpanId, string name, string scope, string kind, DateTime startTime, DateTime endTime)
{
    [Key]
    public string SpanId { get; set; } = spanId;

    public string? ParentSpanId { get; set; } = parentSpanId;

    public string TraceId { get; set; } = traceId;

    public string Name { get; set; } = name;
    public string Scope { get; set; } = scope;
    public DateTime StartTime { get; set; } = startTime;
    public DateTime EndTime { get; set; } = endTime;

    public Span? Parent { get; set; }
    public List<Span> Children { get; set; } = [];

    public string Kind { get; set; } = kind;
}
