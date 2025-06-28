namespace Signals.Traces;
public record Span(string Id, string ParentId, string Name, DateTime StartTime);