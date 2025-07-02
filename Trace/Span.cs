using Signals.Common;

namespace Signals.Trace;

public class Span
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string? TraceId { get; set; } = null;

    public string SpanId { get; set; } = default!;
    public string? ParentSpanId { get; set; } = null;

    public string? Kind { get; set; } = string.Empty;

    public DateTime StartTime { get; set; } = DateTime.UtcNow;
    public DateTime EndTime { get; set; } = DateTime.UtcNow;
    public TimeSpan Duration => EndTime - StartTime;

    public ICollection<SpanAttribute> Attributes { get; set; } = [];

    public static async Task<Span> FromProto(OpenTelemetry.Proto.Trace.V1.Span protoSpan, SignalsDbContext db)
    {
        var attributeTasks = protoSpan.Attributes.Select(a => Common.Attribute.FromProtoAsync(a, db));
        var attributes = await Task.WhenAll(attributeTasks);

        var epoch = DateTimeOffset.FromUnixTimeSeconds(0);
        long startTicks = (long)protoSpan.StartTimeUnixNano / 100;
        long endTicks = (long)protoSpan.EndTimeUnixNano / 100;

        return new Span
        {
            TraceId = protoSpan.TraceId.ToBase64(),
            ParentSpanId = protoSpan.ParentSpanId.ToBase64(),
            SpanId = protoSpan.SpanId.ToBase64(),
            Name = protoSpan.Name,
            Kind = protoSpan.Kind.ToString(),
            StartTime = epoch.AddTicks(startTicks).UtcDateTime,
            EndTime = epoch.AddTicks(endTicks).UtcDateTime,
            Attributes = [.. attributes.Select(SpanAttribute.FromAttribute)]
        };
    }
}