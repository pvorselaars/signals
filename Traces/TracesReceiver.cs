using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Traces;
public class TracesReceiver(TracesDbContext db) : TraceService.TraceServiceBase
{
    private readonly TracesDbContext _db = db;

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        foreach (var resourceSpan in request.ResourceSpans)
        {
            foreach (var scopeSpan in resourceSpan.ScopeSpans)
            {
                foreach (var span in scopeSpan.Spans)
                {
                    var epoch = DateTimeOffset.FromUnixTimeSeconds(0);
                    long ticks = (long)span.StartTimeUnixNano / 100;
                    var trace = new Span(
                        Convert.ToBase64String(span.SpanId.ToByteArray()),
                        Convert.ToBase64String(span.ParentSpanId.ToByteArray()),
                        span.Name,
                        epoch.AddTicks(ticks).UtcDateTime);
                    _db.Traces.Add(trace);
                }
            }
        }

        await _db.SaveChangesAsync();

        return new ExportTraceServiceResponse();
    }
}
