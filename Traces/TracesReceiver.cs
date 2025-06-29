using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Traces;
public class TracesReceiver(TracesDbContext db) : TraceService.TraceServiceBase
{
    private readonly TracesDbContext _db = db;

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        var spanMap = new Dictionary<string, Span>();

        foreach (var resourceSpan in request.ResourceSpans)
        {
            foreach (var scopeSpan in resourceSpan.ScopeSpans)
            {
                foreach (var span in scopeSpan.Spans)
                {
                    var traceId = Convert.ToBase64String(span.TraceId.ToByteArray());
                    var spanId = Convert.ToBase64String(span.SpanId.ToByteArray());
                    var parentSpanId = span.ParentSpanId.Length > 0
                        ? Convert.ToBase64String(span.ParentSpanId.ToByteArray())
                        : null;

                    var epoch = DateTimeOffset.FromUnixTimeSeconds(0);
                    var start = epoch.AddTicks((long)span.StartTimeUnixNano / 100).UtcDateTime;
                    var end = epoch.AddTicks((long)span.EndTimeUnixNano / 100).UtcDateTime;

                    var scope = scopeSpan.Scope.Name;

                    var newSpan = new Span(traceId, spanId, parentSpanId, span.Name, scope, span.Kind.ToString(), start, end);
                    spanMap[spanId] = newSpan;
                }
            }
        }

        foreach (var span in spanMap.Values)
        {
            if (span.ParentSpanId != null && spanMap.TryGetValue(span.ParentSpanId, out var parent))
            {
                span.Parent = parent;
                parent.Children.Add(span);
            }
        }

        foreach (var span in spanMap.Values)
        {
            if (span.ParentSpanId == null)
                _db.Traces.Add(span);
        }

        try
        {
            await _db.SaveChangesAsync();
        }
        catch (DbUpdateException ex)
        {
            Console.WriteLine("Database update exception:");
            Console.WriteLine(ex.Message);
            if (ex.InnerException != null)
                Console.WriteLine(ex.InnerException.Message);
        }


        return new ExportTraceServiceResponse();
    }
}
