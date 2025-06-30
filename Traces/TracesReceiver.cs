using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Traces;
public class TracesReceiver(SignalsDbContext db) : TraceService.TraceServiceBase
{
    private readonly SignalsDbContext _db = db;

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        var spanMap = new Dictionary<string, Span>();

        foreach (var resourceSpan in request.ResourceSpans)
        {
            foreach (var scopeSpan in resourceSpan.ScopeSpans)
            {
                var scope = _db.Scopes
                    .FirstOrDefault(s => s.Name == scopeSpan.Scope.Name && s.Version == scopeSpan.Scope.Version);

                if (scope == null)
                {
                    scope = new Scope(scopeSpan.Scope.Name, scopeSpan.Scope.Version, scopeSpan.SchemaUrl);
                    _db.Scopes.Add(scope);
                }

                foreach (var span in scopeSpan.Spans)
                {
                    spanMap[span.SpanId.ToBase64()] = Span.FromProto(span, scope);
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
