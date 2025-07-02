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

        await AddResourceSpansAsync([.. request.ResourceSpans]);

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

    private async Task AddResourceSpansAsync(OpenTelemetry.Proto.Trace.V1.ResourceSpans[] protoResourceSpans)
    {

        var tasks = protoResourceSpans.Select(async span =>
        {
            var entity = await Resource.FromProto(span, _db);
            if (entity.Id == 0)
            {
                _db.Resources.Add(entity);
            }

            await AddScopeSpansAsync([.. span.ScopeSpans]);
        });

        await Task.WhenAll(tasks);
    }

    private async Task AddScopeSpansAsync(OpenTelemetry.Proto.Trace.V1.ScopeSpans[] protoScopeSpans)
    {
        var tasks = protoScopeSpans.Select(async span =>
        {
            var entity = await Scope.FromProto(span, _db);
            if (entity.Id == 0)
            {
                _db.Scopes.Add(entity);
            }

            await AddSpansAsync([.. span.Spans]);
        });

        await Task.WhenAll(tasks);
    }

    private async Task AddSpansAsync(OpenTelemetry.Proto.Trace.V1.Span[] protoSpans)
    {
        var tasks = protoSpans.Select(async span =>
        {
            var entity = await Span.FromProto(span, _db);
            _db.Spans.Add(entity);
        });

        await Task.WhenAll(tasks);
    }
}
