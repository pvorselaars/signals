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

        foreach (var resourceSpan in request.ResourceSpans)
        {
            var span = await ResourceSpan.FromProto(resourceSpan, _db);
            _db.ResourceSpans.Add(span);
        }

        await _db.SaveChangesAsync();

        return new ExportTraceServiceResponse();
    }
}
