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

        await _db.AddResourceSpansAsync([.. request.ResourceSpans]);

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
