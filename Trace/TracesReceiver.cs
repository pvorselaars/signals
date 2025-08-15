using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Trace;

public class TracesReceiver(Database db) : TraceService.TraceServiceBase
{

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        await db.AddAsync(request.ResourceSpans);

        return new ExportTraceServiceResponse();
    }

}
