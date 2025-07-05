using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Trace;

public class TracesReceiver(Database data) : TraceService.TraceServiceBase
{

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        await data.AddResourcesAsync(request.ResourceSpans.Select(s => s.Resource));

        return new ExportTraceServiceResponse();
    }

}
