using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Telemetry.Traces;

public class TracesReceiver(Repository repository) : TraceService.TraceServiceBase
{

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        repository.InsertTraces(request.ResourceSpans);
        return new ExportTraceServiceResponse();
    }

}