using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;
using Signals.Repository;

namespace Signals.Receivers;

public class TracesReceiver(Database db) : TraceService.TraceServiceBase
{

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        db.InsertTraces(request.ResourceSpans[0]);
        return new ExportTraceServiceResponse();
    }

}