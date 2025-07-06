using Grpc.Core;
using OpenTelemetry.Proto.Collector.Trace.V1;

namespace Signals.Trace;

public class TracesReceiver(Database data) : TraceService.TraceServiceBase
{

    public override async Task<ExportTraceServiceResponse> Export(
        ExportTraceServiceRequest request,
        ServerCallContext context)
    {
        await data.AddAsync(request.ResourceSpans.Select(s => s.Resource));
        await data.AddAsync(request.ResourceSpans.SelectMany(s => s.ScopeSpans).Select(s => s.Scope));
        await data.AddAsync(request.ResourceSpans.SelectMany(s => s.ScopeSpans).SelectMany(s =>  s.Spans));


        return new ExportTraceServiceResponse();
    }

}
