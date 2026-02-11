using Grpc.Core;
using OpenTelemetry.Proto.Collector.Metrics.V1;

namespace Signals.Telemetry.Metrics;

public class MetricsReceiver(Repository repository) : MetricsService.MetricsServiceBase
{

    public override async Task<ExportMetricsServiceResponse> Export(
        ExportMetricsServiceRequest request,
        ServerCallContext context)
    {
        repository.InsertMetrics(request.ResourceMetrics);
        return new ExportMetricsServiceResponse();
    }

}