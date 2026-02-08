using Grpc.Core;
using OpenTelemetry.Proto.Collector.Metrics.V1;
using Signals.Repository;

namespace Signals.Receivers;

public class MetricsReceiver(Database db) : MetricsService.MetricsServiceBase
{

    public override async Task<ExportMetricsServiceResponse> Export(
        ExportMetricsServiceRequest request,
        ServerCallContext context)
    {
        db.InsertMetrics(request.ResourceMetrics[0]);
        return new ExportMetricsServiceResponse();
    }

}