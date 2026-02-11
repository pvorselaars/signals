using Grpc.Core;
using OpenTelemetry.Proto.Collector.Logs.V1;

namespace Signals.Telemetry.Logs;

public class LogsReceiver(Repository repository) : LogsService.LogsServiceBase
{

    public override async Task<ExportLogsServiceResponse> Export(
        ExportLogsServiceRequest request,
        ServerCallContext context)
    {
        repository.InsertLogs(request.ResourceLogs);
        return new ExportLogsServiceResponse();
    }

}
