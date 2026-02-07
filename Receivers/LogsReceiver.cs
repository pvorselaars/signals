using Grpc.Core;
using OpenTelemetry.Proto.Collector.Logs.V1;

namespace Signals.Receivers;

public class LogsReceiver(Database db) : LogsService.LogsServiceBase
{

    public override async Task<ExportLogsServiceResponse> Export(
        ExportLogsServiceRequest request,
        ServerCallContext context)
    {
        db.InsertLogs(request.ResourceLogs[0]);
        return new ExportLogsServiceResponse();
    }

}
