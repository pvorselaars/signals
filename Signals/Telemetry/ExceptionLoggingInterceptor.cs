using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Signals.Telemetry;

public class ExceptionLoggingInterceptor(ILogger<ExceptionLoggingInterceptor> logger) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception while processing {Method}", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "An internal error occurred while processing the request."));
        }
    }
}
