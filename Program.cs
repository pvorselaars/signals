using Microsoft.AspNetCore.Hosting.StaticWebAssets;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Signals;
using Signals.UI;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Logs;
using System.Reflection;
using Signals.Receivers;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(443, listenOptions =>
    {
        listenOptions.UseHttps();
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
    });

    options.ListenAnyIP(4317, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});


builder.Services.AddSingleton<Database>();
builder.Services.AddScoped<Database.Query>();
builder.Services.AddGrpc();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

var serviceVersion = Assembly.GetEntryAssembly()?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";

builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource
                .AddService(serviceName: builder.Environment.ApplicationName, serviceVersion: serviceVersion, serviceInstanceId: Guid.Empty.ToString()))
                .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation(options =>
                        {
                            options.Filter = httpContext =>
                            {
                                var path = httpContext.Request.Path.ToString();
                                return !path.StartsWith("/opentelemetry");
                            };
                        })
                        .AddOtlpExporter())
                .WithMetrics(metrics => metrics.AddAspNetCoreInstrumentation()
                        .AddOtlpExporter())
                .WithLogging(logging => logging.AddOtlpExporter());

StaticWebAssetsLoader.UseStaticWebAssets(builder.Environment, builder.Configuration);

var app = builder.Build();

app.UseHsts();
app.UseAntiforgery();
app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<TracesReceiver>();
app.MapGrpcService<MetricsReceiver>();
app.MapGrpcService<LogsReceiver>();

app.Run();
