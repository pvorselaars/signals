using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Hosting.StaticWebAssets;
using Signals.UI;
using Signals.Trace;
using Signals;

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

builder.Services.AddScoped<Store>();
builder.Services.AddSingleton<Database>();
builder.Services.AddGrpc();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource.AddService(serviceName: "Signals", serviceVersion: "0.0.1", serviceInstanceId: Guid.Empty.ToString()))
                .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation(options =>
                {
                    options.Filter = httpContext =>
                    {
                        var path = httpContext.Request.Path.ToString();
                        return !path.StartsWith("/opentelemetry");
                    };
                }
                )
                .AddOtlpExporter());

StaticWebAssetsLoader.UseStaticWebAssets(builder.Environment, builder.Configuration);

var app = builder.Build();


using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<Database>();
    db.Create();
}


app.UseHsts();
app.UseAntiforgery();
app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<TracesReceiver>();

app.Run();
