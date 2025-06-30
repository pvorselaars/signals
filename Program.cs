using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Hosting.StaticWebAssets;
using Microsoft.EntityFrameworkCore;
using Signals.UI;
using Signals.Traces;
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

builder.Services.AddGrpc();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource.AddService("Signals"))
                .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation(options =>
                {
                    options.Filter = httpContext =>
                    {
                        var path = httpContext.Request.Path.ToString();
                        return !path.StartsWith("/opentelemetry") && !path.StartsWith("/_framework");
                    };
                }
                )
                .AddEntityFrameworkCoreInstrumentation()
                .AddOtlpExporter());

builder.Services.AddDbContext<SignalsDbContext>(options =>
    options.UseSqlite("Data Source=signals.db"));

StaticWebAssetsLoader.UseStaticWebAssets(builder.Environment, builder.Configuration);

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<SignalsDbContext>();
    db.Database.EnsureCreated();
}

app.UseHsts();
app.UseAntiforgery();
app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<TracesReceiver>(); 

app.Run();
