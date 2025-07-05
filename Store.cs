using Microsoft.AspNetCore.Components.Server.Circuits;
using OpenTelemetry.Proto.Resource.V1;

namespace Signals;
public class Store : IAsyncDisposable
{
    private readonly ILogger<Store> _logger;
    private readonly Database _db;
    private PeriodicTimer? _timer;
    private CancellationTokenSource _cts = new();

    private Task? _pollingTask;

    public Store(Database database, ILogger<Store> logger)
    {
        _db = database;
        _logger = logger;

        StartPolling();
    }
    public Dictionary<long, Resource> Resources { get; set; } = [];

    public event Func<Task>? OnChange;

    private void StartPolling()
    {
        _pollingTask = Task.Run(async () =>
        {
            _timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
            try
            {
                while (await _timer.WaitForNextTickAsync(_cts.Token))
                {
                    _logger.LogInformation("Polling...");
                    Resources = await _db.GetResourcesAsync();
                    
                    if (OnChange != null)
                        await OnChange.Invoke();
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Polling cancelled.");
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _pollingTask!;
        _timer?.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

}
