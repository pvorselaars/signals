using OpenTelemetry.Proto.Resource.V1;

namespace Signals;
public class Store : IAsyncDisposable
{
    private readonly ILogger<Store> _logger;
    private readonly Database _db;
    private Timer? _timer;

    private CancellationTokenSource _cts = new();

    public Store(Database database, ILogger<Store> logger)
    {
        _db = database;
        _logger = logger;
        _timer = new(RefreshAsync, null, 0, 5000);

    }
    public Dictionary<long, Resource> Resources { get; set; } = [];

    public event Func<Task>? OnChange;

    public async void RefreshAsync(object? state)
    {
        if (_cts.Token.IsCancellationRequested) return;

        try
        {
            Resources = await _db.GetResourcesAsync();

            if (OnChange != null)
                await OnChange.Invoke();

        }
        catch (OperationCanceledException)
        {

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during refresh");

        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _timer?.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

}
