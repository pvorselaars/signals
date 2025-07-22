using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Signals;
public class Store : IAsyncDisposable
{
    private readonly ILogger<Store> _logger;
    private readonly Database _db;
    private readonly Timer? _timer;

    private readonly CancellationTokenSource _cts = new();

    public Store(Database database, ILogger<Store> logger)
    {
        _db = database;
        _logger = logger;
        _timer = new(RefreshAsync, null, 0, 5000);

    }
    public IEnumerable<Resource> Resources { get; private set; } = [];
    public IEnumerable<InstrumentationScope> Scopes { get; private set; } = [];
    public IEnumerable<Span> Spans { get; private set; } = [];

    public event Func<Task>? OnChange;

    public async void RefreshAsync(object? state)
    {
        if (_cts.Token.IsCancellationRequested) return;

        try
        {
            Resources = await _db.GetResourcesAsync();
            Scopes = await _db.GetScopesAsync();
            Spans = await _db.GetSpansAsync();

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
