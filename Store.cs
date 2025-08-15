using System.Runtime.CompilerServices;
using System.Threading.Tasks;
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
    public Dictionary<long, Resource> Resources { get; private set; } = [];
    public Dictionary<long, InstrumentationScope> Scopes { get; private set; } = [];
    public IEnumerable<Span> Spans { get; private set; } = [];

    public List<long> SelectedResources { get; private set; } = [];
    public async Task SelectResource(long resourceId)
    {
        SelectedResources.Add(resourceId);
        SelectedScopes = [];
        await GetAsync();
    }

    public async Task DeselectResource(long resourceId)
    {
        SelectedResources.Remove(resourceId);
        SelectedScopes = [];
        await GetAsync();
    }

    public List<long> SelectedScopes { get; private set; } = [];

    public async Task SelectScope(long scopeId)
    {
        SelectedScopes.Add(scopeId);
        await GetAsync();
    }

    public async Task DeselectScope(long scopeId)
    {
        SelectedScopes.Remove(scopeId);
        await GetAsync();
    }

    public event Func<Task>? OnChange;

    public async void RefreshAsync(object? state)
    {
        if (_cts.Token.IsCancellationRequested) return;

        try
        {
            await GetAsync();

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

    private async Task GetAsync()
    {
        Resources = await _db.GetResourcesAsync();
        Scopes = SelectedResources.Count > 0 ? await _db.GetScopesAsync(SelectedResources) : await _db.GetScopesAsync();
        Spans = SelectedScopes.Count > 0 ? await _db.GetSpansAsync(SelectedScopes) : await _db.GetSpansAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _timer?.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

}
