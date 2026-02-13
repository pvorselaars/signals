using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Signals.Common.Utilities;

namespace Signals.Telemetry.Charts
{
    public abstract class ChartBase : ComponentBase, IAsyncDisposable
    {
        [Inject]
        protected IJSRuntime JS { get; set; } = null!;

        [Parameter] 
        public DateTime StartTime { get; set; } = DateTime.Now.AddHours(-1);
        [Parameter] 
        public DateTime EndTime { get; set; } = DateTime.Now;

        protected ElementReference SvgRef;
        protected int CurrentWidth = 1000;
        private DotNetObjectReference<ChartBase>? _dotNetRef;

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender)
            {
                _dotNetRef = DotNetObjectReference.Create(this);
                await JS.InvokeVoidAsync("resizeInterop.observeResize", SvgRef, _dotNetRef);
                var w = await JS.InvokeAsync<double>("resizeInterop.getWidth", SvgRef);
                if (w > 0 && Math.Abs(w - CurrentWidth) > 1)
                {
                    CurrentWidth = (int)w;
                    StateHasChanged();
                }
            }
        }

        [JSInvokable]
        public Task NotifyWidthChanged(double width)
        {
            if (width > 0 && Math.Abs(width - CurrentWidth) > 1)
            {
                CurrentWidth = (int)width;
                InvokeAsync(StateHasChanged);
            }
            return Task.CompletedTask;
        }

        protected int MapUnixNanoToX(ulong unixNano)
        {
            ulong startNs = (ulong)(StartTime.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) * 100;
            ulong endNs = (ulong)(EndTime.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) * 100;

            var totalNs = endNs - startNs;
            if (totalNs <= 0) return 0;

            var posNs = unixNano - startNs;
            if (posNs <= 0) return 0;
            if (posNs >= totalNs) return CurrentWidth;

            var ratio = (decimal)posNs / totalNs;
            var x = ratio * CurrentWidth;
            return (int)Math.Round(x);
        }

        protected int MapDateTimeToX(DateTime time)
        {
            return MapUnixNanoToX(time.ToUnixTimeNanoseconds());
        }

        protected IEnumerable<DateTime> GetTicks()
        {
            var duration = EndTime - StartTime;

            var targetTicks = 10;
            var roughMs = duration.TotalMilliseconds / targetTicks;
            var interval = ChooseNiceInterval(TimeSpan.FromMilliseconds(roughMs));

            var first = AlignToInterval(StartTime.ToUniversalTime(), interval);
            for (var t = first; t <= EndTime; t = t.Add(interval))
                yield return t;
        }
        
        private static TimeSpan ChooseNiceInterval(TimeSpan rough)
        {
            var candidates = new[]
            {
                TimeSpan.FromMicroseconds(1),
                TimeSpan.FromMicroseconds(10),
                TimeSpan.FromMicroseconds(100),
                TimeSpan.FromMilliseconds(1),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(10),
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(5),
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(30),
                TimeSpan.FromHours(1),
                TimeSpan.FromHours(6),
                TimeSpan.FromDays(1)
            };
            return candidates.OrderBy(c => Math.Abs((c - rough).TotalMilliseconds)).First();
        }

        protected static string FormatTick(DateTimeOffset t)
        {
            // choose format by size
            if (t.Second == 0 && t.Minute == 0) return t.ToString("HH:mm");
            if (t.Second == 0) return t.ToString("HH:mm");
            return t.ToString("HH:mm:ss.fff");
        }

        private static DateTime AlignToInterval(DateTimeOffset t, TimeSpan interval)
        {
            if (interval.TotalDays >= 1)
                return new DateTime(t.Year, t.Month, t.Day, 0, 0, 0);
            
            var ticks = interval.Ticks;
            var alignedTicks = t.Ticks / ticks * ticks;
            return new DateTime(alignedTicks);
        }

        public async ValueTask DisposeAsync()
        {
            try {
                await JS.InvokeVoidAsync("resizeInterop.unobserveResize", SvgRef);
            } catch { 
                // Ignore
            }
            _dotNetRef?.Dispose();
        }
    }
}
