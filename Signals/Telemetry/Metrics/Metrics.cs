using Google.Protobuf.Collections;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;


namespace OpenTelemetry.Proto.Metrics.V1
{

    public partial class Metric
    {
        public RepeatedField<KeyValue> Attributes { get; set; } = [];
    }
}

namespace Signals.Telemetry
{
    public sealed partial class Repository : IDisposable
    {

        public void InsertMetrics(ResourceMetrics resourceMetrics)
        {
        }

        private long GetOrCreateScope(InstrumentationScope scope)
        {
            return 0;
        }

        public List<Metric> QueryMetrics(Query query)
        {
            return [];
        }

        public List<string> GetUniqueMetricScopes()
        {

            return [];
        }

        public IEnumerable<Metric> GetMetricsForTrace(Span span)
        {
            return [];
        }

    }
}