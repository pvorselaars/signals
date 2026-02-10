using Google.Protobuf;
using LiteDB;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace OpenTelemetry.Proto.Trace.V1
{
    public partial class Span
    {
        public string ServiceName { get; set; } = "";
        public string ScopeName { get; set; } = "";
        public string GetFormattedStartTime()
        {
            var dateTime = DateTimeOffset.FromUnixTimeSeconds((long)(StartTimeUnixNano / 1_000_000_000));
            return dateTime.ToString("o");
        }

        public string GetFormattedEndTime()
        {
            var dateTime = DateTimeOffset.FromUnixTimeSeconds((long)(EndTimeUnixNano / 1_000_000_000));
            return dateTime.ToString("o");
        }

        public string GetFormattedDuration()
        {
            var nanoseconds = EndTimeUnixNano - StartTimeUnixNano;
            var duration = TimeSpan.FromTicks((long)nanoseconds / 100); // Convert nanoseconds to ticks (1 tick = 100 ns)
            return duration.ToString(@"hh\:mm\:ss\.fffffff");
        }
    }
}

namespace Signals.Telemetry
{
    public sealed partial class Repository : IDisposable
    {

        private ILiteCollection<Span> Spans => _db.GetCollection<Span>("spans");

        public void InsertTraces(IEnumerable<ResourceSpans> resourceSpans) => Spans.InsertBulk(FlattenSpans(resourceSpans));

        private static IEnumerable<Span> FlattenSpans(IEnumerable<ResourceSpans> resourceSpans)
        {
            foreach (var resourceSpan in resourceSpans)
            {
                var resourceAttributes = resourceSpan.Resource?.Attributes ?? Enumerable.Empty<KeyValue>();

                foreach (var scopeSpan in resourceSpan.ScopeSpans)
                {
                    var scopeAttributes = scopeSpan.Scope?.Attributes ?? [];
                    scopeAttributes.Add(new KeyValue { Key = "scope.name", Value = new AnyValue { StringValue = scopeSpan.Scope?.Name ?? "unknown" } });
                    scopeAttributes.Add(new KeyValue { Key = "scope.version", Value = new AnyValue { StringValue = scopeSpan.Scope?.Version ?? string.Empty } });

                    foreach (var span in scopeSpan.Spans)
                    {
                        span.Attributes.AddRange(resourceAttributes);
                        span.Attributes.AddRange(scopeAttributes);
                        yield return span;
                    }
                }
            }
        }

        public List<Span> QuerySpans(Query query)
        {
            var conditions = new List<BsonExpression>();

            // Time range
            if (query.StartTime.HasValue)
                conditions.Add(BsonExpression.Create($"StartTimeUnixNano >= {query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L}"));

            if (query.EndTime.HasValue)
                conditions.Add(BsonExpression.Create($"EndTimeUnixNano <= {query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L}"));

            if (query.ParentSpanId != null) {
                conditions.Add(BsonExpression.Create($"ParentSpanId = @0", query.ParentSpanId.ToByteArray()));
            } else if (query.ParentSpanId == ByteString.Empty) {
                conditions.Add(BsonExpression.Create($"ParentSpanId = null"));
            }

            if (!string.IsNullOrEmpty(query.SpanName))
                conditions.Add(BsonExpression.Create($"Name LIKE \"%{query.SpanName}%\""));

            return conditions.Count > 0
                ? [.. Spans.Find(LiteDB.Query.And([.. conditions]))]
                : [.. Spans.FindAll()];
        }


        public List<string> GetUniqueTraceScopes()
        {

            return [];
        }

    }

}
