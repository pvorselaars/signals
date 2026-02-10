using Google.Protobuf;
using LiteDB;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using System.Collections.Generic;
using System.Linq;

namespace OpenTelemetry.Proto.Logs.V1
{
    public partial class LogRecord
    {
        public string GetFormattedBody()
        {
            var formattedBody = Body.StringValue ?? string.Empty;

            foreach (var attribute in Attributes)
            {
                formattedBody = formattedBody.Replace("{" + attribute.Key + "}", GetAnyValueString(attribute.Value));
            }

            return formattedBody;
        }

        public string GetFormattedTime()
        {
            var dateTime = DateTimeOffset.FromUnixTimeSeconds((long)(TimeUnixNano / 1_000_000_000));
            return dateTime.ToString("o");
        }

        private static string GetAnyValueString(AnyValue value)
        {
            return value.ValueCase switch
            {
                AnyValue.ValueOneofCase.StringValue => value.StringValue,
                AnyValue.ValueOneofCase.BoolValue => value.BoolValue ? "true" : "false",
                AnyValue.ValueOneofCase.IntValue => value.IntValue.ToString(),
                AnyValue.ValueOneofCase.DoubleValue => value.DoubleValue.ToString(),
                AnyValue.ValueOneofCase.ArrayValue => string.Join(", ", value.ArrayValue.Values.Select(GetAnyValueString)),
                _ => value.ToString(),
            };
        }
    }
}

namespace Signals.Telemetry
{
    public sealed partial class Repository : IDisposable
    {
        private ILiteCollection<LogRecord> Logs => _db.GetCollection<LogRecord>("logs");

        public void InsertLogs(IEnumerable<ResourceLogs> logs) => Logs.InsertBulk(FlattenLogRecords(logs));

        private static IEnumerable<LogRecord> FlattenLogRecords(IEnumerable<ResourceLogs> logs)
        {
            foreach (var resourceLogs in logs)
            {
                var resourceAttributes = resourceLogs.Resource?.Attributes ?? Enumerable.Empty<KeyValue>();

                foreach (var scopeLogs in resourceLogs.ScopeLogs)
                {
                    var scopeAttributes = scopeLogs.Scope?.Attributes ?? [];
                    scopeAttributes.Add(new KeyValue { Key = "scope.name", Value = new AnyValue { StringValue = scopeLogs.Scope?.Name ?? "unknown" } });
                    scopeAttributes.Add(new KeyValue { Key = "scope.version", Value = new AnyValue { StringValue = scopeLogs.Scope?.Version ?? string.Empty } });

                    foreach (var logRecord in scopeLogs.LogRecords)
                    {
                        logRecord.Attributes.AddRange(resourceAttributes);
                        logRecord.Attributes.AddRange(scopeAttributes);
                        yield return logRecord; 
                    }
                }
            }
        }

        public List<LogRecord> QueryLogs(Query query)
        {
            var conditions = new List<BsonExpression>();

            // Time range
            if (query.StartTime.HasValue)
                conditions.Add(BsonExpression.Create($"TimeUnixNano >= {query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L}"));

            if (query.EndTime.HasValue)
                conditions.Add(BsonExpression.Create($"TimeUnixNano <= {query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L}"));

            // Text filter
            if (!string.IsNullOrEmpty(query.Text))
                conditions.Add(BsonExpression.Create($"Body.StringValue ANY LIKE '%{query.Text}%'"));

            var result = conditions.Count > 0
                ? Logs.Find(LiteDB.Query.And([.. conditions]))
                : Logs.Find(LiteDB.Query.All());

            return [.. result];
        }

        public Dictionary<string, long> GetLogCountByService(DateTimeOffset? from, DateTimeOffset? to)
        {
            return Logs.FindAll()
                .Where(log =>
                    (!from.HasValue || DateTimeOffset.FromUnixTimeSeconds((long)(log.TimeUnixNano / 1_000_000_000)) >= from.Value) &&
                    (!to.HasValue || DateTimeOffset.FromUnixTimeSeconds((long)(log.TimeUnixNano / 1_000_000_000)) <= to.Value))
                .GroupBy(log => log.Attributes.FirstOrDefault(attr => attr.Key == "service.name")?.Value.StringValue ?? "unknown")
                .ToDictionary(group => group.Key, group => (long)group.Count());
        }

        public long GetLogCountForTrace(ByteString traceId)
        {
            return Logs.FindAll()
                .LongCount(log => log.TraceId.Equals(traceId));
        }

        public long GetLogCountForSpan(ByteString spanId)
        {
            return Logs.FindAll()
                .LongCount(log => log.SpanId.Equals(spanId));
        }

        public IEnumerable<string> GetUniqueLogScopes()
        {
            return Logs.FindAll()
                .SelectMany(log => log.Attributes.Where(attr => attr.Key == "scope.name").Select(attr => attr.Value.StringValue))
                .Distinct();
        }

        public IEnumerable<string> GetUniqueServices()
        {
            return Logs.FindAll()
                .Select(log => log.Attributes.FirstOrDefault(attr => attr.Key == "service.name")!.Value.StringValue)
                .Distinct();
        }

    }

}
