using Google.Protobuf;
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

namespace Signals.Repository
{
    public sealed partial class Database : IDisposable
    {

        public void InsertTraces(ResourceSpans resourceSpans)
        {
            using var command = _connection.CreateCommand();

            foreach (var scopeSpan in resourceSpans.ScopeSpans)
            {
                var scopeId = GetOrCreateScope(scopeSpan.Scope);
                var resourceId = GetOrCreateResource(resourceSpans.Resource);

                command.CommandText = @"
                INSERT INTO spans (
                    resource_id, scope_id, trace_id, span_id, parent_span_id,
                    start_time_unix_nano, end_time_unix_nano,
                    name, json
                ) VALUES (
                    @resource_id, @scope_id, @trace_id, @span_id, @parent_span_id,
                    @start_time_unix_nano, @end_time_unix_nano,
                    @name, @json
                )
            ";

                foreach (var span in scopeSpan.Spans.OrderBy(s => s.ParentSpanId.Length))
                {
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@resource_id", resourceId);
                    command.Parameters.AddWithValue("@scope_id", scopeId);
                    command.Parameters.AddWithValue("@trace_id", span.TraceId.ToByteArray());
                    command.Parameters.AddWithValue("@span_id", span.SpanId.ToByteArray());
                    command.Parameters.AddWithValue("@parent_span_id", span.ParentSpanId.ToByteArray());
                    command.Parameters.AddWithValue("@start_time_unix_nano", (long)span.StartTimeUnixNano);
                    command.Parameters.AddWithValue("@end_time_unix_nano", (long)span.EndTimeUnixNano);
                    command.Parameters.AddWithValue("@name", span.Name);
                    command.Parameters.AddWithValue("@json", JsonFormatter.Default.Format(span));

                    command.ExecuteNonQuery();
                }
            }
        }

        public List<Span> QuerySpans(Query query)
        {
            var conditions = new List<string>();
            var command = _connection.CreateCommand();

            // Time range
            if (query.StartTime.HasValue)
            {
                conditions.Add("t.start_time_unix_nano >= @from");
                command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
            }
            if (query.EndTime.HasValue)
            {
                conditions.Add("t.start_time_unix_nano <= @to");
                command.Parameters.AddWithValue("@to", query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
            }

            // Service filter
            if (!string.IsNullOrEmpty(query.ServiceName))
            {
                conditions.Add("r.service_name = @service");
                command.Parameters.AddWithValue("@service", query.ServiceName);
            }

            // Scope filter
            if (!string.IsNullOrEmpty(query.ScopeName))
            {
                conditions.Add("s.scope_name = @scope");
                command.Parameters.AddWithValue("@scope", query.ScopeName);
            }

            // Span name filter
            if (!string.IsNullOrEmpty(query.SpanName))
            {
                conditions.Add("t.name = @span_name");
                command.Parameters.AddWithValue("@span_name", query.SpanName);
            }

            // Parent filter
            if (query.ParentSpanId != null)
            {
                conditions.Add("t.parent_span_id = @parent_span_id");
                command.Parameters.AddWithValue("@parent_span_id", query.ParentSpanId.ToByteArray());
            } else if (query.ParentSpanId == ByteString.Empty) // Special case to filter root spans
            {
                conditions.Add("t.parent_span_id IS NULL");
            }

            // Text filter
            if (!string.IsNullOrEmpty(query.Text))
            {
                conditions.Add("t.span_name LIKE @text");
                command.Parameters.AddWithValue("@text", $"%{query.Text}%");
            }

            var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

            command.CommandText = $@"
            SELECT 
                t.json
            FROM spans t
            JOIN resources r ON t.resource_id = r.id
            JOIN scopes s ON t.scope_id = s.id
            {whereClause}
            ORDER BY t.start_time_unix_nano DESC
            LIMIT @limit OFFSET @offset
        ";
            command.Parameters.AddWithValue("@limit", query.Limit);
            command.Parameters.AddWithValue("@offset", query.Offset);

            var results = new List<Span>();
            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                var span = JsonParser.Default.Parse<Span>(reader.GetString(0)); // Assuming the JSON is in the 9th column (index 8)
                results.Add(span);
            }

            return results;
        }


        public List<string> GetUniqueTraceScopes()
        {
            var command = _connection.CreateCommand();
            command.CommandText = @"
            SELECT DISTINCT s.scope_name
            FROM spans 
            JOIN scopes s ON spans.scope_id = s.id
            ORDER BY scope_name";

            var scopes = new List<string>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                scopes.Add(reader.GetString(0));
            }

            return scopes;
        }

    }

}
