using OpenTelemetry.Proto.Trace.V1;

namespace Signals.Repository;

public sealed partial class Database : IDisposable
{

    public void InsertTraces(ResourceSpans resourceSpans)
    {
        using var command = _connection.CreateCommand();

        foreach (var scopeSpan in resourceSpans.ScopeSpans)
        {
            var scopeName = scopeSpan.Scope?.Name ?? "unknown";
            var resourceId = GetOrCreateResource(resourceSpans.Resource);

            command.CommandText = @"
                INSERT INTO traces (
                    resource_id, scope_name, trace_id, span_id, parent_span_id,
                    start_time_unix_nano, end_time_unix_nano,
                    span_name, span_kind, status_code, status_message
                ) VALUES (
                    @resource_id, @scope_name, @trace_id, @span_id, @parent_span_id,
                    @start_time_unix_nano, @end_time_unix_nano,
                    @span_name, @span_kind, @status_code, @status_message
                )
            ";

            foreach (var span in scopeSpan.Spans.OrderBy(s => s.ParentSpanId.Length))
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@resource_id", resourceId);
                command.Parameters.AddWithValue("@scope_name", scopeName);
                command.Parameters.AddWithValue("@trace_id", Convert.ToHexString(span.TraceId.ToByteArray()));
                command.Parameters.AddWithValue("@span_id", Convert.ToHexString(span.SpanId.ToByteArray()));
                command.Parameters.AddWithValue("@parent_span_id",
                    span.ParentSpanId.Length > 0 ? Convert.ToHexString(span.ParentSpanId.ToByteArray()) : (object)DBNull.Value);
                command.Parameters.AddWithValue("@start_time_unix_nano", (long)span.StartTimeUnixNano);
                command.Parameters.AddWithValue("@end_time_unix_nano", (long)span.EndTimeUnixNano);
                command.Parameters.AddWithValue("@span_name", span.Name);
                command.Parameters.AddWithValue("@span_kind", (int)span.Kind);
                command.Parameters.AddWithValue("@status_code", span.Status?.Code != null ? (int)span.Status.Code : DBNull.Value);
                command.Parameters.AddWithValue("@status_message", span.Status?.Message ?? (object)DBNull.Value);

                command.ExecuteNonQuery();
            }
        }
    }

    public List<TraceRecord> QueryTraces(Query query)
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
            conditions.Add("t.scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Span name filter
        if (!string.IsNullOrEmpty(query.SpanName))
        {
            conditions.Add("t.span_name = @span_name");
            command.Parameters.AddWithValue("@span_name", query.SpanName);
        }

        // Parent filter
        if (!string.IsNullOrEmpty(query.ParentSpanId))
        {
            conditions.Add("t.parent_span_id = @parent_span_id");
            command.Parameters.AddWithValue("@parent_span_id", query.ParentSpanId);
        } else if (query.ParentSpanId == string.Empty) // Special case to filter root spans
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
                t.span_id, r.service_name, t.scope_name, t.trace_id, t.span_id, t.parent_span_id,
                t.start_time_unix_nano, t.end_time_unix_nano,
                t.span_name, t.span_kind, t.status_code, t.status_message
            FROM traces t
            JOIN resources r ON t.resource_id = r.id
            {whereClause}
            ORDER BY t.start_time_unix_nano DESC
            LIMIT @limit OFFSET @offset
        ";
        command.Parameters.AddWithValue("@limit", query.Limit);
        command.Parameters.AddWithValue("@offset", query.Offset);

        var results = new List<TraceRecord>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new TraceRecord
            {
                Id = reader.GetString(0), // span_id as string
                ServiceName = reader.GetString(1),
                ScopeName = reader.GetString(2),
                TraceId = reader.GetString(3),
                SpanId = reader.GetString(4),
                ParentSpanId = reader.IsDBNull(5) ? null : reader.GetString(5),
                StartTimeUnixNano = reader.GetInt64(6),
                EndTimeUnixNano = reader.GetInt64(7),
                SpanName = reader.GetString(8),
                SpanKind = reader.GetInt32(9),
                StatusCode = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                StatusMessage = reader.IsDBNull(11) ? null : reader.GetString(11)
            });
        }

        return results;
    }

    public List<TraceRecord> GetChildTraces(string parentSpanId)
    {
        var command = _connection.CreateCommand();
        command.CommandText = @"
            SELECT 
                t.span_id, r.service_name, t.scope_name, t.trace_id, t.span_id, t.parent_span_id,
                t.start_time_unix_nano, t.end_time_unix_nano,
                t.span_name, t.span_kind, t.status_code, t.status_message
            FROM traces t
            JOIN resources r ON t.resource_id = r.id
            WHERE t.parent_span_id = @parent_span_id
            ORDER BY t.start_time_unix_nano DESC
        ";
        command.Parameters.AddWithValue("@parent_span_id", parentSpanId);

        var results = new List<TraceRecord>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new TraceRecord
            {
                Id = reader.GetString(0), // span_id as string
                ServiceName = reader.GetString(1),
                ScopeName = reader.GetString(2),
                TraceId = reader.GetString(3),
                SpanId = reader.GetString(4),
                ParentSpanId = reader.IsDBNull(5) ? null : reader.GetString(5),
                StartTimeUnixNano = reader.GetInt64(6),
                EndTimeUnixNano = reader.GetInt64(7),
                SpanName = reader.GetString(8),
                SpanKind = reader.GetInt32(9),
                StatusCode = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                StatusMessage = reader.IsDBNull(11) ? null : reader.GetString(11)
            });
        }

        return results;
    }

    public List<string> GetUniqueTraceScopes()
    {
        var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT scope_name FROM traces ORDER BY scope_name";

        var scopes = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            scopes.Add(reader.GetString(0));
        }

        return scopes;
    }

}

public class TraceRecord
{
    public string Id { get; set; } = ""; // SpanId as string
    public string ServiceName { get; set; } = "";
    public string ScopeName { get; set; } = "";
    public string TraceId { get; set; } = "";
    public string SpanId { get; set; } = "";
    public string? ParentSpanId { get; set; }
    public long StartTimeUnixNano { get; set; }
    public long EndTimeUnixNano { get; set; }
    public string SpanName { get; set; } = "";
    public int SpanKind { get; set; }
    public int StatusCode { get; set; }
    public string? StatusMessage { get; set; }

    public DateTimeOffset GetStartTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(StartTimeUnixNano / 1_000_000).ToLocalTime();
    }

    public DateTimeOffset GetEndTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(EndTimeUnixNano / 1_000_000).ToLocalTime();
    }
}