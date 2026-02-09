using OpenTelemetry.Proto.Logs.V1;

namespace Signals.Repository;

public sealed partial class Database : IDisposable
{

    public void InsertLogs(ResourceLogs resourceLogs)
    {
        using var command = _connection.CreateCommand();

        foreach (var scopeLog in resourceLogs.ScopeLogs)
        {
            var scopeName = scopeLog.Scope?.Name ?? "unknown";
            var resourceId = GetOrCreateResource(resourceLogs.Resource);

            command.CommandText = @"
                INSERT INTO logs (
                    resource_id, scope_name, time_unix_nano, observed_time_unix_nano,
                    severity_number, severity_text, body, trace_id, span_id
                ) VALUES (
                    @resource_id, @scope_name, @time_unix_nano, @observed_time_unix_nano,
                    @severity_number, @severity_text, @body, @trace_id, @span_id
                )
            ";

            foreach (var logRecord in scopeLog.LogRecords)
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@resource_id", resourceId);
                command.Parameters.AddWithValue("@scope_name", scopeName);
                command.Parameters.AddWithValue("@time_unix_nano", (long)logRecord.TimeUnixNano);
                command.Parameters.AddWithValue("@observed_time_unix_nano",
                    logRecord.ObservedTimeUnixNano > 0 ? (long)logRecord.ObservedTimeUnixNano : DBNull.Value);
                command.Parameters.AddWithValue("@severity_number", (int)logRecord.SeverityNumber);
                command.Parameters.AddWithValue("@severity_text", logRecord.SeverityText ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@body", GetLogBody(logRecord));
                command.Parameters.AddWithValue("@trace_id", Convert.ToHexString(logRecord.TraceId.ToByteArray()) ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@span_id", Convert.ToHexString(logRecord.SpanId.ToByteArray()) ?? (object)DBNull.Value);

                command.ExecuteNonQuery();
            }
        }
    }


    public List<LogRecord> QueryLogs(Query query)
    {
        var conditions = new List<string>();
        var command = _connection.CreateCommand();

        // Time range
        if (query.StartTime.HasValue)
        {
            conditions.Add("l.time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (query.EndTime.HasValue)
        {
            conditions.Add("l.time_unix_nano <= @to");
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
            conditions.Add("l.scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Severity filter
        if (query.MinSeverity.HasValue)
        {
            conditions.Add("l.severity_number >= @severity");
            command.Parameters.AddWithValue("@severity", query.MinSeverity.Value);
        }

        // Text filter
        if (!string.IsNullOrEmpty(query.Text))
        {
            conditions.Add("l.body LIKE @text");
            command.Parameters.AddWithValue("@text", $"%{query.Text}%");
        }

        var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT 
                l.id, r.service_name, l.scope_name, l.time_unix_nano, l.observed_time_unix_nano,
                l.severity_number, l.severity_text, l.body, r.attributes_json
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            {whereClause}
            ORDER BY l.time_unix_nano DESC
            LIMIT @limit OFFSET @offset
        ";
        command.Parameters.AddWithValue("@limit", query.Limit);
        command.Parameters.AddWithValue("@offset", query.Offset);

        var results = new List<LogRecord>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new LogRecord
            {
                Id = reader.GetInt64(0),
                ServiceName = reader.GetString(1),
                ScopeName = reader.GetString(2),
                TimeUnixNano = reader.GetInt64(3),
                ObservedTimeUnixNano = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                SeverityNumber = reader.GetInt32(5),
                SeverityText = reader.IsDBNull(6) ? null : reader.GetString(6),
                Body = reader.IsDBNull(7) ? null : reader.GetString(7),
                AttributesJson = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return results;
    }

    public Dictionary<string, long> GetLogCountByService(DateTimeOffset? from, DateTimeOffset? to)
    {
        var command = _connection.CreateCommand();
        var conditions = new List<string>();

        if (from.HasValue)
        {
            conditions.Add("l.time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", from.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (to.HasValue)
        {
            conditions.Add("l.time_unix_nano <= @to");
            command.Parameters.AddWithValue("@to", to.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }

        var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT r.service_name, COUNT(*) as count
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            {whereClause}
            GROUP BY r.service_name
            ORDER BY count DESC
        ";

        var results = new Dictionary<string, long>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results[reader.GetString(0)] = reader.GetInt64(1);
        }

        return results;
    }

    public long GetLogCountForTrace(string traceId)
    {
        var command = _connection.CreateCommand();
        command.CommandText = @"
            SELECT COUNT(*) 
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            WHERE l.trace_id = @trace_id
        ";
        command.Parameters.AddWithValue("@trace_id", traceId);

        return (long)command.ExecuteScalar()!;
    }

    public long GetLogCountForSpan(string spanId)
    {
        var command = _connection.CreateCommand();
        command.CommandText = @"
            SELECT COUNT(*) 
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            WHERE l.span_id = @span_id
        ";
        command.Parameters.AddWithValue("@span_id", spanId);

        return (long)command.ExecuteScalar()!;
    }

    private static string GetLogBody(OpenTelemetry.Proto.Logs.V1.LogRecord logRecord)
    {
        var body = logRecord.Body.StringValue ?? string.Empty;

        foreach (var attribute in logRecord.Attributes)
        {
            body = body.Replace("{" + attribute.Key + "}", GetAnyValueString(attribute.Value));
        }

        return body;
    }

    public List<string> GetUniqueLogScopes()
    {
        var command = _connection.CreateCommand();

        command.CommandText = @"
            SELECT DISTINCT l.scope_name 
            FROM logs l
            ORDER BY l.scope_name
        ";

        var scopes = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            scopes.Add(reader.GetString(0));
        }

        return scopes;
    }

}

public class LogRecord
{
    public long Id { get; set; }
    public string ServiceName { get; set; } = "";
    public string ScopeName { get; set; } = "";
    public long TimeUnixNano { get; set; }
    public long ObservedTimeUnixNano { get; set; }
    public int SeverityNumber { get; set; }
    public string? SeverityText { get; set; }
    public string? Body { get; set; }
    public string? AttributesJson { get; set; }

    public DateTimeOffset GetTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(TimeUnixNano / 1_000_000).ToLocalTime();
    }
}
