using Microsoft.Data.Sqlite;
using System.Text.Json;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;

namespace Signals;

public sealed class Database : IDisposable
{
    private readonly SqliteConnection _connection;

    public Database()
    {
        _connection = new SqliteConnection($"Data Source=signals.db");
        _connection.Open();

        ExecuteNonQuery("PRAGMA journal_mode = WAL");
        ExecuteNonQuery("PRAGMA synchronous = NORMAL");
        ExecuteNonQuery("PRAGMA cache_size = 10000");
        ExecuteNonQuery("PRAGMA temp_store = MEMORY");

        CreateSchema();
    }

    private void CreateSchema()
    {
        ExecuteNonQuery(File.ReadAllText("schema.sql"));
    }

    public void InsertLogs(ResourceLogs resourceLogs)
    {
        var serviceName = GetResourceAttribute(resourceLogs.Resource, "service.name") ?? "unknown";
        var serviceVersion = GetResourceAttribute(resourceLogs.Resource, "service.version");
        var serviceInstanceId = GetResourceAttribute(resourceLogs.Resource, "service.instance.id");
        var resourceJson = JsonSerializer.Serialize(resourceLogs.Resource);

        using var command = _connection.CreateCommand();

        command.CommandText = @"
            INSERT INTO logs (
                service_name, service_version, service_instance_id, scope_name,
                time_unix_nano, observed_time_unix_nano,
                severity_number, severity_text, body,
                attributes_json, resource_json
            ) VALUES (
                @service_name, @service_version, @service_instance_id, @scope_name,
                @time_unix_nano, @observed_time_unix_nano,
                @severity_number, @severity_text, @body,
                @attributes_json, @resource_json
            )
        ";

        foreach (var scopeLog in resourceLogs.ScopeLogs)
        {
            var scopeName = scopeLog.Scope?.Name ?? "unknown";

            foreach (var logRecord in scopeLog.LogRecords)
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@service_name", serviceName);
                command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@scope_name", scopeName);
                command.Parameters.AddWithValue("@time_unix_nano", (long)logRecord.TimeUnixNano);
                command.Parameters.AddWithValue("@observed_time_unix_nano",
                    logRecord.ObservedTimeUnixNano > 0 ? (long)logRecord.ObservedTimeUnixNano : DBNull.Value);
                command.Parameters.AddWithValue("@severity_number", (int)logRecord.SeverityNumber);
                command.Parameters.AddWithValue("@severity_text", logRecord.SeverityText ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@body", GetLogBody(logRecord));
                command.Parameters.AddWithValue("@attributes_json", JsonSerializer.Serialize(logRecord.Attributes));
                command.Parameters.AddWithValue("@resource_json", resourceJson);

                command.ExecuteNonQuery();
            }
        }

    }

    public void InsertTraces(ResourceSpans resourceSpans)
    {
        var serviceName = GetResourceAttribute(resourceSpans.Resource, "service.name") ?? "unknown";
        var serviceVersion = GetResourceAttribute(resourceSpans.Resource, "service.version");
        var serviceInstanceId = GetResourceAttribute(resourceSpans.Resource, "service.instance.id");
        var resourceJson = JsonSerializer.Serialize(resourceSpans.Resource);

        using var command = _connection.CreateCommand();

        command.CommandText = @"
            INSERT INTO traces (
                service_name, service_version, service_instance_id, scope_name,
                trace_id, span_id, parent_span_id,
                start_time_unix_nano, end_time_unix_nano,
                span_name, span_kind, status_code, status_message,
                attributes_json, events_json, links_json, resource_json
            ) VALUES (
                @service_name, @service_version, @service_instance_id, @scope_name,
                @trace_id, @span_id, @parent_span_id,
                @start_time_unix_nano, @end_time_unix_nano,
                @span_name, @span_kind, @status_code, @status_message,
                @attributes_json, @events_json, @links_json, @resource_json
            )
        ";

        foreach (var scopeSpan in resourceSpans.ScopeSpans)
        {
            var scopeName = scopeSpan.Scope?.Name ?? "unknown";

            foreach (var span in scopeSpan.Spans)
            {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@service_name", serviceName);
                command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@scope_name", scopeName);
                command.Parameters.AddWithValue("@trace_id", span.TraceId.ToByteArray());
                command.Parameters.AddWithValue("@span_id", span.SpanId.ToByteArray());
                command.Parameters.AddWithValue("@parent_span_id",
                    span.ParentSpanId.Length > 0 ? span.ParentSpanId.ToByteArray() : (object)DBNull.Value);
                command.Parameters.AddWithValue("@start_time_unix_nano", (long)span.StartTimeUnixNano);
                command.Parameters.AddWithValue("@end_time_unix_nano", (long)span.EndTimeUnixNano);
                command.Parameters.AddWithValue("@span_name", span.Name);
                command.Parameters.AddWithValue("@span_kind", (int)span.Kind);
                command.Parameters.AddWithValue("@status_code", span.Status?.Code != null ? (int)span.Status.Code : DBNull.Value);
                command.Parameters.AddWithValue("@status_message", span.Status?.Message ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@attributes_json", JsonSerializer.Serialize(span.Attributes));
                command.Parameters.AddWithValue("@events_json", JsonSerializer.Serialize(span.Events));
                command.Parameters.AddWithValue("@links_json", JsonSerializer.Serialize(span.Links));
                command.Parameters.AddWithValue("@resource_json", resourceJson);

                command.ExecuteNonQuery();
            }
        }

    }

    public void InsertMetrics(ResourceMetrics resourceMetrics)
    {
        var serviceName = GetResourceAttribute(resourceMetrics.Resource, "service.name") ?? "unknown";
        var serviceVersion = GetResourceAttribute(resourceMetrics.Resource, "service.version");
        var serviceInstanceId = GetResourceAttribute(resourceMetrics.Resource, "service.instance.id");
        var resourceJson = JsonSerializer.Serialize(resourceMetrics.Resource);

        using var command = _connection.CreateCommand();

        command.CommandText = @"
            INSERT INTO metrics (
                service_name, service_version, service_instance_id, scope_name,
                metric_name, metric_type, metric_unit, metric_description,
                time_unix_nano,
                value_double, value_int, count, sum_value,
                attributes_json, resource_json
            ) VALUES (
                @service_name, @service_version, @service_instance_id, @scope_name,
                @metric_name, @metric_type, @metric_unit, @metric_description,
                @time_unix_nano,
                @value_double, @value_int, @count, @sum_value,
                @attributes_json, @resource_json
            )
        ";

        foreach (var scopeMetric in resourceMetrics.ScopeMetrics)
        {
            var scopeName = scopeMetric.Scope?.Name ?? "unknown";

            foreach (var metric in scopeMetric.Metrics)
            {
                InsertMetricDataPoints(command, serviceName, serviceVersion, serviceInstanceId,
                    scopeName, metric, resourceJson);
            }
        }

    }

    public class Query
    {
        public event Action? OnChange;
        private void NotifyStateChanged() => OnChange?.Invoke();

        private DateTimeOffset? _startTime;
        public DateTimeOffset? StartTime { get => _startTime; set { if (_startTime != value) { _startTime = value; NotifyStateChanged(); } } }
        private DateTimeOffset? _endTime;
        public DateTimeOffset? EndTime { get => _endTime; set { if (_endTime != value) { _endTime = value; NotifyStateChanged(); } } }
        private string? _serviceName;
        public string? ServiceName { get => _serviceName; set { if (_serviceName != value) { _serviceName = value; NotifyStateChanged(); } } }
        private string? _scopeName;
        public string? ScopeName { get => _scopeName; set { if (_scopeName != value) { _scopeName = value; NotifyStateChanged(); } } }
        private int? _minSeverity;
        public int? MinSeverity { get => _minSeverity; set { if (_minSeverity != value) { _minSeverity = value; NotifyStateChanged(); } } }
        private string? _spanName;
        public string? SpanName { get => _spanName; set { if (_spanName != value) { _spanName = value; NotifyStateChanged(); } } }
        private string? _metricName;
        public string? MetricName { get => _metricName; set { if (_metricName != value) { _metricName = value; NotifyStateChanged(); } } }
        private int _limit = 1000;
        public int Limit { get => _limit; set { if (_limit != value) { _limit = value; NotifyStateChanged(); } } }
        private int _offset = 0;
        public int Offset { get => _offset; set { if (_offset != value) { _offset = value; NotifyStateChanged(); } } }
        private string _text = string.Empty;
        public string Text { get => _text; set { if (_text != value) { _text = value; NotifyStateChanged(); } } }
    }

    public List<LogRecord> QueryLogs(Query query)
    {
        var conditions = new List<string>();
        var command = _connection.CreateCommand();

        // Time range
        if (query.StartTime.HasValue)
        {
            conditions.Add("time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (query.EndTime.HasValue)
        {
            conditions.Add("time_unix_nano <= @to");
            command.Parameters.AddWithValue("@to", query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }

        // Service filter
        if (!string.IsNullOrEmpty(query.ServiceName))
        {
            conditions.Add("service_name = @service");
            command.Parameters.AddWithValue("@service", query.ServiceName);
        }

        // Scope filter
        if (!string.IsNullOrEmpty(query.ScopeName))
        {
            conditions.Add("scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Severity filter
        if (query.MinSeverity.HasValue)
        {
            conditions.Add("severity_number >= @severity");
            command.Parameters.AddWithValue("@severity", query.MinSeverity.Value);
        }

        // Text filter
        if (!string.IsNullOrEmpty(query.Text))
        {
            conditions.Add("body LIKE @text");
            command.Parameters.AddWithValue("@text", $"%{query.Text}%");
        }

        var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT 
                id, service_name, scope_name, time_unix_nano, observed_time_unix_nano,
                severity_number, severity_text, body, attributes_json, resource_json
            FROM logs
            {whereClause}
            ORDER BY time_unix_nano DESC
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
                AttributesJson = reader.IsDBNull(8) ? null : reader.GetString(8),
                ResourceJson = reader.IsDBNull(9) ? null : reader.GetString(9)
            });
        }

        return results;
    }

    public List<TraceRecord> QueryTraces(Query query)
    {
        var conditions = new List<string>();
        var command = _connection.CreateCommand();

        // Time range
        if (query.StartTime.HasValue)
        {
            conditions.Add("start_time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (query.EndTime.HasValue)
        {
            conditions.Add("start_time_unix_nano <= @to");
            command.Parameters.AddWithValue("@to", query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }

        // Service filter
        if (!string.IsNullOrEmpty(query.ServiceName))
        {
            conditions.Add("service_name = @service");
            command.Parameters.AddWithValue("@service", query.ServiceName);
        }

        // Scope filter
        if (!string.IsNullOrEmpty(query.ScopeName))
        {
            conditions.Add("scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Span name filter
        if (!string.IsNullOrEmpty(query.SpanName))
        {
            conditions.Add("span_name = @span_name");
            command.Parameters.AddWithValue("@span_name", query.SpanName);
        }

        // Text filter
        if (!string.IsNullOrEmpty(query.Text))
        {
            conditions.Add("span_name LIKE @text");
            command.Parameters.AddWithValue("@text", $"%{query.Text}%");
        }

        var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT 
                id, service_name, scope_name, trace_id, span_id, parent_span_id,
                start_time_unix_nano, end_time_unix_nano,
                span_name, span_kind, status_code, status_message,
                attributes_json, events_json, links_json, resource_json
            FROM traces
            {whereClause}
            ORDER BY start_time_unix_nano DESC
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
                Id = reader.GetInt64(0),
                ServiceName = reader.GetString(1),
                ScopeName = reader.GetString(2),
                TraceId = (byte[])reader.GetValue(3),
                SpanId = (byte[])reader.GetValue(4),
                ParentSpanId = reader.IsDBNull(5) ? null : (byte[])reader.GetValue(5),
                StartTimeUnixNano = reader.GetInt64(6),
                EndTimeUnixNano = reader.GetInt64(7),
                SpanName = reader.GetString(8),
                SpanKind = reader.GetInt32(9),
                StatusCode = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                StatusMessage = reader.IsDBNull(11) ? null : reader.GetString(11),
                AttributesJson = reader.IsDBNull(12) ? null : reader.GetString(12),
                EventsJson = reader.IsDBNull(13) ? null : reader.GetString(13),
                LinksJson = reader.IsDBNull(14) ? null : reader.GetString(14),
                ResourceJson = reader.IsDBNull(15) ? null : reader.GetString(15)
            });
        }

        return results;
    }

    public List<MetricRecord> QueryMetrics(Query query)
    {
        var conditions = new List<string>();
        var command = _connection.CreateCommand();

        // Time range
        if (query.StartTime.HasValue)
        {
            conditions.Add("time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (query.EndTime.HasValue)
        {
            conditions.Add("time_unix_nano <= @to");
            command.Parameters.AddWithValue("@to", query.EndTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }

        // Service filter
        if (!string.IsNullOrEmpty(query.ServiceName))
        {
            conditions.Add("service_name = @service");
            command.Parameters.AddWithValue("@service", query.ServiceName);
        }

        // Scope filter
        if (!string.IsNullOrEmpty(query.ScopeName))
        {
            conditions.Add("scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Metric name filter
        if (!string.IsNullOrEmpty(query.MetricName))
        {
            conditions.Add("metric_name = @metric_name");
            command.Parameters.AddWithValue("@metric_name", query.MetricName);
        }

        // Text filter
        if (!string.IsNullOrEmpty(query.Text))
        {
            conditions.Add("metric_name LIKE @text");
            command.Parameters.AddWithValue("@text", $"%{query.Text}%");
        }

        var whereClause = conditions.Count != 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT 
                id, service_name, scope_name, metric_name, metric_type,
                time_unix_nano, value_double, value_int, count, sum_value,
                attributes_json, resource_json, metric_description
            FROM metrics
            {whereClause}
            ORDER BY time_unix_nano DESC
            LIMIT @limit OFFSET @offset
        ";
        command.Parameters.AddWithValue("@limit", query.Limit);
        command.Parameters.AddWithValue("@offset", query.Offset);

        var results = new List<MetricRecord>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            results.Add(new MetricRecord
            {
                Id = reader.GetInt64(0),
                ServiceName = reader.GetString(1),
                ScopeName = reader.GetString(2),
                MetricName = reader.GetString(3),
                MetricType = reader.GetString(4),
                TimeUnixNano = reader.GetInt64(5),
                ValueDouble = reader.IsDBNull(6) ? null : reader.GetDouble(6),
                ValueInt = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                Count = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                SumValue = reader.IsDBNull(9) ? null : reader.GetDouble(9),
                AttributesJson = reader.IsDBNull(10) ? null : reader.GetString(10),
                ResourceJson = reader.IsDBNull(11) ? null : reader.GetString(11),
                Description = reader.IsDBNull(12) ? "" : reader.GetString(12)
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
            conditions.Add("time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", from.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (to.HasValue)
        {
            conditions.Add("time_unix_nano <= @to");
            command.Parameters.AddWithValue("@to", to.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }

        var whereClause = conditions.Any() ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT service_name, COUNT(*) as count
            FROM logs
            {whereClause}
            GROUP BY service_name
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

    public List<string> GetUniqueServices()
    {
        var services = new HashSet<string>();

        var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT service_name FROM logs";
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read()) services.Add(reader.GetString(0));
        }

        command.CommandText = "SELECT DISTINCT service_name FROM traces";
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read()) services.Add(reader.GetString(0));
        }

        command.CommandText = "SELECT DISTINCT service_name FROM metrics";
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read()) services.Add(reader.GetString(0));
        }

        return services.OrderBy(s => s).ToList();
    }

    public List<string> GetUniqueScopes(string? serviceName = null)
    {
        var scopes = new HashSet<string>();
        var whereClause = serviceName != null ? "WHERE service_name = @service" : "";

        foreach (var table in new[] { "logs", "traces", "metrics" })
        {
            var command = _connection.CreateCommand();
            command.CommandText = $"SELECT DISTINCT scope_name FROM {table} {whereClause}";
            if (serviceName != null)
            {
                command.Parameters.AddWithValue("@service", serviceName);
            }

            using var reader = command.ExecuteReader();
            while (reader.Read()) scopes.Add(reader.GetString(0));
        }

        return scopes.OrderBy(s => s).ToList();
    }

    private static string? GetResourceAttribute(Resource resource, string key)
    {
        return resource?.Attributes
            ?.FirstOrDefault(a => a.Key == key)
            ?.Value?.StringValue;
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

    private static string GetAnyValueString(OpenTelemetry.Proto.Common.V1.AnyValue value)
    {
        switch (value.ValueCase)
        {
            case OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.StringValue:
                return value.StringValue;
            case OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.BoolValue:
                return value.BoolValue ? "true" : "false";
            case OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.IntValue:
                return value.IntValue.ToString();
            case OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.DoubleValue:
                return value.DoubleValue.ToString();
            case OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.ArrayValue:
                return string.Join(", ", value.ArrayValue.Values.Select(GetAnyValueString));
            default:
                return value.ToString();
        }
    }

    private void InsertMetricDataPoints(SqliteCommand command, string serviceName,
        string? serviceVersion, string? serviceInstanceId, string scopeName,
        Metric metric, string resourceJson)
    {

        switch (metric.DataCase)
        {
            case Metric.DataOneofCase.Gauge:
                foreach (var dataPoint in metric.Gauge.DataPoints)
                {
                    command.Parameters.Clear();
                    AddMetricParameters(command, serviceName, serviceVersion, serviceInstanceId,
                        scopeName, metric.Name, "gauge", metric.Unit, metric.Description,
                        (long)dataPoint.TimeUnixNano, dataPoint.AsDouble, null, null, null,
                        JsonSerializer.Serialize(dataPoint.Attributes), resourceJson);
                    command.ExecuteNonQuery();
                }
                break;

            case Metric.DataOneofCase.Sum:
                foreach (var dataPoint in metric.Sum.DataPoints)
                {
                    command.Parameters.Clear();
                    AddMetricParameters(command, serviceName, serviceVersion, serviceInstanceId,
                        scopeName, metric.Name, "sum", metric.Unit, metric.Description,
                        (long)dataPoint.TimeUnixNano, dataPoint.AsDouble, null, null, null,
                        JsonSerializer.Serialize(dataPoint.Attributes), resourceJson);
                    command.ExecuteNonQuery();
                }
                break;

            case Metric.DataOneofCase.Histogram:
                foreach (var dataPoint in metric.Histogram.DataPoints)
                {
                    command.Parameters.Clear();
                    AddMetricParameters(command, serviceName, serviceVersion, serviceInstanceId,
                        scopeName, metric.Name, "histogram", metric.Unit, metric.Description,
                        (long)dataPoint.TimeUnixNano, null, null, (long?)dataPoint.Count, dataPoint.Sum,
                        JsonSerializer.Serialize(dataPoint.Attributes), resourceJson);
                    command.ExecuteNonQuery();
                }
                break;

            case Metric.DataOneofCase.Summary:
                foreach (var dataPoint in metric.Summary.DataPoints)
                {
                    command.Parameters.Clear();
                    AddMetricParameters(command, serviceName, serviceVersion, serviceInstanceId,
                        scopeName, metric.Name, "summary", metric.Unit, metric.Description,
                        (long)dataPoint.TimeUnixNano, null, null, (long?)dataPoint.Count, dataPoint.Sum,
                        JsonSerializer.Serialize(dataPoint.Attributes), resourceJson);
                    command.ExecuteNonQuery();
                }
                break;

            case Metric.DataOneofCase.ExponentialHistogram:
                foreach (var dataPoint in metric.ExponentialHistogram.DataPoints)
                {
                    command.Parameters.Clear();
                    AddMetricParameters(command, serviceName, serviceVersion, serviceInstanceId,
                        scopeName, metric.Name, "exponential_histogram", metric.Unit, metric.Description,
                        (long)dataPoint.TimeUnixNano, null, null, (long?)dataPoint.Count, dataPoint.Sum,
                        JsonSerializer.Serialize(dataPoint.Attributes), resourceJson);
                    command.ExecuteNonQuery();
                }
                break;

            default:
                // Unsupported metric type
                break;
        }

    }

    private void AddMetricParameters(SqliteCommand command, string serviceName,
        string? serviceVersion, string? serviceInstanceId, string scopeName,
        string metricName, string metricType, string? metricUnit, string? metricDescription,
        long timeUnixNano, double? valueDouble, long? valueInt, long? count, double? sumValue,
        string attributesJson, string resourceJson)
    {
        command.Parameters.AddWithValue("@service_name", serviceName);
        command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@scope_name", scopeName);
        command.Parameters.AddWithValue("@metric_name", metricName);
        command.Parameters.AddWithValue("@metric_type", metricType);
        command.Parameters.AddWithValue("@metric_unit", metricUnit ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@metric_description", metricDescription ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@time_unix_nano", timeUnixNano);
        command.Parameters.AddWithValue("@value_double", valueDouble ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@value_int", valueInt ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@count", count ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@sum_value", sumValue ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@attributes_json", attributesJson);
        command.Parameters.AddWithValue("@resource_json", resourceJson);
    }

    private void ExecuteNonQuery(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _connection?.Dispose();
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
    public string? ResourceJson { get; set; }

    public DateTimeOffset GetTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(TimeUnixNano / 1_000_000).ToLocalTime();
    }
}

public class TraceRecord
{
    public long Id { get; set; }
    public string ServiceName { get; set; } = "";
    public string ScopeName { get; set; } = "";
    public byte[] TraceId { get; set; } = [];
    public byte[] SpanId { get; set; } = [];
    public byte[]? ParentSpanId { get; set; }
    public long StartTimeUnixNano { get; set; }
    public long EndTimeUnixNano { get; set; }
    public string SpanName { get; set; } = "";
    public int SpanKind { get; set; }
    public int StatusCode { get; set; }
    public string? StatusMessage { get; set; }
    public string? AttributesJson { get; set; }
    public string? EventsJson { get; set; }
    public string? LinksJson { get; set; }
    public string? ResourceJson { get; set; }

    public DateTimeOffset GetStartTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(StartTimeUnixNano / 1_000_000).ToLocalTime();
    }

    public DateTimeOffset GetEndTime()
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(EndTimeUnixNano / 1_000_000).ToLocalTime();
    }
}

public class MetricRecord
{
    public long Id { get; set; }
    public string ServiceName { get; set; } = "";
    public string ScopeName { get; set; } = "";
    public string MetricName { get; set; } = "";
    public string Description { get; set; } = "";
    public string MetricType { get; set; } = "";
    public long TimeUnixNano { get; set; }
    public double? ValueDouble { get; set; }
    public long? ValueInt { get; set; }
    public long? Count { get; set; }
    public double? SumValue { get; set; }
    public string? AttributesJson { get; set; }
    public string? ResourceJson { get; set; }
}