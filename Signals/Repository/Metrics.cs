using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Metrics.V1;

namespace Signals.Repository;

public sealed partial class Database : IDisposable
{

    public void InsertMetrics(ResourceMetrics resourceMetrics)
    {
        using var command = _connection.CreateCommand();

        foreach (var scopeMetric in resourceMetrics.ScopeMetrics)
        {
            var scopeName = scopeMetric.Scope?.Name ?? "unknown";
            var resourceId = GetOrCreateResource(resourceMetrics.Resource);

            foreach (var metric in scopeMetric.Metrics)
            {
                var metricId = GetOrCreateMetric(scopeName, metric.Name, (int)metric.DataCase, metric.Unit, metric.Description);
                InsertMetricDataPoints(command, resourceId, metricId, metric);
            }
        }
    }

    public List<MetricRecord> QueryMetrics(Query query)
    {
        var conditions = new List<string>();
        var command = _connection.CreateCommand();

        // Time range
        if (query.StartTime.HasValue)
        {
            conditions.Add("dp.time_unix_nano >= @from");
            command.Parameters.AddWithValue("@from", query.StartTime.Value.ToUnixTimeSeconds() * 1_000_000_000L);
        }
        if (query.EndTime.HasValue)
        {
            conditions.Add("dp.time_unix_nano <= @to");
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
            conditions.Add("m.scope_name = @scope");
            command.Parameters.AddWithValue("@scope", query.ScopeName);
        }

        // Metric name filter
        if (!string.IsNullOrEmpty(query.MetricName))
        {
            conditions.Add("m.metric_name = @metric_name");
            command.Parameters.AddWithValue("@metric_name", query.MetricName);
        }

        // Text filter
        if (!string.IsNullOrEmpty(query.Text))
        {
            conditions.Add("m.metric_name LIKE @text");
            command.Parameters.AddWithValue("@text", $"%{query.Text}%");
        }

        var whereClause = conditions.Count != 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

        command.CommandText = $@"
            SELECT 
                dp.id, r.service_name, m.scope_name, m.metric_name, m.metric_type,
                dp.time_unix_nano, dp.value_double, dp.value_int, dp.count, dp.sum_value,
                m.metric_description, dp.min_value, dp.max_value
            FROM data_points dp
            JOIN metrics m ON dp.metric_id = m.id
            JOIN resources r ON dp.resource_id = r.id
            {whereClause}
            ORDER BY dp.time_unix_nano DESC
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
                MetricType = (Metric.DataOneofCase)reader.GetInt32(4),
                TimeUnixNano = reader.GetInt64(5),
                ValueDouble = reader.IsDBNull(6) ? null : reader.GetDouble(6),
                ValueInt = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                Count = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                SumValue = reader.IsDBNull(9) ? null : reader.GetDouble(9),
                Description = reader.IsDBNull(10) ? "" : reader.GetString(10),
                MinValue = reader.IsDBNull(11) ? null : reader.GetDouble(11),
                MaxValue = reader.IsDBNull(12) ? null : reader.GetDouble(12)

            });
        }

        return results;
    }

    private static void InsertMetricDataPoints(SqliteCommand command, long resourceId, long metricId, Metric metric)
    {
        var dataPoints = new List<(long timeUnixNano, double? valueDouble, long? valueInt, long? count, double? sumValue, double? minValue, double? maxValue)>();

        switch (metric.DataCase)
        {
            case Metric.DataOneofCase.Gauge:
                foreach (var dataPoint in metric.Gauge.DataPoints)
                {
                    var (valueDouble, valueInt) = GetDataPointValues(dataPoint);
                    dataPoints.Add(((long)dataPoint.TimeUnixNano, valueDouble, valueInt, null, null, null, null));
                }
                break;

            case Metric.DataOneofCase.Sum:
                foreach (var dataPoint in metric.Sum.DataPoints)
                {
                    var (valueDouble, valueInt) = GetDataPointValues(dataPoint);
                    dataPoints.Add(((long)dataPoint.TimeUnixNano, valueDouble, valueInt, null, null, null, null));
                }
                break;

            case Metric.DataOneofCase.Histogram:
                foreach (var dataPoint in metric.Histogram.DataPoints)
                {
                    dataPoints.Add(((long)dataPoint.TimeUnixNano, null, null, (long)dataPoint.Count, dataPoint.Sum, dataPoint.Min, dataPoint.Max));
                }
                break;

            default:
                return; // Unsupported metric type
        }

        if (dataPoints.Count == 0) return;

        // Batch insert all data points
        var valuesClauses = new List<string>();
        for (int i = 0; i < dataPoints.Count; i++)
        {
            valuesClauses.Add($"(@resource_id, @metric_id, @time_unix_nano_{i}, @value_double_{i}, @value_int_{i}, @count_{i}, @sum_value_{i}, @min_value_{i}, @max_value_{i})");
        }

        command.CommandText = $@"
            INSERT INTO data_points (
                resource_id, metric_id, time_unix_nano,
                value_double, value_int, count, sum_value, min_value, max_value
            ) VALUES {string.Join(", ", valuesClauses)}
        ";

        command.Parameters.Clear();
        command.Parameters.AddWithValue("@resource_id", resourceId);
        command.Parameters.AddWithValue("@metric_id", metricId);

        for (int i = 0; i < dataPoints.Count; i++)
        {
            var (timeUnixNano, valueDouble, valueInt, count, sumValue, minValue, maxValue) = dataPoints[i];
            command.Parameters.AddWithValue($"@time_unix_nano_{i}", timeUnixNano);
            command.Parameters.AddWithValue($"@value_double_{i}", valueDouble ?? (object)DBNull.Value);
            command.Parameters.AddWithValue($"@value_int_{i}", valueInt ?? (object)DBNull.Value);
            command.Parameters.AddWithValue($"@count_{i}", count ?? (object)DBNull.Value);
            command.Parameters.AddWithValue($"@sum_value_{i}", sumValue ?? (object)DBNull.Value);
            command.Parameters.AddWithValue($"@min_value_{i}", minValue ?? (object)DBNull.Value);
            command.Parameters.AddWithValue($"@max_value_{i}", maxValue ?? (object)DBNull.Value);
        }

        command.ExecuteNonQuery();
    }

    private static (double? valueDouble, long? valueInt) GetDataPointValues(NumberDataPoint dataPoint)
    {
        return dataPoint.ValueCase switch
        {
            NumberDataPoint.ValueOneofCase.AsDouble => (dataPoint.AsDouble, null),
            NumberDataPoint.ValueOneofCase.AsInt => (null, dataPoint.AsInt),
            _ => (null, null)
        };
    }

    private long GetOrCreateMetric(string scopeName, string metricName, int metricType, string? metricUnit, string? metricDescription)
    {
        using var command = _connection.CreateCommand();

        command.CommandText = @"
            SELECT id FROM metrics 
            WHERE scope_name = @scope_name 
            AND metric_name = @metric_name 
            AND metric_type = @metric_type
        ";

        command.Parameters.AddWithValue("@scope_name", scopeName);
        command.Parameters.AddWithValue("@metric_name", metricName);
        command.Parameters.AddWithValue("@metric_type", metricType);

        var existingId = command.ExecuteScalar();
        if (existingId != null)
        {
            return (long)existingId;
        }

        command.CommandText = @"
            INSERT INTO metrics (scope_name, metric_name, metric_type, metric_unit, metric_description)
            VALUES (@scope_name, @metric_name, @metric_type, @metric_unit, @metric_description);
            SELECT last_insert_rowid();
        ";

        command.Parameters.Clear();
        command.Parameters.AddWithValue("@scope_name", scopeName);
        command.Parameters.AddWithValue("@metric_name", metricName);
        command.Parameters.AddWithValue("@metric_type", metricType);
        command.Parameters.AddWithValue("@metric_unit", metricUnit ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@metric_description", metricDescription ?? (object)DBNull.Value);

        return (long)command.ExecuteScalar()!;
    }

    public List<string> GetUniqueMetricScopes()
    {
        var scopes = new List<string>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT scope_name FROM metrics";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            scopes.Add(reader.GetString(0));
        }
        return scopes;
    }

    public List<MetricRecord> GetMetricsForSpan(TraceRecord span)
    {
        var command = _connection.CreateCommand();

        // Get metrics from the same resource during the span's execution time
        command.CommandText = @"
            SELECT 
                dp.id, r.service_name, m.scope_name, m.metric_name, m.metric_type,
                dp.time_unix_nano, dp.value_double, dp.value_int, dp.count, dp.sum_value,
                m.metric_description
            FROM data_points dp
            JOIN metrics m ON dp.metric_id = m.id
            JOIN resources r ON dp.resource_id = r.id
            WHERE r.service_name = @service_name
            AND dp.time_unix_nano >= @start_time
            AND dp.time_unix_nano <= @end_time
            ORDER BY dp.time_unix_nano
        ";

        command.Parameters.AddWithValue("@service_name", span.ServiceName);
        command.Parameters.AddWithValue("@start_time", span.StartTimeUnixNano);
        command.Parameters.AddWithValue("@end_time", span.EndTimeUnixNano);

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
                MetricType = (Metric.DataOneofCase)reader.GetInt32(4),
                TimeUnixNano = reader.GetInt64(5),
                ValueDouble = reader.IsDBNull(6) ? null : reader.GetDouble(6),
                ValueInt = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                Count = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                SumValue = reader.IsDBNull(9) ? null : reader.GetDouble(9),
                Description = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return results;
    }

}

public class MetricRecord
{
    public long Id { get; set; }
    public string ServiceName { get; set; } = "";
    public string ScopeName { get; set; } = "";
    public string MetricName { get; set; } = "";
    public string Description { get; set; } = "";
    public Metric.DataOneofCase MetricType { get; set; }
    public long TimeUnixNano { get; set; }
    public double? ValueDouble { get; set; }
    public long? ValueInt { get; set; }
    public long? Count { get; set; }
    public double? SumValue { get; set; }
    public double? MinValue { get; set; }
    public double? MaxValue { get; set; }
}