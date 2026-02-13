using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Signals.Common.Utilities;


namespace OpenTelemetry.Proto.Metrics.V1
{

    public partial class Metric
    {
        public string ServiceName { get; set; } = "";
        public string ServiceInstanceId { get; set; } = "";
        public InstrumentationScope Scope { get; set; }

        public long Samples => DataCase switch
        {
            DataOneofCase.Gauge => Gauge.DataPoints.Count,
            DataOneofCase.Sum => Sum.DataPoints.Count,
            DataOneofCase.Histogram => Histogram.DataPoints.Count,
            _ => 0
        };

        public IEnumerable<NumberDataPoint> GetDataPoints() => DataCase switch
        {
            DataOneofCase.Gauge => Gauge.DataPoints,
            DataOneofCase.Sum => Sum.DataPoints,
            _ => Enumerable.Empty<NumberDataPoint>()
        };

    }
}

namespace Signals.Telemetry
{
    public sealed partial class Repository : IDisposable
    {

        public void InsertMetrics(IEnumerable<ResourceMetrics> resourceMetrics)
        {
            foreach (var resourceMetric in resourceMetrics)
            {
                var resourceId = GetOrCreateResource(resourceMetric.Resource);

                foreach (var scopeMetric in resourceMetric.ScopeMetrics)
                {
                    var scopeId = GetOrCreateScope(scopeMetric.Scope);

                    foreach (var metric in scopeMetric.Metrics)
                    {
                        var metricId = GetOrCreateMetric(metric);
                        InsertMetricDataPoints(resourceId, metricId, scopeId, metric);
                    }
                }
            }
        }


        public List<Metric> QueryMetrics() => QueryMetrics(new Query());

        public List<Metric> QueryMetrics(Query query)
        {
            var conditions = new List<string>();
            var command = _connection.CreateCommand();

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

            // Time filters
            conditions.Add("dp.time_unix_nano >= @start_time");
            command.Parameters.AddWithValue("@start_time", query.StartTime.ToUnixTimeNanoseconds());
        
            conditions.Add("dp.time_unix_nano <= @end_time");
            command.Parameters.AddWithValue("@end_time", query.EndTime.ToUnixTimeNanoseconds());

            var whereClause = conditions.Count != 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

            command.CommandText = $@"
                SELECT
                    m.id,
                    r.service_name,
                    r.service_instance_id,
                    s.scope_name,
                    m.metric_name,
                    m.metric_type,
                    m.metric_description
                FROM data_points AS dp
                LEFT JOIN resources AS r ON r.id = dp.resource_id
                JOIN metrics   AS m ON m.id = dp.metric_id
                JOIN scopes    AS s ON s.id = dp.scope_id
                {whereClause}
                GROUP BY dp.resource_id, dp.metric_id, dp.scope_id
                ORDER BY m.metric_name DESC
                LIMIT @limit OFFSET @offset
            ";
            command.Parameters.AddWithValue("@limit", query.Limit);
            command.Parameters.AddWithValue("@offset", query.Offset);

            var results = new List<Metric>();
            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                var metricId = reader.GetInt64(0);
                var metric = new Metric
                {
                    ServiceName = reader.GetString(1),
                    Scope = new InstrumentationScope { Name = reader.GetString(3) },
                    Name = reader.GetString(4),
                    Description = reader.IsDBNull(6) ? "" : reader.GetString(6),
                };

                switch ((Metric.DataOneofCase)reader.GetInt32(5))
                {
                    case Metric.DataOneofCase.Gauge:
                        metric.Gauge = new Gauge();
                        break;
                    case Metric.DataOneofCase.Sum:
                        metric.Sum = new Sum();
                        break;
                    case Metric.DataOneofCase.Histogram:
                        metric.Histogram = new Histogram();
                        break;
                }

                GetDataPoints(metric, metricId, query);

                results.Add(metric);
            }

            return results;
        }

        private void GetDataPoints(Metric metric, long metricId, Query query)
        {
            var conditions = new List<string>();
            var command = _connection.CreateCommand();

            conditions.Add("dp.time_unix_nano >= @start_time");
            command.Parameters.AddWithValue("@start_time", query.StartTime.ToUnixTimeNanoseconds());

            conditions.Add("dp.time_unix_nano <= @end_time");
            command.Parameters.AddWithValue("@end_time", query.EndTime.ToUnixTimeNanoseconds());

            if (query.ServiceName != null)
            {
                conditions.Add("r.service_name = @service_name");
                command.Parameters.AddWithValue("@service_name", query.ServiceName);
            }

            conditions.Add("dp.metric_id = @metric_id");
            command.Parameters.AddWithValue("@metric_id", metricId);

            var whereClause = conditions.Count != 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

            command.CommandText = $@"
            SELECT 
                r.service_name, dp.time_unix_nano, dp.value_double, dp.value_int, dp.count, dp.sum_value, dp.min_value, dp.max_value
            FROM data_points dp
            JOIN resources r ON dp.resource_id = r.id
            {whereClause}
            ORDER BY r.service_name DESC, dp.time_unix_nano DESC
        ";

            using var reader = command.ExecuteReader();

            if (metric.DataCase == Metric.DataOneofCase.Gauge)
            {
                while (reader.Read())
                {
                    metric.ServiceName = reader.GetString(0);
                    var dataPoint = new NumberDataPoint
                    {
                        TimeUnixNano = (ulong)reader.GetInt64(1),
                        AsDouble = reader.GetDouble(2),
                        AsInt = reader.GetInt64(3)
                    };
                    metric.Gauge.DataPoints.Add(dataPoint);
                }
            }
            else if (metric.DataCase == Metric.DataOneofCase.Sum)
            {
                while (reader.Read())
                {
                    var dataPoint = new NumberDataPoint
                    {
                        TimeUnixNano = (ulong)reader.GetInt64(1),
                        AsDouble = reader.GetDouble(2),
                        AsInt = reader.GetInt64(3)
                    };
                    metric.Sum.DataPoints.Add(dataPoint);
                }
            }
        }

        private void InsertMetricDataPoints(long resourceId, long metricId, long scopeId, Metric metric)
        {
            var command = _connection.CreateCommand();

            var datapoints = new List<NumberDataPoint>();

            switch (metric.DataCase)
            {
                case Metric.DataOneofCase.Gauge:
                    datapoints.AddRange(metric.Gauge.DataPoints);
                    break;

                case Metric.DataOneofCase.Sum:
                    datapoints.AddRange(metric.Sum.DataPoints);
                    break;

                default:
                    return;
            }

            if (datapoints.Count == 0) return;

            var valuesClauses = new List<string>();
            for (int i = 0; i < datapoints.Count; i++)
            {
                valuesClauses.Add($"(@resource_id, @metric_id, @scope_id, @time_unix_nano_{i}, @value_double_{i}, @value_int_{i})");
            }

            command.CommandText = $@"
                INSERT INTO data_points (
                    resource_id, metric_id, scope_id, time_unix_nano,
                    value_double, value_int
                ) VALUES {string.Join(", ", valuesClauses)}
            ";

            command.Parameters.Clear();
            command.Parameters.AddWithValue("@resource_id", resourceId);
            command.Parameters.AddWithValue("@metric_id", metricId);
            command.Parameters.AddWithValue("@scope_id", scopeId);

            for (int i = 0; i < datapoints.Count; i++)
            {
                command.Parameters.AddWithValue($"@time_unix_nano_{i}", datapoints[i].TimeUnixNano);
                command.Parameters.AddWithValue($"@value_double_{i}", datapoints[i].AsDouble);
                command.Parameters.AddWithValue($"@value_int_{i}", datapoints[i].AsInt);
            }

            command.ExecuteNonQuery();
        }


        private long GetOrCreateMetric(Metric metric)
        {
            using var command = _connection.CreateCommand();

            command.CommandText = @"
            SELECT id FROM metrics 
            WHERE metric_name = @metric_name 
            AND metric_type = @metric_type
        ";

            command.Parameters.AddWithValue("@metric_name", metric.Name);
            command.Parameters.AddWithValue("@metric_type", metric.DataCase);

            var existingId = command.ExecuteScalar();
            if (existingId != null)
            {
                return (long)existingId;
            }

            command.CommandText = @"
            INSERT INTO metrics (metric_name, metric_type, metric_unit, metric_description)
            VALUES (@metric_name, @metric_type, @metric_unit, @metric_description);
            SELECT last_insert_rowid();
        ";

            command.Parameters.Clear();
            command.Parameters.AddWithValue("@metric_name", metric.Name);
            command.Parameters.AddWithValue("@metric_type", metric.DataCase);
            command.Parameters.AddWithValue("@metric_unit", metric.Unit);
            command.Parameters.AddWithValue("@metric_description", metric.Description);

            return (long)command.ExecuteScalar()!;
        }

        public List<MetricRecord> GetMetricsForTrace(Span span)
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
}