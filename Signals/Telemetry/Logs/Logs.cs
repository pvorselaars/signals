using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;

namespace OpenTelemetry.Proto.Logs.V1
{
    public partial class LogRecord
    {
        public string ServiceName { get; set; }
        public string ServiceInstanceId { get; set; }
        public InstrumentationScope Scope { get; set; }

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

        public void InsertLogs(IEnumerable<ResourceLogs> resourceLogs)
        {
            using var command = _connection.CreateCommand();

            foreach (var resourceLog in resourceLogs)
            {
                var resourceId = GetOrCreateResource(resourceLog.Resource);

                foreach (var scopeLog in resourceLog.ScopeLogs)
                {
                    var scopeId = GetOrCreateScope(scopeLog.Scope);

                    command.CommandText = @"
                        INSERT INTO logs (
                            resource_id, scope_id, time_unix_nano, observed_time_unix_nano,
                            severity_number, severity_text, body, trace_id, span_id
                        ) VALUES (
                            @resource_id, @scope_id, @time_unix_nano, @observed_time_unix_nano,
                            @severity_number, @severity_text, @body, @trace_id, @span_id
                        )
                    ";

                    foreach (var logRecord in scopeLog.LogRecords)
                    {
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@resource_id", resourceId);
                        command.Parameters.AddWithValue("@scope_id", scopeId);
                        command.Parameters.AddWithValue("@time_unix_nano", (long)logRecord.TimeUnixNano);
                        command.Parameters.AddWithValue("@observed_time_unix_nano",
                            logRecord.ObservedTimeUnixNano > 0 ? (long)logRecord.ObservedTimeUnixNano : DBNull.Value);
                        command.Parameters.AddWithValue("@severity_number", (int)logRecord.SeverityNumber);
                        command.Parameters.AddWithValue("@severity_text", logRecord.SeverityText ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@body", logRecord.GetFormattedBody());
                        command.Parameters.AddWithValue("@trace_id", logRecord.TraceId.ToByteArray());
                        command.Parameters.AddWithValue("@span_id", logRecord.SpanId.ToByteArray());

                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        public List<LogRecord> QueryLogs() => QueryLogs(new Query());

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
                conditions.Add("s.scope_name = @scope");
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

            // Trace filter
            if (query.TraceId != null)
            {
                conditions.Add("l.trace_id = @trace_id");
                command.Parameters.AddWithValue("@trace_id", query.TraceId.ToByteArray());
            }


            var whereClause = conditions.Count != 0 ? "WHERE " + string.Join(" AND ", conditions) : "";

            command.CommandText = $@"
                SELECT 
                    r.id as resource_id, r.service_name, s.id as scope_id, s.scope_name, 
                    l.time_unix_nano, l.observed_time_unix_nano,
                    l.severity_number, l.severity_text, l.body,
                    l.trace_id, l.span_id, r.service_instance_id
                FROM logs l
                JOIN resources r ON l.resource_id = r.id
                JOIN scopes s ON l.scope_id = s.id
                {whereClause}
                ORDER BY l.time_unix_nano DESC
                LIMIT @limit OFFSET @offset
            ";
            command.Parameters.AddWithValue("@limit", query.Limit);
            command.Parameters.AddWithValue("@offset", query.Offset);

            var logs = new List<LogRecord>();

            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                logs.Add(new LogRecord
                {
                    ServiceName = reader.GetString(1),
                    ServiceInstanceId = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                    Scope = new InstrumentationScope {
                        Name = reader.GetString(3),
                        Version = "" // Version is not currently stored in the database, but could be added in the future
                    },
                    TimeUnixNano = (ulong)reader.GetInt64(4),
                    ObservedTimeUnixNano = reader.IsDBNull(5) ? 0UL : (ulong)reader.GetInt64(5),
                    SeverityNumber = (SeverityNumber)reader.GetInt32(6),
                    SeverityText = reader.IsDBNull(7) ? null : reader.GetString(7),
                    Body = reader.IsDBNull(8) ? null : new AnyValue { StringValue = reader.GetString(8) },
                    TraceId = reader.IsDBNull(9) ? ByteString.Empty : ByteString.CopyFrom((byte[])reader.GetValue(9)),
                    SpanId = reader.IsDBNull(10) ? ByteString.Empty : ByteString.CopyFrom((byte[])reader.GetValue(10))
                });
            }

            return logs;
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

        public long GetLogCountForTrace(ByteString traceId)
        {
            var command = _connection.CreateCommand();
            command.CommandText = @"
            SELECT COUNT(*) 
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            WHERE l.trace_id = @trace_id
        ";
            command.Parameters.AddWithValue("@trace_id", traceId.ToByteArray());

            return (long)command.ExecuteScalar()!;
        }

        public long GetLogCountForSpan(ByteString spanId)
        {
            var command = _connection.CreateCommand();
            command.CommandText = @"
            SELECT COUNT(*) 
            FROM logs l
            JOIN resources r ON l.resource_id = r.id
            WHERE l.span_id = @span_id
        ";
            command.Parameters.AddWithValue("@span_id", spanId.ToByteArray());

            return (long)command.ExecuteScalar()!;
        }

        public List<string> GetUniqueLogScopes()
        {
            var command = _connection.CreateCommand();

            command.CommandText = @"
            SELECT DISTINCT s.scope_name 
            FROM logs l
            JOIN scopes s ON l.scope_id = s.id
            ORDER BY s.scope_name
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

}
