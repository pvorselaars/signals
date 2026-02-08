using Microsoft.Data.Sqlite;
using System.Text.Json;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;

namespace Signals.Repository;

public sealed partial class Database : IDisposable
{
    private readonly SqliteConnection _connection;

    public Database(string connectionString)
    {
        _connection = new SqliteConnection(connectionString);
        _connection.Open();

        ExecuteNonQuery("PRAGMA journal_mode = WAL");
        ExecuteNonQuery("PRAGMA synchronous = NORMAL");
        ExecuteNonQuery("PRAGMA cache_size = 10000");
        ExecuteNonQuery("PRAGMA temp_store = MEMORY");

        CreateSchema();
    }

    public Database() : this("Data Source=signals.db")
    {
    }

    private void CreateSchema()
    {
        ExecuteNonQuery(File.ReadAllText("schema.sql"));
    }

    public class Query
    {
        public event Action? OnChange;
        internal void NotifyStateChanged() => OnChange?.Invoke();

        private DateTimeOffset? _startTime = DateTimeOffset.UtcNow.AddHours(-1).ToLocalTime();
        public DateTimeOffset? StartTime { get => _startTime; set { if (_startTime != value) { _startTime = value; NotifyStateChanged(); } } }
        private DateTimeOffset? _endTime = DateTimeOffset.UtcNow.ToLocalTime();
        public DateTimeOffset? EndTime { get => _endTime; set { if (_endTime != value) { _endTime = value; NotifyStateChanged(); } } }
        private string? _serviceName;
        public string? ServiceName { get => _serviceName; set { if (_serviceName != value) { _serviceName = value; NotifyStateChanged(); } } }
        private string? _scopeName;
        public string? ScopeName { get => _scopeName; set { if (_scopeName != value) { _scopeName = value; NotifyStateChanged(); } } }
        private int _limit = 1000;
        public int Limit { get => _limit; set { if (_limit != value) { _limit = value; NotifyStateChanged(); } } }
        private int _offset = 0;
        public int Offset { get => _offset; set { if (_offset != value) { _offset = value; NotifyStateChanged(); } } }
        private string _text = string.Empty;
        public string Text { get => _text; set { if (_text != value) { _text = value; NotifyStateChanged(); } } }

        private int? _minSeverity;
        public int? MinSeverity { get => _minSeverity; set { if (_minSeverity != value) { _minSeverity = value; NotifyStateChanged(); } } }

        private string? _metricName;
        public string? MetricName { get => _metricName; set { if (_metricName != value) { _metricName = value; NotifyStateChanged(); } } }

        private string? _spanName;
        public string? SpanName { get => _spanName; set { if (_spanName != value) { _spanName = value; NotifyStateChanged(); } } }

        private string? _partentSpanId;
        public string? ParentSpanId { get => _partentSpanId; set { if (_partentSpanId != value) { _partentSpanId = value; NotifyStateChanged(); } } }
    }

    public List<string> GetUniqueServices()
    {
        var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT service_name FROM resources ORDER BY service_name";

        var services = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            services.Add(reader.GetString(0));
        }

        return services;
    }

    public List<string> GetUniqueScopes(string? serviceName = null)
    {
        var command = _connection.CreateCommand();

        command.CommandText = "SELECT DISTINCT scope_name FROM logs ORDER BY scope_name";

        var scopes = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            scopes.Add(reader.GetString(0));
        }

        return scopes;
    }

    private static string? GetResourceAttribute(Resource resource, string key)
    {
        return resource?.Attributes
            ?.FirstOrDefault(a => a.Key == key)
            ?.Value?.StringValue;
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

    private long GetOrCreateResource(Resource resource)
    {
        var serviceName = GetResourceAttribute(resource, "service.name") ?? "unknown";
        var serviceVersion = GetResourceAttribute(resource, "service.version");
        var serviceInstanceId = GetResourceAttribute(resource, "service.instance.id");
        var attributesJson = JsonSerializer.Serialize(resource?.Attributes);

        using var command = _connection.CreateCommand();

        // Try to find existing resource
        command.CommandText = @"
            SELECT id FROM resources 
            WHERE service_name = @service_name 
            AND (service_version = @service_version OR (service_version IS NULL AND @service_version IS NULL))
            AND (service_instance_id = @service_instance_id OR (service_instance_id IS NULL AND @service_instance_id IS NULL))
        ";

        command.Parameters.AddWithValue("@service_name", serviceName);
        command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);

        var existingId = command.ExecuteScalar();
        if (existingId != null)
        {
            return (long)existingId;
        }

        // Create new resource
        command.CommandText = @"
            INSERT INTO resources (service_name, service_version, service_instance_id, attributes_json)
            VALUES (@service_name, @service_version, @service_instance_id, @attributes_json);
            SELECT last_insert_rowid();
        ";

        command.Parameters.Clear();
        command.Parameters.AddWithValue("@service_name", serviceName);
        command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@attributes_json", attributesJson);

        return (long)command.ExecuteScalar()!;
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