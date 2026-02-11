using Microsoft.Data.Sqlite;
using Google.Protobuf;
using OpenTelemetry.Proto.Logs.V1;

namespace Signals.Telemetry;

public sealed partial class Repository : IDisposable
{
    private readonly SqliteConnection _connection;

    public Repository(string connectionString)
    {
        _connection = new SqliteConnection(connectionString);
        _connection.Open();

        ExecuteNonQuery("PRAGMA journal_mode = WAL");
        ExecuteNonQuery("PRAGMA synchronous = NORMAL");
        ExecuteNonQuery("PRAGMA cache_size = 10000");
        ExecuteNonQuery("PRAGMA temp_store = MEMORY");

        CreateSchema();
    }

    public Repository() : this("Data Source=signals.db")
    {
    }

    private void CreateSchema()
    {
        ExecuteNonQuery(@"
            CREATE TABLE IF NOT EXISTS resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                service_version TEXT,
                service_instance_id TEXT,
                json TEXT
            );
            CREATE TABLE IF NOT EXISTS scopes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_name TEXT NOT NULL,
                scope_version TEXT
            );
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                -- Core identifiers for filtering
                resource_id INTEGER,
                scope_id INTEGER NOT NULL,
                trace_id BLOB,
                span_id BLOB,
                -- Timestamp for range queries (indexed)
                time_unix_nano UNSIGNED BIG INT NOT NULL,
                observed_time_unix_nano INTEGER,
                -- Log details
                severity_number INTEGER,
                severity_text TEXT,
                body TEXT,
                FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE,
                FOREIGN KEY(scope_id) REFERENCES scopes(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS spans (
                -- Core identifiers
                resource_id INTEGER,
                scope_id INTEGER NOT NULL,
                -- Trace identifiers
                trace_id BLOB NOT NULL,
                -- 16 bytes
                span_id BLOB PRIMARY KEY,
                -- 8 bytes
                parent_span_id BLOB,
                -- Timestamps
                start_time_unix_nano UNSIGNED BIG INT NOT NULL,
                end_time_unix_nano UNSIGNED BIG INT NOT NULL,
                name TEXT,
                json TEXT,
                FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE,
                FOREIGN KEY(scope_id) REFERENCES scopes(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_type INTEGER NOT NULL,
                metric_unit TEXT,
                metric_description TEXT
            );
            CREATE TABLE IF NOT EXISTS data_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_id INTEGER,
                scope_id INTEGER NOT NULL,
                metric_id INTEGER NOT NULL,
                time_unix_nano UNSIGNED BIG INT NOT NULL,
                value_double REAL,
                value_int INTEGER,
                count INTEGER,
                sum_value REAL,
                min_value REAL,
                max_value REAL,
                FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE,
                FOREIGN KEY(metric_id) REFERENCES metrics(id) ON DELETE CASCADE,
                FOREIGN KEY(scope_id) REFERENCES scopes(id) ON DELETE CASCADE
            );
        ");
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

        private SeverityNumber? _minSeverity;
        public SeverityNumber? MinSeverity { get => _minSeverity; set { if (_minSeverity != value) { _minSeverity = value; NotifyStateChanged(); } } }

        private string? _metricName;
        public string? MetricName { get => _metricName; set { if (_metricName != value) { _metricName = value; NotifyStateChanged(); } } }

        private string? _spanName;
        public string? SpanName { get => _spanName; set { if (_spanName != value) { _spanName = value; NotifyStateChanged(); } } }

        private ByteString? _partentSpanId;
        public ByteString? ParentSpanId { get => _partentSpanId; set { if (_partentSpanId != value) { _partentSpanId = value; NotifyStateChanged(); } } }

        private ByteString? _traceId;
        public ByteString? TraceId { get => _traceId; set { if (_traceId != value) { _traceId = value; NotifyStateChanged(); } } }
    }

    private void ExecuteNonQuery(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public void Dispose() => _connection?.Dispose();
}