CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Core identifiers for filtering
    service_name TEXT NOT NULL,
    service_version TEXT,
    service_instance_id TEXT,
    scope_name TEXT NOT NULL,
    -- Timestamp for range queries (indexed)
    time_unix_nano INTEGER NOT NULL,
    observed_time_unix_nano INTEGER,
    -- Log details
    severity_number INTEGER,
    severity_text TEXT,
    body TEXT,
    -- Full JSON for complete data
    attributes_json TEXT,
    -- Log record attributes
    resource_json TEXT,
    -- Resource attributes
    -- Metadata
    indexed_at INTEGER DEFAULT (strftime('%s', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_logs_scope ON logs(scope_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_logs_service_scope ON logs(service_name, scope_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_logs_severity ON logs(severity_number, time_unix_nano);
CREATE TABLE IF NOT EXISTS traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Core identifiers
    service_name TEXT NOT NULL,
    service_version TEXT,
    service_instance_id TEXT,
    scope_name TEXT NOT NULL,
    -- Trace identifiers
    trace_id BLOB NOT NULL,
    -- 16 bytes
    span_id BLOB NOT NULL,
    -- 8 bytes
    parent_span_id BLOB,
    -- Timestamps
    start_time_unix_nano INTEGER NOT NULL,
    end_time_unix_nano INTEGER NOT NULL,
    -- Span details
    span_name TEXT NOT NULL,
    span_kind INTEGER,
    status_code INTEGER,
    status_message TEXT,
    -- Full JSON
    attributes_json TEXT,
    events_json TEXT,
    links_json TEXT,
    resource_json TEXT,
    -- Metadata
    indexed_at INTEGER DEFAULT (strftime('%s', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_traces_start_time ON traces(start_time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_traces_trace_id ON traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_traces_span_id ON traces(span_id);
CREATE INDEX IF NOT EXISTS idx_traces_service ON traces(service_name, start_time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_traces_scope ON traces(scope_name, start_time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_traces_service_scope ON traces(service_name, scope_name, start_time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_traces_span_name ON traces(span_name, start_time_unix_nano);
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Core identifiers
    service_name TEXT NOT NULL,
    service_version TEXT,
    service_instance_id TEXT,
    scope_name TEXT NOT NULL,
    -- Metric identifiers
    metric_name TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    -- 'gauge', 'sum', 'histogram', 'summary'
    metric_unit TEXT,
    metric_description TEXT,
    -- Timestamp
    time_unix_nano INTEGER NOT NULL,
    -- Metric values (use appropriate field based on type)
    value_double REAL,
    value_int INTEGER,
    -- For histograms/summaries
    count INTEGER,
    sum_value REAL,
    min_value REAL,
    max_value REAL,
    -- Full JSON for complex data
    attributes_json TEXT,
    exemplars_json TEXT,
    buckets_json TEXT,
    -- For histograms
    quantiles_json TEXT,
    -- For summaries
    resource_json TEXT,
    -- Metadata
    indexed_at INTEGER DEFAULT (strftime('%s', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_metrics_time ON metrics(time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_metrics_service ON metrics(service_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_metrics_scope ON metrics(scope_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_metrics_service_scope ON metrics(service_name, scope_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name, time_unix_nano);
CREATE INDEX IF NOT EXISTS idx_metrics_service_name ON metrics(service_name, metric_name, time_unix_nano);