CREATE TABLE IF NOT EXISTS resources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    service_version TEXT,
    service_instance_id TEXT,
    attributes_json TEXT
);
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Core identifiers for filtering
    resource_id INTEGER,
    scope_name TEXT NOT NULL,
    trace_id TEXT,
    span_id TEXT,
    -- Timestamp for range queries (indexed)
    time_unix_nano INTEGER NOT NULL,
    observed_time_unix_nano INTEGER,
    -- Log details
    severity_number INTEGER,
    severity_text TEXT,
    body TEXT,
    FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS traces (
    -- Core identifiers
    resource_id INTEGER,
    scope_name TEXT NOT NULL,
    -- Trace identifiers
    trace_id TEXT NOT NULL,
    -- 16 bytes
    span_id TEXT PRIMARY KEY,
    -- 8 bytes
    parent_span_id TEXT,
    -- Timestamps
    start_time_unix_nano INTEGER NOT NULL,
    end_time_unix_nano INTEGER NOT NULL,
    -- Span details
    span_name TEXT NOT NULL,
    span_kind INTEGER,
    status_code INTEGER,
    status_message TEXT,
    FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_type INTEGER NOT NULL,
    metric_unit TEXT,
    metric_description TEXT
);
CREATE TABLE IF NOT EXISTS data_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id INTEGER,
    metric_id INTEGER NOT NULL,
    time_unix_nano INTEGER NOT NULL,
    value_double REAL,
    value_int INTEGER,
    count INTEGER,
    sum_value REAL,
    min_value REAL,
    max_value REAL,
    FOREIGN KEY(resource_id) REFERENCES resources(id) ON DELETE CASCADE,
    FOREIGN KEY(metric_id) REFERENCES metrics(id) ON DELETE CASCADE
);