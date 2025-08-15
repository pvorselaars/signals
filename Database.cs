using System.Data.SQLite;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Signals;

public class Database
{
    private const string connectionString = "Data Source=signals.db;Version=3";

    private readonly SQLiteConnection connection = new(connectionString);

    public void Create()
    {
        connection.Open();

        string sql = @"

        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        );

        CREATE TABLE IF NOT EXISTS scopes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            version TEXT
        );

        CREATE TABLE IF NOT EXISTS spans (
            id TEXT PRIMARY KEY,
            parent TEXT,
            name TEXT,
            start INTEGER,
            end INTEGER,
            FOREIGN KEY(parent) REFERENCES spans(id)
        );

        CREATE TABLE IF NOT EXISTS keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS ""values"" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS attributes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key INTEGER,
            value INTEGER,
            FOREIGN KEY(key) REFERENCES keys(id),
            FOREIGN KEY(value) REFERENCES ""values""(id)
        );

        CREATE TABLE IF NOT EXISTS resource_attributes (
            resource INTEGER,
            attribute INTEGER,
            FOREIGN KEY(resource) REFERENCES resources(id),
            FOREIGN KEY(attribute) REFERENCES attributes(id)
        );

        CREATE TABLE IF NOT EXISTS scope_attributes (
            scope INTEGER,
            attribute INTEGER,
            FOREIGN KEY(scope) REFERENCES scopes(id),
            FOREIGN KEY(attribute) REFERENCES attributes(id)
        );

        CREATE TABLE IF NOT EXISTS span_attributes (
            span TEXT,
            attribute INTEGER,
            FOREIGN KEY(span) REFERENCES spans(id),
            FOREIGN KEY(attribute) REFERENCES attributes(id)
        );

        CREATE TABLE IF NOT EXISTS resource_scopes (
            resource INTEGER,
            scope INTEGER,
            FOREIGN KEY(resource) REFERENCES resources(id),
            FOREIGN KEY(scope) REFERENCES scopes(id)
        );

        CREATE TABLE IF NOT EXISTS scope_spans (
            scope INTEGER,
            span INTEGER,
            FOREIGN KEY(scope) REFERENCES scopes(id),
            FOREIGN KEY(span) REFERENCES spans(id)
        );

        CREATE INDEX IF NOT EXISTS idx_resource_attributes_resource ON resource_attributes(resource);
        CREATE INDEX IF NOT EXISTS idx_resource_attributes_attribute ON resource_attributes(attribute);

        CREATE INDEX IF NOT EXISTS idx_scope_attributes_scope ON scope_attributes(scope);
        CREATE INDEX IF NOT EXISTS idx_scope_attributes_attribute ON scope_attributes(attribute);

        CREATE INDEX IF NOT EXISTS idx_span_attributes_span ON span_attributes(span);
        CREATE INDEX IF NOT EXISTS idx_span_attributes_attribute ON span_attributes(attribute);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_value ON keys(value);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_values_value ON ""values""(value);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_attributes_key_value ON attributes(key, value);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_scopes ON resource_scopes(resource, scope);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_scope_spans ON scope_spans(scope, span);

        ";
        using SQLiteCommand cmd = new(sql, connection);
        cmd.ExecuteNonQuery();
    }

    public async Task AddAsync(IEnumerable<ResourceSpans> resourceSpans)
    {

        foreach (var resourceSpan in resourceSpans)
        {

            var resourceId = await AddIfNotExistsAsync(resourceSpan.Resource);

            foreach (var scopeSpan in resourceSpan.ScopeSpans)
            {
                var scopeId = await AddIfNotExistsAsync(scopeSpan.Scope, resourceId);
                await AddAsync(scopeSpan.Spans, scopeId);
            }
        }

    }

    private async Task AddAsync(IEnumerable<Span> spans, long scopeId)
    {
        var tasks = new List<Task>();

        foreach (var span in spans)
            tasks.Add(AddAsync(span, scopeId));

        await Task.WhenAll(tasks);

    }

    private async Task<long> AddIfNotExistsAsync(InstrumentationScope scope, long resourceId)
    {
        long scopeId = await GetIdAsync(scope);
        if (scopeId == -1)
            scopeId = await AddAsync(scope);

        await using var transaction = connection.BeginTransaction();

        await using SQLiteCommand insertLink = new("INSERT OR IGNORE INTO resource_scopes (resource, scope) VALUES ($resource, $scope)", connection);
        insertLink.Parameters.AddWithValue("$resource", resourceId);
        insertLink.Parameters.AddWithValue("$scope", scopeId);
        await insertLink.ExecuteNonQueryAsync();
        await transaction.CommitAsync();

        return scopeId;
    }

    private async Task<long> AddIfNotExistsAsync(Resource resource)
    {
        long resourceId = await GetIdAsync(resource);

        if (resourceId == -1)
            resourceId = await AddAsync(resource);

        return resourceId;
    }

    private async Task AddAsync(Span span, long scopeId)
    {
        await using var transaction = connection.BeginTransaction();

        await using SQLiteCommand insertResource = new("INSERT OR IGNORE INTO spans (id, parent, name) VALUES ($id, $parent, $name)", connection);
        insertResource.Parameters.AddWithValue("$id", span.SpanId.ToBase64());
        insertResource.Parameters.AddWithValue("$parent", span.ParentSpanId.ToBase64());
        insertResource.Parameters.AddWithValue("$name", span.Name);
        await insertResource.ExecuteNonQueryAsync();
        long spanId = connection.LastInsertRowId;

        await using SQLiteCommand insertLink = new("INSERT OR IGNORE INTO scope_spans (scope, span) VALUES ($scope, $span)", connection);
        insertLink.Parameters.AddWithValue("$scope", scopeId);
        insertLink.Parameters.AddWithValue("$span", spanId);
        await insertLink.ExecuteNonQueryAsync();

        foreach (var a in span.Attributes)
        {
            var keyId = GetOrInsertIdAsync(transaction, "keys", a.Key);
            var valueId = GetOrInsertIdAsync(transaction, "\"values\"", a.Value.StringValue);

            await Task.WhenAll(keyId, valueId);

            var attributeId = await GetOrInsertAttributeIdAsync(transaction, keyId.Result, valueId.Result);

            await using var link = connection.CreateCommand();
            link.Transaction = transaction;
            link.CommandText = @"
            INSERT INTO span_attributes (span, attribute)
            VALUES ($span, $attribute);";
            link.Parameters.AddWithValue("$span", span.SpanId.ToBase64());
            link.Parameters.AddWithValue("$attribute", attributeId);
            await link.ExecuteNonQueryAsync();

        }

        await transaction.CommitAsync();
    }


    private async Task<long> GetIdAsync(InstrumentationScope scope)
    {

        string query = $@"
        SELECT id
        FROM scopes
        WHERE name = $name AND version = $version
        LIMIT 1;";

        await using SQLiteCommand cmd = new(query, connection);

        cmd.Parameters.AddWithValue("$name", scope.Name);
        cmd.Parameters.AddWithValue("$version", scope.Version);

        long scopeId = -1;
        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
            scopeId = reader.GetInt64(0);

        return scopeId;
    }

    private async Task<long> AddAsync(InstrumentationScope scope)
    {
        await using var transaction = connection.BeginTransaction();

        await using SQLiteCommand insertResource = new("INSERT INTO scopes (name, version) VALUES ($name, $version)", connection);
        insertResource.Parameters.AddWithValue("$name", scope.Name);
        insertResource.Parameters.AddWithValue("$version", scope.Version);
        await insertResource.ExecuteNonQueryAsync();
        long scopeId = connection.LastInsertRowId;

        foreach (var a in scope.Attributes)
        {
            var keyId = GetOrInsertIdAsync(transaction, "keys", a.Key);
            var valueId = GetOrInsertIdAsync(transaction, "\"values\"", a.Value.StringValue);

            await Task.WhenAll(keyId, valueId);

            var attributeId = await GetOrInsertAttributeIdAsync(transaction, keyId.Result, valueId.Result);

            await using var link = connection.CreateCommand();
            link.Transaction = transaction;
            link.CommandText = @"
            INSERT INTO scope_attributes (scope, attribute)
            VALUES ($scope, $attribute);";
            link.Parameters.AddWithValue("$scope", scopeId);
            link.Parameters.AddWithValue("$attribute", attributeId);
            await link.ExecuteNonQueryAsync();

        }

        await transaction.CommitAsync();

        return scopeId;
    }

    private async Task<long> GetIdAsync(Resource resource)
    {
        var attributes = resource.Attributes.Where(a => a.Key.StartsWith("service"));
        var conditions = attributes.Select((a, i) => $"(k.value = @key{i} AND v.value = @value{i})");
        var where = string.Join(" OR ", conditions);

        string query = $@"
        SELECT r.id
        FROM resources r
        JOIN (
            SELECT ra.resource
            FROM resource_attributes ra
            WHERE ra.attribute IN (
                SELECT a.id
                FROM attributes a
                JOIN keys k ON a.key = k.id
                JOIN ""values"" v ON a.value = v.id
                WHERE {where}
            )
        )
        LIMIT 1;";

        await using SQLiteCommand cmd = new(query, connection);

        int i = 0;
        foreach (var a in attributes)
        {
            cmd.Parameters.AddWithValue($"@key{i}", a.Key);
            cmd.Parameters.AddWithValue($"@value{i}", a.Value.StringValue);
            i++;
        }

        long resourceId = -1;
        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
            resourceId = reader.GetInt64(0);

        return resourceId;
    }

    private async Task<long> AddAsync(Resource resource)
    {
        await using var transaction = connection.BeginTransaction();

        await using SQLiteCommand insertResource = new("INSERT INTO resources DEFAULT VALUES", connection);
        await insertResource.ExecuteNonQueryAsync();
        long resourceId = connection.LastInsertRowId;

        foreach (var a in resource.Attributes)
        {
            var keyId = GetOrInsertIdAsync(transaction, "keys", a.Key);
            var valueId = GetOrInsertIdAsync(transaction, "\"values\"", a.Value.StringValue);

            await Task.WhenAll(keyId, valueId);

            var attributeId = await GetOrInsertAttributeIdAsync(transaction, keyId.Result, valueId.Result);

            await using var link = connection.CreateCommand();
            link.Transaction = transaction;
            link.CommandText = @"
            INSERT INTO resource_attributes (resource, attribute)
            VALUES ($resource, $attribute);";
            link.Parameters.AddWithValue("$resource", resourceId);
            link.Parameters.AddWithValue("$attribute", attributeId);
            await link.ExecuteNonQueryAsync();

        }

        await transaction.CommitAsync();

        return resourceId;
    }

    // TOOD: store AnyValue
    private async Task<long> GetOrInsertAttributeIdAsync(SQLiteTransaction transaction, long key, long value)
    {
        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = $"INSERT OR IGNORE INTO attributes (key, value) VALUES ($key, $value)";
        insert.Parameters.AddWithValue("$key", key);
        insert.Parameters.AddWithValue("$value", value);
        await insert.ExecuteNonQueryAsync();

        await using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT id FROM attributes WHERE key = $key AND value = $value";
        select.Parameters.AddWithValue("$key", key);
        select.Parameters.AddWithValue("$value", value);
        var result = await select.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private async Task<long> GetOrInsertIdAsync(SQLiteTransaction transaction, string table, string value)
    {
        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = $"INSERT OR IGNORE INTO {table} (value) VALUES ($value)";
        insert.Parameters.AddWithValue("$value", value);
        await insert.ExecuteNonQueryAsync();

        await using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT id FROM {table} WHERE value = $value";
        select.Parameters.AddWithValue("$value", value);
        var result = await select.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    public async Task<IEnumerable<Resource>> GetResourcesAsync()
    {
        await using var select = connection.CreateCommand();
        select.CommandText = @"SELECT
                                r.id AS ResourceId,
                                k.value AS Key,
                                v.value AS Value
                            FROM resources r
                            JOIN resource_attributes ra ON ra.resource = r.id
                            JOIN attributes a ON a.id = ra.attribute
                            JOIN keys k ON k.id = a.key
                            JOIN ""values"" v ON v.id = a.value
                            ORDER BY r.id;";

        await using var reader = await select.ExecuteReaderAsync();

        Dictionary<long, Resource> result = [];

        while (await reader.ReadAsync())
        {

            long resourceId = reader.GetInt64(0);
            string key = reader.GetString(1);
            string value = reader.GetString(2);

            if (!result.ContainsKey(resourceId))
            {
                result[resourceId] = new();
            }

            var val = new AnyValue
            {
                StringValue = value
            };

            result[resourceId].Attributes.Add(new KeyValue { Key = key, Value = val });

        }

        return result.Values;

    }

    public async Task<IEnumerable<InstrumentationScope>> GetScopesAsync()
    {
        await using var select = connection.CreateCommand();
        select.CommandText = @"SELECT
                                    s.id,
                                    s.name,
                                    s.version,
                                    k.value AS Key,
                                    v.value AS Value
                                FROM scopes s
                                LEFT JOIN scope_attributes sa ON s.id = sa.scope
                                LEFT JOIN attributes a ON a.id = sa.attribute
                                LEFT JOIN keys k ON k.id = a.key
                                LEFT JOIN 'values' v ON v.id = a.value
                                ORDER BY s.id;";

        await using var reader = await select.ExecuteReaderAsync();

        Dictionary<long, InstrumentationScope> result = [];

        while (await reader.ReadAsync())
        {

            long scopeId = reader.GetInt64(0);
            string key = reader.GetString(1);
            string value = reader.GetString(2);

            if (!result.ContainsKey(scopeId))
            {
                result[scopeId] = new();
            }

            var val = new AnyValue
            {
                StringValue = value
            };

            result[scopeId].Attributes.Add(new KeyValue { Key = key, Value = val });

        }

        return result.Values;

    }

    public async Task<IEnumerable<Span>> GetSpansAsync()
    {
        await using var select = connection.CreateCommand();
        select.CommandText = @"SELECT
                                    s.id,
                                    s.parent,
                                    s.name,
                                    k.value AS Key,
                                    v.value AS Value
                                FROM spans s
                                LEFT JOIN span_attributes sa ON s.id = sa.span
                                LEFT JOIN attributes a ON a.id = sa.attribute
                                LEFT JOIN keys k ON k.id = a.key
                                LEFT JOIN 'values' v ON v.id = a.value
                                ORDER BY s.parent;";

        await using var reader = await select.ExecuteReaderAsync();

        Dictionary<string, Span> result = [];

        while (await reader.ReadAsync())
        {

            string spanId = reader.GetString(0);
            string name = reader.GetString(2);

            if (!result.ContainsKey(spanId))
            {
                result[spanId] = new Span
                {
                    Name = name
                };
            }

            if (!reader.IsDBNull(2))
            {

                string key = reader.GetString(3);
                string value = reader.GetString(4);

                var val = new AnyValue
                {
                    StringValue = value
                };

                result[spanId].Attributes.Add(new KeyValue { Key = key, Value = val });

            }


        }

        return result.Values;

    }

}