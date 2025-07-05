using System.Data.SQLite;
using Google.Protobuf.WellKnownTypes;
using OpenTelemetry.Proto.Resource.V1;

namespace Signals;

public class Database
{
    const string connectionString = "Data Source=signals2.db;Version=3";

    private readonly SQLiteConnection connection = new(connectionString);

    public void Create()
    {
        connection.Open();

        string sql = @"

        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT
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

        CREATE INDEX IF NOT EXISTS idx_resource_attributes_resource ON resource_attributes(resource);
        CREATE INDEX IF NOT EXISTS idx_resource_attributes_attribute ON resource_attributes(attribute);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_value ON keys(value);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_values_value ON ""values""(value);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_attributes_key_value ON attributes(key, value);

        ";
        SQLiteCommand cmd = new(sql, connection);
        cmd.ExecuteNonQuery();
        cmd.Dispose();
    }

    public async Task AddResourcesAsync(IEnumerable<Resource> resources)
    {
        var tasks = new List<Task>();

        foreach (var resource in resources)
        {

            tasks.Add(ProcessResourceAsync(resource));

        }

        await Task.WhenAll(tasks);

    }

    private async Task ProcessResourceAsync(Resource resource)
    {
        if (!await ResourceExistsAsync(resource))
            await AddResourceAsync(resource);

    }

    private async Task AddResourceAsync(Resource resource)
    {
        using var transaction = connection.BeginTransaction();

        using SQLiteCommand insertResource = new("INSERT INTO resources DEFAULT VALUES", connection);
        await insertResource.ExecuteNonQueryAsync();
        long resourceId = connection.LastInsertRowId;

        foreach (var a in resource.Attributes)
        {
            var keyId = GetOrInsertIdAsync(transaction, "keys", a.Key);
            var valueId = GetOrInsertIdAsync(transaction, "\"values\"", a.Value.StringValue);

            await Task.WhenAll(keyId, valueId);

            var attributeId = await GetOrInsertAttributeIdAsync(transaction, keyId.Result, valueId.Result);

            using var link = connection.CreateCommand();
            link.Transaction = transaction;
            link.CommandText = @"
            INSERT INTO resource_attributes (resource, attribute)
            VALUES ($resource, $attribute);";
            link.Parameters.AddWithValue("$resource", resourceId);
            link.Parameters.AddWithValue("$attribute", attributeId);
            await link.ExecuteNonQueryAsync();

        }

        await transaction.CommitAsync();
    }

    private async Task<long> GetOrInsertAttributeIdAsync(SQLiteTransaction transaction, long key, long value)
    {
        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = $"INSERT OR IGNORE INTO attributes (key, value) VALUES ($key, $value)";
        insert.Parameters.AddWithValue("$key", key);
        insert.Parameters.AddWithValue("$value", value);
        await insert.ExecuteNonQueryAsync();

        using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT id FROM attributes WHERE key = $key AND value = $value";
        select.Parameters.AddWithValue("$key", key);
        select.Parameters.AddWithValue("$value", value);
        var result = await select.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private async Task<long> GetOrInsertIdAsync(SQLiteTransaction transaction, string table, string value)
    {
        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = $"INSERT OR IGNORE INTO {table} (value) VALUES ($value)";
        insert.Parameters.AddWithValue("$value", value);
        await insert.ExecuteNonQueryAsync();

        using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT id FROM {table} WHERE value = $value";
        select.Parameters.AddWithValue("$value", value);
        var result = await select.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private async Task<bool> ResourceExistsAsync(Resource resource)
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

        using SQLiteCommand cmd = new(query, connection);

        int i = 0;
        foreach (var a in attributes)
        {
            cmd.Parameters.AddWithValue($"@key{i}", a.Key);
            cmd.Parameters.AddWithValue($"@value{i}", a.Value.StringValue);
            i++;
        }

        using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync();
    }

    public async Task<Dictionary<long, Resource>> GetResourcesAsync()
    {

        using var select = connection.CreateCommand();
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

        using var reader = await select.ExecuteReaderAsync();

        Dictionary<long, Resource> result = [];

        while (await reader.ReadAsync())
        {

            long resourceId = reader.GetInt64(0);
            string key = reader.GetString(1);
            string value = reader.GetString(2);

            if (!result.TryGetValue(resourceId, out var r))
            {
                result[resourceId] = new();
            }

            var val = new OpenTelemetry.Proto.Common.V1.AnyValue
            {
                StringValue = value
            };

            result[resourceId].Attributes.Add(new OpenTelemetry.Proto.Common.V1.KeyValue { Key = key, Value = val });

        }

        return result;

    }

}