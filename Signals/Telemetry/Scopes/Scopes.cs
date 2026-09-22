using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Common.V1;

namespace Signals.Telemetry;

public sealed partial class Repository
{
    private static long GetOrCreateScope(SqliteTransaction transaction, InstrumentationScope scope)
    {
        using var command = transaction.Connection!.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            SELECT id FROM scopes 
            WHERE scope_name = @scope_name
            AND scope_version = @scope_version
        ";

        command.Parameters.AddWithValue("@scope_name", scope.Name);
        command.Parameters.AddWithValue("@scope_version", scope.Version ?? (object)DBNull.Value);
        var existingId = command.ExecuteScalar();
        if (existingId != null)
            return (long)existingId;

        command.CommandText = @"
            INSERT INTO scopes (scope_name, scope_version)
            VALUES (@scope_name, @scope_version);
            SELECT last_insert_rowid();
        ";

        return (long)command.ExecuteScalar()!;
    }

    // With no table, returns every known scope name. Given a table name (e.g.
    // "spans" or "logs"), returns only scope names actually used by rows in
    // that table - both call sites just need the JOIN's target table name,
    // which is always a hardcoded literal from our own code, never user input.
    public List<string> GetUniqueScopes(string? table = null)
    {
        using var connection = CreateConnection();
        using var command = connection.CreateCommand();
        command.CommandText = table is null
            ? "SELECT DISTINCT scope_name FROM scopes ORDER BY scope_name"
            : $@"
                SELECT DISTINCT s.scope_name
                FROM {table} t
                JOIN scopes s ON t.scope_id = s.id
                ORDER BY s.scope_name
            ";
        using var reader = command.ExecuteReader();

        var scopes = new List<string>();
        while (reader.Read())
        {
            scopes.Add(reader.GetString(0));
        }
        return scopes;
    }
}