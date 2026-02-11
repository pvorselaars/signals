using OpenTelemetry.Proto.Common.V1;

namespace Signals.Telemetry;

public sealed partial class Repository : IDisposable
{
    private long GetOrCreateScope(InstrumentationScope scope)
    {
        var command = _connection.CreateCommand();
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

    public List<InstrumentationScope> GetUniqueScopes()
    {
        var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT scope_name FROM scopes";
        var reader = command.ExecuteReader();

        var scopes = new List<InstrumentationScope>();
        while (reader.Read())
        {
            scopes.Add(new InstrumentationScope
            {
                Name = reader.GetString(0)
            });

        }
        return scopes;
    }
}