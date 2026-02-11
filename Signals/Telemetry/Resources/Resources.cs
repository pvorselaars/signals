using Google.Protobuf;
using OpenTelemetry.Proto.Resource.V1;

namespace Signals.Telemetry;

public sealed partial class Repository : IDisposable
{
    private static string? GetResourceAttribute(Resource resource, string key)
    {
        return resource?.Attributes
            ?.FirstOrDefault(a => a.Key == key)
            ?.Value?.StringValue;
    }

    private long GetOrCreateResource(Resource resource)
    {
        var serviceName = GetResourceAttribute(resource, "service.name") ?? "unknown";
        var serviceVersion = GetResourceAttribute(resource, "service.version");
        var serviceInstanceId = GetResourceAttribute(resource, "service.instance.id");
        var json = JsonFormatter.Default.Format(resource);

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
            INSERT INTO resources (service_name, service_version, service_instance_id, json)
            VALUES (@service_name, @service_version, @service_instance_id, @json);
            SELECT last_insert_rowid();
        ";

        command.Parameters.Clear();
        command.Parameters.AddWithValue("@service_name", serviceName);
        command.Parameters.AddWithValue("@service_version", serviceVersion ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@service_instance_id", serviceInstanceId ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@json", json);

        return (long)command.ExecuteScalar()!;
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
}