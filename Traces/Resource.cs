using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

public class Resource
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public List<Attribute> Attributes { get; set; } = [];

    public static async Task<Resource> FromProto(OpenTelemetry.Proto.Resource.V1.Resource protoResource, SignalsDbContext db)
    {

        var serviceKey = protoResource.Attributes.FirstOrDefault(a => a.Key == "service.name");

        if (serviceKey != null)
        {
            var service = await db.Resources.FirstOrDefaultAsync(s => s.Name == serviceKey.Value.StringValue);

            if (service != null)
            {
                return service;
            }
        }

        var attributeTasks = protoResource.Attributes.Select(a => Attribute.FromProtoAsync(a, db));
        var attributes = await Task.WhenAll(attributeTasks);

        return new Resource
        {
            Name = serviceKey?.Value.StringValue ?? "unknown",
            Attributes = [.. attributes]
        };

    }
}