using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

public class Resource
{
    public int Id { get; set; }

    public DateTime LastSignal { get; set; } = DateTime.UtcNow;
    public DateTime FirstSignal { get; set; } = DateTime.UtcNow;
    public ICollection<ResourceAttribute> Attributes { get; set; } = [];

    public static async Task<Resource> FromProto(OpenTelemetry.Proto.Trace.V1.ResourceSpans protoResourceSpan, SignalsDbContext db)
    {

        var attributeTasks = protoResourceSpan.Resource.Attributes.Select(a => Attribute.FromProtoAsync(a, db));
        var attributes = await Task.WhenAll(attributeTasks);

        var serviceAttribute = attributes.FirstOrDefault(a => a.Key.Key == "service.name");
        var instanceAttribute = attributes.FirstOrDefault(a => a.Key.Key == "service.instance.id");

        if (instanceAttribute == null && serviceAttribute != null)
        {
            var resource = await db.Resources
                                   .Include(r => r.Attributes)
                                       .ThenInclude(a => a.Key)
                                   .Include(r => r.Attributes)
                                       .ThenInclude(a => a.Value)
                                   .Where(r => r.Attributes.Any(a => a.Key == serviceAttribute.Key && a.Value == serviceAttribute.Value))
                                   .FirstOrDefaultAsync();

            if (resource != null)
            {
                resource.LastSignal = DateTime.UtcNow;
                return resource;
            }

            return new Resource
            {
                Attributes = [.. attributes.Select(ResourceAttribute.FromAttribute)]
            };
        }
        else if (instanceAttribute != null)
        {
            var resource = await db.Resources
                                   .Include(r => r.Attributes)
                                       .ThenInclude(a => a.Key)
                                   .Include(r => r.Attributes)
                                       .ThenInclude(a => a.Value)
                                   .Where(r => r.Attributes.Any(a => a.Key == instanceAttribute.Key && a.Value == instanceAttribute.Value))
                                   .FirstOrDefaultAsync();

            if (resource != null)
            {
                resource.LastSignal = DateTime.UtcNow;
                return resource;
            }

            return new Resource
            {
                Attributes = [.. attributes.Select(ResourceAttribute.FromAttribute)]
            };
        }
        else
        {
            return new Resource
            {
                Attributes = [.. attributes.Select(ResourceAttribute.FromAttribute)]
            };
        }
    }
}