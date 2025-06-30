using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

public class ResourceSpan
{
    public int Id { get; set; }

    public List<Attribute> Attributes { get; set; } = [];

    public static async Task<ResourceSpan> FromProto(OpenTelemetry.Proto.Trace.V1.ResourceSpans protoResourceSpan, SignalsDbContext db)
    {
        var resourceAttributes = protoResourceSpan.Resource.Attributes
            .Select(Attribute.FromProto)
            .ToList();

        var keys = resourceAttributes.Select(a => a.Key).Distinct().ToList();
        var candidateAttributes = await db.Attributes
            .Where(a => keys.Contains(a.Key))
            .ToListAsync();

        var existingSet = candidateAttributes.Select(a => (a.Key, a.Value)).ToHashSet();
        var newAttributes = resourceAttributes
            .Where(ra => !existingSet.Contains((ra.Key, ra.Value)))
            .ToList();

        return new ResourceSpan
        {
            Attributes = [.. candidateAttributes, .. newAttributes],
        };
    }
}