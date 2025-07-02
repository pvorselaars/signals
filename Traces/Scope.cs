using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

public class Scope
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Version { get; set; } = string.Empty;
    
    public ICollection<ScopeAttribute> Attributes { get; set; } = [];
    
    public static async Task<Scope> FromProto(OpenTelemetry.Proto.Trace.V1.ScopeSpans protoScopeSpan, SignalsDbContext db)
    {

        var existingScope = await db.Set<Scope>()
            .Where(s => s.Name == protoScopeSpan.Scope.Name && s.Version == protoScopeSpan.Scope.Version)
            .FirstOrDefaultAsync();

        if (existingScope != null)
            return existingScope;

        var attributeTasks = protoScopeSpan.Scope.Attributes.Select(a => Attribute.FromProtoAsync(a, db));
        var attributes = await Task.WhenAll(attributeTasks);
        
        return new Scope
        {
            Name = protoScopeSpan.Scope.Name,
            Version = protoScopeSpan.Scope.Version,
            Attributes = [.. attributes.Select(ScopeAttribute.FromAttribute)]
        };
    }
}