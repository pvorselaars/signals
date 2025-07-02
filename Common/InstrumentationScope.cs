using Microsoft.EntityFrameworkCore;

namespace Signals.Common;

public class InstrumentationScope
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Version { get; set; } = string.Empty;
    
    public ICollection<ScopeAttribute> Attributes { get; set; } = [];
    
    public static async Task<InstrumentationScope> FromProtoAsync(OpenTelemetry.Proto.Trace.V1.ScopeSpans protoScopeSpan, SignalsDbContext db)
    {

        var existingScope = await db.Scopes
            .Where(s => s.Name == protoScopeSpan.Scope.Name && s.Version == protoScopeSpan.Scope.Version)
            .FirstOrDefaultAsync();

        if (existingScope != null)
            return existingScope;

        var attributeTasks = protoScopeSpan.Scope.Attributes.Select(a => Attribute.FromProtoAsync(a, db));
        var attributes = await Task.WhenAll(attributeTasks);
        
        return new InstrumentationScope
        {
            Name = protoScopeSpan.Scope.Name,
            Version = protoScopeSpan.Scope.Version,
            Attributes = [.. attributes.Select(ScopeAttribute.FromAttribute)]
        };
    }
}