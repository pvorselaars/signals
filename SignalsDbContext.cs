using Microsoft.EntityFrameworkCore;
using Signals.Traces;

namespace Signals;

public class SignalsDbContext(DbContextOptions<SignalsDbContext> options) : DbContext(options)
{
    public DbSet<Resource> Resources { get; set; }
    public DbSet<Traces.Attribute> Attributes { get; set; }
    public DbSet<AttributeKey> Keys { get; set; }
    public DbSet<AttributeValue> Values { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Resource>()
            .HasMany(r => r.Attributes)
            .WithMany(a => a.Resources);

    }

    public async Task AddResourceSpansAsync(OpenTelemetry.Proto.Trace.V1.ResourceSpans[] protoResourceSpans)
    {

        var tasks = protoResourceSpans.Select(async span =>
        {
            var entity = await Resource.FromProto(span, this);
            if (entity.Id == 0)
            {
                Resources.Add(entity);
            }
        });

        await Task.WhenAll(tasks);

    }
}

    
