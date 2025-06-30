using Microsoft.EntityFrameworkCore;
using Signals.Traces;

namespace Signals;
public class SignalsDbContext(DbContextOptions<SignalsDbContext> options) : DbContext(options)
{
    public DbSet<ResourceSpan> ResourceSpans { get; set; }
    public DbSet<Traces.Attribute> Attributes { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder) {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ResourceSpan>()
            .HasMany(s => s.Attributes)
            .WithMany(r => r.ResourceSpans);

    }
}
    
