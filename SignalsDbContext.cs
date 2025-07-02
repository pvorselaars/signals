using Microsoft.EntityFrameworkCore;
using Signals.Common;

namespace Signals;

public class SignalsDbContext(DbContextOptions<SignalsDbContext> options) : DbContext(options)
{
    public DbSet<Resource> Resources { get; set; }
    public DbSet<InstrumentationScope> Scopes { get; set; }
    public DbSet<Trace.Span> Spans { get; set; }
    public DbSet<Common.Attribute> Attributes { get; set; }
    public DbSet<AttributeKey> Keys { get; set; }
    public DbSet<AttributeValue> Values { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Resource>()
            .HasMany(r => r.Attributes)
            .WithMany(a => a.Resources);

        modelBuilder.Entity<InstrumentationScope>()
            .HasMany(r => r.Attributes)
            .WithMany(a => a.Scopes);
            
        modelBuilder.Entity<Trace.Span>()
            .HasMany(r => r.Attributes)
            .WithMany(a => a.Spans);

        modelBuilder.Entity<AttributeKey>()
            .HasIndex(k => k.Key)
            .IsUnique();

    }
}
