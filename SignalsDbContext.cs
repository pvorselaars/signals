using Microsoft.EntityFrameworkCore;
using Signals.Traces;

namespace Signals;
public class SignalsDbContext(DbContextOptions<SignalsDbContext> options) : DbContext(options)
{
    public DbSet<Scope> Scopes { get; set; }
    public DbSet<Span> Traces { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Span>()
            .HasOne(s => s.Parent)
            .WithMany(s => s.Children)
            .HasForeignKey(s => s.ParentSpanId)
            .OnDelete(DeleteBehavior.Restrict);

        modelBuilder.Entity<Span>()
            .HasOne(s => s.Scope);
    }
}
    
