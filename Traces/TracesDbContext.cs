using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;
public class TracesDbContext(DbContextOptions<TracesDbContext> options) : DbContext(options)
{
    public DbSet<Span> Traces { get; set; }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Span>()
            .HasOne(s => s.Parent)
            .WithMany(s => s.Children)
            .HasForeignKey(s => s.ParentSpanId)
            .OnDelete(DeleteBehavior.Restrict);
    }
}
