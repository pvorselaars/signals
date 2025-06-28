using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;
public class TracesDbContext(DbContextOptions<TracesDbContext> options) : DbContext(options)
{
    public DbSet<Span> Traces { get; set; }
}
