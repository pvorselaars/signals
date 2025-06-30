using System.ComponentModel.DataAnnotations;

namespace Signals.Traces;

public class Scope(string name, string version, string? schemaUrl)
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = name;
    public string Version { get; set; } = version;
    public string? SchemaUrl { get; set; } = schemaUrl;

    public override string ToString() => string.IsNullOrEmpty(Version) ? Name : $"{Name} ({Version})";
}