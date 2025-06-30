namespace Signals.Traces;

public class Attribute 
{
    public int Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;

    public List<ResourceSpan> ResourceSpans { get; set; } = [];
    public static Attribute FromProto(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute)
    {
        return new Attribute
        {
            Key = protoAttribute.Key,
            Value = protoAttribute.Value.ToString()
        };
    }
}