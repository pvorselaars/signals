using Microsoft.EntityFrameworkCore;

namespace Signals.Common;

public class Attribute
{
    public int Id { get; set; }

    public int KeyId { get; set; }
    public AttributeKey Key { get; set; } = default!;

    public int ValueId { get; set; }
    public AttributeValue Value { get; set; } = default!;

    public static async Task<Attribute> FromProtoAsync(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {

        var key = await AttributeKey.FromProto(protoAttribute, db);
        var value = await AttributeValue.FromProtoAsync(protoAttribute, db);

        var trackedAttribute = await db.Attributes.FirstOrDefaultAsync(a => a.Key == key && a.Value == value);

        if (trackedAttribute != null)
            return trackedAttribute;

        var attribute = await db.Attributes.Where(a => a.Key == key && a.Value == value).FirstOrDefaultAsync();

        if (attribute != null)
            return attribute;

        var newAttribute = new Attribute
        {
            Key = await AttributeKey.FromProto(protoAttribute, db),
            Value = await AttributeValue.FromProtoAsync(protoAttribute, db),
        };

        db.Attributes.Add(newAttribute);
        return newAttribute;
    }

    public override string ToString() => $"{Key.Key}: {Value.Value}";
}

public class ResourceAttribute : Attribute
{
    public ICollection<Resource> Resources { get; set; } = default!;

    public static ResourceAttribute FromAttribute(Attribute attribute)
    {
        return new ResourceAttribute
        {
            Key = attribute.Key,
            Value = attribute.Value
        };
    }

}

public class ScopeAttribute : Attribute
{
    public ICollection<InstrumentationScope> Scopes { get; set; } = default!;

    public static ScopeAttribute FromAttribute(Attribute attribute)
    {
        return new ScopeAttribute
        {
            Key = attribute.Key,
            Value = attribute.Value
        };
    }

}

public class SpanAttribute : Attribute
{
    public ICollection<Trace.Span> Spans { get; set; } = default!;

    public static SpanAttribute FromAttribute(Attribute attribute)
    {
        return new SpanAttribute
        {
            Key = attribute.Key,
            Value = attribute.Value
        };
    }

}

public class AttributeKey
{
    public int Id { get; set; }
    public string Key { get; set; } = default!;

    public List<Attribute> Attributes { get; set; } = [];

    public static async Task<AttributeKey> FromProto(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {
        var trackedKey = db.Keys.Local.FirstOrDefault(k => k.Key == protoAttribute.Key);
        if (trackedKey != null)
            return trackedKey;

        var key = await db.Keys.Where(k => k.Key == protoAttribute.Key).FirstOrDefaultAsync();

        if (key != null)
            return key;

        var newKey = new AttributeKey { Key = protoAttribute.Key };

        db.Keys.Add(newKey);
        return newKey;
    }
}

public class AttributeValue
{
    public int Id { get; set; }
    public string Value { get; set; } = default!;

    public List<Attribute> Attributes { get; set; } = [];

    public static async Task<AttributeValue> FromProtoAsync(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {
        var value = protoAttribute.Value.ValueCase switch
        {
            OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.StringValue => protoAttribute.Value.StringValue,
            OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.IntValue => protoAttribute.Value.IntValue.ToString(),
            OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.DoubleValue => protoAttribute.Value.DoubleValue.ToString(),
            OpenTelemetry.Proto.Common.V1.AnyValue.ValueOneofCase.BoolValue => protoAttribute.Value.BoolValue.ToString(),
            _ => throw new NotSupportedException($"Unsupported value type: {protoAttribute.Value.ValueCase}")
        };

        var trackedValue = db.Values.Local.FirstOrDefault(k => k.Value == value);
        if (trackedValue != null)
            return trackedValue;

        var storedValue = await db.Values.Where(v => v.Value == value).FirstOrDefaultAsync();
        if (storedValue != null)
            return storedValue;

        var newValue = new AttributeValue { Value = value};

        db.Values.Add(newValue);
        return newValue;
    }
}