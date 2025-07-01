using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

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

        var attribute = await db.Attributes.Where(a => a.Key == key && a.Value == value).FirstOrDefaultAsync();

        if (attribute != null)
            return attribute;

        return new Attribute
        {
            Key = await AttributeKey.FromProto(protoAttribute, db),
            Value = await AttributeValue.FromProtoAsync(protoAttribute, db),
        };
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

public class AttributeKey
{
    public int Id { get; set; }
    public string Key { get; set; } = default!;

    public List<Attribute> Attributes { get; set; } = [];

    public static async Task<AttributeKey> FromProto(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {
        var key = await db.Keys.Where(k => k.Key == protoAttribute.Key).FirstOrDefaultAsync();

        if (key != null)
        {
            return key;
        }

        return new AttributeKey
        {
            Key = protoAttribute.Key
        };
    }
}

public class AttributeValue
{
    public int Id { get; set; }
    public string Value { get; set; } = default!;

    public List<Attribute> Attributes { get; set; } = [];
    
    public static async Task<AttributeValue> FromProtoAsync(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {

        var value = await db.Values.Where(v => v.Value == protoAttribute.Value.StringValue).FirstOrDefaultAsync();

        if (value != null)
        {
            return value;
        }

        return new AttributeValue
        {
            Value = protoAttribute.Value.StringValue
        };
    }
}