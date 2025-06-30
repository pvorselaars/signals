using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace Signals.Traces;

public class Attribute 
{
    public int Id { get; set; }

    public int KeyId { get; set; }
    public AttributeKey Key { get; set; } = default!;

    public int ValueId { get; set; }
    public AttributeValue Value { get; set; } = default!;

    public ICollection<Resource> Resources { get; set; } = [];
    public static async Task<Attribute> FromProtoAsync(OpenTelemetry.Proto.Common.V1.KeyValue protoAttribute, SignalsDbContext db)
    {

        return new Attribute
        {
            Key = await AttributeKey.FromProto(protoAttribute, db),
            Value = await AttributeValue.FromProtoAsync(protoAttribute, db),
        };
    }
}

public class ResourceAttribute : Attribute
{
    public int ResourceId { get; set; }
    public Resource Resource { get; set; } = default!;
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