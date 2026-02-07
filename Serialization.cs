using Google.Protobuf;
using LiteDB;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Logs.V1;
using System.Runtime.Serialization;
using System.Text.Json;

namespace Signals;

public static class Serialization
{
    public static void Configure()
    {
        RegisterProtobufType<ResourceSpans>();
        RegisterProtobufType<ResourceMetrics>();
        RegisterProtobufType<ResourceLogs>();
    }

    private static void RegisterProtobufType<T>() where T : class, IMessage<T>, new()
    {
        BsonMapper.Global.RegisterType<T>(
            serialize: (msg) =>
            {
                var settings = JsonFormatter.Settings.Default;
                var formatter = new JsonFormatter(settings);
                var json = formatter.Format(msg);
                
                using var doc = JsonDocument.Parse(json);
                return ParseToBson(doc.RootElement);
            },
            deserialize: (bson) =>
            {
                // Convert Int64 back to String for Protobuf compatibility
                var sanitized = ConvertBsonInt64ToString(bson);
                var parser = new JsonParser(JsonParser.Settings.Default.WithIgnoreUnknownFields(true));
                return parser.Parse<T>(LiteDB.JsonSerializer.Serialize(sanitized));
            }
        );
    }

    private static BsonValue ParseToBson(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var doc = new BsonDocument();
                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Name.EndsWith("UnixNano") && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        if (long.TryParse(prop.Value.GetString(), out var n))
                        {
                            doc[prop.Name] = new BsonValue(n);
                        }
                        else 
                        {
                             doc[prop.Name] = new BsonValue(0L);
                        }
                    }
                    else
                    {
                        doc[prop.Name] = ParseToBson(prop.Value);
                    }
                }
                return doc;
            
            case JsonValueKind.Array:
                var arr = new BsonArray();
                foreach (var item in element.EnumerateArray())
                {
                    arr.Add(ParseToBson(item));
                }
                return arr;

            case JsonValueKind.String:
                return new BsonValue(element.GetString());

            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l)) return new BsonValue(l);
                if (element.TryGetInt32(out var i)) return new BsonValue(i);
                return new BsonValue(element.GetDouble());

            case JsonValueKind.True:
                return new BsonValue(true);
            
            case JsonValueKind.False:
                return new BsonValue(false);

            case JsonValueKind.Null:
            default:
                return BsonValue.Null;
        }
    }

    private static BsonValue ConvertBsonInt64ToString(BsonValue value)
    {
        if (value.IsDocument)
        {
            var doc = value.AsDocument;
            var newDoc = new BsonDocument();
            foreach (var element in doc)
            {
                newDoc[element.Key] = ConvertBsonInt64ToString(element.Value);
            }
            return newDoc;
        }
        else if (value.IsArray)
        {
            var arr = value.AsArray;
            var newArr = new BsonArray();
            foreach (var item in arr)
            {
                newArr.Add(ConvertBsonInt64ToString(item));
            }
            return newArr;
        }
        else if (value.IsInt64)
        {
            return new BsonValue(value.AsInt64.ToString());
        }
        return value;
    }

    private static object ConvertTimestamps(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var dict = new Dictionary<string, object>();
            foreach (var prop in element.EnumerateObject())
            {
                if (prop.Name.EndsWith("UnixNano") && prop.Value.ValueKind == JsonValueKind.String)
                {
                    dict[prop.Name] = long.TryParse(prop.Value.GetString(), out var n) ? n : 0L;
                }
                else
                {
                    dict[prop.Name] = ConvertTimestamps(prop.Value);
                }
            }
            return dict;
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            return element.EnumerateArray().Select(ConvertTimestamps).ToList();
        }
        else if (element.ValueKind == JsonValueKind.String)
        {
            return element.GetString()!;
        }
        else if (element.ValueKind == JsonValueKind.Number)
        {
            return element.TryGetInt64(out var l) ? l : element.GetDouble();
        }
        else if (element.ValueKind == JsonValueKind.True)
        {
            return true;
        }
        else if (element.ValueKind == JsonValueKind.False)
        {
            return false;
        }
        return null!;
    }
}