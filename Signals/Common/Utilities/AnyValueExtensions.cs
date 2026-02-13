using OpenTelemetry.Proto.Common.V1;
using static OpenTelemetry.Proto.Common.V1.AnyValue;

namespace Signals.Common.Utilities;
public static partial class AnyValueExtensions { 
    public static string GetAnyValueString(this AnyValue value)
    {
        return value.ValueCase switch
        {
            ValueOneofCase.StringValue => value.StringValue,
            ValueOneofCase.BoolValue => value.BoolValue ? "true" : "false",
            ValueOneofCase.IntValue => value.IntValue.ToString(),
            ValueOneofCase.DoubleValue => value.DoubleValue.ToString(),
            ValueOneofCase.ArrayValue => string.Join(", ", value.ArrayValue.Values),
            _ => value.ToString(),
        };
    }
}