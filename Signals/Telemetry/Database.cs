using Google.Protobuf;
using LiteDB;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Signals.Telemetry;

public sealed partial class Repository : IDisposable
{
    private readonly LiteDatabase _db;

    public Repository(string path = "signals.db")
    {
        _db = new LiteDatabase(path);

        // Register custom type mappers for protobuf collections
        var mapper = BsonMapper.Global;
        
        mapper.RegisterType
        (
            serialize: SerializeLogRecord,
            deserialize: document => DeserializeLogRecord(document.AsDocument)
        );

        mapper.RegisterType
        (
            serialize: SerializeSpan,
            deserialize: document => DeserializeSpan(document.AsDocument)
        );
        
    }

    public class Query
    {
        public event Action? OnChange;
        internal void NotifyStateChanged() => OnChange?.Invoke();

        private DateTimeOffset? _startTime = DateTimeOffset.UtcNow.AddHours(-1).ToLocalTime();
        public DateTimeOffset? StartTime { get => _startTime; set { if (_startTime != value) { _startTime = value; NotifyStateChanged(); } } }
        private DateTimeOffset? _endTime = DateTimeOffset.UtcNow.ToLocalTime();
        public DateTimeOffset? EndTime { get => _endTime; set { if (_endTime != value) { _endTime = value; NotifyStateChanged(); } } }
        private string? _serviceName;
        public string? ServiceName { get => _serviceName; set { if (_serviceName != value) { _serviceName = value; NotifyStateChanged(); } } }
        private string? _scopeName;
        public string? ScopeName { get => _scopeName; set { if (_scopeName != value) { _scopeName = value; NotifyStateChanged(); } } }
        private int _limit = 1000;
        public int Limit { get => _limit; set { if (_limit != value) { _limit = value; NotifyStateChanged(); } } }
        private int _offset = 0;
        public int Offset { get => _offset; set { if (_offset != value) { _offset = value; NotifyStateChanged(); } } }
        private string _text = string.Empty;
        public string Text { get => _text; set { if (_text != value) { _text = value; NotifyStateChanged(); } } }

        private int? _minSeverity;
        public int? MinSeverity { get => _minSeverity; set { if (_minSeverity != value) { _minSeverity = value; NotifyStateChanged(); } } }

        private string? _metricName;
        public string? MetricName { get => _metricName; set { if (_metricName != value) { _metricName = value; NotifyStateChanged(); } } }

        private string? _spanName;
        public string? SpanName { get => _spanName; set { if (_spanName != value) { _spanName = value; NotifyStateChanged(); } } }

        private ByteString? _partentSpanId;
        public ByteString? ParentSpanId { get => _partentSpanId; set { if (_partentSpanId != value) { _partentSpanId = value; NotifyStateChanged(); } } }
    }

    public void Dispose() => _db.Dispose();

    private static BsonDocument SerializeSpan(Span span)
    {
        var document = new BsonDocument
        {
            ["Name"] = span.Name,
            ["StartTimeUnixNano"] = (long)span.StartTimeUnixNano,
            ["EndTimeUnixNano"] = (long)span.EndTimeUnixNano
        };

        if (span.TraceId?.Length > 0)
        {
            document["TraceId"] = new BsonValue(span.TraceId.ToByteArray());
        }

        if (span.SpanId?.Length > 0)
        {
            document["SpanId"] = new BsonValue(span.SpanId.ToByteArray());
        }

        if (span.ParentSpanId?.Length > 0)
        {
            document["ParentSpanId"] = new BsonValue(span.ParentSpanId.ToByteArray());
        }

        if (span.Attributes.Count > 0)
        {
            var attributesArray = new BsonArray();
            foreach (var attribute in span.Attributes)
            {
                attributesArray.Add(SerializeKeyValue(attribute));
            }

            document["Attributes"] = attributesArray;
        }

        return document;
    }

    private static BsonDocument SerializeLogRecord(LogRecord logRecord)
    {
        var document = new BsonDocument
        {
            ["TimeUnixNano"] = (long)logRecord.TimeUnixNano,
            ["SeverityNumber"] = (int)logRecord.SeverityNumber
        };

        if (logRecord.Body != null)
        {
            document["Body"] = logRecord.GetFormattedBody();
        }

        if (logRecord.TraceId?.Length > 0)
        {
            document["TraceId"] = new BsonValue(logRecord.TraceId.ToByteArray());
        }

        if (logRecord.SpanId?.Length > 0)
        {
            document["SpanId"] = new BsonValue(logRecord.SpanId.ToByteArray());
        }

        if (logRecord.SeverityText != null)
        {
            document["SeverityText"] = logRecord.SeverityText;
        }

        if (logRecord.Attributes.Count > 0)
        {
            var attributesArray = new BsonArray();
            foreach (var attribute in logRecord.Attributes)
            {
                attributesArray.Add(SerializeKeyValue(attribute));
            }

            document["Attributes"] = attributesArray;
        }

        return document;
    }

    private static BsonDocument SerializeMetric(Metric metric)
    {
        var document = new BsonDocument
        {
            ["Name"] = metric.Name,
            ["Description"] = metric.Description,
            ["Unit"] = metric.Unit
        };

        if (metric.DataCase != Metric.DataOneofCase.Gauge)
        {
            var datapoints = new BsonArray();
            foreach (var dp in metric.Gauge.DataPoints)
            {
                datapoints.Add(new BsonDocument
                {
                    ["Value"] = dp.HasAsDouble ? dp.AsDouble : dp.AsInt,
                    ["TimeUnixNano"] = (long)dp.TimeUnixNano,
                });
            }

            document["DataPoints"] = datapoints;
        }

        if (metric.Attributes.Count > 0)
        {
            var attributesArray = new BsonArray();
            foreach (var attribute in metric.Attributes)
            {
                attributesArray.Add(SerializeKeyValue(attribute));
            }

            document["Attributes"] = attributesArray;
        }

        return document;
    }

    private static Metric DeserializeMetric(BsonDocument document)
    {
        var metric = new Metric
        {
            Name = document.TryGetValue("Name", out var name) ? name.AsString : string.Empty,
            Description = document.TryGetValue("Description", out var description) ? description.AsString : string.Empty,
            Unit = document.TryGetValue("Unit", out var unit) ? unit.AsString : string.Empty
        };

        if (document.TryGetValue("DataPoints", out var dataPoints) && dataPoints.IsArray)
        {
            foreach (var dp in dataPoints.AsArray.Where(item => item.IsDocument))
            {
                var dpDoc = dp.AsDocument;
                var dataPoint = new GaugeDataPoint
                {
                    TimeUnixNano = (ulong)dpDoc.TryGetValue("TimeUnixNano", out var time) ? time.AsInt64 : 0
                };

                if (dpDoc.TryGetValue("Value", out var value))
                {
                    if (value.IsDouble)
                        dataPoint.AsDouble = value.AsDouble;
                    else if (value.IsInt32 || value.IsInt64)
                        dataPoint.AsInt = value.AsInt64;
                }

                metric.Gauge.DataPoints.Add(dataPoint);
            }
        }

        if (document.TryGetValue("Attributes", out var attributes) && attributes.IsArray)
        {
            foreach (var attribute in attributes.AsArray.Where(item => item.IsDocument))
            {
                metric.Attributes.Add(DeserializeKeyValue(attribute.AsDocument));
            }
        }

        return metric;
    }

    private static BsonDocument SerializeKeyValue(KeyValue keyValue)
    {
        var document = new BsonDocument
        {
            ["Key"] = keyValue.Key
        };

        if (keyValue.Value != null)
        {
            document["Value"] = SerializeAnyValue(keyValue.Value);
        }

        return document;
    }

    private static BsonDocument SerializeAnyValue(AnyValue value)
    {
        return value.ValueCase switch
        {
            AnyValue.ValueOneofCase.StringValue => new BsonDocument { ["StringValue"] = value.StringValue },
            AnyValue.ValueOneofCase.BoolValue => new BsonDocument { ["BoolValue"] = value.BoolValue },
            AnyValue.ValueOneofCase.IntValue => new BsonDocument { ["IntValue"] = value.IntValue },
            AnyValue.ValueOneofCase.DoubleValue => new BsonDocument { ["DoubleValue"] = value.DoubleValue },
            AnyValue.ValueOneofCase.BytesValue => new BsonDocument { ["BytesValue"] = new BsonValue(value.BytesValue.ToByteArray()) },
            AnyValue.ValueOneofCase.ArrayValue => new BsonDocument
            {
                ["ArrayValue"] = new BsonDocument
                {
                    ["Values"] = new BsonArray(value.ArrayValue.Values.Select(SerializeAnyValue))
                }
            },
            AnyValue.ValueOneofCase.KvlistValue => new BsonDocument
            {
                ["KvlistValue"] = new BsonDocument
                {
                    ["Values"] = new BsonArray(value.KvlistValue.Values.Select(SerializeKeyValue))
                }
            },
            _ => new BsonDocument()
        };
    }

    private static Span DeserializeSpan(BsonDocument document)
    {
        var span = new Span
        {
            Name = document.TryGetValue("Name", out var name) ? name.AsString : string.Empty,
            StartTimeUnixNano = document.TryGetValue("StartTimeUnixNano", out var startTime) ? (ulong)startTime.AsInt64 : 0,
            EndTimeUnixNano = document.TryGetValue("EndTimeUnixNano", out var endTime) ? (ulong)endTime.AsInt64 : 0
        };

        if (document.TryGetValue("TraceId", out var traceIdValue) && traceIdValue.IsBinary)
        {
            span.TraceId = ByteString.CopyFrom(traceIdValue.AsBinary);
        }

        if (document.TryGetValue("SpanId", out var spanIdValue) && spanIdValue.IsBinary)
        {
            span.SpanId = ByteString.CopyFrom(spanIdValue.AsBinary);
        }

        if (document.TryGetValue("ParentSpanId", out var parentSpanIdValue) && parentSpanIdValue.IsBinary)
        {
            span.ParentSpanId = ByteString.CopyFrom(parentSpanIdValue.AsBinary);
        }

        if (document.TryGetValue("Attributes", out var attributesValue) && attributesValue.IsArray)
        {
            foreach (var attributeValue in attributesValue.AsArray.Where(item => item.IsDocument))
            {
                span.Attributes.Add(DeserializeKeyValue(attributeValue.AsDocument));
            }
        }

        return span;
    }

    private static LogRecord DeserializeLogRecord(BsonDocument document)
    {
        var logRecord = new LogRecord();

        if (document.TryGetValue("TimeUnixNano", out var timeValue))
        {
            logRecord.TimeUnixNano = (ulong)timeValue.AsInt64;
        }

        if (document.TryGetValue("SeverityNumber", out var severityValue))
        {
            logRecord.SeverityNumber = (SeverityNumber)severityValue.AsInt32;
        }

        if (document.TryGetValue("Body", out var bodyValue) && bodyValue.IsDocument)
        {
            logRecord.Body = DeserializeAnyValue(bodyValue.AsDocument);
        }

        if (document.TryGetValue("TraceId", out var traceIdValue) && traceIdValue.IsBinary)
        {
            logRecord.TraceId = ByteString.CopyFrom(traceIdValue.AsBinary);
        }

        if (document.TryGetValue("SpanId", out var spanIdValue) && spanIdValue.IsBinary)
        {
            logRecord.SpanId = ByteString.CopyFrom(spanIdValue.AsBinary);
        }

        if (document.TryGetValue("Attributes", out var attributesValue) && attributesValue.IsArray)
        {
            foreach (var attributeValue in attributesValue.AsArray.Where(item => item.IsDocument))
            {
                logRecord.Attributes.Add(DeserializeKeyValue(attributeValue.AsDocument));
            }
        }

        return logRecord;
    }

    private static KeyValue DeserializeKeyValue(BsonDocument document)
    {
        var keyValue = new KeyValue
        {
            Key = document.TryGetValue("Key", out var key) ? key.AsString : string.Empty
        };

        if (document.TryGetValue("Value", out var value) && value.IsDocument)
        {
            keyValue.Value = DeserializeAnyValue(value.AsDocument);
        }

        return keyValue;
    }

    private static AnyValue DeserializeAnyValue(BsonDocument document)
    {
        var anyValue = new AnyValue();

        if (document.TryGetValue("StringValue", out var stringValue))
            anyValue.StringValue = stringValue.AsString;
        else if (document.TryGetValue("BoolValue", out var boolValue))
            anyValue.BoolValue = boolValue.AsBoolean;
        else if (document.TryGetValue("IntValue", out var intValue))
            anyValue.IntValue = intValue.AsInt64;
        else if (document.TryGetValue("DoubleValue", out var doubleValue))
            anyValue.DoubleValue = doubleValue.AsDouble;
        else if (document.TryGetValue("BytesValue", out var bytesValue) && bytesValue.IsBinary)
            anyValue.BytesValue = ByteString.CopyFrom(bytesValue.AsBinary);
        else if (document.TryGetValue("ArrayValue", out var arrayValue) && arrayValue.IsDocument)
            anyValue.ArrayValue = DeserializeArrayValue(arrayValue.AsDocument);
        else if (document.TryGetValue("KvlistValue", out var kvListValue) && kvListValue.IsDocument)
            anyValue.KvlistValue = DeserializeKvListValue(kvListValue.AsDocument);

        return anyValue;
    }

    private static ArrayValue DeserializeArrayValue(BsonDocument document)
    {
        var arrayValue = new ArrayValue();

        if (document.TryGetValue("Values", out var values) && values.IsArray)
        {
            foreach (var item in values.AsArray.Where(value => value.IsDocument))
            {
                arrayValue.Values.Add(DeserializeAnyValue(item.AsDocument));
            }
        }

        return arrayValue;
    }

    private static KeyValueList DeserializeKvListValue(BsonDocument document)
    {
        var kvList = new KeyValueList();

        if (document.TryGetValue("Values", out var values) && values.IsArray)
        {
            foreach (var item in values.AsArray.Where(value => value.IsDocument))
            {
                kvList.Values.Add(DeserializeKeyValue(item.AsDocument));
            }
        }

        return kvList;
    }
}