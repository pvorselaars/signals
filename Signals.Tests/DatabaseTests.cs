using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Common.V1;
using Google.Protobuf;
using Signals.Repository;
using static Signals.Repository.Database;

[assembly: Parallelize(Workers = 1, Scope = ExecutionScope.MethodLevel)] // Ensure tests run sequentially to avoid database conflicts
namespace Tests;

[TestClass]
public class DatabaseTests
{
    private Database _database = null!;
    private string _testDbPath = null!;

    [TestInitialize]
    public void Setup()
    {
        // Create a unique test database for each test
        _testDbPath = "tests.db";
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        // We'll need to modify Database to accept a connection string
        _database = new Database($"Data Source={_testDbPath}");
    }

    [TestCleanup]
    public void Cleanup()
    {
        _database?.Dispose();
    }

    [TestMethod]
    public void InsertLogs_ShouldStoreLogsCorrectly()
    {
        // Arrange
        var resourceLogs = CreateTestResourceLogs();

        // Act
        _database.InsertLogs(resourceLogs);

        // Assert
        var query = new Query();
        var logs = _database.QueryLogs(query);

        Assert.HasCount(2, logs);
        Assert.AreEqual("test-service", logs[0].ServiceName);
        Assert.AreEqual("Test log message 1", logs[0].Body);
        Assert.AreEqual(9, logs[0].SeverityNumber); // INFO level
    }

    [TestMethod]
    public void InsertTraces_ShouldStoreTracesCorrectly()
    {
        // Arrange
        var resourceSpans = CreateTestResourceSpans();

        // Act
        _database.InsertTraces(resourceSpans);

        // Assert
        var query = new Query();
        var traces = _database.QueryTraces(query);

        Assert.HasCount(2, traces);
        Assert.AreEqual("test-service", traces[1].ServiceName);
        Assert.AreEqual("root-span", traces[1].SpanName);
    }

    [TestMethod]
    public void InsertMetrics_ShouldStoreMetricsCorrectly()
    {
        // Arrange
        var resourceMetrics = CreateTestResourceMetrics();

        // Act
        _database.InsertMetrics(resourceMetrics);

        // Assert
        var query = new Query();
        var metrics = _database.QueryMetrics(query);

        Assert.HasCount(2, metrics);
        Assert.AreEqual("test-service", metrics[0].ServiceName);
        Assert.AreEqual("test.counter", metrics[0].MetricName);
    }

    [TestMethod]
    public void SpanOrdering_ShouldInsertParentBeforeChild()
    {
        // Arrange - Create spans with parent-child relationship in wrong order
        var resourceSpans = CreateTestResourceSpansWithHierarchy();

        // Act
        _database.InsertTraces(resourceSpans);

        // Assert - Should not throw foreign key constraint error
        var query = new Query();
        var traces = _database.QueryTraces(query);

        Assert.HasCount(3, traces);

        // Verify parent-child relationships exist
        var rootSpan = traces.First(t => t.ParentSpanId == null);
        var childSpans = traces.Where(t => t.ParentSpanId != null).ToList();

        Assert.IsNotNull(rootSpan);
        Assert.HasCount(2, childSpans);
    }

    [TestMethod]
    public void GetLogCountForSpan_ShouldReturnCorrectCount()
    {
        // Arrange
        var resourceLogs = CreateTestResourceLogsWithTraceContext();
        var resourceSpans = CreateTestResourceSpans();

        // Act
        _database.InsertLogs(resourceLogs);
        _database.InsertTraces(resourceSpans);

        var spans = _database.QueryTraces(new Query());
        var span = spans.First();

        var logCount = _database.GetLogCountForSpan(span.TraceId, span.SpanId);

        // Assert
        Assert.AreEqual(2, logCount);
    }

    [TestMethod]
    public void GetMetricsForSpan_ShouldReturnCorrelatedMetrics()
    {
        // Arrange
        var resourceMetrics = CreateTestResourceMetrics();
        var resourceSpans = CreateTestResourceSpans();

        // Act
        _database.InsertMetrics(resourceMetrics);
        _database.InsertTraces(resourceSpans);

        var spans = _database.QueryTraces(new Query());
        var span = spans.First();

        var metrics = _database.GetMetricsForSpan(span);

        // Assert
        Assert.IsNotEmpty(metrics);
        Assert.AreEqual("test-service", metrics[0].ServiceName);
    }

    [TestMethod]
    public void QueryLogs_WithTextFilter_ShouldReturnMatchingLogs()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        var query = new Query { Text = "message 1" };

        // Act
        var logs = _database.QueryLogs(query);

        // Assert
        Assert.HasCount(1, logs);
        Assert.IsNotNull(logs[0].Body);
        Assert.Contains("message 1", logs[0].Body!);
    }

    [TestMethod]
    public void QueryTraces_WithServiceFilter_ShouldReturnMatchingTraces()
    {
        // Arrange
        _database.InsertTraces(CreateTestResourceSpans());

        var query = new Query { ServiceName = "test-service" };

        // Act
        var traces = _database.QueryTraces(query);

        // Assert
        Assert.HasCount(2, traces);
        Assert.IsTrue(traces.All(t => t.ServiceName == "test-service"));
    }

    [TestMethod]
    public void QueryMetrics_WithTimeRange_ShouldReturnMatchingMetrics()
    {
        // Arrange
        _database.InsertMetrics(CreateTestResourceMetrics());

        var baseTime = DateTimeOffset.UtcNow.AddHours(-1);
        var query = new Query
        {
            StartTime = baseTime.AddMinutes(-30),
            EndTime = baseTime.AddMinutes(30)
        };

        // Act
        var metrics = _database.QueryMetrics(query);

        // Assert
        Assert.IsNotEmpty(metrics);
    }

    [TestMethod]
    public void BatchInsertDataPoints_ShouldInsertAllPointsEfficiently()
    {
        // Arrange
        var resourceMetrics = CreateTestResourceMetricsWithManyDataPoints();

        // Act
        _database.InsertMetrics(resourceMetrics);

        // Assert
        var query = new Query();
        var metrics = _database.QueryMetrics(query);

        Assert.HasCount(10, metrics); // Should have all 10 data points
    }

    // Helper methods to create test data

    private ResourceLogs CreateTestResourceLogs()
    {
        var resource = CreateTestResource();
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime,
            SeverityNumber = SeverityNumber.Info,
            SeverityText = "INFO",
            Body = new AnyValue { StringValue = "Test log message 1" }
        });

        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime - 1_000_000,
            SeverityNumber = SeverityNumber.Error,
            SeverityText = "ERROR",
            Body = new AnyValue { StringValue = "Test log message 2" }
        });

        var resourceLogs = new ResourceLogs { Resource = resource };
        resourceLogs.ScopeLogs.Add(scopeLogs);
        return resourceLogs;
    }

    private ResourceLogs CreateTestResourceLogsWithTraceContext()
    {
        var resourceLogs = CreateTestResourceLogs();
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
        var spanId = ByteString.CopyFrom(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 });

        foreach (var scopeLog in resourceLogs.ScopeLogs)
        {
            foreach (var logRecord in scopeLog.LogRecords)
            {
                logRecord.TraceId = traceId;
                logRecord.SpanId = spanId;
            }
        }

        return resourceLogs;
    }

    private ResourceSpans CreateTestResourceSpans()
    {
        var resource = CreateTestResource();
        var scopeSpans = new ScopeSpans
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = ByteString.CopyFrom(new byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 }),
            Name = "root-span",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = baseTime - 1_000_000_000, // 1 second duration
            EndTimeUnixNano = baseTime
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = ByteString.CopyFrom(new byte[8] { 9, 10, 11, 12, 13, 14, 15, 16 }),
            Name = "child-span",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = baseTime - 500_000_000,
            EndTimeUnixNano = baseTime - 250_000_000
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return resourceSpans;
    }

    private ResourceSpans CreateTestResourceSpansWithHierarchy()
    {
        var resource = CreateTestResource();
        var scopeSpans = new ScopeSpans
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
        var rootSpanId = ByteString.CopyFrom(new byte[8] { 1, 1, 1, 1, 1, 1, 1, 1 });
        var child1SpanId = ByteString.CopyFrom(new byte[8] { 2, 2, 2, 2, 2, 2, 2, 2 });
        var child2SpanId = ByteString.CopyFrom(new byte[8] { 3, 3, 3, 3, 3, 3, 3, 3 });

        // Add in wrong order - children first, then parent
        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = child1SpanId,
            ParentSpanId = rootSpanId,
            Name = "child-span-1",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = baseTime + 100_000_000,
            EndTimeUnixNano = baseTime + 300_000_000
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = child2SpanId,
            ParentSpanId = rootSpanId,
            Name = "child-span-2",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = baseTime + 400_000_000,
            EndTimeUnixNano = baseTime + 600_000_000
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = rootSpanId,
            Name = "root-span",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = baseTime,
            EndTimeUnixNano = baseTime + 1_000_000_000
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return resourceSpans;
    }

    private ResourceMetrics CreateTestResourceMetrics()
    {
        var resource = CreateTestResource();
        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        // Counter metric
        var counterMetric = new Metric
        {
            Name = "test.counter",
            Description = "Test counter metric",
            Unit = "requests",
            Sum = new Sum
            {
                AggregationTemporality = AggregationTemporality.Cumulative
            }
        };

        counterMetric.Sum.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = baseTime,
            AsInt = 42
        });

        // Gauge metric  
        var gaugeMetric = new Metric
        {
            Name = "test.gauge",
            Description = "Test gauge metric",
            Unit = "percent",
            Gauge = new Gauge()
        };

        gaugeMetric.Gauge.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = baseTime + 1_000_000,
            AsDouble = 85.5
        });

        scopeMetrics.Metrics.Add(counterMetric);
        scopeMetrics.Metrics.Add(gaugeMetric);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return resourceMetrics;
    }

    private ResourceMetrics CreateTestResourceMetricsWithManyDataPoints()
    {
        var resource = CreateTestResource();
        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        var counterMetric = new Metric
        {
            Name = "test.batch.counter",
            Description = "Test batch counter metric",
            Unit = "operations",
            Sum = new Sum
            {
                AggregationTemporality = AggregationTemporality.Cumulative
            }
        };

        // Add 10 data points
        for (int i = 0; i < 10; i++)
        {
            counterMetric.Sum.DataPoints.Add(new NumberDataPoint
            {
                TimeUnixNano = baseTime + (ulong)(i * 1_000_000),
                AsInt = i * 10
            });
        }

        scopeMetrics.Metrics.Add(counterMetric);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return resourceMetrics;
    }

    private Resource CreateTestResource()
    {
        var resource = new Resource();

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.name",
            Value = new AnyValue { StringValue = "test-service" }
        });

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.version",
            Value = new AnyValue { StringValue = "1.0.0" }
        });

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.instance.id",
            Value = new AnyValue { StringValue = "test-instance-1" }
        });

        return resource;
    }
}