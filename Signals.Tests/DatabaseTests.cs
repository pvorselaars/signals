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
public class DatabaseTests(TestContext context)
{
    private Database _database = null!;
    private string _testDbPath = null!;

    public TestContext TestContext { get; set; } = context;

    [TestInitialize]
    public void Setup()
    {
        // Create a unique test database for each test
        _testDbPath = $"TestData_{TestContext.TestName}.db";
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
    public void InsertTraces_ShouldStoreTracesCorrectly()
    {
        // Arrange
        var resourceSpans = CreateTestResourceSpans();

        // Act
        _database.InsertTraces(resourceSpans);

        // Assert
        var query = new Query();
        var traces = _database.QuerySpans(query);

        Assert.HasCount(2, traces);
        Assert.AreEqual("test-service", traces[1].ServiceName);
        Assert.AreEqual("root-span", traces[1].Name);
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
        Assert.AreEqual("test.counter", metrics[0].Name);
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
        var traces = _database.QuerySpans(query);

        Assert.HasCount(3, traces);

        // Verify parent-child relationships exist
        var rootSpan = traces.First(t => t.ParentSpanId.Length == 0);
        var childSpans = traces.Where(t => t.ParentSpanId.Length != 0).ToList();

        Assert.IsNotNull(rootSpan);
        Assert.HasCount(2, childSpans);
    }

    [TestMethod]
    public void GetMetricsForSpan_ShouldReturnCorrelatedMetrics()
    {
        // Arrange
        var time = DateTimeOffset.UtcNow;
        var resourceMetrics = CreateTestResourceMetrics(time);
        var resourceSpans = CreateTestResourceSpans(null, time);

        // Act
        _database.InsertMetrics(resourceMetrics);
        _database.InsertTraces(resourceSpans);

        // Query root span
        var spans = _database.QuerySpans(new Query { ParentSpanId = ByteString.Empty } );

        var metrics = _database.GetMetricsForTrace(spans[0]);

        // Assert
        Assert.IsNotEmpty(metrics);
        Assert.AreEqual("test-service", metrics[0].ServiceName);
    }


    [TestMethod]
    public void QueryTraces_WithServiceFilter_ShouldReturnMatchingTraces()
    {
        // Arrange
        _database.InsertTraces(CreateTestResourceSpans());

        var query = new Query { ServiceName = "test-service" };

        // Act
        var traces = _database.QuerySpans(query);

        // Assert
        Assert.HasCount(2, traces);
        Assert.IsTrue(traces.All(t => t.ServiceName == "test-service"));
    }

    [TestMethod]
    public void QueryMetrics_WithTimeRange_ShouldReturnMatchingMetrics()
    {
        // Arrange
        var baseTime = DateTimeOffset.UtcNow.AddHours(-1);
        _database.InsertMetrics(CreateTestResourceMetrics(baseTime));

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
        var spanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]);

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

    private ResourceSpans CreateTestResourceSpans(ByteString? traceId = null, DateTimeOffset? time = null)
    {
        var resource = CreateTestResource();
        var scopeSpans = new ScopeSpans
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        time ??= DateTimeOffset.UtcNow;
        var baseTime = (ulong)(time.Value.ToUnixTimeSeconds() * 1_000_000_000);

        traceId ??= ByteString.CopyFrom(Guid.NewGuid().ToByteArray());

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
            Name = "root-span",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = baseTime - 1_000_000_000, // 1 second duration
            EndTimeUnixNano = baseTime
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = ByteString.CopyFrom([9, 10, 11, 12, 13, 14, 15, 16]),
            ParentSpanId = scopeSpans.Spans[0].SpanId,
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
        var rootSpanId = ByteString.CopyFrom([1, 1, 1, 1, 1, 1, 1, 1]);
        var child1SpanId = ByteString.CopyFrom([2, 2, 2, 2, 2, 2, 2, 2]);
        var child2SpanId = ByteString.CopyFrom([3, 3, 3, 3, 3, 3, 3, 3]);

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = child1SpanId,
            ParentSpanId = rootSpanId,
            Name = "child-span-1",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = baseTime - 100_000_000,
            EndTimeUnixNano = baseTime - 300_000_000
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = child2SpanId,
            ParentSpanId = rootSpanId,
            Name = "child-span-2",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = baseTime - 400_000_000,
            EndTimeUnixNano = baseTime - 600_000_000
        });

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = rootSpanId,
            Name = "root-span",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = baseTime - 1_000_000_000,
            EndTimeUnixNano = baseTime
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return resourceSpans;
    }

    private ResourceMetrics CreateTestResourceMetrics(DateTimeOffset? time = null)
    {
        var resource = CreateTestResource();
        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        time ??= DateTimeOffset.UtcNow;

        var baseTime = (ulong)(time.Value.ToUnixTimeSeconds() * 1_000_000_000);

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
            TimeUnixNano = baseTime - 1_000_000,
            AsInt = 42
        });

        var gaugeMetric = new Metric
        {
            Name = "test.gauge",
            Description = "Test gauge metric",
            Unit = "percent",
            Gauge = new Gauge()
        };

        gaugeMetric.Gauge.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = baseTime - 2_000_000,
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
                TimeUnixNano = baseTime - (ulong)(i * 1_000_000),
                AsInt = i * 10
            });
        }

        scopeMetrics.Metrics.Add(counterMetric);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return resourceMetrics;
    }

    private static Resource CreateTestResource()
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