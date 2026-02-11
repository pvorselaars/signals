using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Common.V1;
using Google.Protobuf;
using Signals.Telemetry;
using static Signals.Telemetry.Repository;
using Signals.Common.Utilities;

namespace Tests;

[TestClass]
public class IntegrationTests(TestContext testContext)
{
    private Repository _repository = null!;
    private string _testDbPath = null!;

    public TestContext TestContext { get; set; } = testContext;

    [TestInitialize]
    public void Setup()
    {
        // Create a unique test database for each test
        _testDbPath = $"TestData_{TestContext.TestName}.db";
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        _repository = new Repository($"Data Source={_testDbPath};");
    }

    [TestCleanup]
    public void Cleanup() => _repository?.Dispose();

    [TestMethod]
    public void FullWorkflow_InsertAndQueryAllTelemetryTypes_ShouldWorkCorrectly()
    {
        // Arrange - Create a complete telemetry scenario
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
        var rootSpanId = ByteString.CopyFrom([1, 1, 1, 1, 1, 1, 1, 1]);
        var childSpanId = ByteString.CopyFrom([2, 2, 2, 2, 2, 2, 2, 2]);

        var time = DateTimeOffset.UtcNow.AddMinutes(-15);

        // Create traces with parent-child relationship
        var traces = CreateIntegratedTraces(traceId, rootSpanId, childSpanId, time);

        // Create logs correlated to the traces
        var logs = CreateIntegratedLogs(traceId, rootSpanId, childSpanId, time);

        // Create metrics from the same service and timeframe
        var metrics = CreateIntegratedMetrics(time);

        // Act - Insert all telemetry data
        _repository.InsertTraces(traces);
        _repository.InsertLogs(logs);
        _repository.InsertMetrics(metrics);

        // Assert - Verify data integrity and correlations

        // 1. Verify traces are inserted with proper hierarchy
        var allTraces = _repository.QuerySpans(new Query());
        Assert.HasCount(2, allTraces);

        var rootSpan = allTraces.FirstOrDefault(t => t.ParentSpanId == ByteString.Empty);
        var childSpan = allTraces.FirstOrDefault(t => t.ParentSpanId != ByteString.Empty);

        Assert.IsNotNull(rootSpan);
        Assert.IsNotNull(childSpan);
        Assert.AreEqual("api-request", rootSpan.Name);
        Assert.AreEqual("database-query", childSpan.Name);

        // 2. Verify logs are correlated to spans
        var logCount = _repository.GetLogCountForSpan(rootSpan.SpanId);
        Assert.AreEqual(2, logCount); // Should have 2 logs for root span

        var childLogCount = _repository.GetLogCountForSpan(childSpan.SpanId);
        Assert.AreEqual(1, childLogCount); // Should have 1 log for child span

        // 3. Verify logs can be queried independently
        var allLogs = _repository.QueryLogs(new Query());
        Assert.HasCount(3, allLogs);

        var errorLogs = _repository.QueryLogs(new Query { MinSeverity = SeverityNumber.Error });
        Assert.HasCount(1, errorLogs);

        // 4. Verify metrics correlation with spans
        var correlatedMetrics = _repository.QueryMetrics(new Query { StartTime = time, EndTime = time.AddSeconds(1) });
        Assert.IsNotEmpty(correlatedMetrics);
        Assert.IsTrue(correlatedMetrics.All(m => m.ServiceName == "integration-test-service"));

        // 5. Verify complex queries work
        var serviceQuery = new Query { ServiceName = "integration-test-service" };
        var serviceTraces = _repository.QuerySpans(serviceQuery);
        var serviceLogs = _repository.QueryLogs(serviceQuery);
        var serviceMetrics = _repository.QueryMetrics(serviceQuery);

        Assert.HasCount(2, serviceTraces);
        Assert.HasCount(3, serviceLogs);
        Assert.IsNotEmpty(serviceMetrics);

        // 6. Verify time-based queries
        var timeQuery = new Query
        {
            StartTime = time.AddSeconds(-5),
            EndTime = time.AddSeconds(5)
        };

        var timeFilteredTraces = _repository.QuerySpans(timeQuery);
        var timeFilteredLogs = _repository.QueryLogs(timeQuery);
        var timeFilteredMetrics = _repository.QueryMetrics(timeQuery);

        Assert.HasCount(2, timeFilteredTraces);
        Assert.HasCount(3, timeFilteredLogs);
        Assert.IsNotEmpty(timeFilteredMetrics);
    }

    [TestMethod]
    public void MultiServiceScenario_ShouldIsolateDataCorrectly()
    {
        // Arrange - Create data for multiple services
        var service1Traces = CreateServiceTraces("service-1", "v1.0.0");
        var service2Traces = CreateServiceTraces("service-2", "v2.0.0");

        var service1Logs = CreateServiceLogs("service-1", "v1.0.0");
        var service2Logs = CreateServiceLogs("service-2", "v2.0.0");

        _repository.InsertTraces(service1Traces);
        _repository.InsertTraces(service2Traces);
        _repository.InsertLogs(service1Logs);
        _repository.InsertLogs(service2Logs);

        // Act
        var service1Query = new Query { ServiceName = "service-1" };
        var service2Query = new Query { ServiceName = "service-2" };

        var service1TracesResult = _repository.QuerySpans(service1Query);
        var service2TracesResult = _repository.QuerySpans(service2Query);

        var service1LogsResult = _repository.QueryLogs(service1Query);
        var service2LogsResult = _repository.QueryLogs(service2Query);

        // Assert - Verify data isolation
        Assert.HasCount(1, service1TracesResult);
        Assert.HasCount(1, service2TracesResult);
        Assert.HasCount(1, service1LogsResult);
        Assert.HasCount(1, service2LogsResult);

        Assert.IsTrue(service1TracesResult.All(t => t.ServiceName == "service-1"));
        Assert.IsTrue(service2TracesResult.All(t => t.ServiceName == "service-2"));
    }

    private static IEnumerable<ResourceSpans> CreateIntegratedTraces(ByteString traceId, ByteString rootSpanId, ByteString childSpanId, DateTimeOffset time)
    {
        var resource = CreateTestResource("integration-test-service", "1.0.0");
        var scopeSpans = new ScopeSpans
        {
            Scope = new InstrumentationScope { Name = "integration-test-scope" }
        };

        // Root span
        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = rootSpanId,
            Name = "api-request",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = time.ToUnixTimeNanoseconds(),
            EndTimeUnixNano = time.AddSeconds(1).ToUnixTimeNanoseconds()
        });

        // Child span
        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = childSpanId,
            ParentSpanId = rootSpanId,
            Name = "database-query",
            Kind = Span.Types.SpanKind.Client,
            StartTimeUnixNano = time.AddSeconds(0.2).ToUnixTimeNanoseconds(),
            EndTimeUnixNano = time.AddSeconds(0.8).ToUnixTimeNanoseconds()
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return [resourceSpans];
    }

    private static IEnumerable<ResourceLogs> CreateIntegratedLogs(ByteString traceId, ByteString rootSpanId, ByteString childSpanId, DateTimeOffset time)
    {
        var resource = CreateTestResource("integration-test-service", "1.0.0");
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = "integration-test-scope" }
        };

        // Log for root span - start
        scopeLogs.LogRecords.Add(new LogRecord
        {
            TimeUnixNano = time.ToUnixTimeNanoseconds(),
            TraceId = traceId,
            SpanId = rootSpanId,
            SeverityNumber = SeverityNumber.Info,
            SeverityText = "INFO",
            Body = new AnyValue { StringValue = "API request started" }
        });

        // Log for child span
        scopeLogs.LogRecords.Add(new LogRecord
        {
            TimeUnixNano = time.AddSeconds(0.5).ToUnixTimeNanoseconds(),
            TraceId = traceId,
            SpanId = childSpanId,
            SeverityNumber = SeverityNumber.Debug,
            SeverityText = "DEBUG",
            Body = new AnyValue { StringValue = "Executing database query" }
        });

        // Log for root span - error
        scopeLogs.LogRecords.Add(new LogRecord
        {
            TimeUnixNano = time.AddSeconds(1).ToUnixTimeNanoseconds(),
            TraceId = traceId,
            SpanId = rootSpanId,
            SeverityNumber = SeverityNumber.Error,
            SeverityText = "ERROR",
            Body = new AnyValue { StringValue = "Request processing encountered an error" }
        });

        var resourceLogs = new ResourceLogs { Resource = resource };
        resourceLogs.ScopeLogs.Add(scopeLogs);
        return [resourceLogs];
    }

    private static IEnumerable<ResourceMetrics> CreateIntegratedMetrics(DateTimeOffset time)
    {
        var resource = CreateTestResource("integration-test-service", "1.0.0");
        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "integration-test-scope" }
        };

        // Request counter
        var requestCounter = new Metric
        {
            Name = "http.requests.total",
            Description = "Total HTTP requests",
            Unit = "requests",
            Sum = new Sum { AggregationTemporality = AggregationTemporality.Cumulative }
        };

        requestCounter.Sum.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = time.ToUnixTimeNanoseconds(),
            AsInt = 1
        });

        // Response time gauge
        var responseTimeGauge = new Metric
        {
            Name = "http.request.duration",
            Description = "HTTP request duration",
            Unit = "ms",
            Gauge = new Gauge()
        };

        responseTimeGauge.Gauge.DataPoints.Add(new NumberDataPoint
        {
            TimeUnixNano = time.AddSeconds(1).ToUnixTimeNanoseconds(),
            AsDouble = 1000.0 // 1 second in milliseconds
        });

        scopeMetrics.Metrics.Add(requestCounter);
        scopeMetrics.Metrics.Add(responseTimeGauge);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return [resourceMetrics];
    }

    private static IEnumerable<ResourceSpans> CreateServiceTraces(string serviceName, string version)
    {
        var resource = CreateTestResource(serviceName, version);
        var scopeSpans = new ScopeSpans
        {
            Scope = new InstrumentationScope { Name = $"{serviceName}-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
        var spanId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray().Take(8).ToArray());

        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = spanId,
            Name = $"{serviceName}-operation",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = baseTime,
            EndTimeUnixNano = baseTime + 500_000_000
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return [resourceSpans];
    }

    private static IEnumerable<ResourceLogs> CreateServiceLogs(string serviceName, string version)
    {
        var resource = CreateTestResource(serviceName, version);
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = $"{serviceName}-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        scopeLogs.LogRecords.Add(new LogRecord
        {
            TimeUnixNano = baseTime,
            SeverityNumber = SeverityNumber.Info,
            SeverityText = "INFO",
            Body = new AnyValue { StringValue = $"Log from {serviceName}" }
        });

        var resourceLogs = new ResourceLogs { Resource = resource };
        resourceLogs.ScopeLogs.Add(scopeLogs);
        return [resourceLogs];
    }

    private static Resource CreateTestResource(string serviceName, string version)
    {
        var resource = new Resource();

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.name",
            Value = new AnyValue { StringValue = serviceName }
        });

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.version",
            Value = new AnyValue { StringValue = version }
        });

        resource.Attributes.Add(new KeyValue
        {
            Key = "service.instance.id",
            Value = new AnyValue { StringValue = $"{serviceName}-instance-1" }
        });

        return resource;
    }
}