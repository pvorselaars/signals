using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Trace.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Common.V1;
using Google.Protobuf;
using Signals.Repository;
using static Signals.Repository.Database;

namespace Signals.Tests;

[TestClass]
public class IntegrationTests
{
    private Database _database = null!;
    private string _testDbPath = null!;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"integration_test_{Guid.NewGuid()}.db");
        _database = new Database(_testDbPath);
    }

    [TestCleanup]
    public void Cleanup()
    {
        _database?.Dispose();
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
    }

    [TestMethod]
    public void FullWorkflow_InsertAndQueryAllTelemetryTypes_ShouldWorkCorrectly()
    {
        // Arrange - Create a complete telemetry scenario
        var traceId = ByteString.CopyFrom(Guid.NewGuid().ToByteArray());
        var rootSpanId = ByteString.CopyFrom(new byte[8] { 1, 1, 1, 1, 1, 1, 1, 1 });
        var childSpanId = ByteString.CopyFrom(new byte[8] { 2, 2, 2, 2, 2, 2, 2, 2 });

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        // Create traces with parent-child relationship
        var traces = CreateIntegratedTraces(traceId, rootSpanId, childSpanId, baseTime);

        // Create logs correlated to the traces
        var logs = CreateIntegratedLogs(traceId, rootSpanId, childSpanId, baseTime);

        // Create metrics from the same service and timeframe
        var metrics = CreateIntegratedMetrics(baseTime);

        // Act - Insert all telemetry data
        _database.InsertTraces(traces);
        _database.InsertLogs(logs);
        _database.InsertMetrics(metrics);

        // Assert - Verify data integrity and correlations

        // 1. Verify traces are inserted with proper hierarchy
        var allTraces = _database.QueryTraces(new Query());
        Assert.AreEqual(2, allTraces.Count);

        var rootSpan = allTraces.FirstOrDefault(t => t.ParentSpanId == null);
        var childSpan = allTraces.FirstOrDefault(t => t.ParentSpanId != null);

        Assert.IsNotNull(rootSpan);
        Assert.IsNotNull(childSpan);
        Assert.AreEqual("api-request", rootSpan.SpanName);
        Assert.AreEqual("database-query", childSpan.SpanName);

        // 2. Verify logs are correlated to spans
        var logCount = _database.GetLogCountForSpan(rootSpan.TraceId, rootSpan.SpanId);
        Assert.AreEqual(2, logCount); // Should have 2 logs for root span

        var childLogCount = _database.GetLogCountForSpan(childSpan.TraceId, childSpan.SpanId);
        Assert.AreEqual(1, childLogCount); // Should have 1 log for child span

        // 3. Verify logs can be queried independently
        var allLogs = _database.QueryLogs(new Query());
        Assert.AreEqual(3, allLogs.Count);

        var errorLogs = _database.QueryLogs(new Query { MinSeverity = 17 }); // ERROR level
        Assert.AreEqual(1, errorLogs.Count);

        // 4. Verify metrics correlation with spans
        var correlatedMetrics = _database.GetMetricsForSpan(rootSpan);
        Assert.IsTrue(correlatedMetrics.Count > 0);
        Assert.IsTrue(correlatedMetrics.All(m => m.ServiceName == "integration-test-service"));

        // 5. Verify complex queries work
        var serviceQuery = new Query { ServiceName = "integration-test-service" };
        var serviceTraces = _database.QueryTraces(serviceQuery);
        var serviceLogs = _database.QueryLogs(serviceQuery);
        var serviceMetrics = _database.QueryMetrics(serviceQuery);

        Assert.AreEqual(2, serviceTraces.Count);
        Assert.AreEqual(3, serviceLogs.Count);
        Assert.IsTrue(serviceMetrics.Count > 0);

        // 6. Verify time-based queries
        var timeQuery = new Query
        {
            StartTime = DateTimeOffset.FromUnixTimeMilliseconds((long)baseTime / 1_000_000).AddSeconds(-10),
            EndTime = DateTimeOffset.FromUnixTimeMilliseconds((long)baseTime / 1_000_000).AddSeconds(10)
        };

        var timeFilteredTraces = _database.QueryTraces(timeQuery);
        var timeFilteredLogs = _database.QueryLogs(timeQuery);
        var timeFilteredMetrics = _database.QueryMetrics(timeQuery);

        Assert.AreEqual(2, timeFilteredTraces.Count);
        Assert.AreEqual(3, timeFilteredLogs.Count);
        Assert.IsTrue(timeFilteredMetrics.Count > 0);
    }

    [TestMethod]
    public void MultiServiceScenario_ShouldIsolateDataCorrectly()
    {
        // Arrange - Create data for multiple services
        var service1Traces = CreateServiceTraces("service-1", "v1.0.0");
        var service2Traces = CreateServiceTraces("service-2", "v2.0.0");

        var service1Logs = CreateServiceLogs("service-1", "v1.0.0");
        var service2Logs = CreateServiceLogs("service-2", "v2.0.0");

        // Act
        _database.InsertTraces(service1Traces);
        _database.InsertTraces(service2Traces);
        _database.InsertLogs(service1Logs);
        _database.InsertLogs(service2Logs);

        // Assert - Verify data isolation
        var service1Query = new Query { ServiceName = "service-1" };
        var service2Query = new Query { ServiceName = "service-2" };

        var service1TracesResult = _database.QueryTraces(service1Query);
        var service2TracesResult = _database.QueryTraces(service2Query);

        var service1LogsResult = _database.QueryLogs(service1Query);
        var service2LogsResult = _database.QueryLogs(service2Query);

        Assert.AreEqual(1, service1TracesResult.Count);
        Assert.AreEqual(1, service2TracesResult.Count);
        Assert.AreEqual(1, service1LogsResult.Count);
        Assert.AreEqual(1, service2LogsResult.Count);

        Assert.IsTrue(service1TracesResult.All(t => t.ServiceName == "service-1"));
        Assert.IsTrue(service2TracesResult.All(t => t.ServiceName == "service-2"));
    }

    // Helper methods for creating integrated test data

    private ResourceSpans CreateIntegratedTraces(ByteString traceId, ByteString rootSpanId, ByteString childSpanId, ulong baseTime)
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
            StartTimeUnixNano = baseTime,
            EndTimeUnixNano = baseTime + 1_000_000_000 // 1 second
        });

        // Child span
        scopeSpans.Spans.Add(new Span
        {
            TraceId = traceId,
            SpanId = childSpanId,
            ParentSpanId = rootSpanId,
            Name = "database-query",
            Kind = Span.Types.SpanKind.Client,
            StartTimeUnixNano = baseTime + 100_000_000, // 100ms after root
            EndTimeUnixNano = baseTime + 300_000_000     // 300ms total
        });

        var resourceSpans = new ResourceSpans { Resource = resource };
        resourceSpans.ScopeSpans.Add(scopeSpans);
        return resourceSpans;
    }

    private ResourceLogs CreateIntegratedLogs(ByteString traceId, ByteString rootSpanId, ByteString childSpanId, ulong baseTime)
    {
        var resource = CreateTestResource("integration-test-service", "1.0.0");
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = "integration-test-scope" }
        };

        // Log for root span - start
        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime + 10_000_000, // 10ms after span start
            TraceId = traceId,
            SpanId = rootSpanId,
            SeverityNumber = SeverityNumber.Info,
            SeverityText = "INFO",
            Body = new AnyValue { StringValue = "API request started" }
        });

        // Log for child span
        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime + 150_000_000, // During child span
            TraceId = traceId,
            SpanId = childSpanId,
            SeverityNumber = SeverityNumber.Debug,
            SeverityText = "DEBUG",
            Body = new AnyValue { StringValue = "Executing database query" }
        });

        // Log for root span - error
        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime + 800_000_000, // Near end of root span
            TraceId = traceId,
            SpanId = rootSpanId,
            SeverityNumber = SeverityNumber.Error,
            SeverityText = "ERROR",
            Body = new AnyValue { StringValue = "Request processing encountered an error" }
        });

        var resourceLogs = new ResourceLogs { Resource = resource };
        resourceLogs.ScopeLogs.Add(scopeLogs);
        return resourceLogs;
    }

    private ResourceMetrics CreateIntegratedMetrics(ulong baseTime)
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
            TimeUnixNano = baseTime + 50_000_000, // During request processing
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
            TimeUnixNano = baseTime + 1_000_000_000, // At request completion
            AsDouble = 1000.0 // 1 second in milliseconds
        });

        scopeMetrics.Metrics.Add(requestCounter);
        scopeMetrics.Metrics.Add(responseTimeGauge);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return resourceMetrics;
    }

    private ResourceSpans CreateServiceTraces(string serviceName, string version)
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
        return resourceSpans;
    }

    private ResourceLogs CreateServiceLogs(string serviceName, string version)
    {
        var resource = CreateTestResource(serviceName, version);
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = $"{serviceName}-scope" }
        };

        var baseTime = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000);

        scopeLogs.LogRecords.Add(new OpenTelemetry.Proto.Logs.V1.LogRecord
        {
            TimeUnixNano = baseTime,
            SeverityNumber = SeverityNumber.Info,
            SeverityText = "INFO",
            Body = new AnyValue { StringValue = $"Log from {serviceName}" }
        });

        var resourceLogs = new ResourceLogs { Resource = resource };
        resourceLogs.ScopeLogs.Add(scopeLogs);
        return resourceLogs;
    }

    private Resource CreateTestResource(string serviceName, string version)
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