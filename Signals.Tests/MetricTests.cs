
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using Signals.Common.Utilities;
using Signals.Telemetry;
using static Signals.Telemetry.Repository;

namespace Tests;

[TestClass]
public class MetricTests
{
    private Repository _repository = null!;

    [TestInitialize]
    public void Setup() => _repository = new Repository($"Data Source=:memory:");

    [TestCleanup]
    public void Cleanup() => _repository?.Dispose();

    [TestMethod]
    public void InsertMetrics_ShouldStoreMetricsCorrectly()
    {
        // Arrange
        _repository.InsertMetrics(CreateTestResourceMetrics());

        // Act
        var metrics = _repository.QueryMetrics();

        // Assert
        Assert.HasCount(2, metrics);
        Assert.AreEqual("test-service", metrics[0].ServiceName);
        Assert.AreEqual("test-scope", metrics[0].Scope.Name);
        Assert.AreEqual("test.gauge", metrics[0].Name);
        Assert.AreEqual("Test gauge metric", metrics[0].Description);
        Assert.AreEqual(Metric.DataOneofCase.Gauge, metrics[0].DataCase);
        Assert.AreEqual("test-service", metrics[1].ServiceName);
        Assert.AreEqual("test-scope", metrics[1].Scope.Name);
        Assert.AreEqual("test.counter", metrics[1].Name);
        Assert.AreEqual("Test counter metric", metrics[1].Description);
        Assert.AreEqual(Metric.DataOneofCase.Sum, metrics[1].DataCase);
        Assert.AreEqual(1, metrics[0].Samples);
        Assert.AreEqual(1, metrics[1].Samples);
    }

    [TestMethod]
    public void QueryMetrics_WithNameFilter_ShouldReturnMatchingMetrics()
    {
        // Arrange
        _repository.InsertMetrics(CreateTestResourceMetrics());

        var query = new Query { MetricName = "test.gauge" };

        // Act
        var metrics = _repository.QueryMetrics(query);

        // Assert
        Assert.HasCount(1, metrics);
        Assert.AreEqual("test.gauge", metrics[0].Name);
    }
    
    [TestMethod]
    public void BatchInsertDataPoints_ShouldInsertAllPointsEfficiently()
    {
        // Arrange
        var resourceMetrics = CreateTestResourceMetricsWithManyDataPoints();

        // Act
        _repository.InsertMetrics(resourceMetrics);

        // Assert
        var query = new Query();
        var metrics = _repository.QueryMetrics(query);

        Assert.HasCount(1, metrics);
        Assert.AreEqual(10, metrics[0].Samples);
    }


    [TestMethod]
    public void QueryMetrics_WithTimeRange_ShouldReturnMatchingMetrics()
    {
        // Arrange
        _repository.InsertMetrics(CreateTestResourceMetrics());

        var query = new Query
        {
            StartTime = DateTime.UtcNow.AddMinutes(-30),
            EndTime = DateTime.UtcNow.AddMinutes(30)
        };

        // Act
        var metrics = _repository.QueryMetrics(query);

        // Assert
        Assert.IsNotEmpty(metrics);
    }
    private static IEnumerable<ResourceMetrics> CreateTestResourceMetrics()
    {
        var resource = new Resource
        {
            Attributes = {
                new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "test-service" } }
            }
        };
        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var time = DateTimeOffset.UtcNow.AddMinutes(-5);

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
            TimeUnixNano = time.AddSeconds(-1).ToUnixTimeNanoseconds(),
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
            TimeUnixNano = time.AddSeconds(-3).ToUnixTimeNanoseconds(),
            AsDouble = 85.5
        });

        scopeMetrics.Metrics.Add(counterMetric);
        scopeMetrics.Metrics.Add(gaugeMetric);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return [resourceMetrics];
    }

    private static IEnumerable<ResourceMetrics> CreateTestResourceMetricsWithManyDataPoints()
    {
        var resource = new Resource
        {
            Attributes = {
                new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "test-service" } }
            }
        };

        var scopeMetrics = new ScopeMetrics
        {
            Scope = new InstrumentationScope { Name = "test-scope" }
        };

        var time = DateTimeOffset.UtcNow.AddMinutes(-30);


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

        for (int i = 0; i < 10; i++)
        {
            counterMetric.Sum.DataPoints.Add(new NumberDataPoint
            {
                TimeUnixNano = time.AddSeconds(i).ToUnixTimeNanoseconds(),
                AsInt = i * 10
            });
        }

        scopeMetrics.Metrics.Add(counterMetric);

        var resourceMetrics = new ResourceMetrics { Resource = resource };
        resourceMetrics.ScopeMetrics.Add(scopeMetrics);
        return [resourceMetrics];
    }

}