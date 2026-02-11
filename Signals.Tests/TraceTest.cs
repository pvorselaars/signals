using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Signals.Common.Utilities;
using Signals.Telemetry;
using static Signals.Telemetry.Repository;

namespace Tests;

[TestClass]
public class TraceTests
{
    private Repository _repository = null!;

    [TestInitialize]
    public void Setup() => _repository = new Repository($"Data Source=:memory:");

    [TestCleanup]
    public void Cleanup() => _repository?.Dispose();

    [TestMethod]
    public void InsertTraces_ShouldStoreTracesCorrectly()
    {
        // Arrange
        _repository.InsertTraces(CreateTestResourceTraces());

        // Act
        var traces = _repository.QuerySpans();

        // Assert
        Assert.HasCount(4, traces);
        Assert.AreEqual("test-service", traces[0].ServiceName);
        Assert.AreEqual("test-scope", traces[0].Scope.Name);
        Assert.AreEqual("Child 2", traces[0].Name); // Spans should be ordered by time descending
        Assert.AreEqual("test-service", traces[1].ServiceName);
        Assert.AreEqual("test-scope", traces[1].Scope.Name);
        Assert.AreEqual("Child 1", traces[1].Name);
        Assert.AreEqual("test-service", traces[2].ServiceName);
        Assert.AreEqual("test-scope", traces[2].Scope.Name);
        Assert.AreEqual("Root", traces[2].Name);
        Assert.AreEqual("other-service", traces[3].ServiceName);
        Assert.AreEqual("other-scope", traces[3].Scope.Name);
        Assert.AreEqual("Other Root", traces[3].Name);
    }

    [TestMethod]
    public void QueryTraces_WithSpanNameFilter_ShouldReturnMatchingTraces()
    {
        // Arrange
        _repository.InsertTraces(CreateTestResourceTraces());

        // Act
        var query = new Query { SpanName = "Child 1" };
        var traces = _repository.QuerySpans(query);

        // Assert
        Assert.HasCount(1, traces);
        Assert.AreEqual("Child 1", traces[0].Name);
    }

    [TestMethod]
    public void QueryTraces_WithServiceFilter_ShouldReturnMatchingTraces()
    {
        // Arrange
        _repository.InsertTraces(CreateTestResourceTraces());

        var query = new Query { ServiceName = "test-service" };

        // Act
        var traces = _repository.QuerySpans(query);

        // Assert
        Assert.HasCount(3, traces);
        Assert.IsTrue(traces.All(t => t.ServiceName == "test-service"));
    }

    private static IEnumerable<ResourceSpans> CreateTestResourceTraces()
    {
        var time = DateTimeOffset.UtcNow.AddMinutes(-30);
        return [new ResourceSpans
        {
            Resource = new Resource { 
                Attributes = { 
                    new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "test-service" } },
                }
            },
            ScopeSpans = { 
                new ScopeSpans { 
                    Scope = new InstrumentationScope { Name = "test-scope" }, 
                    Spans =
                    {
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Root",
                            StartTimeUnixNano = time.ToUnixTimeNanoseconds(),
                            EndTimeUnixNano = time.AddSeconds(5).ToUnixTimeNanoseconds()
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([8, 7, 6, 5, 4, 3, 2, 1]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 1",
                            StartTimeUnixNano = time.AddSeconds(1).ToUnixTimeNanoseconds(),
                            EndTimeUnixNano = time.AddSeconds(2).ToUnixTimeNanoseconds()
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([2, 3, 4, 5, 6, 7, 8, 9]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 2",
                            StartTimeUnixNano = time.AddSeconds(4).ToUnixTimeNanoseconds(),
                            EndTimeUnixNano = time.AddSeconds(5).ToUnixTimeNanoseconds()
                        }
                    }
                }
            }
        },
        new ResourceSpans
        {
            Resource = new Resource { 
                Attributes = { 
                    new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "other-service" } },
                }
            },
            ScopeSpans = { 
                new ScopeSpans { 
                    Scope = new InstrumentationScope { Name = "other-scope" }, 
                    Spans =
                    {
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0]),
                            SpanId = ByteString.CopyFrom([9, 8, 7, 6, 5, 4, 3, 2]),
                            Name = "Other Root",
                            StartTimeUnixNano = time.AddSeconds(-6).ToUnixTimeNanoseconds(),
                            EndTimeUnixNano = time.AddSeconds(-5).ToUnixTimeNanoseconds()
                        }
                    }
                }
            }
        }];
    }

}
