using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Signals.Telemetry;
using static Signals.Telemetry.Repository;

namespace Tests;

[TestClass]
public class TraceTests(TestContext context)
{
    private Repository _repository = null!;

    public TestContext TestContext { get; set; } = context;

    [TestInitialize]
    public void Setup() => _repository = new Repository(":memory:");

    [TestCleanup]
    public void Cleanup() => _repository.Dispose();

    [TestMethod]
    public void InsertTraces_ShouldStoreTracesCorrectly()
    {
        // Arrange
        _repository.InsertTraces(CreateTestResourceTraces());

        // Act
        var query = new Query()
        {
            ParentSpanId = ByteString.Empty // Only query root spans
        };
        var traces = _repository.QuerySpans(query);

        // Assert
        Assert.HasCount(1, traces);
        Assert.AreEqual("test-service", traces[0].Attributes.FirstOrDefault(attr => attr.Key == "service.name")?.Value.StringValue);
        Assert.AreEqual("test-scope", traces[0].Attributes.FirstOrDefault(attr => attr.Key == "scope.name")?.Value.StringValue);
        Assert.AreEqual("Root", traces[0].Name);
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

    private static IEnumerable<ResourceSpans> CreateTestResourceTraces()
    {
        var time = DateTimeOffset.UtcNow.AddMinutes(-15).ToUnixTimeSeconds();
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
                            StartTimeUnixNano = (ulong)time * 1_000_000_000L,
                            EndTimeUnixNano = (ulong) (time + 5 * 1_000_000_000L) // 5 seconds later
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([8, 7, 6, 5, 4, 3, 2, 1]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 1",
                            StartTimeUnixNano = (ulong)(time + 2) * 1_000_000_000,
                            EndTimeUnixNano = (ulong)(time + 3) * 1_000_000_000
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([2, 3, 4, 5, 6, 7, 8, 9]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 2",
                            StartTimeUnixNano = (ulong)(time + 4) * 1_000_000_000,
                            EndTimeUnixNano = (ulong)(time + 5) * 1_000_000_000
                        }
                    }
                }
            }
        }];
    }

}
