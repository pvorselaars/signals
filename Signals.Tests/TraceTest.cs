using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Signals.Repository;
using static Signals.Repository.Database;

namespace Tests;

[TestClass]
public class TraceTests(TestContext context)
{
    private Database _database = null!;
    private string _testDbPath = null!;

    public TestContext TestContext { get; set; } = context;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = $"TestData_{TestContext.TestName}.db";
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

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
        _database.InsertTraces(CreateTestResourceTraces());

        // Act
        var query = new Query();
        var traces = _database.QuerySpans(query);

        // Assert
        Assert.HasCount(1, traces);
        Assert.AreEqual("test-service", traces[0].ServiceName);
        Assert.AreEqual("test-scope", traces[0].ScopeName);
        Assert.AreEqual("Root", traces[0].Name);
    }

    [TestMethod]
    public void QueryTraces_WithSpanNameFilter_ShouldReturnMatchingTraces()
    {
        // Arrange
        _database.InsertTraces(CreateTestResourceTraces());

        // Act
        var query = new Query { SpanName = "Child 1" };
        var traces = _database.QuerySpans(query);

        // Assert
        Assert.HasCount(1, traces);
        Assert.AreEqual("Child 1", traces[0].Name);
    }

    private static ResourceSpans CreateTestResourceTraces()
    {
        return new ResourceSpans
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
                            StartTimeUnixNano = (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1_000_000_000,
                            EndTimeUnixNano = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 5) * 1_000_000_000
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([8, 7, 6, 5, 4, 3, 2, 1]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 1",
                            StartTimeUnixNano = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 2) * 1_000_000_000,
                            EndTimeUnixNano = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 3) * 1_000_000_000
                        },
                        new Span
                        {
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([2, 3, 4, 5, 6, 7, 8, 9]),
                            ParentSpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8]),
                            Name = "Child 2",
                            StartTimeUnixNano = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 4) * 1_000_000_000,
                            EndTimeUnixNano = (ulong)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 5) * 1_000_000_000
                        }
                    }
                }
            }
        };
    }

}
