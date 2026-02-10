using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Resource.V1;
using Signals.Telemetry;
using static Signals.Telemetry.Repository;

namespace Tests;

[TestClass]
public class LogTests(TestContext context)
{
    private Repository _repository = null!;

    public TestContext TestContext { get; set; } = context;

    [TestInitialize]
    public void Setup()
    {
        _repository = new Repository(":memory:");
    }

    [TestCleanup]
    public void Cleanup()
    {
        _repository?.Dispose();
    }

    [TestMethod]
    public void InsertLogs_ShouldStoreLogsCorrectly()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        // Act
        var query = new Query();
        var logs = _repository.QueryLogs(query);

        // Assert
        Assert.HasCount(2, logs);
    }

    [TestMethod]
    public void QueryLogs_WithTextFilter_ShouldReturnMatchingLogs()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        var query = new Query { Text = "message 1" };

        // Act
        var logs = _repository.QueryLogs(query);

        // Assert
        Assert.HasCount(1, logs);
        Assert.AreEqual("Test log message 1", logs[0].Body.StringValue);
    }

    [TestMethod]
    public void GetLogCountForTrace_ShouldReturnCorrectCount()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        // Act
        var logCount = _repository.GetLogCountForTrace(ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]));

        // Assert
        Assert.AreEqual(2, logCount);
    }

    [TestMethod]
    public void GetUniqueServices_ShouldReturnDistinctServiceNames()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        // Act
        var services = _repository.GetUniqueServices();

        // Assert
        Assert.HasCount(1, services);
        Assert.AreEqual("test-service", services.First());
    }

    [TestMethod]
    public void GetUniqueScopes_ShouldReturnDistinctScopeNames()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        // Act
        var scopes = _repository.GetUniqueLogScopes();

        // Assert
        Assert.HasCount(1, scopes);
        Assert.AreEqual("test-scope", scopes.First());
    }

    [TestMethod]
    public void GetLogCountByService_ShouldReturnCorrectCount()
    {
        // Arrange
        _repository.InsertLogs(CreateTestResourceLogs());

        // Act
        var logCount = _repository.GetLogCountByService(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow);

        // Assert
        Assert.HasCount(1, logCount);
        Assert.AreEqual(2, logCount["test-service"]);
    }


    private static IEnumerable<ResourceLogs> CreateTestResourceLogs()
    {
        return [new ResourceLogs
        {
            SchemaUrl = "http://example.com/schema",
            Resource = new Resource {
                Attributes = { 
                    new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "test-service" } },
                    new KeyValue { Key = "service.instance", Value = new AnyValue { StringValue = "test-instance" } } 
                }
            },
            ScopeLogs = {
                new ScopeLogs
                {
                    SchemaUrl = "http://example.com/schema",
                    Scope = new InstrumentationScope { Name = "test-scope" },
                    LogRecords =
                    {
                        new LogRecord
                        {
                            Body = new AnyValue { StringValue = "Test log message 1" },
                            SeverityNumber = SeverityNumber.Info,
                            TimeUnixNano = (ulong)DateTimeOffset.UtcNow.AddMinutes(-15).ToUnixTimeSeconds() * 1_000_000_000, // Convert seconds to nanoseconds
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8])
                        },
                        new LogRecord
                        {
                            Body = new AnyValue { StringValue = "Test log message 2" },
                            SeverityNumber = SeverityNumber.Error,
                            TimeUnixNano = (ulong)DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeSeconds() * 1_000_000_000, // Convert seconds to nanoseconds
                            TraceId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                            SpanId = ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8])
                        }
                    }
                }
            }
        }];
    }
}