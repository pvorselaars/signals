using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Resource.V1;
using Signals.Repository;
using static Signals.Repository.Database;

namespace Tests;

[TestClass]
public class LogTests(TestContext context)
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
    public void InsertLogs_ShouldStoreLogsCorrectly()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        // Act
        var query = new Query();
        var logs = _database.QueryLogs(query);

        // Assert
        Assert.HasCount(1, logs);
        Assert.HasCount(1, logs[0].ScopeLogs);
        Assert.HasCount(2, logs[0].ScopeLogs[0].LogRecords);
        Assert.AreEqual("test-service", logs[0].Resource.Attributes.FirstOrDefault(a => a.Key == "service.name")?.Value.StringValue);
        Assert.AreEqual("test-scope", logs[0].ScopeLogs[0].Scope.Name);
        Assert.AreEqual("Test log message 2", logs[0].ScopeLogs[0].LogRecords[0].Body.StringValue); // Logs should be ordered by time descending
        Assert.AreEqual(SeverityNumber.Error, logs[0].ScopeLogs[0].LogRecords[0].SeverityNumber);
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
        Assert.AreEqual("Test log message 1", logs[0].ScopeLogs[0].LogRecords[0].Body.StringValue);
    }

    [TestMethod]
    public void GetLogCountForTrace_ShouldReturnCorrectCount()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        // Act
        var logCount = _database.GetLogCountForTrace(ByteString.CopyFrom([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]));

        // Assert
        Assert.AreEqual(2, logCount);
    }

    [TestMethod]
    public void GetUniqueServices_ShouldReturnDistinctServiceNames()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        // Act
        var services = _database.GetUniqueServices();

        // Assert
        Assert.HasCount(1, services);
        Assert.AreEqual("test-service", services[0]);
    }

    [TestMethod]
    public void GetUniqueScopes_ShouldReturnDistinctScopeNames()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        // Act
        var scopes = _database.GetUniqueLogScopes();

        // Assert
        Assert.HasCount(1, scopes);
        Assert.AreEqual("test-scope", scopes[0]);
    }

    [TestMethod]
    public void GetLogCountByService_ShouldReturnCorrectCount()
    {
        // Arrange
        _database.InsertLogs(CreateTestResourceLogs());

        // Act
        var logCount = _database.GetLogCountByService(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow);

        // Assert
        Assert.HasCount(1, logCount);
        Assert.AreEqual(2, logCount["test-service"]);
    }


    private static ResourceLogs CreateTestResourceLogs()
    {
        return new ResourceLogs
        {
            Resource = new Resource { Attributes = { 
                new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "test-service" } },
                new KeyValue { Key = "service.instance", Value = new AnyValue { StringValue = "test-instance" } } 
                }
            },
            ScopeLogs = {
                new ScopeLogs
                {
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
        };
    }
}