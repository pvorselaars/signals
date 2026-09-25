using Microsoft.Data.Sqlite;

namespace Signals.Telemetry;

// Collects "column = @param"-style conditions for a dynamic SQL WHERE clause
// and adds the matching parameter to the command in one step, replacing the
// hand-duplicated "build a list of conditions, join with AND" pattern that
// previously appeared in every query method.
internal sealed class SqlConditions(SqliteCommand command)
{
    private readonly List<string> _conditions = [];

    public void Add(string condition, string paramName, object value)
    {
        _conditions.Add(condition);
        command.Parameters.AddWithValue(paramName, value);
    }

    public void AddRaw(string condition) => _conditions.Add(condition);

    public string WhereClause => _conditions.Count != 0 ? "WHERE " + string.Join(" AND ", _conditions) : "";
}
