using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Bench;

// SqlBulkCopy never tells the server how many rows are coming, so the grant is a blind guess.
// The one hint it does emit is ORDER(...), which lets the server skip the sort for the clustered
// index when the rows really do arrive in that order. Does that shrink the grant?
internal static class OrderHintProbe
{
    public static async Task RunAsync(string connectionString, int rows, int columns)
    {
        await using var con = new SqlConnection(connectionString);
        await con.OpenAsync();

        foreach (var (label, orderHint, secondaryIndexes) in new[]
        {
            ("clustered PK + 2 nonclustered, no hint ", false, 2),
            ("clustered PK + 2 nonclustered, ORDER   ", true, 2),
            ("clustered PK only, no hint             ", false, 0),
            ("clustered PK only, ORDER               ", true, 0),
            ("heap                                   ", false, -1),
        })
        {
            await Exec(con, "IF OBJECT_ID('OrderProbe') IS NOT NULL DROP TABLE OrderProbe;");
            var cols = string.Join(", ", Enumerable.Range(0, columns).Select(i => $"[C{i}] {Program.ColumnType(i)} NULL"));
            await Exec(con, $"CREATE TABLE OrderProbe (Id int NOT NULL, {cols});");
            if (secondaryIndexes >= 0)
            {
                await Exec(con, "ALTER TABLE OrderProbe ADD CONSTRAINT PK_OrderProbe PRIMARY KEY CLUSTERED (Id);");
                for (int x = 0; x < secondaryIndexes; x++)
                {
                    await Exec(con, $"CREATE NONCLUSTERED INDEX IX_OrderProbe_{x} ON OrderProbe ([C{x * 2}]);");
                }
            }

            var monitor = await GrantMonitor.StartAsync(connectionString);
            var sw = Stopwatch.StartNew();

            using (var bcp = new SqlBulkCopy(con) { DestinationTableName = "OrderProbe", EnableStreaming = true, BatchSize = 0, BulkCopyTimeout = 300 })
            {
                bcp.ColumnMappings.Add(0, "Id");
                for (int i = 0; i < columns; i++)
                {
                    bcp.ColumnMappings.Add(i + 1, $"C{i}");
                }
                if (orderHint)
                {
                    bcp.ColumnOrderHints.Add("Id", SortOrder.Ascending);
                }
                await bcp.WriteToServerAsync(new AscendingReader(rows, columns));
            }

            sw.Stop();
            await monitor.DisposeAsync();
            Console.WriteLine($"{label}: {sw.ElapsedMilliseconds,5} ms, requested {monitor.MaxRequestedKb / 1024.0,6:F1} MB, required {monitor.MaxRequiredKb / 1024.0:F2} MB");
        }

        await Exec(con, "IF OBJECT_ID('OrderProbe') IS NOT NULL DROP TABLE OrderProbe;");
    }

    static async Task Exec(SqlConnection con, string sql)
    {
        await using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300;
        await cmd.ExecuteNonQueryAsync();
    }

    // rows arrive in ascending Id order, which is exactly what synthesized identity values do
    sealed class AscendingReader : IDataReader
    {
        private readonly int _rows;
        private readonly int _columns;
        private int _index = -1;
        private readonly DateTime _epoch = new(2020, 1, 1);

        public AscendingReader(int rows, int columns) { _rows = rows; _columns = columns; }

        public bool Read() => ++_index < _rows;

        public object GetValue(int i) => i == 0 ? _index + 1 : ((i - 1) % 5) switch
        {
            0 => _index * 31 + i,
            1 => $"value-{_index}-{i}",
            2 => (_index + i) % 2 == 0,
            3 => _epoch.AddMinutes(_index % 100000),
            _ => decimal.Divide((_index % 1000) * 100 + i, 7),
        };

        public int FieldCount => _columns + 1;
        public string GetName(int i) => i == 0 ? "Id" : $"C{i - 1}";
        public int GetOrdinal(string name) => name == "Id" ? 0 : int.Parse(name.Substring(1)) + 1;
        public Type GetFieldType(int i) => GetValue(i).GetType();
        public bool IsDBNull(int i) => false;
        public int GetValues(object[] values)
        {
            for (int i = 0; i < FieldCount; i++) { values[i] = GetValue(i); }
            return FieldCount;
        }
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => 0;
        public void Close() { }
        public void Dispose() { }
        public bool NextResult() => false;
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long o, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long o, char[]? b, int bo, int l) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public double GetDouble(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => throw new NotSupportedException();
        public string GetString(int i) => (string)GetValue(i);
        public DataTable? GetSchemaTable() => null;
    }
}
